"""rrq_example.py: A simple example demonstrating the RRQ system."""

import asyncio
import logging
import random
import signal
from contextlib import suppress
from datetime import timedelta

from rrq.client import RRQClient
from rrq.exc import RetryJob
from rrq.registry import JobRegistry
from rrq.settings import RRQSettings
from rrq.store import JobStore
from rrq.worker import RRQWorker

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
# Set RRQ internal loggers to DEBUG for more detail in the example
logging.getLogger("rrq").setLevel(logging.DEBUG)
logger = logging.getLogger("RRQExample")


# --- Example Job Handlers ---
async def successful_task(ctx, message: str):
    delay = random.uniform(0.1, 0.5)
    logger.info(
        f"SUCCESS_TASK (Job {ctx['job_id']}, Try {ctx['job_try']}): Received '{message}'. Sleeping for {delay:.2f}s..."
    )
    await asyncio.sleep(delay)
    logger.info(f"SUCCESS_TASK (Job {ctx['job_id']}): Finished successfully.")
    return {"status": "success", "processed_message": message, "slept_for": delay}


async def failing_task(ctx, data: dict):
    attempt = ctx["job_try"]
    logger.warning(
        f"FAILING_TASK (Job {ctx['job_id']}, Try {attempt}): Received data {data}. Simulating failure..."
    )
    await asyncio.sleep(0.1)
    # Example: Fail permanently after max retries defined in Job/Settings
    raise ValueError(f"Simulated failure on attempt {attempt}")


async def retry_task(ctx, counter_limit: int):
    attempt = ctx["job_try"]
    logger.info(
        f"RETRY_TASK (Job {ctx['job_id']}, Try {attempt}): Received limit {counter_limit}. Will retry if attempt < {counter_limit}."
    )
    await asyncio.sleep(0.2)
    if attempt < counter_limit:
        logger.warning(
            f"RETRY_TASK (Job {ctx['job_id']}, Try {attempt}): Raising RetryJob (defer 1s)."
        )
        raise RetryJob(defer_seconds=1.0)  # Request specific retry delay
    else:
        logger.info(
            f"RETRY_TASK (Job {ctx['job_id']}, Try {attempt}): Reached limit {counter_limit}. Finishing."
        )
        return {"status": "completed_after_retries", "attempts": attempt}


# --- Main Execution ---
async def main():
    logger.info("--- Starting RRQ Example ---")

    # 1. Settings - Use a different Redis DB for the example (e.g., DB 2)
    settings = RRQSettings(
        redis_dsn="redis://localhost:6379/2",
        default_max_retries=3,  # Lower retries for example
        worker_health_check_interval_seconds=5,  # Frequent health check
        worker_shutdown_grace_period_seconds=5,
    )
    logger.info(f"Using Redis DB: {settings.redis_dsn}")

    # Ensure Redis DB is clean before starting (optional, good for examples)
    try:
        temp_store = JobStore(settings=settings)
        await temp_store.redis.flushdb()
        logger.info(f"Flushed Redis DB {settings.redis_dsn.split('/')[-1]}")
        await temp_store.aclose()
    except Exception as e:
        logger.error(f"Could not flush Redis DB: {e}. Please ensure Redis is running.")
        return

    # 2. Registry
    registry = JobRegistry()
    registry.register("handle_success", successful_task)
    registry.register("handle_failure", failing_task)
    registry.register("handle_retry", retry_task)
    logger.info(f"Registered handlers: {registry.get_registered_functions()}")

    # 3. Client
    client = RRQClient(settings=settings)

    # 4. Enqueue Jobs
    logger.info("Enqueueing jobs...")
    job1 = await client.enqueue("handle_success", "Hello World!")
    job2 = await client.enqueue("handle_failure", {"id": 123, "value": "abc"})
    job3 = await client.enqueue(
        "handle_retry", 3
    )  # Expect 3 attempts (1 initial + 2 retries)
    job4 = await client.enqueue(
        "handle_success", "Deferred Message!", _defer_by=timedelta(seconds=5)
    )
    job5 = await client.enqueue(
        "handle_success",
        "Another message",
        _queue_name="high_priority",  # Example custom queue
    )

    if all([job1, job2, job3, job4, job5]):
        logger.info("Jobs enqueued successfully.")
        logger.info(f"  - Job 1 (Success): {job1.id}")
        logger.info(f"  - Job 2 (Failure): {job2.id}")
        logger.info(f"  - Job 3 (Retry): {job3.id}")
        logger.info(f"  - Job 4 (Deferred): {job4.id}")
        logger.info(f"  - Job 5 (CustomQ): {job5.id}")
    else:
        logger.error("Some jobs failed to enqueue.")
        await client.close()
        return

    await client.close()  # Close client connection if no longer needed

    # 5. Worker Setup
    # Run worker polling both default and the custom queue
    worker = RRQWorker(
        settings=settings,
        job_registry=registry,
        queues=[settings.default_queue_name, "high_priority"],
    )

    # 6. Run Worker (with graceful shutdown handling)
    logger.info(f"Starting worker {worker.worker_id}...")
    worker_task = asyncio.create_task(run_worker_async(worker), name="RRQWorkerRunLoop")

    # Keep the main script running until interrupted (Ctrl+C)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def signal_handler():
        logger.info("Shutdown signal received. Setting stop event.")
        if not stop_event.is_set():
            stop_event.set()
        # Allow worker to handle shutdown via its own signal handlers first
        # If worker doesn't shutdown gracefully, worker_task might need cancellation

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            logger.warning(
                f"Signal handler for {sig.name} not supported on this platform."
            )

    logger.info("Example running. Press Ctrl+C to stop.")

    # Wait for stop event or worker task completion (e.g., if it errors out)
    done, pending = await asyncio.wait(
        [worker_task, stop_event.wait()], return_when=asyncio.FIRST_COMPLETED
    )

    logger.info("Stop event triggered or worker task finished.")

    # Initiate worker shutdown if it hasn't stopped itself
    if not worker_task.done():
        logger.info("Requesting worker shutdown...")
        worker._request_shutdown()  # Ask worker to shutdown gracefully
        try:
            await asyncio.wait_for(
                worker_task, timeout=settings.worker_shutdown_grace_period_seconds + 5
            )
            logger.info("Worker task completed after shutdown request.")
        except TimeoutError:
            logger.warning(
                "Worker did not shut down gracefully within extended timeout. Cancelling task."
            )
            worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await worker_task
        except Exception as e:
            logger.error(f"Error waiting for worker shutdown: {e}", exc_info=True)
            worker_task.cancel()  # Ensure cancellation on other errors
            with suppress(asyncio.CancelledError):
                await worker_task

    logger.info("--- RRQ Example Finished ---")


async def run_worker_async(worker: RRQWorker):
    """Helper function to run the worker's main loop asynchronously."""
    # We don't use worker.run() here because it's synchronous.
    # Instead, we directly await the async _run_loop method.
    try:
        await worker._run_loop()
    except Exception as e:
        logger.error(
            f"Worker {worker.worker_id} _run_loop exited with error: {e}", exc_info=True
        )
    finally:
        # Ensure resources are closed even if _run_loop fails unexpectedly
        await worker._close_resources()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught in __main__. Exiting.")
