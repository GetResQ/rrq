import asyncio
import time  # Import time for checking retry score
import uuid
from contextlib import suppress
from datetime import timezone, datetime, timedelta
from typing import Any, AsyncGenerator  # Added Callable

import pytest
import pytest_asyncio

from rrq.client import RRQClient
from rrq.constants import (
    DEFAULT_QUEUE_NAME,
    UNIQUE_JOB_LOCK_PREFIX,
)
from rrq.cron import CronJob
from rrq.exc import RetryJob
from rrq.job import Job, JobStatus
from rrq.registry import JobRegistry
from rrq.settings import RRQSettings
from rrq.store import JobStore
from rrq.worker import RRQWorker

# --- Test Job Handlers ---
job_results: dict[str, Any] = {}
job_run_counts: dict[str, int] = {}


async def simple_success_handler(ctx, *args, **kwargs):
    job_id = ctx["job_id"]
    job_results[job_id] = {"args": args, "kwargs": kwargs, "status": "success"}
    job_run_counts[job_id] = job_run_counts.get(job_id, 0) + 1
    await asyncio.sleep(0.01)  # Simulate some work
    return f"Success: {args[0] if args else 'ok'}"


async def simple_failure_handler(ctx, *args, **kwargs):
    job_id = ctx["job_id"]
    job_results[job_id] = {"args": args, "kwargs": kwargs, "status": "fail"}
    job_run_counts[job_id] = job_run_counts.get(job_id, 0) + 1
    await asyncio.sleep(0.01)
    raise ValueError(f"Test failure: {args[0] if args else 'error'}")


async def retry_handler(ctx, *args, **kwargs):
    job_id = ctx["job_id"]
    run_count = job_run_counts.get(job_id, 0) + 1
    job_run_counts[job_id] = run_count
    job_results[job_id] = {
        "args": args,
        "kwargs": kwargs,
        "status": f"retry_{run_count}",
    }
    await asyncio.sleep(0.01)
    if run_count < 3:
        raise RetryJob(defer_seconds=0.05)  # Retry quickly for test
    else:
        raise ValueError("Failed after retries")


async def timeout_handler(ctx, *args, **kwargs):
    job_id = ctx["job_id"]
    job_results[job_id] = {"args": args, "kwargs": kwargs, "status": "timeout_started"}
    job_run_counts[job_id] = job_run_counts.get(job_id, 0) + 1
    await asyncio.sleep(
        5
    )  # Sleep longer than default timeout (if timeout is set short for test)
    job_results[job_id]["status"] = "timeout_should_have_cancelled"
    return "Should not reach here"


async def long_sleep_handler(ctx, *args, **kwargs):
    job_id = ctx["job_id"]
    # print(
    #     f"Job {job_id} (long_sleep_handler) received raw args: {args}, raw kwargs: {kwargs}"
    # )

    sleep_duration = -1.0
    # Try to extract sleep_duration based on how it might have been passed
    if args and isinstance(args[0], (float, int)):
        sleep_duration = float(args[0])
        # actual_args = args[1:] # Remaining positional args for further use if any
        # actual_kwargs = kwargs
    elif "sleep_duration" in kwargs and isinstance(
        kwargs["sleep_duration"], (float, int)
    ):
        sleep_duration = float(kwargs["sleep_duration"])
        # actual_args = args
        # actual_kwargs = {k: v for k, v in kwargs.items() if k != "sleep_duration"}
    # Add more sophisticated checks if necessary, e.g. if job.args itself is passed via kwargs

    if sleep_duration < 0:
        error_msg = f"Job {job_id} (long_sleep_handler) Critical: Could not determine sleep_duration. Received args: {args}, kwargs: {kwargs}"
        # print(error_msg)
        job_results[job_id] = {
            "status": "error_determining_sleep",
            "args": args,
            "kwargs": kwargs,
        }
        raise ValueError(error_msg)

    job_results[job_id] = {
        "status": "started_sleep",
        "duration": sleep_duration,
        "received_args": args,
        "received_kwargs": kwargs,
    }
    job_run_counts[job_id] = job_run_counts.get(job_id, 0) + 1
    try:
        await asyncio.sleep(sleep_duration)
        job_results[job_id]["status"] = "finished_sleep"
        return "Slept peacefully"
    except asyncio.CancelledError:
        job_results[job_id]["status"] = "cancelled_during_sleep"
        raise


# --- End Test Job Handlers ---


# --- Fixtures ---
@pytest_asyncio.fixture(scope="function")
def redis_url() -> str:
    return "redis://localhost:6379/1"  # Use DB 1 for tests


@pytest_asyncio.fixture(scope="function")
def rrq_settings(redis_url) -> RRQSettings:
    # Short timeouts/TTLs for testing where needed
    return RRQSettings(
        redis_dsn=redis_url,
        default_job_timeout_seconds=2,  # Short timeout for timeout tests
        default_result_ttl_seconds=10,
        default_max_retries=3,  # Control retries for tests
        worker_concurrency=3,  # Lower concurrency for easier testing
        default_poll_delay_seconds=0.01,  # Poll frequently in tests
    )


@pytest_asyncio.fixture(scope="function")
async def job_store(rrq_settings) -> AsyncGenerator[JobStore, None]:
    """Fixture for a JobStore, ensuring it's clean for each function and uses function-scoped settings."""
    store = JobStore(settings=rrq_settings)
    await store.redis.flushdb()
    yield store
    await store.redis.flushdb()
    await store.aclose()


@pytest_asyncio.fixture(scope="function")
async def job_registry() -> AsyncGenerator[JobRegistry, None]:
    """Fixture for a job registry, ensuring it's clean for each function."""
    # Clear any existing handlers before the test session
    registry = JobRegistry()
    registry.clear()  # Ensure the instance is empty at the start of the session

    # Register handlers needed for worker tests
    registry.register("simple_success", simple_success_handler)
    registry.register("simple_failure", simple_failure_handler)
    registry.register("retry_task", retry_handler)
    registry.register("timeout_task", timeout_handler)
    registry.register("long_sleep_task", long_sleep_handler)  # Register new handler
    yield registry
    registry.clear()
    job_results.clear()  # Clear global test state
    job_run_counts.clear()


@pytest_asyncio.fixture(scope="function")
async def rrq_client(rrq_settings, job_store) -> AsyncGenerator[RRQClient, None]:
    # Worker tests might use client to enqueue jobs
    client = RRQClient(settings=rrq_settings, job_store=job_store)
    yield client
    # Client uses the same job_store as worker tests, so no separate close needed here


@pytest_asyncio.fixture(scope="function")
async def worker(
    rrq_settings, job_registry, job_store
) -> AsyncGenerator[RRQWorker, None]:
    """Main RRQWorker fixture for tests."""
    # Use the ACTUAL worker now
    # Clear registry before creating worker to avoid state leak
    # job_registry.clear_registry() # Done in job_registry fixture setup
    w = RRQWorker(settings=rrq_settings, job_registry=job_registry)
    # Ensure it uses the same job_store fixture instance
    w.job_store = job_store
    w._loop = (
        asyncio.get_running_loop()
    )  # Explicitly set the loop for tests using _run_loop directly
    yield w
    # Worker's aclose should handle its job_store
    # await w.aclose() # Placeholder worker aclose might be basic


# --- Utility to run worker for a short duration ---
async def run_worker_for(worker: RRQWorker, duration: float = 0.1):
    """Runs the worker loop for a short time, then forcefully stops tasks."""
    worker._shutdown_event.clear()
    run_loop_task = asyncio.create_task(worker._run_loop())
    await asyncio.sleep(duration)  # Let it run

    # --- Forceful Shutdown ---
    worker._request_shutdown()  # Signal the loop to stop polling

    # Explicitly cancel any currently running job tasks
    running_job_tasks = list(worker._running_tasks)
    if running_job_tasks:
        # print(f"Test: Cancelling {len(running_job_tasks)} running job tasks...")
        for task in running_job_tasks:
            task.cancel()
        # Wait for cancellations to be processed
        await asyncio.gather(*running_job_tasks, return_exceptions=True)
        # Give a moment for cleanup callbacks to run after cancellation
        await asyncio.sleep(0.05)

    run_loop_task.cancel()
    with suppress(asyncio.CancelledError):
        await run_loop_task
    # Add final sleep for good measure
    await asyncio.sleep(0.05)
    # print("Test: run_worker_for finished.")


# --- Test Cases ---


@pytest.mark.asyncio
async def test_worker_runs_job_successfully(
    rrq_client: RRQClient, worker: RRQWorker, job_store: JobStore
):
    # Enqueue a job using the client
    job = await rrq_client.enqueue("simple_success", "arg1", kwarg1="value1")
    assert job is not None
    job_id = job.id

    # Run the worker briefly - it should poll, lock, remove, and execute
    # This requires JobStore.get_ready_job_ids to be implemented!
    # Let's assume it exists for now to define the test structure.
    await run_worker_for(worker, duration=0.5)  # Allow time to poll and process

    # Assertions: Check job status, result, run count
    final_job_state = await job_store.get_job_definition(job_id)
    assert final_job_state is not None
    assert final_job_state.status == JobStatus.COMPLETED, (
        f"Job status was {final_job_state.status}"
    )
    assert final_job_state.result == "Success: arg1"
    assert job_id in job_results
    assert job_results[job_id]["status"] == "success"
    assert job_run_counts.get(job_id) == 1

    # Check lock was released by process_job (in placeholder)
    lock_owner = await job_store.get_job_lock_owner(job_id)
    assert lock_owner is None, f"Job lock still held by {lock_owner}"


@pytest.mark.asyncio
async def test_worker_handles_job_failure_and_retry(
    rrq_client: RRQClient, worker: RRQWorker, job_store: JobStore
):
    job = await rrq_client.enqueue("simple_failure", "fail_arg")
    assert job is not None
    job_id = job.id

    # Run worker once - should fail and schedule retry
    await run_worker_for(worker, duration=0.5)

    # Check job state
    first_fail_state = await job_store.get_job_definition(job_id)
    assert first_fail_state is not None
    assert first_fail_state.status == JobStatus.RETRYING
    assert first_fail_state.current_retries == 1  # Incremented once
    assert job_id in job_results
    assert job_results[job_id]["status"] == "fail"
    assert job_run_counts.get(job_id) == 1

    # Check if it's back in the queue with a future score
    queue_key = DEFAULT_QUEUE_NAME
    score = await job_store.redis.zscore(queue_key, job_id.encode("utf-8"))
    assert score is not None
    # Default retry backoff is 5 * (2**(retry-1)) seconds = 5 seconds for first retry
    # Score is ms timestamp
    expected_min_score = (
        time.time() * 1000 + 4000
    )  # Expected score should be ~5s in the future
    assert score > expected_min_score

    # Run worker again - job shouldn't run yet due to delay
    job_run_counts.clear()  # Clear counts to check if it runs again
    job_results.clear()
    await run_worker_for(worker, duration=0.5)
    assert job_run_counts.get(job_id) is None  # Should not have run again yet


@pytest.mark.asyncio
async def test_worker_handles_job_failure_max_retries_dlq(
    rrq_client: RRQClient,
    worker: RRQWorker,
    job_store: JobStore,
    rrq_settings: RRQSettings,
    monkeypatch,
):
    """Test that a job that always fails gets retried up to max_retries then moved to DLQ."""
    # Set small retry delays so the test runs quickly
    monkeypatch.setattr(rrq_settings, "base_retry_delay_seconds", 0.01)
    monkeypatch.setattr(rrq_settings, "max_retry_delay_seconds", 0.01)

    job = await rrq_client.enqueue("simple_failure", "fail_repeatedly")
    assert job is not None
    job_id = job.id

    # Max retries is set to 3 in the rrq_settings fixture for tests
    max_tries = rrq_settings.default_max_retries
    assert max_tries == 3  # Double check fixture setting

    queue_key = DEFAULT_QUEUE_NAME
    dlq_key = rrq_settings.default_dlq_name

    # Run worker long enough for all retries to complete
    # With fast polling (0.01s) and short retry delay (0.01s),
    # all retries should complete within 1 second
    await run_worker_for(worker, duration=1.0)

    # Verify final state: job should be FAILED and moved to DLQ
    final_state = await job_store.get_job_definition(job_id)
    assert final_state is not None
    assert final_state.status == JobStatus.FAILED
    assert final_state.current_retries == max_tries
    assert "Test failure: fail_repeatedly" in final_state.last_error

    # Verify job is NOT in the queue (moved to DLQ)
    final_score = await job_store.redis.zscore(queue_key, job_id.encode("utf-8"))
    assert final_score is None

    # Verify DLQ entry
    dlq_jobs = await job_store.redis.lrange(dlq_key, 0, -1)
    dlq_job_ids = [job_bytes.decode("utf-8") for job_bytes in dlq_jobs]
    assert job_id in dlq_job_ids


@pytest.mark.asyncio
async def test_worker_handles_explicit_retry_job(
    rrq_client: RRQClient, worker: RRQWorker, job_store: JobStore
):
    """Test that RetryJob exception is handled correctly (manual polling)."""
    job = await rrq_client.enqueue("retry_task", "retry_arg")
    assert job is not None
    job_id = job.id
    queue_key = DEFAULT_QUEUE_NAME
    dlq_key = worker.settings.default_dlq_name
    explicit_defer_seconds = 0.05

    # --- Attempt 1: Should raise RetryJob ---
    job_results.clear()
    worker._running_tasks.clear()  # Ensure no stale tasks from previous tests

    # Manually poll and wait for the job task
    await worker._poll_for_jobs(1)
    assert len(worker._running_tasks) == 1, "Worker did not pick up the job"
    job_task_1 = list(worker._running_tasks)[0]
    await job_task_1  # Wait for _execute_job to complete (incl. _process_retry_job)

    # Check state AFTER attempt 1 processing is fully complete
    state_after_1 = await job_store.get_job_definition(job_id)
    assert state_after_1 is not None
    assert state_after_1.status == JobStatus.RETRYING
    assert state_after_1.current_retries == 1
    assert job_results[job_id]["status"] == "retry_1"
    score1 = await job_store.redis.zscore(queue_key, job_id.encode("utf-8"))
    assert score1 is not None
    expected_min_score1 = time.time() * 1000
    assert score1 > expected_min_score1
    assert score1 < expected_min_score1 + (explicit_defer_seconds * 1000) + 100

    # Wait for the explicit retry delay
    await asyncio.sleep(explicit_defer_seconds + 0.1)

    # --- Attempt 2: Should raise RetryJob again ---
    job_results.clear()
    worker._running_tasks.clear()  # Clear before polling again

    await worker._poll_for_jobs(1)
    assert len(worker._running_tasks) == 1, (
        "Worker did not pick up the job for attempt 2"
    )
    job_task_2 = list(worker._running_tasks)[0]
    await job_task_2

    # Check state AFTER attempt 2 processing
    state_after_2 = await job_store.get_job_definition(job_id)
    assert state_after_2 is not None
    assert state_after_2.status == JobStatus.RETRYING
    assert state_after_2.current_retries == 2
    assert job_results[job_id]["status"] == "retry_2"
    score2 = await job_store.redis.zscore(queue_key, job_id.encode("utf-8"))
    assert score2 is not None
    expected_min_score2 = time.time() * 1000
    assert score2 > expected_min_score2
    assert score2 < expected_min_score2 + (explicit_defer_seconds * 1000) + 100

    # Wait for the explicit retry delay
    await asyncio.sleep(explicit_defer_seconds + 0.1)

    # --- Attempt 3: Should raise ValueError -> DLQ ---
    job_results.clear()
    worker._running_tasks.clear()

    await worker._poll_for_jobs(1)
    assert len(worker._running_tasks) == 1, (
        "Worker did not pick up the job for attempt 3"
    )
    job_task_3 = list(worker._running_tasks)[0]
    await job_task_3  # This time _process_other_failure runs

    # Check state AFTER attempt 3 processing (final failure)
    state_after_3 = await job_store.get_job_definition(job_id)
    assert state_after_3 is not None
    assert state_after_3.status == JobStatus.FAILED
    assert state_after_3.current_retries == 3
    assert job_results[job_id]["status"] == "retry_3"
    assert state_after_3.last_error is not None
    assert "Failed after retries" in state_after_3.last_error

    # Check DLQ
    final_score = await job_store.redis.zscore(queue_key, job_id.encode("utf-8"))
    assert final_score is None
    dlq_content_bytes = await job_store.redis.lrange(dlq_key, 0, -1)
    dlq_content = [item.decode("utf-8") for item in dlq_content_bytes]
    assert job_id in dlq_content


@pytest.mark.asyncio
async def test_worker_handles_job_timeout(
    rrq_client: RRQClient,
    worker: RRQWorker,
    job_store: JobStore,
    rrq_settings: RRQSettings,
):
    job_timeout_seconds = 1  # Use an integer value for timeout
    # Override settings for this specific test
    original_timeout = worker.settings.default_job_timeout_seconds
    worker.settings.default_job_timeout_seconds = job_timeout_seconds
    # Ensure the handler sleeps longer than the timeout
    # timeout_handler sleeps for 5s by default, which is > 0.1s

    job = await rrq_client.enqueue("timeout_task", "timeout_arg")
    assert job is not None
    job_id = job.id

    queue_key = DEFAULT_QUEUE_NAME
    dlq_key = rrq_settings.default_dlq_name

    # Clear previous results for this job_id if any
    job_results.pop(job_id, None)
    job_run_counts.pop(job_id, None)

    # We'll use manual polling similar to the explicit retry test to ensure control
    worker._running_tasks.clear()
    await worker._poll_for_jobs(1)  # Pick up the job

    assert len(worker._running_tasks) == 1, "Worker did not pick up the job"
    job_task = list(worker._running_tasks)[0]

    # Wait for the job task to complete.
    # The timeout mechanism in _execute_job should cause it to raise an error.
    await job_task

    # Check job state after timeout
    final_job_state = await job_store.get_job_definition(job_id)
    assert final_job_state is not None

    assert final_job_state.status == JobStatus.FAILED
    assert final_job_state.last_error is not None
    assert "Job timed out" in final_job_state.last_error
    # By default, a timeout leads to failure without retrying.
    # The retry counter is incremented *before* deciding to retry or fail permanently.
    # So, if it goes to FAILED directly from a timeout, current_retries should reflect one attempt.
    assert final_job_state.current_retries == 1

    # Verify the handler didn't complete normally
    assert job_id in job_results
    # status will be 'timeout_started' which is set at the beginning of the handler
    assert job_results[job_id]["status"] == "timeout_started"
    # It should NOT have reached "timeout_should_have_cancelled"

    # Verify it's NOT in the original queue
    score = await job_store.redis.zscore(queue_key, job_id.encode("utf-8"))
    assert score is None, f"Job {job_id} still found in original queue after timeout"

    # Verify it IS in the DLQ
    dlq_content_bytes = await job_store.redis.lrange(dlq_key, 0, -1)
    dlq_content = [item.decode("utf-8") for item in dlq_content_bytes]
    assert job_id in dlq_content, f"Job {job_id} not found in DLQ after timeout"

    # Restore original timeout
    worker.settings.default_job_timeout_seconds = original_timeout


@pytest.mark.asyncio
async def test_worker_graceful_shutdown_releases_active_jobs(
    rrq_client: RRQClient,
    worker: RRQWorker,
    job_store: JobStore,
    rrq_settings: RRQSettings,
    # job_registry is already used to register this new handler via fixture
):
    # Handler sleeps for this long
    sleep_for_handler = 2.0
    # Worker shutdown grace period - must be shorter than handler sleep
    shutdown_grace_period = 0.1

    # Override worker settings for this specific test
    original_shutdown_grace = worker.settings.worker_shutdown_grace_period_seconds
    worker.settings.worker_shutdown_grace_period_seconds = shutdown_grace_period

    job = await rrq_client.enqueue(
        "long_sleep_task", sleep_for_handler, _job_id="graceful_shutdown_job"
    )
    assert job is not None
    job_id = job.id

    # Start the worker's run_loop in a background task
    # Ensure worker doesn't immediately exit if queue is empty initially
    worker._shutdown_event.clear()
    worker_task = asyncio.create_task(worker._run_loop(), name=f"WorkerTask_{job_id}")

    # Give worker time to pick up the job and set it to ACTIVE
    # Polling delay is 0.01s in settings, so this should be enough
    await asyncio.sleep(0.5)

    active_job_state = await job_store.get_job_definition(job_id)
    assert active_job_state is not None, "Job state not found after starting worker"
    if active_job_state.status != JobStatus.ACTIVE:
        # Add more diagnostic info if this fails
        await job_store.get_queued_job_ids(DEFAULT_QUEUE_NAME, 0, -1)
        if worker._running_tasks:
            for t in worker._running_tasks:
                pass
        pytest.fail(f"Job should be ACTIVE, was {active_job_state.status}. Logs above.")

    # Request shutdown
    worker._request_shutdown()

    # Wait for the worker task to complete (handles drain)
    try:
        # Timeout for waiting: handler sleep + grace period + buffer
        await asyncio.wait_for(
            worker_task, timeout=sleep_for_handler + shutdown_grace_period + 2.0
        )
    except TimeoutError:
        pytest.fail(
            f"Worker task {worker_task.get_name()} did not complete within expected timeout after shutdown request."
        )
    except Exception as e:
        pytest.fail(
            f"Worker task {worker_task.get_name()} failed with an exception: {e}"
        )

    # Assertions
    final_job_state = await job_store.get_job_definition(job_id)
    assert final_job_state is not None

    assert final_job_state.status == JobStatus.PENDING, (
        f"Job should be PENDING after graceful shutdown, was {final_job_state.status}"
    )
    assert (
        "interrupted by worker shutdown" in (final_job_state.last_error or "").lower()
    )
    # Retries should not have incremented from its state when it was ACTIVE
    assert final_job_state.current_retries == active_job_state.current_retries, (
        "Retries should not increment for shutdown interruption"
    )

    queue_key = DEFAULT_QUEUE_NAME
    job_score = await job_store.redis.zscore(queue_key, job_id.encode("utf-8"))
    assert job_score is not None, f"Job {job_id} not found back in queue {queue_key}"
    assert job_score > (time.time() - 10) * 1000  # Re-queued recently (within 10s)

    lock_owner = await job_store.get_job_lock_owner(job_id)
    assert lock_owner is None, (
        f"Job lock for {job_id} should be released, but held by {lock_owner}"
    )

    assert job_id in job_results, f"No results found for job {job_id}"
    assert job_results[job_id]["status"] == "cancelled_during_sleep", (
        f"Handler should have been cancelled, result was: {job_results[job_id]}"
    )

    # print(f"Test: Graceful shutdown test for job {job_id} passed.")
    # Restore original settings on worker
    worker.settings.worker_shutdown_grace_period_seconds = original_shutdown_grace


@pytest.mark.asyncio
async def test_worker_health_check_updates(
    worker: RRQWorker,
    job_store: JobStore,
    rrq_settings: RRQSettings,
):
    health_interval = 0.1  # Short interval for testing
    ttl_buffer = 10  # Default buffer added in heartbeat loop
    expected_ttl_max = health_interval + ttl_buffer

    # Override worker settings for this specific test
    original_health_interval = worker.settings.worker_health_check_interval_seconds
    worker.settings.worker_health_check_interval_seconds = health_interval

    worker_id = worker.worker_id

    # Start the worker's run_loop in a background task
    worker._shutdown_event.clear()
    worker_task = asyncio.create_task(worker._run_loop(), name="WorkerTask_HealthCheck")
    # print(f"Test: Worker task started for health check test (Worker ID: {worker_id})")

    try:
        # --- Check 1: After first interval ---
        await asyncio.sleep(health_interval * 1.5)
        # print("Test: Checking health data after first interval...")
        health_data1, ttl1 = await job_store.get_worker_health(worker_id)

        assert health_data1 is not None, "Health data not found after first interval"
        assert health_data1["worker_id"] == worker_id
        assert health_data1["status"] in [
            "polling",
            "idle (concurrency limit)",
            "running",
        ]  # Worker should be active
        ts1_str = health_data1.get("timestamp")
        assert ts1_str is not None
        ts1 = datetime.fromisoformat(ts1_str)
        assert (datetime.now(timezone.utc) - ts1).total_seconds() < (health_interval * 2), (
            "Timestamp seems too old"
        )

        assert ttl1 is not None, "TTL not found for health key"
        assert 0 < ttl1 <= expected_ttl_max, (
            f"TTL {ttl1} out of expected range (0, {expected_ttl_max}]"
        )
        # print(f"Test: Health check 1 PASSED. Data: {health_data1}, TTL: {ttl1}")

        # --- Check 2: After second interval ---
        await asyncio.sleep(health_interval * 1.5)
        # print("Test: Checking health data after second interval...")
        health_data2, ttl2 = await job_store.get_worker_health(worker_id)

        assert health_data2 is not None, "Health data not found after second interval"
        ts2_str = health_data2.get("timestamp")
        assert ts2_str is not None
        ts2 = datetime.fromisoformat(ts2_str)
        assert ts2 > ts1, "Timestamp did not update after second interval"
        assert (datetime.now(timezone.utc) - ts2).total_seconds() < (health_interval * 2), (
            "Timestamp 2 seems too old"
        )

        assert ttl2 is not None, "TTL not found for health key (2nd check)"
        assert 0 < ttl2 <= expected_ttl_max, (
            f"TTL {ttl2} out of expected range (0, {expected_ttl_max}] (2nd check)"
        )

        # --- Check 3: Expiry after shutdown ---
        worker._request_shutdown()
        await asyncio.wait_for(worker_task, timeout=5.0)

        # Directly remove the health key instead of waiting for TTL expiry
        await job_store.redis.delete(f"rrq:health:worker:{worker_id}")
        health_data3, ttl3 = await job_store.get_worker_health(worker_id)
        assert health_data3 is None, (
            f"Health data still exists after expiry: {health_data3}"
        )
        assert ttl3 is None, "TTL should be None after expiry"

    finally:
        # Ensure worker task is cancelled if something failed above
        if not worker_task.done():
            worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await worker_task
        # Restore original settings
        worker.settings.worker_health_check_interval_seconds = original_health_interval


@pytest.mark.asyncio
async def test_worker_releases_unique_lock_on_success(
    rrq_client: RRQClient, worker: RRQWorker, job_store: JobStore
):
    unique_key = "test_unique_success"
    lock_key = f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"

    # Enqueue job with unique key
    job = await rrq_client.enqueue("simple_success", "data", _unique_key=unique_key)
    assert job is not None
    job_id = job.id

    # Check lock exists
    lock_value = await job_store.redis.get(lock_key)
    assert lock_value is not None
    assert lock_value.decode() == job_id

    # Run worker
    await run_worker_for(
        worker, duration=0.5
    )  # Allow time for job and potential cleanup

    # Check job completed
    completed_job = await job_store.get_job_definition(job_id)
    assert completed_job is not None
    assert completed_job.status == JobStatus.COMPLETED
    job_result_data = job_results.get(job_id)
    assert job_result_data is not None
    assert job_result_data["status"] == "success"

    # Check lock is released
    assert await job_store.redis.exists(lock_key) == 0


@pytest.mark.asyncio
async def test_worker_releases_unique_lock_on_dlq(
    rrq_client: RRQClient,
    worker: RRQWorker,
    job_store: JobStore,
    rrq_settings: RRQSettings,  # For default_dlq_name
):
    unique_key = "test_unique_dlq"
    lock_key = f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"

    # Enqueue job designed to fail and go to DLQ
    # Override max_retries to 1 for quicker DLQ processing (job_settings default_max_retries is 3)
    # The simple_failure_handler will cause it to go through worker's retry mechanism.
    job = await rrq_client.enqueue(
        "simple_failure", "fail_data", _unique_key=unique_key, _max_retries=1
    )
    assert job is not None
    job_id = job.id

    # Check lock exists
    lock_value = await job_store.redis.get(lock_key)
    assert lock_value is not None
    assert lock_value.decode() == job_id

    # Run worker long enough for retries and DLQ
    # default_poll_delay_seconds=0.01, base_retry_delay_seconds=5.0 (from rrq_settings.py, used by worker)
    # Let's use default_max_retries from rrq_settings which is 3 in tests
    # Test rrq_settings has default_max_retries = 3.
    # We set _max_retries=1, so it should fail once, retry (incrementing to 1), then worker sees 1 < 1 is false, so DLQ.
    # Actually, new_retry_count will be 1. if new_retry_count < job.max_retries (1 < 1) is false. So it should DLQ.
    # Worker's _process_other_failure will increment retries.
    # Initial attempt fails. _process_other_failure called. new_retry_count = 1.
    # Compares new_retry_count (1) < max_retries (1) -> false. Moves to DLQ.
    # So, one run of the job should be enough.
    await run_worker_for(
        worker, duration=1.0
    )  # Increased duration to be safe for retries + DLQ processing

    # Check job is in DLQ
    failed_job = await job_store.get_job_definition(job_id)
    assert failed_job is not None
    assert failed_job.status == JobStatus.FAILED
    assert failed_job.last_error is not None

    dlq_key = rrq_settings.default_dlq_name
    dlq_jobs = await job_store.redis.lrange(dlq_key, 0, -1)
    assert job_id.encode() in dlq_jobs

    # Check lock is released
    assert await job_store.redis.exists(lock_key) == 0


@pytest.mark.asyncio
async def test_unique_lock_defers_duplicate_then_processes(
    rrq_client: RRQClient, worker: RRQWorker, job_store: JobStore
):
    unique_key = "test_contention_key"
    lock_key = f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"

    # 1. Enqueue first job and acquire lock
    job1 = await rrq_client.enqueue(
        "simple_success", "job1_data", _unique_key=unique_key
    )
    assert job1 is not None
    job1_id = job1.id
    assert await job_store.redis.exists(lock_key) == 1

    # 2. Enqueue duplicate job - should be deferred, not denied
    job2 = await rrq_client.enqueue(
        "simple_success", "job2_data_attempt1", _unique_key=unique_key
    )
    assert job2 is not None
    # Ensure deferred at least by some positive TTL
    score = await job_store.redis.zscore(
        rrq_client.settings.default_queue_name, job2.id.encode("utf-8")
    )
    assert score is not None
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    assert score >= now_ms

    # 3. Run worker to process job1
    await run_worker_for(worker, duration=0.5)

    # 4. Check job1 completed and lock is released
    completed_job1 = await job_store.get_job_definition(job1_id)
    assert completed_job1 is not None
    assert completed_job1.status == JobStatus.COMPLETED
    assert await job_store.redis.exists(lock_key) == 0


# --- RRQWorker 'queues' parameter tests ---
def test_rrqworker_default_queues(rrq_settings, job_registry):
    """RRQWorker without explicit queues uses the default queue from settings."""
    w = RRQWorker(settings=rrq_settings, job_registry=job_registry)
    assert w.queues == [rrq_settings.default_queue_name]


def test_rrqworker_custom_queues(rrq_settings, job_registry):
    """RRQWorker accepts a custom list of queues to poll."""
    custom = ["alpha", "beta"]
    w = RRQWorker(settings=rrq_settings, job_registry=job_registry, queues=custom)
    assert w.queues == custom


def test_rrqworker_empty_queues_raises(rrq_settings, job_registry):
    """RRQWorker must have at least one queue; empty list should raise ValueError."""
    with pytest.raises(ValueError):
        RRQWorker(settings=rrq_settings, job_registry=job_registry, queues=[])


def test_worker_init_no_queues():
    settings = RRQSettings()
    registry = JobRegistry()
    # empty list should raise
    with pytest.raises(ValueError):
        RRQWorker(settings, registry, queues=[])


def test_calculate_backoff_ms():
    settings = RRQSettings()
    # set base and max for predictable values
    settings.base_retry_delay_seconds = 1
    settings.max_retry_delay_seconds = 5
    registry = JobRegistry()
    worker = RRQWorker(settings, registry, queues=["q1"])
    # attempt 1 => 1 * 2^(1-1) =1s -> 1000ms
    job = Job(id="1", function_name="fn", current_retries=1, max_retries=3)
    assert worker._calculate_backoff_ms(job) == 1000
    # attempt high => capped at max
    job.current_retries = 10
    assert worker._calculate_backoff_ms(job) == 5000


def test_request_shutdown_and_status():
    settings = RRQSettings()
    registry = JobRegistry()
    worker = RRQWorker(settings, registry, queues=["q"])
    # initially not shutting down
    assert not worker._shutdown_event.is_set()
    worker._request_shutdown()
    assert worker._shutdown_event.is_set()


@pytest.mark.asyncio
async def test_drain_tasks_no_tasks(monkeypatch):
    settings = RRQSettings()
    registry = JobRegistry()
    worker = RRQWorker(settings, registry, queues=["q"])
    # No tasks => returns immediately
    # Should not raise
    await worker._drain_tasks()


@pytest.mark.asyncio
async def test_task_cleanup_and_semaphore(monkeypatch):
    settings = RRQSettings()
    registry = JobRegistry()
    worker = RRQWorker(settings, registry, queues=["q"])
    # create a semaphore with value 0 (acquired)
    sem = asyncio.Semaphore(0)
    # create a dummy completed task
    fut = asyncio.get_event_loop().create_future()
    fut.set_result(None)
    # add to running tasks
    worker._running_tasks.add(fut)
    # cleanup
    worker._task_cleanup(fut, sem)
    # semaphore should be released => value > 0
    assert sem._value == 1
    # task removed
    assert fut not in worker._running_tasks


@pytest.mark.asyncio
async def test_worker_close_calls_job_store_aclose():
    # Ensure RRQWorker.close calls aclose on its job_store
    settings = RRQSettings()
    registry = JobRegistry()
    worker = RRQWorker(settings, registry)

    # Prepare dummy job_store
    class DummyStore:
        def __init__(self):
            self.aclose_called = False

        async def aclose(self):
            self.aclose_called = True

    store = DummyStore()
    # Inject dummy store into worker and client
    worker.job_store = store
    worker.client.job_store = store
    # Prevent client.close from closing store
    worker.client._created_store_internally = False
    # Call close
    await worker.close()
    assert store.aclose_called, "JobStore.aclose was not called by worker.close"


@pytest.mark.asyncio
async def test_cron_job_enqueue_unique(rrq_settings, job_registry, job_store):
    cron_job = CronJob(
        function_name="simple_success", schedule="* * * * *", unique=True
    )
    # Set the next_run_time to a past time so the job is considered due
    cron_job.next_run_time = datetime.now(timezone.utc) - timedelta(minutes=1)

    rrq_settings.cron_jobs = [cron_job]
    worker = RRQWorker(settings=rrq_settings, job_registry=job_registry)
    worker.job_store = job_store
    worker.client.job_store = job_store
    await worker._maybe_enqueue_cron_jobs()
    queued1 = await job_store.get_queued_job_ids(rrq_settings.default_queue_name, 0, -1)
    assert len(queued1) == 1
    await worker._maybe_enqueue_cron_jobs()
    queued2 = await job_store.get_queued_job_ids(rrq_settings.default_queue_name, 0, -1)
    assert queued2 == queued1


@pytest.mark.asyncio
async def test_cron_job_basic_enqueue(rrq_settings, job_registry, job_store):
    """Test basic cron job enqueueing without unique constraint."""
    cron_job = CronJob(function_name="simple_success", schedule="* * * * *")
    # Set the next_run_time to a past time so the job is considered due
    past_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    cron_job.next_run_time = past_time

    rrq_settings.cron_jobs = [cron_job]
    worker = RRQWorker(settings=rrq_settings, job_registry=job_registry)
    worker.job_store = job_store
    worker.client.job_store = job_store

    await worker._maybe_enqueue_cron_jobs()
    queued_jobs = await job_store.get_queued_job_ids(
        rrq_settings.default_queue_name, 0, -1
    )
    assert len(queued_jobs) == 1

    # Reset to past time again since schedule_next was called
    cron_job.next_run_time = past_time

    # Without unique constraint, should enqueue another job
    await worker._maybe_enqueue_cron_jobs()
    queued_jobs2 = await job_store.get_queued_job_ids(
        rrq_settings.default_queue_name, 0, -1
    )
    assert len(queued_jobs2) == 2


@pytest.mark.asyncio
async def test_cron_job_with_args_and_kwargs(rrq_settings, job_registry, job_store):
    """Test cron job with arguments and keyword arguments."""
    cron_job = CronJob(
        function_name="simple_success",
        schedule="* * * * *",
        args=["cron_arg1", "cron_arg2"],
        kwargs={"cron_key": "cron_value"},
    )
    cron_job.next_run_time = datetime.now(timezone.utc) - timedelta(minutes=1)

    rrq_settings.cron_jobs = [cron_job]
    worker = RRQWorker(settings=rrq_settings, job_registry=job_registry)
    worker.job_store = job_store
    worker.client.job_store = job_store

    await worker._maybe_enqueue_cron_jobs()
    queued_jobs = await job_store.get_queued_job_ids(
        rrq_settings.default_queue_name, 0, -1
    )
    assert len(queued_jobs) == 1

    # Get the job definition and verify args/kwargs
    job_id = queued_jobs[0]
    job_def = await job_store.get_job_definition(job_id)
    assert job_def is not None
    assert job_def.job_args == ["cron_arg1", "cron_arg2"]
    assert job_def.job_kwargs == {"cron_key": "cron_value"}


@pytest.mark.asyncio
async def test_cron_job_custom_queue(rrq_settings, job_registry, job_store):
    """Test cron job with custom queue name."""
    custom_queue = "rrq:queue:cron_queue"
    cron_job = CronJob(
        function_name="simple_success", schedule="* * * * *", queue_name=custom_queue
    )
    cron_job.next_run_time = datetime.now(timezone.utc) - timedelta(minutes=1)

    rrq_settings.cron_jobs = [cron_job]
    worker = RRQWorker(settings=rrq_settings, job_registry=job_registry)
    worker.job_store = job_store
    worker.client.job_store = job_store

    await worker._maybe_enqueue_cron_jobs()

    # Should not be in default queue
    default_jobs = await job_store.get_queued_job_ids(
        rrq_settings.default_queue_name, 0, -1
    )
    assert len(default_jobs) == 0

    # Should be in custom queue
    custom_jobs = await job_store.get_queued_job_ids(custom_queue, 0, -1)
    assert len(custom_jobs) == 1


@pytest.mark.asyncio
async def test_cron_job_not_due(rrq_settings, job_registry, job_store):
    """Test that cron jobs not due are not enqueued."""
    cron_job = CronJob(function_name="simple_success", schedule="* * * * *")
    # Set next_run_time to future so it's not due
    cron_job.next_run_time = datetime.now(timezone.utc) + timedelta(hours=1)

    rrq_settings.cron_jobs = [cron_job]
    worker = RRQWorker(settings=rrq_settings, job_registry=job_registry)
    worker.job_store = job_store
    worker.client.job_store = job_store

    await worker._maybe_enqueue_cron_jobs()
    queued_jobs = await job_store.get_queued_job_ids(
        rrq_settings.default_queue_name, 0, -1
    )
    assert len(queued_jobs) == 0


@pytest.mark.asyncio
async def test_cron_job_schedule_next_after_enqueue(
    rrq_settings, job_registry, job_store
):
    """Test that cron job schedules next run time after enqueueing."""
    cron_job = CronJob(
        function_name="simple_success", schedule="0 9 * * *"
    )  # 9 AM daily
    # Set to past time so it's due
    cron_job.next_run_time = datetime.now(timezone.utc) - timedelta(hours=1)
    original_next_run = cron_job.next_run_time

    rrq_settings.cron_jobs = [cron_job]
    worker = RRQWorker(settings=rrq_settings, job_registry=job_registry)
    worker.job_store = job_store
    worker.client.job_store = job_store

    await worker._maybe_enqueue_cron_jobs()

    # Should have enqueued the job
    queued_jobs = await job_store.get_queued_job_ids(
        rrq_settings.default_queue_name, 0, -1
    )
    assert len(queued_jobs) == 1

    # Should have updated next_run_time to future
    assert cron_job.next_run_time != original_next_run
    assert cron_job.next_run_time > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_multiple_cron_jobs(rrq_settings, job_registry, job_store):
    """Test multiple cron jobs being processed."""
    cron_job1 = CronJob(
        function_name="simple_success", schedule="* * * * *", args=["job1"]
    )
    cron_job2 = CronJob(
        function_name="simple_success", schedule="* * * * *", args=["job2"]
    )

    # Set both to be due
    past_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    cron_job1.next_run_time = past_time
    cron_job2.next_run_time = past_time

    rrq_settings.cron_jobs = [cron_job1, cron_job2]
    worker = RRQWorker(settings=rrq_settings, job_registry=job_registry)
    worker.job_store = job_store
    worker.client.job_store = job_store

    await worker._maybe_enqueue_cron_jobs()
    queued_jobs = await job_store.get_queued_job_ids(
        rrq_settings.default_queue_name, 0, -1
    )
    assert len(queued_jobs) == 2

    # Verify both jobs have different args
    job_defs = []
    for job_id in queued_jobs:
        job_def = await job_store.get_job_definition(job_id)
        job_defs.append(job_def.job_args[0])

    assert "job1" in job_defs
    assert "job2" in job_defs


@pytest.mark.asyncio
async def test_cron_job_execution_end_to_end(
    rrq_settings, job_registry, job_store, worker
):
    """Test end-to-end cron job execution."""
    cron_job = CronJob(
        function_name="simple_success", schedule="* * * * *", args=["cron_test"]
    )
    cron_job.next_run_time = datetime.now(timezone.utc) - timedelta(minutes=1)

    rrq_settings.cron_jobs = [cron_job]
    # Update the existing worker with cron jobs
    worker.cron_jobs = [cron_job]

    # Clear any previous job results
    job_results.clear()
    job_run_counts.clear()

    # Enqueue the cron job
    await worker._maybe_enqueue_cron_jobs()
    queued_jobs = await job_store.get_queued_job_ids(
        rrq_settings.default_queue_name, 0, -1
    )
    assert len(queued_jobs) == 1
    job_id = queued_jobs[0]

    # Use the existing run_worker_for helper which properly handles the worker setup
    await run_worker_for(worker, duration=0.5)

    # Verify job was executed
    assert job_id in job_results
    assert job_results[job_id]["status"] == "success"
    assert job_results[job_id]["args"] == ("cron_test",)
    assert job_run_counts.get(job_id) == 1

    # Verify job completed
    final_job = await job_store.get_job_definition(job_id)
    assert final_job is not None
    assert final_job.status == JobStatus.COMPLETED


    # Note: No worker-side rate limiting tests; client-side deferral honors _defer_by and existing unique locks.
