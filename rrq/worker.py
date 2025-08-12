"""This module defines the RRQWorker class, the core component responsible for
processing jobs from the Reliable Redis Queue (RRQ) system.
"""

import asyncio

# Use standard logging instead of custom one if appropriate
import logging
import os
import random
import signal
import time
import uuid
from contextlib import suppress
from datetime import timezone, datetime
from typing import (
    Any,
    Optional,
)

from rrq.client import RRQClient

from .constants import (
    DEFAULT_WORKER_ID_PREFIX,
)
from .exc import RetryJob
from .job import Job, JobStatus
from .registry import JobRegistry
from .settings import RRQSettings
from .store import JobStore
from .cron import CronJob

logger = logging.getLogger(__name__)


class RRQWorker:
    """An asynchronous worker process for the RRQ system.

    Polls specified queues for ready jobs, acquires locks, executes job handlers,
    manages job lifecycle states (success, failure, retry, timeout, DLQ),
    handles graceful shutdown, and reports health status.
    """

    SIGNALS = (signal.SIGINT, signal.SIGTERM)

    def __init__(
        self,
        settings: RRQSettings,
        job_registry: JobRegistry,
        queues: Optional[list[str]] = None,
        worker_id: Optional[str] = None,
        burst: bool = False,
    ):
        """Initializes the RRQWorker.

        Args:
            settings: The RRQSettings instance for configuration.
            job_registry: The JobRegistry containing the handler functions.
            queues: A list of queue names (without prefix) to poll.
                    If None, defaults to `settings.default_queue_name`.
            worker_id: A unique identifier for this worker instance.
                       If None, one is generated automatically.
        """
        self.settings = settings
        self.job_registry = job_registry
        self.queues = (
            queues if queues is not None else [self.settings.default_queue_name]
        )
        if not self.queues:
            raise ValueError("Worker must be configured with at least one queue.")

        self.job_store = JobStore(settings=settings)
        self.client = RRQClient(settings=settings, job_store=self.job_store)
        self.worker_id = (
            worker_id
            or f"{DEFAULT_WORKER_ID_PREFIX}{os.getpid()}_{uuid.uuid4().hex[:6]}"
        )
        # Burst mode: process existing jobs then exit
        self.burst = burst

        self.cron_jobs: list[CronJob] = list(self.settings.cron_jobs)

        self._semaphore = asyncio.Semaphore(self.settings.worker_concurrency)
        self._running_tasks: set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        self._loop = None  # Will be set in run()
        self._health_check_task: Optional[asyncio.Task] = None
        self._cron_task: Optional[asyncio.Task] = None
        self.status: str = "initializing"  # Worker status (e.g., initializing, running, polling, idle, stopped)
        logger.info(
            f"Initializing RRQWorker {self.worker_id} for queues: {self.queues}"
        )

    def _calculate_jittered_delay(
        self, base_delay: float, jitter_factor: float = 0.5
    ) -> float:
        """Calculate a jittered delay to prevent thundering herd effects.

        Args:
            base_delay: The base delay in seconds.
            jitter_factor: Factor for jitter (0.0 to 1.0). Default 0.5 means ±50% jitter.

        Returns:
            The jittered delay in seconds.
        """
        # Clamp jitter_factor to safe range to prevent negative delays
        jitter_factor = max(0.0, min(jitter_factor, 0.99))

        # Calculate jitter range: base_delay * (1 ± jitter_factor)
        min_delay = base_delay * (1 - jitter_factor)
        max_delay = base_delay * (1 + jitter_factor)

        # Ensure min_delay is always positive
        min_delay = max(0.001, min_delay)

        return random.uniform(min_delay, max_delay)

    async def _call_startup_hook(self) -> None:
        if self.settings.on_startup:
            logger.info(f"Worker {self.worker_id} calling on_startup hook...")
            try:
                await self.settings.on_startup()
                logger.info(f"Worker {self.worker_id} on_startup hook completed.")
            except Exception as e:
                logger.error(
                    f"Worker {self.worker_id} error during on_startup hook: {e}",
                    exc_info=True,
                )
                raise

    async def _call_shutdown_hook(self) -> None:
        if self.settings.on_shutdown:
            logger.info(f"Worker {self.worker_id} calling on_shutdown hook...")
            try:
                await self.settings.on_shutdown()
                logger.info(f"Worker {self.worker_id} on_shutdown hook completed.")
            except Exception as e:
                logger.error(
                    f"Worker {self.worker_id} error during on_shutdown hook: {e}",
                    exc_info=True,
                )

    async def run(self) -> None:
        logger.info(f"RRQWorker {self.worker_id} starting.")
        self.status = "running"
        self._loop = asyncio.get_running_loop()
        self._setup_signal_handlers()
        try:
            await self._call_startup_hook()
            await self._run_loop()
        except asyncio.CancelledError:
            logger.info(f"Worker {self.worker_id} run cancelled.")
        finally:
            logger.info(f"Worker {self.worker_id} shutting down cleanly.")
            await self._call_shutdown_hook()
            self.status = "stopped"
            logger.info(f"Worker {self.worker_id} stopped.")

    async def _run_loop(self) -> None:
        """The main asynchronous execution loop for the worker.

        Continuously polls queues for jobs, manages concurrency, and handles shutdown.
        """
        logger.info(f"Worker {self.worker_id} starting run loop.")
        self._health_check_task = self._loop.create_task(self._heartbeat_loop())
        if self.cron_jobs:
            for cj in self.cron_jobs:
                cj.schedule_next()
            self._cron_task = self._loop.create_task(self._cron_loop())

        while not self._shutdown_event.is_set():
            try:
                jobs_to_fetch = self.settings.worker_concurrency - len(
                    self._running_tasks
                )
                if jobs_to_fetch > 0:
                    if self.status != "polling":
                        logger.debug(
                            f"Worker {self.worker_id} polling for up to {jobs_to_fetch} jobs..."
                        )
                        self.status = "polling"
                    # Poll for jobs and get count of jobs started
                    fetched_count = await self._poll_for_jobs(jobs_to_fetch)
                    # In burst mode, exit when no new jobs and no tasks running
                    if self.burst and fetched_count == 0 and not self._running_tasks:
                        logger.info(
                            f"Worker {self.worker_id} burst mode complete: no more jobs."
                        )
                        break
                else:
                    if self.status != "idle (concurrency limit)":
                        logger.debug(
                            f"Worker {self.worker_id} at concurrency limit ({self.settings.worker_concurrency}). Waiting..."
                        )
                        self.status = "idle (concurrency limit)"
                    # At concurrency limit, wait for tasks to finish or poll delay

                    # Use jittered delay to prevent thundering herd effects
                    jittered_delay = self._calculate_jittered_delay(
                        self.settings.default_poll_delay_seconds
                    )
                    await asyncio.sleep(jittered_delay)
            except Exception as e:
                logger.error(
                    f"Worker {self.worker_id} encountered error in main run loop: {e}",
                    exc_info=True,
                )
                # Avoid tight loop on persistent errors with jittered delay
                jittered_delay = self._calculate_jittered_delay(1.0)
                await asyncio.sleep(jittered_delay)

        logger.info(
            f"Worker {self.worker_id} shutdown signal received. Draining tasks..."
        )
        await self._drain_tasks()
        logger.info(f"Worker {self.worker_id} task drain complete.")
        if self._health_check_task:
            self._health_check_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._health_check_task
        if self._cron_task:
            self._cron_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._cron_task

    async def _poll_for_jobs(self, count: int) -> None:
        """Polls configured queues round-robin and attempts to start processing jobs.

        Args:
            count: The maximum number of jobs to attempt to start in this poll cycle.
        """
        fetched_count = 0
        # Simple round-robin polling for now
        # TODO: Add queue prioritization logic if needed.
        for queue_name in self.queues:
            if fetched_count >= count or self._shutdown_event.is_set():
                break

            try:
                ready_job_ids = await self.job_store.get_ready_job_ids(
                    queue_name, count - fetched_count
                )
                if not ready_job_ids:
                    continue  # No jobs ready in this queue

                logger.debug(
                    f"Worker {self.worker_id} found {len(ready_job_ids)} ready jobs in queue '{queue_name}'."
                )

                for job_id in ready_job_ids:
                    if fetched_count >= count or self._shutdown_event.is_set():
                        break

                    try:
                        # Try to acquire lock and remove from queue first (without semaphore)
                        job_acquired = await self._try_acquire_job(job_id, queue_name)
                        if job_acquired:
                            # Only acquire semaphore after successfully getting the job
                            await self._semaphore.acquire()
                            try:
                                # Process the job (we already have the lock and removed from queue)
                                # The semaphore will be released when the job task completes
                                await self._process_acquired_job(
                                    job_acquired, queue_name
                                )
                                fetched_count += 1
                            except Exception as e_process:
                                logger.error(
                                    f"Worker {self.worker_id} exception processing acquired job {job_id}: {e_process}",
                                    exc_info=True,
                                )
                                # Release lock and semaphore since processing failed
                                await self.job_store.release_job_lock(job_id)
                                self._semaphore.release()
                        # If job_acquired is None, another worker got it - continue to next job
                    except Exception as e_try:
                        # Catch errors during the job acquisition itself
                        logger.error(
                            f"Worker {self.worker_id} exception trying to acquire job {job_id}: {e_try}",
                            exc_info=True,
                        )

            except Exception as e_poll:
                logger.error(
                    f"Worker {self.worker_id} error polling queue '{queue_name}': {e_poll}",
                    exc_info=True,
                )
                # Avoid tight loop on polling error with jittered delay
                jittered_delay = self._calculate_jittered_delay(1.0)
                await asyncio.sleep(jittered_delay)
        # For burst mode, return number of jobs fetched in this poll
        return fetched_count

    async def _try_acquire_job(self, job_id: str, queue_name: str) -> Optional[Job]:
        """Attempts to atomically lock and remove a job from the queue.

        Args:
            job_id: The ID of the job to attempt acquiring.
            queue_name: The name of the queue the job ID was retrieved from.

        Returns:
            The Job object if successfully acquired, None otherwise.
        """
        logger.debug(
            f"Worker {self.worker_id} attempting to acquire job {job_id} from queue '{queue_name}'"
        )
        job = await self.job_store.get_job_definition(job_id)
        if not job:
            logger.warning(
                f"Worker {self.worker_id} job definition {job_id} not found during _try_acquire_job from queue {queue_name}."
            )
            return None  # Job vanished between poll and fetch?

        # Determine job-specific timeout and calculate lock timeout
        job_timeout = (
            job.job_timeout_seconds
            if job.job_timeout_seconds is not None
            else self.settings.default_job_timeout_seconds
        )
        lock_timeout_ms = (
            job_timeout + self.settings.default_lock_timeout_extension_seconds
        ) * 1000

        # Atomically acquire the processing lock and remove from queue
        lock_acquired, removed_count = await self.job_store.atomic_lock_and_remove_job(
            job.id, queue_name, self.worker_id, int(lock_timeout_ms)
        )

        if not lock_acquired or removed_count == 0:
            return None  # Another worker got there first

        # Successfully acquired the job
        logger.debug(f"Worker {self.worker_id} successfully acquired job {job.id}")
        return job

    async def _process_acquired_job(self, job: Job, queue_name: str) -> None:
        """Processes a job that has already been acquired (locked and removed from queue).

        Note: This method assumes the worker has already acquired the concurrency semaphore.
        The semaphore will be released when the job task completes via _task_cleanup.

        Args:
            job: The Job object that was successfully acquired.
            queue_name: The name of the queue the job was retrieved from.
        """
        try:
            await self.job_store.update_job_status(job.id, JobStatus.ACTIVE)
            logger.debug(
                f"Worker {self.worker_id} updated status to ACTIVE for job {job.id}"
            )

            # Create and track the execution task
            # The semaphore will be released when this task completes
            task = self._loop.create_task(self._execute_job(job, queue_name))
            self._running_tasks.add(task)
            task.add_done_callback(lambda t: self._task_cleanup(t, self._semaphore))
            logger.info(
                f"Worker {self.worker_id} started job {job.id} ('{job.function_name}') from queue '{queue_name}'"
            )
        except Exception as e_start:
            # Catch errors during status update or task creation
            logger.error(
                f"Worker {self.worker_id} failed to start task for job {job.id} after acquisition: {e_start}",
                exc_info=True,
            )
            # Release the lock since task wasn't started
            await self.job_store.release_job_lock(job.id)
            raise  # Re-raise to be handled by caller

    async def _try_process_job(self, job_id: str, queue_name: str) -> bool:
        """Attempts to lock, fetch definition, and start the execution task for a specific job.

        This method is kept for backward compatibility and uses the optimized approach internally.
        For new code, prefer using _try_acquire_job and _process_acquired_job separately.

        Note: This method handles semaphore acquisition internally for backward compatibility.

        Args:
            job_id: The ID of the job to attempt processing.
            queue_name: The name of the queue the job ID was retrieved from.

        Returns:
            True if the job processing task was successfully started, False otherwise
            (e.g., lock conflict, job definition not found, already removed).
        """
        # Use the optimized approach: acquire job first, then process
        job_acquired = await self._try_acquire_job(job_id, queue_name)
        if not job_acquired:
            return False

        # For backward compatibility, acquire semaphore here since old callers expect it
        await self._semaphore.acquire()
        try:
            # Process the acquired job
            await self._process_acquired_job(job_acquired, queue_name)
            return True
        except Exception as e_process:
            logger.error(
                f"Worker {self.worker_id} failed to process acquired job {job_id}: {e_process}",
                exc_info=True,
            )
            # Release semaphore on error since _process_acquired_job doesn't handle it
            self._semaphore.release()
            # Lock is already released in _process_acquired_job on error
            return False

    async def _execute_job(self, job: Job, queue_name: str) -> None:
        """Executes a single job handler, managing timeouts, errors, retries, and results.

        This method is run within an asyncio Task for each job.
        It ensures the processing lock is released in a finally block.

        Args:
            job: The Job object to execute.
            queue_name: The name of the queue the job was pulled from.
        """
        logger.debug(
            f"Worker {self.worker_id} executing job {job.id} ('{job.function_name}') from queue '{queue_name}'"
        )
        start_time = time.monotonic()
        actual_job_timeout = (
            job.job_timeout_seconds
            if job.job_timeout_seconds is not None
            else self.settings.default_job_timeout_seconds
        )

        try:
            # --- Find Handler ---
            handler = self.job_registry.get_handler(job.function_name)
            if not handler:
                raise ValueError(
                    f"No handler registered for function '{job.function_name}'"
                )

            # --- Prepare Context ---
            context = {
                "job_id": job.id,
                "job_try": job.current_retries + 1,  # Attempt number (1-based)
                "enqueue_time": job.enqueue_time,
                "settings": self.settings,
                "worker_id": self.worker_id,
                "queue_name": queue_name,
                "rrq_client": self.client,
            }

            # --- Execute Handler ---
            result = None
            exc: Optional[BaseException] = None  # Stores caught exception

            try:  # Inner try for handler execution and its specific exceptions
                logger.debug(f"Calling handler '{job.function_name}' for job {job.id}")
                result = await asyncio.wait_for(
                    handler(context, *job.job_args, **job.job_kwargs),
                    timeout=float(actual_job_timeout),
                )
                logger.debug(f"Handler for job {job.id} returned successfully.")
            except TimeoutError as e_timeout:  # Specifically from wait_for
                exc = e_timeout
                logger.warning(
                    f"Job {job.id} execution timed out after {actual_job_timeout}s."
                )
            except RetryJob as e_retry:  # Handler explicitly requests retry
                exc = e_retry
                logger.info(f"Job {job.id} requested retry: {e_retry}")
            except Exception as e_other:  # Any other exception from the handler itself
                exc = e_other
                logger.error(
                    f"Job {job.id} handler '{job.function_name}' raised unhandled exception:",
                    exc_info=e_other,
                )

            # --- Process Outcome ---
            duration = time.monotonic() - start_time
            if exc is None:  # Success
                await self._handle_job_success(job, result)
                logger.info(f"Job {job.id} completed successfully in {duration:.2f}s.")
            elif isinstance(exc, RetryJob):
                await self._process_retry_job(job, exc, queue_name)
                # Logging done within _process_retry_job
            elif isinstance(exc, asyncio.TimeoutError):
                error_msg = (
                    str(exc)
                    if str(exc)
                    else f"Job timed out after {actual_job_timeout}s."
                )
                await self._handle_job_timeout(job, queue_name, error_msg)
                # Logging done within _handle_job_timeout
            else:  # Other unhandled exception from handler
                await self._process_other_failure(job, exc, queue_name)
                # Logging done within _process_other_failure

        except ValueError as ve:  # Catches "handler not found"
            logger.error(f"Job {job.id} fatal error: {ve}. Moving to DLQ.")
            await self._handle_fatal_job_error(job, queue_name, str(ve))
        except asyncio.CancelledError:
            # Catches cancellation of this _execute_job task (e.g., worker shutdown)
            logger.warning(
                f"Job {job.id} execution was cancelled (likely worker shutdown). Handling cancellation."
            )
            await self._handle_job_cancellation_on_shutdown(job, queue_name)
            # Do not re-raise; cancellation is handled.
        except (
            Exception
        ) as critical_exc:  # Safety net for unexpected errors in this method
            logger.critical(
                f"Job {job.id} encountered an unexpected critical error during execution logic: {critical_exc}",
                exc_info=critical_exc,
            )
            # Fallback: Try to move to DLQ to avoid losing the job entirely
            await self._handle_fatal_job_error(
                job, queue_name, f"Critical worker error: {critical_exc}"
            )
        finally:
            # CRITICAL: Ensure the lock is released regardless of outcome
            await self.job_store.release_job_lock(job.id)
            # Logger call moved inside release_job_lock for context

    async def _handle_job_success(self, job: Job, result: Any) -> None:
        """Handles successful job completion: saves result, sets TTL, updates status, and releases unique lock."""
        try:
            ttl = (
                job.result_ttl_seconds
                if job.result_ttl_seconds is not None
                else self.settings.default_result_ttl_seconds
            )
            await self.job_store.save_job_result(job.id, result, ttl_seconds=int(ttl))
            # Status is set to COMPLETED within save_job_result

            if job.job_unique_key:
                logger.debug(
                    f"Job {job.id} completed successfully, releasing unique key: {job.job_unique_key}"
                )
                await self.job_store.release_unique_job_lock(job.job_unique_key)

        except Exception as e_success:
            logger.error(
                f"Error during post-success handling for job {job.id}: {e_success}",
                exc_info=True,
            )
            # Job finished, but result/unique lock release failed.
            # Lock is released in _execute_job's finally. Unique lock might persist.

    async def _process_retry_job(
        self, job: Job, exc: RetryJob, queue_name: str
    ) -> None:
        """Handles job failures where the handler explicitly raised RetryJob.

        Increments retry count, checks against max_retries, and re-queues with
        appropriate delay (custom or exponential backoff) or moves to DLQ.
        """
        log_prefix = f"Worker {self.worker_id} job {job.id} (queue '{queue_name}')"
        max_retries = job.max_retries

        try:
            # Check if we would exceed max retries
            anticipated_retry_count = job.current_retries + 1
            if anticipated_retry_count >= max_retries:
                # Max retries exceeded, increment retry count and move directly to DLQ
                logger.warning(
                    f"{log_prefix} max retries ({max_retries}) exceeded "
                    f"with RetryJob exception. Moving to DLQ."
                )
                # Increment retry count before moving to DLQ
                await self.job_store.increment_job_retries(job.id)
                error_msg = (
                    str(exc) or f"Max retries ({max_retries}) exceeded after RetryJob"
                )
                await self._move_to_dlq(job, queue_name, error_msg)
                return

            # Determine deferral time
            defer_seconds = exc.defer_seconds
            if defer_seconds is None:
                # Create a temporary job representation for backoff calculation
                temp_job_for_backoff = Job(
                    id=job.id,
                    function_name=job.function_name,
                    current_retries=anticipated_retry_count,  # Use anticipated count
                    max_retries=max_retries,
                )
                defer_ms = self._calculate_backoff_ms(temp_job_for_backoff)
                defer_seconds = defer_ms / 1000.0
            else:
                logger.debug(
                    f"{log_prefix} using custom deferral of {defer_seconds}s from RetryJob exception."
                )

            retry_at_score = (time.time() + defer_seconds) * 1000
            target_queue = job.queue_name or self.settings.default_queue_name

            # Atomically increment retries, update status/error, and re-queue
            new_retry_count = await self.job_store.atomic_retry_job(
                job.id, target_queue, retry_at_score, str(exc), JobStatus.RETRYING
            )

            logger.info(
                f"{log_prefix} explicitly retrying in {defer_seconds:.2f}s "
                f"(attempt {new_retry_count}/{max_retries}) due to RetryJob."
            )
        except Exception as e_handle:
            logger.exception(
                f"{log_prefix} CRITICAL error during RetryJob processing: {e_handle}"
            )

    async def _process_other_failure(
        self, job: Job, exc: Exception, queue_name: str
    ) -> None:
        """Handles general job failures (any exception other than RetryJob or timeout/cancellation).

        Increments retry count, checks against max_retries, and re-queues with
        exponential backoff or moves to DLQ.
        """
        log_prefix = f"Worker {self.worker_id} job {job.id} (queue '{queue_name}')"
        logger.debug(f"{log_prefix} processing general failure: {type(exc).__name__}")

        try:
            max_retries = job.max_retries
            last_error_str = str(exc)

            # Check if we would exceed max retries
            anticipated_retry_count = job.current_retries + 1
            if anticipated_retry_count >= max_retries:
                # Max retries exceeded, increment retry count and move directly to DLQ
                logger.warning(
                    f"{log_prefix} failed after max retries ({max_retries}). Moving to DLQ. Error: {str(exc)[:100]}..."
                )
                # Increment retry count before moving to DLQ
                await self.job_store.increment_job_retries(job.id)
                # _move_to_dlq handles setting FAILED status, completion time, and last error.
                await self._move_to_dlq(job, queue_name, last_error_str)
                return

            # Calculate backoff delay using anticipated retry count
            defer_ms = self._calculate_backoff_ms(
                Job(
                    id=job.id,
                    function_name=job.function_name,
                    current_retries=anticipated_retry_count,  # Use anticipated count
                    max_retries=max_retries,
                )
            )
            retry_at_score = (time.time() * 1000) + defer_ms
            target_queue = job.queue_name or self.settings.default_queue_name

            # Atomically increment retries, update status/error, and re-queue
            new_retry_count = await self.job_store.atomic_retry_job(
                job.id, target_queue, retry_at_score, last_error_str, JobStatus.RETRYING
            )

            logger.info(
                f"{log_prefix} failed, retrying in {defer_ms / 1000.0:.2f}s "
                f"(attempt {new_retry_count}/{max_retries}). Error: {str(exc)[:100]}..."
            )

        except Exception as e_handle:
            logger.exception(
                f"{log_prefix} CRITICAL error during general failure processing (original exc: {type(exc).__name__}): {e_handle}"
            )

    async def _move_to_dlq(self, job: Job, queue_name: str, error_message: str) -> None:
        """Moves a job to the Dead Letter Queue (DLQ) and releases its unique lock if present."""

        dlq_name = self.settings.default_dlq_name  # Or derive from original queue_name
        completion_time = datetime.now(timezone.utc)
        try:
            await self.job_store.move_job_to_dlq(
                job_id=job.id,
                dlq_name=dlq_name,
                error_message=error_message,
                completion_time=completion_time,
            )
            logger.warning(
                f"Worker {self.worker_id} moved job {job.id} from queue '{queue_name}' to DLQ '{dlq_name}'. Reason: {error_message}"
            )

            if job.job_unique_key:
                logger.debug(
                    f"Job {job.id} moved to DLQ, releasing unique key: {job.job_unique_key}"
                )
                await self.job_store.release_unique_job_lock(job.job_unique_key)

        except Exception as e_dlq:
            logger.error(
                f"Worker {self.worker_id} critical error trying to move job {job.id} to DLQ '{dlq_name}': {e_dlq}",
                exc_info=True,
            )
            # If moving to DLQ fails, the job might be stuck.
            # The processing lock is released in _execute_job's finally. Unique lock might persist.

    def _task_cleanup(self, task: asyncio.Task, semaphore: asyncio.Semaphore) -> None:
        """Callback executed when a job task finishes or is cancelled.

        Removes the task from the running set and releases the concurrency semaphore.
        Also logs any unexpected exceptions raised by the task itself.

        Args:
            task: The completed or cancelled asyncio Task.
            semaphore: The worker's concurrency semaphore.
        """
        task_name = "N/A"
        try:
            if hasattr(task, "get_name"):  # Ensure get_name exists
                task_name = task.get_name()
            elif hasattr(task, "_coro") and hasattr(task._coro, "__name__"):  # Fallback
                task_name = task._coro.__name__
        except Exception:
            pass  # Ignore errors getting name

        logger.debug(
            f"Worker {self.worker_id} cleaning up task '{task_name}'. Releasing semaphore."
        )
        if task in self._running_tasks:
            self._running_tasks.remove(task)
        else:
            logger.warning(
                f"Worker {self.worker_id} task '{task_name}' already removed during cleanup callback? This might indicate an issue."
            )

        semaphore.release()

        try:
            task.result()  # Check for unexpected exceptions from the task future itself
        except asyncio.CancelledError:
            logger.debug(
                f"Task '{task_name}' in worker {self.worker_id} was cancelled."
            )
        except Exception as e:
            logger.error(
                f"Task '{task_name}' in worker {self.worker_id} raised an unhandled exception during cleanup check: {e}",
                exc_info=True,
            )

    def _setup_signal_handlers(self) -> None:
        """Sets up POSIX signal handlers for graceful shutdown."""
        for sig in self.SIGNALS:
            try:
                self._loop.add_signal_handler(sig, self._request_shutdown)
                logger.debug(
                    f"Worker {self.worker_id} registered signal handler for {sig.name}."
                )
            except (NotImplementedError, AttributeError):
                logger.warning(
                    f"Worker {self.worker_id} could not set signal handler for {sig.name} (likely Windows or unsupported environment). Graceful shutdown via signals may not work."
                )

    def _request_shutdown(self) -> None:
        """Callback triggered by a signal to initiate graceful shutdown."""
        if not self._shutdown_event.is_set():
            logger.info(
                f"Worker {self.worker_id} received shutdown signal. Initiating graceful shutdown..."
            )
            self._shutdown_event.set()
        else:
            logger.info(
                f"Worker {self.worker_id} received another shutdown signal, already shutting down."
            )

    async def _drain_tasks(self) -> None:
        """Waits for currently running job tasks to complete, up to a grace period.

        Tasks that do not complete within the grace period are cancelled.
        """
        if not self._running_tasks:
            logger.debug(f"Worker {self.worker_id}: No active tasks to drain.")
            return

        logger.info(
            f"Worker {self.worker_id}: Waiting for {len(self._running_tasks)} active tasks to complete (grace period: {self.settings.worker_shutdown_grace_period_seconds}s)..."
        )
        grace_period = self.settings.worker_shutdown_grace_period_seconds

        # Use asyncio.shield if we want to prevent cancellation of _drain_tasks itself?
        # For now, assume it runs to completion or the main loop handles its cancellation.
        tasks_to_wait_on = list(self._running_tasks)

        # Wait for tasks with timeout
        done, pending = await asyncio.wait(tasks_to_wait_on, timeout=grace_period)

        if done:
            logger.info(
                f"Worker {self.worker_id}: {len(done)} tasks completed within grace period."
            )
        if pending:
            logger.warning(
                f"Worker {self.worker_id}: {len(pending)} tasks did not complete within grace period. Cancelling remaining tasks..."
            )
            for task in pending:
                task_name = "N/A"
                try:
                    if hasattr(task, "get_name"):
                        task_name = task.get_name()
                except Exception:
                    pass
                logger.warning(
                    f"Worker {self.worker_id}: Cancelling task '{task_name}'."
                )
                task.cancel()

            # Wait for the cancelled tasks to finish propagating the cancellation
            await asyncio.gather(*pending, return_exceptions=True)
            logger.info(
                f"Worker {self.worker_id}: Finished waiting for cancelled tasks."
            )

    async def _heartbeat_loop(self) -> None:
        """Periodically updates the worker's health status key in Redis with a TTL."""
        logger.debug(f"Worker {self.worker_id} starting heartbeat loop.")
        while not self._shutdown_event.is_set():
            try:
                health_data = {
                    "worker_id": self.worker_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "status": self.status,
                    "active_jobs": len(self._running_tasks),
                    "concurrency_limit": self.settings.worker_concurrency,
                    "queues": self.queues,
                }
                ttl = (
                    self.settings.worker_health_check_interval_seconds + 10
                )  # Add buffer
                await self.job_store.set_worker_health(
                    self.worker_id, health_data, int(ttl)
                )
                # Logger call moved into set_worker_health
            except Exception as e:
                # Log error but continue the loop
                logger.error(
                    f"Error updating health check for worker {self.worker_id}: {e}",
                    exc_info=True,
                )

            try:
                # Sleep until the next interval, but wake up if shutdown is requested
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=min(60, self.settings.worker_health_check_interval_seconds),
                )
                # If wait_for doesn't time out, shutdown was requested
                logger.debug(
                    f"Worker {self.worker_id} heartbeat loop exiting due to shutdown event."
                )
                break  # Exit loop if shutdown event is set
            except TimeoutError:
                # This is the normal case, continue loop
                pass
            except Exception as sleep_err:
                # Handle potential errors from wait_for itself
                logger.error(
                    f"Worker {self.worker_id} error during heartbeat sleep: {sleep_err}",
                    exc_info=True,
                )
                await asyncio.sleep(1)  # Avoid tight loop

        logger.debug(f"Worker {self.worker_id} heartbeat loop finished.")

    async def _maybe_enqueue_cron_jobs(self) -> None:
        """Enqueue cron jobs that are due to run."""
        now = datetime.now(timezone.utc)
        for cj in self.cron_jobs:
            if cj.due(now):
                unique_key = f"cron:{cj.function_name}" if cj.unique else None
                try:
                    await self.client.enqueue(
                        cj.function_name,
                        *cj.args,
                        _queue_name=cj.queue_name,
                        _unique_key=unique_key,
                        **cj.kwargs,
                    )
                finally:
                    cj.schedule_next(now)

    async def _cron_loop(self) -> None:
        logger.debug(f"Worker {self.worker_id} starting cron loop.")
        while not self._shutdown_event.is_set():
            try:
                await self._maybe_enqueue_cron_jobs()
            except Exception as e:
                logger.error(
                    f"Worker {self.worker_id} error running cron jobs: {e}",
                    exc_info=True,
                )
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=30)
            except TimeoutError:
                pass
        logger.debug(f"Worker {self.worker_id} cron loop finished.")

    async def _close_resources(self) -> None:
        """Closes the worker's resources, primarily the JobStore connection."""
        logger.info(f"Worker {self.worker_id} closing resources...")
        try:
            await self.job_store.aclose()
            logger.info(f"Worker {self.worker_id} JobStore Redis connection closed.")
        except Exception as e_close:
            logger.error(
                f"Worker {self.worker_id} error closing JobStore: {e_close}",
                exc_info=True,
            )

    def _calculate_backoff_ms(self, job: Job) -> int:
        """Calculates exponential backoff delay in milliseconds based on retry count.

        Uses `base_retry_delay_seconds` and `max_retry_delay_seconds` from settings.

        Args:
            job: The Job object (specifically needs `current_retries`).

        Returns:
            The calculated delay in milliseconds.
        """
        # Simple exponential backoff: base * (2^(retries-1))
        # current_retries is 1-based for calculation after increment.
        retry_attempt = job.current_retries
        if retry_attempt <= 0:
            # Should not happen if called after increment, but safeguard
            retry_attempt = 1

        base_delay = self.settings.base_retry_delay_seconds
        max_delay = self.settings.max_retry_delay_seconds

        delay_seconds = min(max_delay, base_delay * (2 ** (retry_attempt - 1)))
        delay_ms = int(delay_seconds * 1000)
        logger.debug(
            f"Calculated backoff for job {job.id} (attempt {retry_attempt}): "
            f"base_delay={base_delay}s, max_delay={max_delay}s -> {delay_ms}ms"
        )
        return delay_ms

    async def _handle_job_timeout(
        self, job: Job, queue_name: str, error_message: str
    ) -> None:
        """Handles job timeouts by moving them directly to the DLQ."""
        log_message_prefix = f"Worker {self.worker_id} job {job.id} {queue_name}"
        logger.warning(f"{log_message_prefix} processing timeout: {error_message}")

        try:
            # Increment retries as an attempt was made.
            # Even though it's a timeout, it did consume a slot and attempt execution.
            # This also ensures that if _move_to_dlq relies on current_retries for anything, it's accurate.
            await self.job_store.increment_job_retries(job.id)

            # Update the job object with the error message before moving to DLQ
            # _move_to_dlq will set FAILED status and completion_time
            await self._move_to_dlq(job, queue_name, error_message)
            logger.info(f"{log_message_prefix} moved to DLQ due to timeout.")
        except Exception as e_timeout_handle:
            logger.exception(
                f"{log_message_prefix} CRITICAL error in _handle_job_timeout: {e_timeout_handle}"
            )

    async def _handle_fatal_job_error(
        self, job: Job, queue_name: str, error_message: str
    ) -> None:
        """Handles fatal job errors (e.g., handler not found) by moving to DLQ without retries."""
        log_message_prefix = f"Worker {self.worker_id} job {job.id} {queue_name}"
        logger.error(
            f"{log_message_prefix} fatal error: {error_message}. Moving to DLQ."
        )
        try:
            # Increment retries as an attempt was made to process/find handler.
            await self.job_store.increment_job_retries(job.id)
            # Note: _move_to_dlq handles setting FAILED status, completion_time, and last_error.
            await self._move_to_dlq(job, queue_name, error_message)
            logger.info(f"{log_message_prefix} moved to DLQ due to fatal error.")
        except Exception as e_fatal_handle:
            logger.exception(
                f"{log_message_prefix} CRITICAL error in _handle_fatal_job_error: {e_fatal_handle}"
            )

    async def _handle_job_cancellation_on_shutdown(self, job: Job, queue_name: str):
        logger.warning(
            f"Job {job.id} ({job.function_name}) was cancelled. Assuming worker shutdown. Re-queueing."
        )
        try:
            job.status = JobStatus.PENDING
            job.next_scheduled_run_time = datetime.now(timezone.utc)  # Re-queue immediately
            job.last_error = "Job execution interrupted by worker shutdown. Re-queued."
            # Do not increment retries for shutdown interruption

            await self.job_store.save_job_definition(job)
            await self.job_store.add_job_to_queue(
                queue_name, job.id, job.next_scheduled_run_time.timestamp() * 1000
            )
            await self.job_store.release_job_lock(job.id)  # Ensure lock is released

            logger.info(f"Successfully re-queued job {job.id} to {queue_name}.")
        except Exception as e_requeue:
            logger.exception(
                f"Failed to re-queue job {job.id} on cancellation/shutdown: {e_requeue}"
            )
            # Fallback: try to move to DLQ if re-queueing fails catastrophically
            try:
                await self.job_store.move_job_to_dlq(
                    job.id,
                    self.settings.default_dlq_name,
                    f"Failed to re-queue during cancellation: {e_requeue}",
                    datetime.now(timezone.utc),
                )
                logger.info(
                    f"Successfully moved job {job.id} to DLQ due to re-queueing failure."
                )
            except Exception as e_move_to_dlq:
                logger.exception(
                    f"Failed to move job {job.id} to DLQ after re-queueing failure: {e_move_to_dlq}"
                )

    async def close(self) -> None:
        """Gracefully close worker resources."""
        logger.info(f"[{self.worker_id}] Closing RRQ worker...")
        if self.client:  # Check if client exists before closing
            await self.client.close()
        if self.job_store:
            # Close the Redis connection pool
            await self.job_store.aclose()
        logger.info(f"[{self.worker_id}] RRQ worker closed.")
