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
from datetime import timezone, datetime, timedelta
from typing import (
    Any,
    Optional,
)

from rrq.client import RRQClient

from .constants import (
    DEFAULT_WORKER_ID_PREFIX,
)
from .exc import HandlerNotFound, RetryJob
from .executor import (
    ExecutionContext,
    ExecutionRequest,
    Executor,
    StdioExecutor,
    StdioReservation,
)
from .job import Job, JobStatus
from .settings import RRQSettings
from .store import JobStore
from .cron import CronJob
from .telemetry import get_telemetry

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
        queues: Optional[list[str]] = None,
        worker_id: Optional[str] = None,
        burst: bool = False,
        executors: dict[str, Executor] | None = None,
    ):
        """Initializes the RRQWorker.

        Args:
            settings: The RRQSettings instance for configuration.
            queues: A list of queue names (without prefix) to poll.
                    If None, defaults to `settings.default_queue_name`.
            worker_id: A unique identifier for this worker instance.
                       If None, one is generated automatically.
            executors: Mapping of executor names to implementations.
        """
        self.settings = settings
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
        self.default_executor_name = self.settings.default_executor_name
        if executors is None:
            raise ValueError("RRQWorker requires an executors mapping.")
        self.executors = dict(executors)
        if not self.executors:
            raise ValueError("RRQWorker requires at least one executor.")
        if self.default_executor_name not in self.executors:
            raise ValueError(
                f"Default executor '{self.default_executor_name}' is not configured."
            )
        self.executor = self.executors[self.default_executor_name]
        self.executor_routes = dict(self.settings.executor_routes)
        # Burst mode: process existing jobs then exit
        self.burst = burst

        self.cron_jobs: list[CronJob] = list(self.settings.cron_jobs)

        self._semaphore = asyncio.Semaphore(self.settings.worker_concurrency)
        self._running_tasks: set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None  # Will be set in run()
        self._health_check_task: asyncio.Task | None = None
        self._cron_task: asyncio.Task | None = None
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

    async def run(self) -> None:
        logger.info(f"RRQWorker {self.worker_id} starting.")
        self.status = "running"
        self._loop = asyncio.get_running_loop()
        self._setup_signal_handlers()
        telemetry = get_telemetry()
        try:
            try:
                telemetry.worker_started(
                    worker_id=self.worker_id, queues=list(self.queues)
                )
            except Exception as e_telemetry:
                logger.error(
                    f"Worker {self.worker_id} error during telemetry startup: {e_telemetry}",
                    exc_info=True,
                )
            await self._run_loop()
        except asyncio.CancelledError:
            logger.info(f"Worker {self.worker_id} run cancelled.")
        finally:
            logger.info(f"Worker {self.worker_id} shutting down cleanly.")
            self.status = "stopped"
            try:
                telemetry.worker_stopped(worker_id=self.worker_id)
            except Exception as e_telemetry:
                logger.error(
                    f"Worker {self.worker_id} error during telemetry shutdown: {e_telemetry}",
                    exc_info=True,
                )
            try:
                await self.close()
            except Exception as e_close:
                logger.error(
                    f"Worker {self.worker_id} error closing resources during shutdown: {e_close}",
                    exc_info=True,
                )
            logger.info(f"Worker {self.worker_id} stopped.")

    async def _run_loop(self) -> None:
        """The main asynchronous execution loop for the worker.

        Continuously polls queues for jobs, manages concurrency, and handles shutdown.
        """
        logger.info(f"Worker {self.worker_id} starting run loop.")
        loop = self._loop
        assert loop is not None
        self._health_check_task = loop.create_task(self._heartbeat_loop())
        if self.cron_jobs:
            for cj in self.cron_jobs:
                cj.schedule_next()
            self._cron_task = loop.create_task(self._cron_loop())

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
                    if fetched_count == 0:
                        if self.status != "idle (no jobs)":
                            logger.debug(
                                f"Worker {self.worker_id} no jobs found. Waiting..."
                            )
                            self.status = "idle (no jobs)"
                        # Avoid tight polling loop when queues are empty
                        jittered_delay = self._calculate_jittered_delay(
                            self.settings.default_poll_delay_seconds
                        )
                        await asyncio.sleep(jittered_delay)
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

    async def _poll_for_jobs(self, count: int) -> int:
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

                    reservation: StdioReservation | None = None
                    try:
                        job = await self.job_store.get_job_definition(job_id)
                        if not job:
                            logger.warning(
                                f"Worker {self.worker_id} job definition {job_id} not found during polling for queue {queue_name}."
                            )
                            continue

                        executor_name, handler_name = self._split_executor_name(
                            job.function_name
                        )
                        executor: Executor | None = None
                        if handler_name:
                            executor = self._resolve_executor(executor_name, queue_name)
                            if isinstance(executor, StdioExecutor):
                                reservation = await executor.try_reserve()
                                if reservation is None:
                                    continue

                        # Try to acquire lock and remove from queue first (without semaphore)
                        job_acquired = await self._try_lock_job(job, queue_name)
                        if job_acquired:
                            # Only acquire semaphore after successfully getting the job
                            await self._semaphore.acquire()
                            try:
                                # Process the job (we already have the lock and removed from queue)
                                # The semaphore will be released when the job task completes
                                await self._process_acquired_job(
                                    job_acquired,
                                    queue_name,
                                    reservation=reservation,
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
                                if reservation is not None:
                                    await reservation.release()
                        else:
                            if reservation is not None:
                                await reservation.release()
                        # If job_acquired is None, another worker got it - continue to next job
                    except Exception as e_try:
                        # Catch errors during the job acquisition itself
                        logger.error(
                            f"Worker {self.worker_id} exception trying to acquire job {job_id}: {e_try}",
                            exc_info=True,
                        )
                        if reservation is not None:
                            await reservation.release()

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
        return await self._try_lock_job(job, queue_name)

    async def _try_lock_job(self, job: Job, queue_name: str) -> Optional[Job]:
        """Attempts to atomically lock and remove a job from the queue.

        Args:
            job: The Job object to attempt acquiring.
            queue_name: The name of the queue the job ID was retrieved from.

        Returns:
            The Job object if successfully acquired, None otherwise.
        """
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

    async def _process_acquired_job(
        self,
        job: Job,
        queue_name: str,
        *,
        reservation: StdioReservation | None = None,
    ) -> None:
        """Processes a job that has already been acquired (locked and removed from queue).

        Note: This method assumes the worker has already acquired the concurrency semaphore.
        The semaphore will be released when the job task completes via _task_cleanup.

        Args:
            job: The Job object that was successfully acquired.
            queue_name: The name of the queue the job was retrieved from.
        """
        try:
            start_time = datetime.now(timezone.utc)
            await self.job_store.mark_job_started(job.id, self.worker_id, start_time)
            logger.debug(
                f"Worker {self.worker_id} marked job {job.id} ACTIVE at {start_time.isoformat()}"
            )

            # Create and track the execution task
            # The semaphore will be released when this task completes
            loop = self._loop
            assert loop is not None
            task = loop.create_task(
                self._execute_job(job, queue_name, reservation=reservation)
            )
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
            if reservation is not None:
                await reservation.release()
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
            await self._process_acquired_job(job_acquired, queue_name, reservation=None)
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

    def _build_execution_request(
        self,
        job: Job,
        queue_name: str,
        attempt: int,
        timeout_seconds: int,
        function_name: str,
    ) -> ExecutionRequest:
        deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)
        context = ExecutionContext(
            job_id=job.id,
            attempt=attempt,
            enqueue_time=job.enqueue_time,
            queue_name=queue_name,
            deadline=deadline,
            trace_context=job.trace_context,
            worker_id=self.worker_id,
        )
        return ExecutionRequest(
            job_id=job.id,
            function_name=function_name,
            args=job.job_args,
            kwargs=job.job_kwargs,
            context=context,
        )

    def _split_executor_name(self, function_name: str) -> tuple[str | None, str]:
        if "#" not in function_name:
            return None, function_name
        executor_name, handler_name = function_name.split("#", 1)
        return executor_name or None, handler_name

    def _resolve_executor(
        self, executor_name: str | None, queue_name: str
    ) -> Executor | None:
        if executor_name is None:
            executor_name = self.executor_routes.get(
                queue_name, self.default_executor_name
            )
        if executor_name == self.default_executor_name:
            return self.executor
        return self.executors.get(executor_name)

    async def _execute_job(
        self,
        job: Job,
        queue_name: str,
        *,
        reservation: StdioReservation | None = None,
    ) -> None:
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
        attempt = job.current_retries + 1
        telemetry = get_telemetry()

        span_cm = telemetry.job_span(
            job=job,
            worker_id=self.worker_id,
            queue_name=queue_name,
            attempt=attempt,
            timeout_seconds=float(actual_job_timeout),
        )

        try:
            with span_cm as span:
                try:
                    executor_name, handler_name = self._split_executor_name(
                        job.function_name
                    )
                    if not handler_name:
                        error_message = (
                            "Handler name is missing for job function "
                            f"'{job.function_name}'."
                        )
                        logger.error(error_message)
                        if reservation is not None:
                            await reservation.release()
                            reservation = None
                        await self._handle_fatal_job_error(
                            job, queue_name, error_message
                        )
                        span.dlq(
                            duration_seconds=time.monotonic() - start_time,
                            reason=error_message,
                            error=ValueError(error_message),
                        )
                        return

                    executor = self._resolve_executor(executor_name, queue_name)
                    if executor is None:
                        resolved_name = (
                            executor_name
                            or self.executor_routes.get(queue_name)
                            or self.default_executor_name
                        )
                        error_message = f"No executor configured for '{resolved_name}'."
                        logger.error(error_message)
                        if reservation is not None:
                            await reservation.release()
                            reservation = None
                        await self._handle_fatal_job_error(
                            job, queue_name, error_message
                        )
                        span.dlq(
                            duration_seconds=time.monotonic() - start_time,
                            reason=error_message,
                            error=ValueError(error_message),
                        )
                        return

                    request = self._build_execution_request(
                        job,
                        queue_name,
                        attempt,
                        actual_job_timeout,
                        handler_name,
                    )
                    try:
                        if reservation is not None and isinstance(
                            executor, StdioExecutor
                        ):
                            execute_coro = executor.execute_with_reservation(
                                reservation, request
                            )
                            reservation = None
                        else:
                            if reservation is not None:
                                await reservation.release()
                                reservation = None
                            execute_coro = executor.execute(request)
                        outcome = await asyncio.wait_for(
                            execute_coro,
                            timeout=float(actual_job_timeout),
                        )
                    except asyncio.TimeoutError:
                        error_msg = f"Job timed out after {actual_job_timeout}s."
                        await self._handle_job_timeout(job, queue_name, error_msg)
                        span.timeout(
                            duration_seconds=time.monotonic() - start_time,
                            timeout_seconds=float(actual_job_timeout),
                            error_message=error_msg,
                        )
                        return

                    duration = time.monotonic() - start_time
                    if outcome.status == "success":
                        await self._handle_job_success(job, outcome.result)
                        span.success(duration_seconds=duration)
                        logger.info(
                            f"Job {job.id} completed successfully in {duration:.2f}s."
                        )
                    elif outcome.status == "retry":
                        retry_exc = RetryJob(
                            outcome.error_message or "Job requested retry",
                            defer_seconds=outcome.retry_after_seconds,
                        )
                        anticipated_retry_count = job.current_retries + 1
                        delay_seconds = outcome.retry_after_seconds
                        if (
                            delay_seconds is None
                            and anticipated_retry_count < job.max_retries
                        ):
                            temp_job_for_backoff = Job(
                                id=job.id,
                                function_name=job.function_name,
                                current_retries=anticipated_retry_count,
                                max_retries=job.max_retries,
                            )
                            delay_seconds = (
                                self._calculate_backoff_ms(temp_job_for_backoff)
                                / 1000.0
                            )
                        await self._process_retry_job(job, retry_exc, queue_name)
                        if anticipated_retry_count >= job.max_retries:
                            span.dlq(
                                duration_seconds=duration,
                                reason="max_retries",
                                error=retry_exc,
                            )
                        else:
                            span.retry(
                                duration_seconds=duration,
                                delay_seconds=delay_seconds,
                                reason=str(retry_exc) or None,
                            )
                    elif outcome.status == "timeout":
                        error_msg = (
                            outcome.error_message
                            or f"Job timed out after {actual_job_timeout}s."
                        )
                        await self._handle_job_timeout(job, queue_name, error_msg)
                        span.timeout(
                            duration_seconds=duration,
                            timeout_seconds=float(actual_job_timeout),
                            error_message=error_msg,
                        )
                    else:
                        error_message = outcome.error_message or "Job failed"
                        error_exc = Exception(error_message)
                        if outcome.error_type == "handler_not_found":
                            logger.error(
                                "Job %s fatal error: %s. Moving to DLQ.",
                                job.id,
                                error_message,
                            )
                            await self._handle_fatal_job_error(
                                job, queue_name, error_message
                            )
                            span.dlq(
                                duration_seconds=duration,
                                reason=error_message,
                                error=error_exc,
                            )
                            return
                        anticipated_retry_count = job.current_retries + 1
                        delay_seconds = None
                        if anticipated_retry_count < job.max_retries:
                            delay_seconds = (
                                self._calculate_backoff_ms(
                                    Job(
                                        id=job.id,
                                        function_name=job.function_name,
                                        current_retries=anticipated_retry_count,
                                        max_retries=job.max_retries,
                                    )
                                )
                                / 1000.0
                            )
                        await self._process_other_failure(job, error_exc, queue_name)
                        if anticipated_retry_count >= job.max_retries:
                            span.dlq(
                                duration_seconds=duration,
                                reason="max_retries",
                                error=error_exc,
                            )
                        else:
                            span.retry(
                                duration_seconds=duration,
                                delay_seconds=delay_seconds,
                                reason=error_message or None,
                            )

                except HandlerNotFound as hnfe:
                    logger.error(f"Job {job.id} fatal error: {hnfe}. Moving to DLQ.")
                    await self._handle_fatal_job_error(job, queue_name, str(hnfe))
                    span.dlq(
                        duration_seconds=time.monotonic() - start_time,
                        reason=str(hnfe),
                        error=hnfe,
                    )
                except asyncio.CancelledError:
                    # Catches cancellation of this _execute_job task (e.g., worker shutdown)
                    logger.warning(
                        f"Job {job.id} execution was cancelled (likely worker shutdown). Handling cancellation."
                    )
                    await self._handle_job_cancellation_on_shutdown(job, queue_name)
                    span.cancelled(
                        duration_seconds=time.monotonic() - start_time,
                        reason="shutdown",
                    )
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
                    span.dlq(
                        duration_seconds=time.monotonic() - start_time,
                        reason="critical_worker_error",
                        error=critical_exc,
                    )
        finally:
            if reservation is not None:
                await reservation.release()
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
            try:
                next_run_time = datetime.fromtimestamp(
                    float(retry_at_score) / 1000.0, tz=timezone.utc
                )
                await self.job_store.update_job_next_scheduled_run_time(
                    job.id, next_run_time
                )
            except Exception:
                pass

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
            try:
                next_run_time = datetime.fromtimestamp(
                    float(retry_at_score) / 1000.0, tz=timezone.utc
                )
                await self.job_store.update_job_next_scheduled_run_time(
                    job.id, next_run_time
                )
            except Exception:
                pass

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
        loop = self._loop
        if loop is None:
            return
        for sig in self.SIGNALS:
            try:
                loop.add_signal_handler(sig, self._request_shutdown)
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
        telemetry = get_telemetry()
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
                telemetry.worker_heartbeat(
                    worker_id=self.worker_id, health_data=health_data
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
                    if unique_key:
                        # For unique cron jobs, skip enqueueing while the unique lock is held
                        # to avoid accumulating deferred duplicates on every cron tick.
                        ttl = await self.job_store.get_lock_ttl(unique_key)
                        if ttl > 0:
                            logger.debug(
                                f"Skipping cron job '{cj.function_name}' due to active unique lock (TTL: {ttl}s)."
                            )
                            continue
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
            job.next_scheduled_run_time = datetime.now(
                timezone.utc
            )  # Re-queue immediately
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
        seen_executors: set[int] = set()
        executors_to_close = [self.executor, *self.executors.values()]
        for executor in executors_to_close:
            executor_id = id(executor)
            if executor_id in seen_executors:
                continue
            seen_executors.add(executor_id)
            try:
                await executor.close()
            except Exception as e_close:
                logger.error(
                    f"[{self.worker_id}] Error closing executor: {e_close}",
                    exc_info=True,
                )
        if self.client:  # Check if client exists before closing
            await self.client.close()
        if self.job_store:
            # Close the Redis connection pool
            await self.job_store.aclose()
        logger.info(f"[{self.worker_id}] RRQ worker closed.")
