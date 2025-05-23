"""This module defines the RRQClient, used for enqueuing jobs into the RRQ system."""

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Optional

from .job import Job, JobStatus
from .settings import RRQSettings
from .store import JobStore

logger = logging.getLogger(__name__)


class RRQClient:
    """Client interface for interacting with the RRQ (Reliable Redis Queue) system.

    Provides methods primarily for enqueuing jobs.
    """

    def __init__(self, settings: RRQSettings, job_store: Optional[JobStore] = None):
        """Initializes the RRQClient.

        Args:
            settings: The RRQSettings instance containing configuration.
            job_store: Optional JobStore instance. If not provided, a new one
                       will be created based on the settings. This allows sharing
                       a JobStore instance across multiple components.
        """
        self.settings = settings
        # If job_store is not provided, create one. This allows for flexibility:
        # - External management of JobStore (e.g., passed from an application context)
        # - Client creates its own if used standalone.
        if job_store:
            self.job_store = job_store
            self._created_store_internally = False
        else:
            self.job_store = JobStore(settings=self.settings)
            self._created_store_internally = True

    async def close(self) -> None:
        """Closes the underlying JobStore's Redis connection if it was created internally by this client."""
        if self._created_store_internally:
            await self.job_store.aclose()

    async def enqueue(
        self,
        function_name: str,
        *args: Any,
        _queue_name: Optional[str] = None,
        _job_id: Optional[str] = None,
        _unique_key: Optional[str] = None,
        _max_retries: Optional[int] = None,
        _job_timeout_seconds: Optional[int] = None,
        _defer_until: Optional[datetime] = None,
        _defer_by: Optional[timedelta] = None,
        _result_ttl_seconds: Optional[int] = None,
        **kwargs: Any,
    ) -> Optional[Job]:
        """Enqueues a job to be processed by RRQ workers.

        Args:
            function_name: The registered name of the handler function to execute.
            *args: Positional arguments to pass to the handler function.
            _queue_name: Specific queue to enqueue the job to. Defaults to `RRQSettings.default_queue_name`.
            _job_id: User-provided job ID for idempotency or tracking. If None, a UUID is generated.
            _unique_key: If provided, ensures that only one job with this key is active or recently completed.
                         Uses a Redis lock with `default_unique_job_lock_ttl_seconds`.
            _max_retries: Maximum number of retries for this specific job. Overrides `RRQSettings.default_max_retries`.
            _job_timeout_seconds: Timeout (in seconds) for this specific job. Overrides `RRQSettings.default_job_timeout_seconds`.
            _defer_until: A specific datetime (UTC recommended) when the job should become available for processing.
            _defer_by: A timedelta relative to now, specifying when the job should become available.
            _result_ttl_seconds: Time-to-live (in seconds) for the result of this specific job. Overrides `RRQSettings.default_result_ttl_seconds`.
            **kwargs: Keyword arguments to pass to the handler function.

        Returns:
            The created Job object if successfully enqueued, or None if enqueueing was denied
            (e.g., due to a unique key conflict).
        """
        # Determine job ID and enqueue timestamp
        job_id_to_use = _job_id or str(uuid.uuid4())
        enqueue_time_utc = datetime.now(UTC)

        # Compute unique lock TTL: cover deferral window if any
        lock_ttl_seconds = self.settings.default_unique_job_lock_ttl_seconds
        if _defer_by is not None:
            # Defer relative to now
            defer_secs = max(0, int(_defer_by.total_seconds()))
            lock_ttl_seconds = max(lock_ttl_seconds, defer_secs + 1)
        elif _defer_until is not None:
            # Defer until specific datetime
            dt = _defer_until
            # Normalize to UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            elif dt.tzinfo != UTC:
                dt = dt.astimezone(UTC)
            diff = (dt - enqueue_time_utc).total_seconds()
            if diff > 0:
                lock_ttl_seconds = max(lock_ttl_seconds, int(diff) + 1)

        unique_acquired = False
        # Acquire unique lock if requested, with TTL covering defer window
        if _unique_key:
            lock_acquired = await self.job_store.acquire_unique_job_lock(
                unique_key=_unique_key,
                job_id=job_id_to_use,
                lock_ttl_seconds=lock_ttl_seconds,
            )
            if not lock_acquired:
                logger.info(
                    f"Job with unique key '{_unique_key}' already active or recently run. Enqueue denied."
                )
                return None
            unique_acquired = True

        queue_name_to_use = _queue_name or self.settings.default_queue_name

        # Create the Job instance with all provided details and defaults
        job = Job(
            id=job_id_to_use,
            function_name=function_name,
            job_args=list(args),
            job_kwargs=kwargs,
            enqueue_time=enqueue_time_utc,
            status=JobStatus.PENDING,
            current_retries=0,
            max_retries=(
                _max_retries
                if _max_retries is not None
                else self.settings.default_max_retries
            ),
            job_timeout_seconds=(
                _job_timeout_seconds
                if _job_timeout_seconds is not None
                else self.settings.default_job_timeout_seconds
            ),
            result_ttl_seconds=(
                _result_ttl_seconds
                if _result_ttl_seconds is not None
                else self.settings.default_result_ttl_seconds
            ),
            job_unique_key=_unique_key,
            queue_name=queue_name_to_use,  # Store the target queue name
        )

        # Determine the score for the sorted set (queue)
        # Score is a millisecond timestamp for when the job should be processed.
        score_dt = enqueue_time_utc  # Default to immediate processing
        if _defer_until:
            score_dt = _defer_until
        elif _defer_by:
            score_dt = enqueue_time_utc + _defer_by

        # Ensure score_dt is timezone-aware (UTC) if it's naive from user input
        if score_dt.tzinfo is None:
            score_dt = score_dt.replace(tzinfo=UTC)
        elif score_dt.tzinfo != UTC:
            # Convert to UTC if it's aware but not UTC
            score_dt = score_dt.astimezone(UTC)

        score_timestamp_ms = int(score_dt.timestamp() * 1000)
        # Record when the job is next scheduled to run (for deferred execution)
        job.next_scheduled_run_time = score_dt

        # Save the full job definition and add to queue (ensure unique lock is released on error)
        try:
            await self.job_store.save_job_definition(job)
            await self.job_store.add_job_to_queue(
                queue_name_to_use,
                job.id,
                float(score_timestamp_ms),
            )
        except Exception:
            if unique_acquired:
                await self.job_store.release_unique_job_lock(_unique_key)
            raise

        logger.debug(
            f"Enqueued job {job.id} ('{job.function_name}') to queue '{queue_name_to_use}' with score {score_timestamp_ms}"
        )
        return job
