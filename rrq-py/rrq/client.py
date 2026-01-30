"""This module defines the RRQClient, used for enqueuing jobs into the RRQ system."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from .job import Job, JobStatus
from .producer_ffi import RustProducer, RustProducerError
from .settings import RRQSettings
from .store import JobStore
from .telemetry import get_telemetry

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
        if job_store:
            self.job_store = job_store
            self._created_store_internally = False
        else:
            self.job_store = JobStore(settings=self.settings)
            self._created_store_internally = True

        fields_set = self.settings.model_fields_set
        producer_config: dict[str, Any] = {"redis_dsn": self.settings.redis_dsn}
        if "default_queue_name" in fields_set:
            producer_config["queue_name"] = self.settings.default_queue_name
        if "default_max_retries" in fields_set:
            producer_config["max_retries"] = self.settings.default_max_retries
        if "default_job_timeout_seconds" in fields_set:
            producer_config["job_timeout_seconds"] = (
                self.settings.default_job_timeout_seconds
            )
        if "default_result_ttl_seconds" in fields_set:
            producer_config["result_ttl_seconds"] = self.settings.default_result_ttl_seconds
        if "default_unique_job_lock_ttl_seconds" in fields_set:
            producer_config["idempotency_ttl_seconds"] = (
                self.settings.default_unique_job_lock_ttl_seconds
            )

        self._producer = RustProducer.from_config(producer_config)

    async def close(self) -> None:
        """Closes the underlying JobStore's Redis connection if it was created internally by this client."""
        self._producer.close()
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
            _unique_key: If provided, ensures idempotent enqueueing with this key.
            _max_retries: Maximum number of retries for this specific job. Overrides `RRQSettings.default_max_retries`.
            _job_timeout_seconds: Timeout (in seconds) for this specific job. Overrides `RRQSettings.default_job_timeout_seconds`.
            _defer_until: A specific datetime (timezone.utc recommended) when the job should become available for processing.
            _defer_by: A timedelta relative to now, specifying when the job should become available.
            _result_ttl_seconds: Time-to-live (in seconds) for the result of this specific job. Overrides `RRQSettings.default_result_ttl_seconds`.
            **kwargs: Keyword arguments to pass to the handler function.

        Returns:
            The created Job object if successfully enqueued, or None if enqueueing was rate limited.

        Raises:
            ValueError: If the job timeout is not positive or the job ID already exists.
        """
        telemetry = get_telemetry()
        queue_name_to_use = _queue_name or self.settings.default_queue_name
        with telemetry.enqueue_span(
            job_id=_job_id or "unknown",
            function_name=function_name,
            queue_name=queue_name_to_use,
        ) as trace_context:
            defer_until = None
            if _defer_until is not None:
                dt = _defer_until
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                defer_until = dt.isoformat()

            defer_by_seconds = None
            if _defer_by is not None:
                defer_by_seconds = max(0.0, _defer_by.total_seconds())

            mode = "unique" if _unique_key else "enqueue"
            options: dict[str, Any] = {
                "queue_name": _queue_name,
                "job_id": _job_id,
                "unique_key": _unique_key,
                "unique_ttl_seconds": None,
                "max_retries": _max_retries,
                "job_timeout_seconds": _job_timeout_seconds,
                "result_ttl_seconds": _result_ttl_seconds,
                "trace_context": trace_context,
                "defer_until": defer_until,
                "defer_by_seconds": defer_by_seconds,
            }

            request = {
                "mode": mode,
                "function_name": function_name,
                "args": list(args),
                "kwargs": kwargs,
                "options": options,
            }

            try:
                response = await asyncio.to_thread(self._producer.enqueue, request)
            except RustProducerError as exc:
                raise ValueError(str(exc)) from exc

            job_id = response.get("job_id")
            if not job_id:
                return None

            job = await self.job_store.get_job_definition(job_id)
            if job is not None:
                return job

            return Job(
                id=job_id,
                function_name=function_name,
                job_args=list(args),
                job_kwargs=kwargs,
                enqueue_time=datetime.now(timezone.utc),
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
                queue_name=queue_name_to_use,
                trace_context=trace_context,
            )

    async def enqueue_with_unique_key(
        self,
        function_name: str,
        *args: Any,
        unique_key: str,
        _queue_name: Optional[str] = None,
        _job_id: Optional[str] = None,
        _max_retries: Optional[int] = None,
        _job_timeout_seconds: Optional[int] = None,
        _result_ttl_seconds: Optional[int] = None,
        **kwargs: Any,
    ) -> Optional[Job]:
        return await self.enqueue(
            function_name,
            *args,
            _queue_name=_queue_name,
            _job_id=_job_id,
            _unique_key=unique_key,
            _max_retries=_max_retries,
            _job_timeout_seconds=_job_timeout_seconds,
            _result_ttl_seconds=_result_ttl_seconds,
            **kwargs,
        )

    async def enqueue_with_rate_limit(
        self,
        function_name: str,
        *args: Any,
        rate_limit_key: str,
        rate_limit_seconds: float,
        _queue_name: Optional[str] = None,
        _job_id: Optional[str] = None,
        _max_retries: Optional[int] = None,
        _job_timeout_seconds: Optional[int] = None,
        _result_ttl_seconds: Optional[int] = None,
        **kwargs: Any,
    ) -> Optional[Job]:
        telemetry = get_telemetry()
        queue_name_to_use = _queue_name or self.settings.default_queue_name
        with telemetry.enqueue_span(
            job_id=_job_id or "unknown",
            function_name=function_name,
            queue_name=queue_name_to_use,
        ) as trace_context:
            request = {
                "mode": "rate_limit",
                "function_name": function_name,
                "args": list(args),
                "kwargs": kwargs,
                "options": {
                    "queue_name": _queue_name,
                    "job_id": _job_id,
                    "max_retries": _max_retries,
                    "job_timeout_seconds": _job_timeout_seconds,
                    "result_ttl_seconds": _result_ttl_seconds,
                    "trace_context": trace_context,
                    "rate_limit_key": rate_limit_key,
                    "rate_limit_seconds": rate_limit_seconds,
                },
            }

            try:
                response = await asyncio.to_thread(self._producer.enqueue, request)
            except RustProducerError as exc:
                raise ValueError(str(exc)) from exc

            job_id = response.get("job_id")
            if not job_id:
                return None

            job = await self.job_store.get_job_definition(job_id)
            if job is not None:
                return job

            return Job(
                id=job_id,
                function_name=function_name,
                job_args=list(args),
                job_kwargs=kwargs,
                enqueue_time=datetime.now(timezone.utc),
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
                queue_name=queue_name_to_use,
                trace_context=trace_context,
            )

    async def enqueue_with_debounce(
        self,
        function_name: str,
        *args: Any,
        debounce_key: str,
        debounce_seconds: float,
        _queue_name: Optional[str] = None,
        _job_id: Optional[str] = None,
        _max_retries: Optional[int] = None,
        _job_timeout_seconds: Optional[int] = None,
        _result_ttl_seconds: Optional[int] = None,
        **kwargs: Any,
    ) -> Job:
        telemetry = get_telemetry()
        queue_name_to_use = _queue_name or self.settings.default_queue_name
        with telemetry.enqueue_span(
            job_id=_job_id or "unknown",
            function_name=function_name,
            queue_name=queue_name_to_use,
        ) as trace_context:
            request = {
                "mode": "debounce",
                "function_name": function_name,
                "args": list(args),
                "kwargs": kwargs,
                "options": {
                    "queue_name": _queue_name,
                    "job_id": _job_id,
                    "max_retries": _max_retries,
                    "job_timeout_seconds": _job_timeout_seconds,
                    "result_ttl_seconds": _result_ttl_seconds,
                    "trace_context": trace_context,
                    "debounce_key": debounce_key,
                    "debounce_seconds": debounce_seconds,
                },
            }

            try:
                response = await asyncio.to_thread(self._producer.enqueue, request)
            except RustProducerError as exc:
                raise ValueError(str(exc)) from exc

            job_id = response.get("job_id")
            if not job_id:
                raise ValueError("Debounced enqueue did not return a job id")

            job = await self.job_store.get_job_definition(job_id)
            if job is not None:
                return job

            return Job(
                id=job_id,
                function_name=function_name,
                job_args=list(args),
                job_kwargs=kwargs,
                enqueue_time=datetime.now(timezone.utc),
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
                queue_name=queue_name_to_use,
                trace_context=trace_context,
            )

    async def enqueue_deferred(
        self,
        function_name: str,
        *args: Any,
        delay: timedelta,
        _queue_name: Optional[str] = None,
        _job_id: Optional[str] = None,
        _max_retries: Optional[int] = None,
        _job_timeout_seconds: Optional[int] = None,
        _result_ttl_seconds: Optional[int] = None,
        **kwargs: Any,
    ) -> Optional[Job]:
        return await self.enqueue(
            function_name,
            *args,
            _queue_name=_queue_name,
            _job_id=_job_id,
            _defer_by=delay,
            _max_retries=_max_retries,
            _job_timeout_seconds=_job_timeout_seconds,
            _result_ttl_seconds=_result_ttl_seconds,
            **kwargs,
        )
