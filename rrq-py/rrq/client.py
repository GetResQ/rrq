"""RRQ client for enqueuing jobs via the Rust producer FFI."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from .job import JobResult, JobStatus
from .producer_ffi import (
    JobResultModel,
    JobStatusResponseModel,
    RustProducer,
    RustProducerError,
)


def _normalize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat()


def _to_job_result(payload: JobResultModel) -> JobResult:
    try:
        status = JobStatus(payload.status)
    except ValueError:
        status = JobStatus.UNKNOWN
    return JobResult(
        status=status,
        result=payload.result,
        last_error=payload.last_error,
    )


class RRQClient:
    """Thin async wrapper around the Rust producer FFI."""

    def __init__(
        self,
        *,
        config: dict[str, Any] | None = None,
        config_path: str | None = None,
    ) -> None:
        if config is not None and config_path is not None:
            raise ValueError("Provide either config or config_path, not both.")
        if config is not None:
            self._producer = RustProducer.from_config(config)
        else:
            self._producer = RustProducer.from_toml(config_path)

    async def close(self) -> None:
        self._producer.close()

    async def enqueue(
        self,
        function_name: str,
        options: dict[str, Any] | None = None,
    ) -> str:
        options = dict(options) if options else {}
        args = list(options.pop("args", []))
        kwargs = dict(options.pop("kwargs", {}))

        defer_until = options.get("defer_until")
        if isinstance(defer_until, datetime):
            options["defer_until"] = _normalize_datetime(defer_until)

        request = {
            "function_name": function_name,
            "args": args,
            "kwargs": kwargs,
            "options": options,
        }

        try:
            response = await asyncio.to_thread(self._producer.enqueue, request)
        except RustProducerError as exc:
            raise ValueError(str(exc)) from exc

        job_id = response.get("job_id")
        if not job_id:
            raise ValueError("Producer did not return a job_id")
        return job_id

    async def enqueue_with_unique_key(
        self,
        function_name: str,
        unique_key: str,
        options: dict[str, Any] | None = None,
    ) -> str:
        merged = dict(options or {})
        merged["unique_key"] = unique_key
        return await self.enqueue(function_name, merged)

    async def enqueue_with_rate_limit(
        self,
        function_name: str,
        options: dict[str, Any],
    ) -> str | None:
        merged = dict(options)
        request = {
            "function_name": function_name,
            "args": list(merged.pop("args", [])),
            "kwargs": dict(merged.pop("kwargs", {})),
            "options": merged,
            "mode": "rate_limit",
        }
        try:
            response = await asyncio.to_thread(self._producer.enqueue, request)
        except RustProducerError as exc:
            raise ValueError(str(exc)) from exc
        return response.get("job_id")

    async def enqueue_with_debounce(
        self,
        function_name: str,
        options: dict[str, Any],
    ) -> str:
        merged = dict(options)
        request = {
            "function_name": function_name,
            "args": list(merged.pop("args", [])),
            "kwargs": dict(merged.pop("kwargs", {})),
            "options": merged,
            "mode": "debounce",
        }
        try:
            response = await asyncio.to_thread(self._producer.enqueue, request)
        except RustProducerError as exc:
            raise ValueError(str(exc)) from exc
        job_id = response.get("job_id")
        if not job_id:
            raise ValueError("Producer did not return a job_id")
        return job_id

    async def enqueue_deferred(
        self,
        function_name: str,
        options: dict[str, Any],
    ) -> str:
        return await self.enqueue(function_name, options)

    async def get_job_status(self, job_id: str) -> JobResult | None:
        try:
            response: JobStatusResponseModel = await asyncio.to_thread(
                self._producer.get_job_status, job_id
            )
        except RustProducerError as exc:
            raise ValueError(str(exc)) from exc

        if not response.found or response.job is None:
            return None
        return _to_job_result(response.job)
