"""Pluggable telemetry for RRQ.

RRQ intentionally keeps telemetry optional: the core queue semantics must work
even when tracing/metrics libraries are missing or misconfigured.

Telemetry is configured per-process via :func:`configure` and used internally by
RRQClient and the Python executor runtime.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Optional

from .job import Job


class EnqueueSpan(AbstractContextManager[Optional[dict[str, str]]]):
    """Context manager for an enqueue span.

    Entering yields an optional propagation carrier dict to store on the Job.
    """

    def __enter__(self) -> Optional[dict[str, str]]:
        return None

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        return False


class JobSpan(AbstractContextManager["JobSpan"]):
    """Context manager for a job execution span."""

    def __enter__(self) -> "JobSpan":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        self.close()
        return False

    def success(self, *, duration_seconds: float) -> None:
        pass

    def retry(
        self,
        *,
        duration_seconds: float,
        delay_seconds: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> None:
        pass

    def dlq(
        self,
        *,
        duration_seconds: float,
        reason: Optional[str] = None,
        error: Optional[BaseException] = None,
    ) -> None:
        pass

    def timeout(
        self,
        *,
        duration_seconds: float,
        timeout_seconds: float,
        error_message: Optional[str] = None,
    ) -> None:
        pass

    def cancelled(
        self, *, duration_seconds: float, reason: Optional[str] = None
    ) -> None:
        pass

    def close(self) -> None:
        pass


class ExecutorSpan(AbstractContextManager["ExecutorSpan"]):
    """Context manager for an executor span."""

    def __enter__(self) -> "ExecutorSpan":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        self.close()
        return False

    def success(self, *, duration_seconds: float) -> None:
        pass

    def retry(
        self,
        *,
        duration_seconds: float,
        delay_seconds: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> None:
        pass

    def timeout(
        self,
        *,
        duration_seconds: float,
        timeout_seconds: Optional[float] = None,
        error_message: Optional[str] = None,
    ) -> None:
        pass

    def error(self, *, duration_seconds: float, error: BaseException) -> None:
        pass

    def cancelled(
        self, *, duration_seconds: float, reason: Optional[str] = None
    ) -> None:
        pass

    def close(self) -> None:
        pass


class Telemetry:
    """Base telemetry implementation (no-op by default)."""

    enabled: bool = False

    def enqueue_span(
        self, *, job_id: str, function_name: str, queue_name: str
    ) -> EnqueueSpan:
        return _NOOP_ENQUEUE_SPAN

    def job_span(
        self,
        *,
        job: Job,
        worker_id: str,
        queue_name: str,
        attempt: int,
        timeout_seconds: float,
    ) -> JobSpan:
        return _NOOP_JOB_SPAN

    def executor_span(
        self,
        *,
        job_id: str,
        function_name: str,
        queue_name: str,
        attempt: int,
        trace_context: Optional[dict[str, str]],
        worker_id: Optional[str],
    ) -> "ExecutorSpan":
        return _NOOP_EXECUTOR_SPAN

    def worker_started(self, *, worker_id: str, queues: list[str]) -> None:
        pass

    def worker_stopped(self, *, worker_id: str) -> None:
        pass

    def worker_heartbeat(self, *, worker_id: str, health_data: dict[str, Any]) -> None:
        pass


_NOOP_ENQUEUE_SPAN = EnqueueSpan()
_NOOP_JOB_SPAN = JobSpan()
_NOOP_EXECUTOR_SPAN = ExecutorSpan()
_telemetry: Telemetry = Telemetry()


def configure(telemetry: Telemetry) -> None:
    """Configure a process-global telemetry backend."""
    global _telemetry
    _telemetry = telemetry


def disable() -> None:
    """Disable RRQ telemetry for the current process."""
    configure(Telemetry())


def get_telemetry() -> Telemetry:
    """Return the configured telemetry backend (defaults to no-op)."""
    return _telemetry
