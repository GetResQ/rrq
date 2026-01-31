"""Pluggable runner telemetry for RRQ (OpenTelemetry supported)."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runner import ExecutionRequest


class RunnerSpan(AbstractContextManager["RunnerSpan"]):
    """Context manager for a runner span."""

    def __enter__(self) -> "RunnerSpan":
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
        delay_seconds: float | None = None,
        reason: str | None = None,
    ) -> None:
        pass

    def timeout(
        self,
        *,
        duration_seconds: float,
        timeout_seconds: float | None = None,
        error_message: str | None = None,
    ) -> None:
        pass

    def error(self, *, duration_seconds: float, error: BaseException) -> None:
        pass

    def cancelled(self, *, duration_seconds: float, reason: str | None = None) -> None:
        pass

    def close(self) -> None:
        pass


class Telemetry:
    """Base telemetry implementation (no-op by default)."""

    enabled: bool = False

    def runner_span(self, request: ExecutionRequest) -> RunnerSpan:
        return _NOOP_RUNNER_SPAN


_NOOP_RUNNER_SPAN = RunnerSpan()
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
