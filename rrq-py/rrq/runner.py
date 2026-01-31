"""Runner interfaces and Python implementation for RRQ job execution."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field

from .exc import RetryJob
from .registry import Registry
from .telemetry import get_telemetry

logger = logging.getLogger(__name__)


class ExecutionContext(BaseModel):
    """Minimal execution context passed to runtimes."""

    job_id: str
    attempt: int
    enqueue_time: datetime
    queue_name: str
    deadline: datetime | None = None
    trace_context: dict[str, str] | None = None
    worker_id: str | None = None


class ExecutionRequest(BaseModel):
    """Job data sent to an runner."""

    protocol_version: str = "1"
    request_id: str
    job_id: str
    function_name: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    context: ExecutionContext


class ExecutionError(BaseModel):
    """Structured error information from an runner."""

    message: str
    type: str | None = None
    code: str | None = None
    details: dict[str, Any] | None = None


class ExecutionOutcome(BaseModel):
    """Result returned by an runner."""

    job_id: str | None = None
    request_id: str | None = None
    status: Literal["success", "retry", "timeout", "error"]
    result: Any | None = None
    error: ExecutionError | None = None
    retry_after_seconds: float | None = None


class Runner(Protocol):
    async def execute(self, request: ExecutionRequest) -> ExecutionOutcome:
        """Run a job and return an outcome."""

    async def cancel(self, job_id: str, request_id: str | None = None) -> None:
        """Best-effort cancellation for in-flight jobs."""

    async def close(self) -> None:
        """Release runner resources."""


class PythonRunner:
    """Executes Python handlers registered in the Registry."""

    def __init__(
        self,
        *,
        job_registry: Registry,
        worker_id: str | None,
    ) -> None:
        self.job_registry = job_registry
        self.worker_id = worker_id

    async def execute(self, request: ExecutionRequest) -> ExecutionOutcome:
        handler = self.job_registry.get_handler(request.function_name)
        if not handler:
            return ExecutionOutcome(
                job_id=request.job_id,
                request_id=request.request_id,
                status="error",
                error=ExecutionError(
                    message=f"No handler registered for function '{request.function_name}'",
                    type="handler_not_found",
                ),
            )

        if request.context.worker_id is None and self.worker_id is not None:
            request.context.worker_id = self.worker_id

        telemetry = get_telemetry()
        start_time = time.monotonic()
        span_cm = telemetry.runner_span(request)

        with span_cm as span:
            try:
                logger.debug(
                    "Calling handler '%s' for job %s",
                    request.function_name,
                    request.job_id,
                )
                result = await handler(request)
                logger.debug(
                    "Handler for job %s returned successfully.", request.job_id
                )
                if isinstance(result, ExecutionOutcome):
                    span.success(duration_seconds=time.monotonic() - start_time)
                    return result
                span.success(duration_seconds=time.monotonic() - start_time)
                return ExecutionOutcome(
                    job_id=request.job_id,
                    request_id=request.request_id,
                    status="success",
                    result=result,
                )
            except RetryJob as exc:
                logger.info("Job %s requested retry: %s", request.job_id, exc)
                span.retry(
                    duration_seconds=time.monotonic() - start_time,
                    delay_seconds=exc.defer_seconds,
                    reason=str(exc) or None,
                )
                return ExecutionOutcome(
                    job_id=request.job_id,
                    request_id=request.request_id,
                    status="retry",
                    error=ExecutionError(message=str(exc) or "Job requested retry"),
                    retry_after_seconds=exc.defer_seconds,
                )
            except (asyncio.TimeoutError, TimeoutError) as exc:
                error_message = str(exc) or "Job execution timed out."
                logger.warning(
                    "Job %s execution timed out: %s", request.job_id, error_message
                )
                span.timeout(
                    duration_seconds=time.monotonic() - start_time,
                    timeout_seconds=None,
                    error_message=error_message,
                )
                return ExecutionOutcome(
                    job_id=request.job_id,
                    request_id=request.request_id,
                    status="timeout",
                    error=ExecutionError(message=error_message, type="timeout"),
                )
            except Exception as exc:
                logger.error(
                    "Job %s handler '%s' raised unhandled exception:",
                    request.job_id,
                    request.function_name,
                    exc_info=exc,
                )
                span.error(duration_seconds=time.monotonic() - start_time, error=exc)
                return ExecutionOutcome(
                    job_id=request.job_id,
                    request_id=request.request_id,
                    status="error",
                    error=ExecutionError(message=str(exc) or "Unhandled handler error"),
                )

    async def cancel(self, job_id: str, request_id: str | None = None) -> None:
        return None

    async def close(self) -> None:
        return None
