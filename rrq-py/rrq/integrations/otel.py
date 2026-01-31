"""OpenTelemetry runner integration for RRQ."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Optional

from ..runner import ExecutionRequest
from ..telemetry import RunnerSpan, Telemetry, configure


def enable(*, service_name: str = "rrq") -> None:
    """Enable OpenTelemetry tracing for RRQ runner spans in the current process."""
    configure(OtelTelemetry(service_name=service_name))


class _OtelRunnerSpan(RunnerSpan):
    def __init__(
        self,
        *,
        tracer: Any,
        service_name: str,
        request: ExecutionRequest,
    ) -> None:
        self._tracer = tracer
        self._service_name = service_name
        self._request = request
        self._span_cm: Optional[AbstractContextManager[Any]] = None
        self._span = None

    def __enter__(self) -> "_OtelRunnerSpan":
        from opentelemetry import propagate  # type: ignore[import-not-found]
        from opentelemetry.trace import SpanKind  # type: ignore[import-not-found]

        context = None
        if self._request.context.trace_context:
            try:
                context = propagate.extract(dict(self._request.context.trace_context))
            except Exception:
                context = None

        if context is not None:
            self._span_cm = self._tracer.start_as_current_span(
                "rrq.runner", context=context, kind=SpanKind.CONSUMER
            )
        else:
            self._span_cm = self._tracer.start_as_current_span(
                "rrq.runner", kind=SpanKind.CONSUMER
            )
        self._span = self._span_cm.__enter__()

        _otel_set_common_attributes(
            self._span,
            service_name=self._service_name,
            request=self._request,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        if self._span is not None and exc is not None:
            _otel_record_exception(self._span, exc)
        try:
            if self._span_cm is not None:
                return bool(self._span_cm.__exit__(exc_type, exc, tb))
            return False
        finally:
            self._span_cm = None
            self._span = None

    def success(self, *, duration_seconds: float) -> None:
        _otel_set_outcome(self._span, "success", duration_seconds=duration_seconds)

    def retry(
        self,
        *,
        duration_seconds: float,
        delay_seconds: float | None = None,
        reason: str | None = None,
    ) -> None:
        _otel_set_outcome(
            self._span,
            "retry",
            duration_seconds=duration_seconds,
            delay_seconds=delay_seconds,
            reason=reason,
        )

    def timeout(
        self,
        *,
        duration_seconds: float,
        timeout_seconds: float | None = None,
        error_message: str | None = None,
    ) -> None:
        if self._span is not None:
            try:
                if timeout_seconds is not None:
                    self._span.set_attribute(
                        "rrq.timeout_seconds", float(timeout_seconds)
                    )
                if error_message:
                    self._span.set_attribute("rrq.error_message", error_message)
            except Exception:
                pass
        _otel_set_outcome(self._span, "timeout", duration_seconds=duration_seconds)

    def error(self, *, duration_seconds: float, error: BaseException) -> None:
        if self._span is not None:
            _otel_record_exception(self._span, error)
        _otel_set_outcome(self._span, "error", duration_seconds=duration_seconds)

    def cancelled(self, *, duration_seconds: float, reason: str | None = None) -> None:
        _otel_set_outcome(
            self._span, "cancelled", duration_seconds=duration_seconds, reason=reason
        )

    def close(self) -> None:
        return


class OtelTelemetry(Telemetry):
    """OpenTelemetry-backed RRQ runner telemetry."""

    enabled: bool = True

    def __init__(self, *, service_name: str) -> None:
        try:
            from opentelemetry import trace  # type: ignore[import-not-found]
        except Exception as e:
            raise RuntimeError(
                "OpenTelemetry is not installed; install opentelemetry-api and your exporter."
            ) from e
        self._service_name = service_name
        self._tracer = trace.get_tracer("rrq")

    def runner_span(self, request: ExecutionRequest) -> RunnerSpan:
        return _OtelRunnerSpan(
            tracer=self._tracer,
            service_name=self._service_name,
            request=request,
        )


def _otel_set_common_attributes(
    span: Any,
    *,
    service_name: str,
    request: ExecutionRequest,
) -> None:
    if span is None:
        return
    try:
        span.set_attribute("service.name", service_name)
        span.set_attribute("rrq.job_id", request.job_id)
        span.set_attribute("rrq.function", request.function_name)
        span.set_attribute("rrq.queue", request.context.queue_name)
        span.set_attribute("rrq.attempt", request.context.attempt)
        span.set_attribute("span.kind", "consumer")
        span.set_attribute("messaging.system", "redis")
        span.set_attribute("messaging.destination.name", request.context.queue_name)
        span.set_attribute("messaging.destination_kind", "queue")
        span.set_attribute("messaging.operation", "process")
        if request.context.worker_id:
            span.set_attribute("rrq.worker_id", request.context.worker_id)
        if request.context.deadline:
            span.set_attribute("rrq.deadline", request.context.deadline.isoformat())
    except Exception:
        pass


def _otel_set_outcome(
    span: Any,
    outcome: str,
    *,
    duration_seconds: float,
    delay_seconds: float | None = None,
    reason: str | None = None,
) -> None:
    if span is None:
        return
    try:
        span.set_attribute("rrq.outcome", outcome)
        span.set_attribute("rrq.duration_ms", float(duration_seconds) * 1000.0)
        if delay_seconds is not None:
            span.set_attribute("rrq.retry_delay_ms", float(delay_seconds) * 1000.0)
        if reason:
            span.set_attribute("rrq.reason", reason)
    except Exception:
        pass


def _otel_record_exception(span: Any, error: BaseException) -> None:
    if span is None:
        return
    try:
        span.record_exception(error)
    except Exception:
        pass

    try:
        from opentelemetry.trace import Status, StatusCode  # type: ignore[import-not-found]

        span.set_status(Status(StatusCode.ERROR))
    except Exception:
        pass
