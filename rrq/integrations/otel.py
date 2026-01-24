"""OpenTelemetry telemetry integration for RRQ.

This integration is optional and requires OpenTelemetry packages to be installed
and configured by the application (exporters, tracer provider, etc.).
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any, Optional

from ..job import Job
from ..telemetry import ExecutorSpan, EnqueueSpan, JobSpan, Telemetry, configure


def enable(*, service_name: str = "rrq") -> None:
    """Enable OpenTelemetry tracing for RRQ in the current process."""
    configure(OtelTelemetry(service_name=service_name))


class _OtelEnqueueSpan(EnqueueSpan):
    def __init__(
        self,
        *,
        tracer: Any,
        service_name: str,
        job_id: str,
        function_name: str,
        queue_name: str,
    ) -> None:
        self._tracer = tracer
        self._service_name = service_name
        self._job_id = job_id
        self._function_name = function_name
        self._queue_name = queue_name
        self._span_cm: Optional[AbstractContextManager[Any]] = None
        self._span = None

    def __enter__(self) -> Optional[dict[str, str]]:
        from opentelemetry import propagate  # type: ignore[import-not-found]
        from opentelemetry.trace import SpanKind  # type: ignore[import-not-found]

        self._span_cm = self._tracer.start_as_current_span(
            "rrq.enqueue", kind=SpanKind.PRODUCER
        )
        self._span = self._span_cm.__enter__()
        _otel_set_common_attributes(
            self._span,
            job_id=self._job_id,
            function_name=self._function_name,
            queue_name=self._queue_name,
            service_name=self._service_name,
            kind="producer",
        )

        carrier: dict[str, str] = {}
        try:
            propagate.inject(carrier)
        except Exception:
            carrier = {}
        return carrier or None

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


class _OtelJobSpan(JobSpan):
    def __init__(
        self,
        *,
        tracer: Any,
        service_name: str,
        job: Job,
        worker_id: str,
        queue_name: str,
        attempt: int,
        timeout_seconds: float,
    ) -> None:
        self._tracer = tracer
        self._service_name = service_name
        self._job = job
        self._worker_id = worker_id
        self._queue_name = queue_name
        self._attempt = attempt
        self._timeout_seconds = timeout_seconds
        self._span_cm: Optional[AbstractContextManager[Any]] = None
        self._span = None

    def __enter__(self) -> "_OtelJobSpan":
        from opentelemetry import propagate  # type: ignore[import-not-found]
        from opentelemetry.trace import SpanKind  # type: ignore[import-not-found]

        context = None
        if self._job.trace_context:
            try:
                context = propagate.extract(dict(self._job.trace_context))
            except Exception:
                context = None

        if context is not None:
            self._span_cm = self._tracer.start_as_current_span(
                "rrq.job", context=context, kind=SpanKind.CONSUMER
            )
        else:
            self._span_cm = self._tracer.start_as_current_span(
                "rrq.job", kind=SpanKind.CONSUMER
            )
        self._span = self._span_cm.__enter__()

        _otel_set_common_attributes(
            self._span,
            job_id=self._job.id,
            function_name=self._job.function_name,
            queue_name=self._queue_name,
            service_name=self._service_name,
            kind="consumer",
        )
        try:
            self._span.set_attribute("rrq.worker_id", self._worker_id)
            self._span.set_attribute("rrq.attempt", self._attempt)
            self._span.set_attribute(
                "rrq.timeout_seconds", float(self._timeout_seconds)
            )
            self._span.set_attribute(
                "rrq.queue_delay_ms", _calculate_queue_delay_ms(self._job)
            )
        except Exception:
            pass

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
        delay_seconds: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> None:
        _otel_set_outcome(
            self._span,
            "retry",
            duration_seconds=duration_seconds,
            delay_seconds=delay_seconds,
            reason=reason,
        )

    def dlq(
        self,
        *,
        duration_seconds: float,
        reason: Optional[str] = None,
        error: Optional[BaseException] = None,
    ) -> None:
        if self._span is not None and error is not None:
            _otel_record_exception(self._span, error)
        _otel_set_outcome(
            self._span, "dlq", duration_seconds=duration_seconds, reason=reason
        )

    def timeout(
        self,
        *,
        duration_seconds: float,
        timeout_seconds: float,
        error_message: Optional[str] = None,
    ) -> None:
        if self._span is not None:
            try:
                self._span.set_attribute("rrq.timeout_seconds", float(timeout_seconds))
                if error_message:
                    self._span.set_attribute("rrq.error_message", error_message)
            except Exception:
                pass
        _otel_set_outcome(self._span, "timeout", duration_seconds=duration_seconds)

    def cancelled(
        self, *, duration_seconds: float, reason: Optional[str] = None
    ) -> None:
        _otel_set_outcome(
            self._span, "cancelled", duration_seconds=duration_seconds, reason=reason
        )

    def close(self) -> None:
        return


class _OtelExecutorSpan(ExecutorSpan):
    def __init__(
        self,
        *,
        tracer: Any,
        service_name: str,
        job_id: str,
        function_name: str,
        queue_name: str,
        attempt: int,
        trace_context: Optional[dict[str, str]],
        worker_id: Optional[str],
    ) -> None:
        self._tracer = tracer
        self._service_name = service_name
        self._job_id = job_id
        self._function_name = function_name
        self._queue_name = queue_name
        self._attempt = attempt
        self._trace_context = trace_context
        self._worker_id = worker_id
        self._span_cm: Optional[AbstractContextManager[Any]] = None
        self._span = None

    def __enter__(self) -> "_OtelExecutorSpan":
        from opentelemetry import propagate  # type: ignore[import-not-found]
        from opentelemetry.trace import SpanKind  # type: ignore[import-not-found]

        context = None
        if self._trace_context:
            try:
                context = propagate.extract(dict(self._trace_context))
            except Exception:
                context = None

        if context is not None:
            self._span_cm = self._tracer.start_as_current_span(
                "rrq.executor", context=context, kind=SpanKind.INTERNAL
            )
        else:
            self._span_cm = self._tracer.start_as_current_span(
                "rrq.executor", kind=SpanKind.INTERNAL
            )
        self._span = self._span_cm.__enter__()

        _otel_set_common_attributes(
            self._span,
            job_id=self._job_id,
            function_name=self._function_name,
            queue_name=self._queue_name,
            service_name=self._service_name,
            kind="internal",
        )
        try:
            if self._worker_id:
                self._span.set_attribute("rrq.worker_id", self._worker_id)
            self._span.set_attribute("rrq.attempt", self._attempt)
        except Exception:
            pass
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
        delay_seconds: Optional[float] = None,
        reason: Optional[str] = None,
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
        timeout_seconds: Optional[float] = None,
        error_message: Optional[str] = None,
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

    def cancelled(
        self, *, duration_seconds: float, reason: Optional[str] = None
    ) -> None:
        _otel_set_outcome(
            self._span, "cancelled", duration_seconds=duration_seconds, reason=reason
        )

    def close(self) -> None:
        return


class OtelTelemetry(Telemetry):
    """OpenTelemetry-backed RRQ telemetry (traces + propagation)."""

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

    def enqueue_span(
        self, *, job_id: str, function_name: str, queue_name: str
    ) -> EnqueueSpan:
        return _OtelEnqueueSpan(
            tracer=self._tracer,
            service_name=self._service_name,
            job_id=job_id,
            function_name=function_name,
            queue_name=queue_name,
        )

    def job_span(
        self,
        *,
        job: Job,
        worker_id: str,
        queue_name: str,
        attempt: int,
        timeout_seconds: float,
    ) -> JobSpan:
        return _OtelJobSpan(
            tracer=self._tracer,
            service_name=self._service_name,
            job=job,
            worker_id=worker_id,
            queue_name=queue_name,
            attempt=attempt,
            timeout_seconds=timeout_seconds,
        )

    def executor_span(
        self,
        *,
        job_id: str,
        function_name: str,
        queue_name: str,
        attempt: int,
        trace_context: Optional[dict[str, str]],
        worker_id: Optional[str],
    ) -> ExecutorSpan:
        return _OtelExecutorSpan(
            tracer=self._tracer,
            service_name=self._service_name,
            job_id=job_id,
            function_name=function_name,
            queue_name=queue_name,
            attempt=attempt,
            trace_context=trace_context,
            worker_id=worker_id,
        )


def _otel_set_common_attributes(
    span: Any,
    *,
    job_id: str,
    function_name: str,
    queue_name: str,
    service_name: str,
    kind: str,
) -> None:
    if span is None:
        return
    try:
        span.set_attribute("service.name", service_name)
        span.set_attribute("rrq.job_id", job_id)
        span.set_attribute("rrq.function", function_name)
        span.set_attribute("rrq.queue", queue_name)
        span.set_attribute("span.kind", kind)
        span.set_attribute("messaging.system", "redis")
        span.set_attribute("messaging.destination.name", queue_name)
        span.set_attribute("messaging.destination_kind", "queue")
    except Exception:
        pass


def _otel_set_outcome(
    span: Any,
    outcome: str,
    *,
    duration_seconds: float,
    delay_seconds: Optional[float] = None,
    reason: Optional[str] = None,
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


def _calculate_queue_delay_ms(job: Job) -> float:
    scheduled_time = job.next_scheduled_run_time or job.enqueue_time
    dt = scheduled_time
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    elif dt.tzinfo != timezone.utc:
        dt = dt.astimezone(timezone.utc)
    delay_ms = (datetime.now(timezone.utc) - dt).total_seconds() * 1000.0
    return max(0.0, delay_ms)
