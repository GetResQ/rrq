"""Datadog (ddtrace) telemetry integration for RRQ.

This integration is optional and requires `ddtrace` to be installed in the
application environment.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any, Optional

from ..job import Job
from ..telemetry import ExecutorSpan, EnqueueSpan, JobSpan, Telemetry, configure


def enable(
    *,
    service: str = "rrq",
    env: str | None = None,
    version: str | None = None,
    component: str = "rrq",
) -> None:
    """Enable ddtrace-based tracing for RRQ in the current process.

    This does not call `ddtrace.patch_all()`; it only instruments RRQ spans and
    propagation.
    """
    configure(
        DdtraceTelemetry(
            service=service,
            env=env,
            version=version,
            component=component,
        )
    )


class _DdtraceEnqueueSpan(EnqueueSpan):
    def __init__(
        self,
        *,
        tracer: Any,
        propagator: Any,
        service: str,
        component: str,
        job_id: str,
        function_name: str,
        queue_name: str,
        env: str | None,
        version: str | None,
    ) -> None:
        self._tracer = tracer
        self._propagator = propagator
        self._service = service
        self._component = component
        self._job_id = job_id
        self._function_name = function_name
        self._queue_name = queue_name
        self._env = env
        self._version = version
        self._span = None

    def __enter__(self) -> Optional[dict[str, str]]:
        span = self._tracer.trace(
            "rrq.enqueue",
            service=self._service,
            resource=self._function_name,
            span_type="queue",
        )
        self._span = span.__enter__()
        self._set_common_tags(self._span)

        carrier: dict[str, str] = {}
        if self._propagator is not None:
            carrier = _ddtrace_inject(self._propagator, self._span, carrier)
        return {str(k): str(v) for k, v in carrier.items()} or None

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        if self._span is not None and exc is not None:
            _set_span_error(self._span, exc)
        try:
            return self._span.__exit__(exc_type, exc, tb) if self._span else False
        finally:
            self._span = None

    def _set_common_tags(self, span: Any) -> None:
        try:
            span.set_tag("component", self._component)
            span.set_tag("span.kind", "producer")
            span.set_tag("rrq.job_id", self._job_id)
            span.set_tag("rrq.function", self._function_name)
            span.set_tag("rrq.queue", self._queue_name)
            span.set_tag("messaging.system", "redis")
            span.set_tag("messaging.destination.name", self._queue_name)
            span.set_tag("messaging.destination_kind", "queue")
            span.set_tag("messaging.operation", "publish")
            if self._env:
                span.set_tag("env", self._env)
            if self._version:
                span.set_tag("version", self._version)
        except Exception:
            pass


class _DdtraceJobSpan(JobSpan):
    def __init__(
        self,
        *,
        tracer: Any,
        service: str,
        component: str,
        job: Job,
        worker_id: str,
        queue_name: str,
        attempt: int,
        timeout_seconds: float,
        parent_context: Any,
        env: str | None,
        version: str | None,
    ) -> None:
        self._tracer = tracer
        self._service = service
        self._component = component
        self._job = job
        self._worker_id = worker_id
        self._queue_name = queue_name
        self._attempt = attempt
        self._timeout_seconds = timeout_seconds
        self._parent_context = parent_context
        self._env = env
        self._version = version
        self._span = None

    def __enter__(self) -> "_DdtraceJobSpan":
        span = _trace_with_parent(
            self._tracer,
            name="rrq.job",
            service=self._service,
            resource=self._job.function_name,
            span_type="worker",
            parent_context=self._parent_context,
        )
        self._span = span.__enter__()
        self._set_common_tags(self._span)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        if self._span is not None and exc is not None:
            _set_span_error(self._span, exc)
        try:
            return self._span.__exit__(exc_type, exc, tb) if self._span else False
        finally:
            self._span = None

    def success(self, *, duration_seconds: float) -> None:
        self._set_outcome("success", duration_seconds=duration_seconds)

    def retry(
        self,
        *,
        duration_seconds: float,
        delay_seconds: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> None:
        self._set_outcome(
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
            _set_span_error(self._span, error)
        self._set_outcome("dlq", duration_seconds=duration_seconds, reason=reason)

    def timeout(
        self,
        *,
        duration_seconds: float,
        timeout_seconds: float,
        error_message: Optional[str] = None,
    ) -> None:
        if self._span is not None:
            try:
                self._span.set_tag("error", True)
                if error_message:
                    self._span.set_tag("error.msg", error_message)
            except Exception:
                pass
        self._set_outcome(
            "timeout",
            duration_seconds=duration_seconds,
            timeout_seconds=timeout_seconds,
        )

    def cancelled(
        self, *, duration_seconds: float, reason: Optional[str] = None
    ) -> None:
        self._set_outcome(
            "cancelled",
            duration_seconds=duration_seconds,
            reason=reason,
        )

    def close(self) -> None:
        return

    def _set_common_tags(self, span: Any) -> None:
        try:
            span.set_tag("component", self._component)
            span.set_tag("span.kind", "consumer")
            span.set_tag("rrq.job_id", self._job.id)
            span.set_tag("rrq.function", self._job.function_name)
            span.set_tag("rrq.queue", self._queue_name)
            span.set_tag("rrq.worker_id", self._worker_id)
            span.set_tag("rrq.attempt", self._attempt)
            span.set_tag("rrq.timeout_seconds", self._timeout_seconds)
            span.set_tag("messaging.system", "redis")
            span.set_tag("messaging.destination.name", self._queue_name)
            span.set_tag("messaging.destination_kind", "queue")
            span.set_tag("messaging.operation", "process")
            _set_span_metric(
                span, "rrq.queue_delay_ms", _calculate_queue_delay_ms(self._job)
            )
            if self._env:
                span.set_tag("env", self._env)
            if self._version:
                span.set_tag("version", self._version)
        except Exception:
            pass

    def _set_outcome(
        self,
        outcome: str,
        *,
        duration_seconds: float,
        delay_seconds: Optional[float] = None,
        reason: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        if self._span is None:
            return
        try:
            self._span.set_tag("rrq.outcome", outcome)
            _set_span_metric(
                self._span, "rrq.duration_ms", float(duration_seconds) * 1000.0
            )
            if delay_seconds is not None:
                _set_span_metric(
                    self._span, "rrq.retry_delay_ms", float(delay_seconds) * 1000.0
                )
            if timeout_seconds is not None:
                self._span.set_tag("rrq.timeout_seconds", float(timeout_seconds))
            if reason:
                self._span.set_tag("rrq.reason", reason)
        except Exception:
            pass


class _DdtraceExecutorSpan(ExecutorSpan):
    def __init__(
        self,
        *,
        tracer: Any,
        service: str,
        component: str,
        job_id: str,
        function_name: str,
        queue_name: str,
        attempt: int,
        worker_id: Optional[str],
        parent_context: Any,
        env: str | None,
        version: str | None,
    ) -> None:
        self._tracer = tracer
        self._service = service
        self._component = component
        self._job_id = job_id
        self._function_name = function_name
        self._queue_name = queue_name
        self._attempt = attempt
        self._worker_id = worker_id
        self._parent_context = parent_context
        self._env = env
        self._version = version
        self._span = None

    def __enter__(self) -> "_DdtraceExecutorSpan":
        span = _trace_with_parent(
            self._tracer,
            name="rrq.executor",
            service=self._service,
            resource=self._function_name,
            span_type="worker",
            parent_context=self._parent_context,
        )
        self._span = span.__enter__()
        self._set_common_tags(self._span)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        if self._span is not None and exc is not None:
            _set_span_error(self._span, exc)
        try:
            return self._span.__exit__(exc_type, exc, tb) if self._span else False
        finally:
            self._span = None

    def success(self, *, duration_seconds: float) -> None:
        self._set_outcome("success", duration_seconds=duration_seconds)

    def retry(
        self,
        *,
        duration_seconds: float,
        delay_seconds: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> None:
        self._set_outcome(
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
        if self._span is not None and error_message:
            try:
                self._span.set_tag("error.msg", error_message)
            except Exception:
                pass
        self._set_outcome(
            "timeout",
            duration_seconds=duration_seconds,
            timeout_seconds=timeout_seconds,
        )

    def error(self, *, duration_seconds: float, error: BaseException) -> None:
        if self._span is not None:
            _set_span_error(self._span, error)
        self._set_outcome("error", duration_seconds=duration_seconds)

    def cancelled(
        self, *, duration_seconds: float, reason: Optional[str] = None
    ) -> None:
        self._set_outcome(
            "cancelled",
            duration_seconds=duration_seconds,
            reason=reason,
        )

    def close(self) -> None:
        return None

    def _set_common_tags(self, span: Any) -> None:
        try:
            span.set_tag("component", self._component)
            span.set_tag("span.kind", "consumer")
            span.set_tag("rrq.job_id", self._job_id)
            span.set_tag("rrq.function", self._function_name)
            span.set_tag("rrq.queue", self._queue_name)
            span.set_tag("rrq.attempt", self._attempt)
            if self._worker_id:
                span.set_tag("rrq.worker_id", self._worker_id)
            span.set_tag("messaging.system", "redis")
            span.set_tag("messaging.destination.name", self._queue_name)
            span.set_tag("messaging.destination_kind", "queue")
            span.set_tag("messaging.operation", "process")
            if self._env:
                span.set_tag("env", self._env)
            if self._version:
                span.set_tag("version", self._version)
        except Exception:
            pass

    def _set_outcome(
        self,
        outcome: str,
        *,
        duration_seconds: float,
        delay_seconds: Optional[float] = None,
        reason: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        if self._span is None:
            return
        try:
            self._span.set_tag("rrq.outcome", outcome)
            _set_span_metric(
                self._span, "rrq.duration_ms", float(duration_seconds) * 1000.0
            )
            if delay_seconds is not None:
                _set_span_metric(
                    self._span, "rrq.retry_delay_ms", float(delay_seconds) * 1000.0
                )
            if timeout_seconds is not None:
                self._span.set_tag("rrq.timeout_seconds", float(timeout_seconds))
            if reason:
                self._span.set_tag("rrq.reason", reason)
        except Exception:
            pass


class DdtraceTelemetry(Telemetry):
    """ddtrace-backed RRQ telemetry (traces + propagation)."""

    enabled: bool = True

    def __init__(
        self,
        *,
        service: str,
        env: str | None,
        version: str | None,
        component: str,
    ) -> None:
        try:
            from ddtrace import tracer  # type: ignore[import-not-found]
        except Exception as e:
            raise RuntimeError(
                "ddtrace is not installed; install rrq with your app's ddtrace dependency."
            ) from e

        self._tracer = tracer
        self._service = service
        self._env = env
        self._version = version
        self._component = component

        propagator = None
        try:
            from ddtrace.propagation.http import (  # type: ignore[import-not-found]
                HTTPPropagator,
            )

            propagator = HTTPPropagator
        except Exception:
            try:
                from ddtrace.propagation.http import (  # type: ignore[import-not-found]
                    HttpPropagator,
                )

                propagator = HttpPropagator
            except Exception:
                propagator = None
        self._propagator = propagator

    def enqueue_span(
        self, *, job_id: str, function_name: str, queue_name: str
    ) -> EnqueueSpan:
        return _DdtraceEnqueueSpan(
            tracer=self._tracer,
            propagator=self._propagator,
            service=self._service,
            component=self._component,
            job_id=job_id,
            function_name=function_name,
            queue_name=queue_name,
            env=self._env,
            version=self._version,
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
        parent_context = None
        if self._propagator is not None and job.trace_context:
            parent_context = _ddtrace_extract(self._propagator, dict(job.trace_context))
        return _DdtraceJobSpan(
            tracer=self._tracer,
            service=self._service,
            component=self._component,
            job=job,
            worker_id=worker_id,
            queue_name=queue_name,
            attempt=attempt,
            timeout_seconds=timeout_seconds,
            parent_context=parent_context,
            env=self._env,
            version=self._version,
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
        parent_context = None
        if self._propagator is not None and trace_context:
            parent_context = _ddtrace_extract(self._propagator, dict(trace_context))
        return _DdtraceExecutorSpan(
            tracer=self._tracer,
            service=self._service,
            component=self._component,
            job_id=job_id,
            function_name=function_name,
            queue_name=queue_name,
            attempt=attempt,
            worker_id=worker_id,
            parent_context=parent_context,
            env=self._env,
            version=self._version,
        )


def _trace_with_parent(
    tracer: Any,
    *,
    name: str,
    service: str,
    resource: str,
    span_type: str,
    parent_context: Any,
) -> AbstractContextManager[Any]:
    try:
        if parent_context is not None:
            return tracer.trace(
                name,
                service=service,
                resource=resource,
                span_type=span_type,
                child_of=parent_context,
            )
    except TypeError:
        pass

    if parent_context is not None:
        try:
            tracer.context_provider.activate(parent_context)
        except Exception:
            pass
    return tracer.trace(name, service=service, resource=resource, span_type=span_type)


def _ddtrace_inject(
    propagator: Any, span: Any, carrier: dict[str, str]
) -> dict[str, str]:
    """Inject a ddtrace span context into a string carrier.

    ddtrace has historically provided `HTTPPropagator.inject(span.context, headers)`.
    This helper keeps RRQ compatible across ddtrace major versions by trying a
    couple of safe call patterns and returning an empty carrier on failure.
    """
    try:
        propagator.inject(span.context, carrier)
        return carrier
    except TypeError:
        try:
            # Some versions may accept reversed arguments.
            propagator.inject(carrier, span.context)
            return carrier
        except Exception:
            return {}
    except Exception:
        return {}


def _ddtrace_extract(propagator: Any, carrier: dict[str, str]) -> Any:
    """Extract a ddtrace context from a string carrier.

    Returns a ddtrace Context/SpanContext (or None) suitable for parenting.
    """
    try:
        return propagator.extract(carrier)
    except TypeError:
        try:
            # Some versions may accept keyword carriers.
            return propagator.extract(headers=carrier)
        except Exception:
            return None
    except Exception:
        return None


def _set_span_metric(span: Any, name: str, value: float) -> None:
    try:
        span.set_metric(name, value)
    except Exception:
        try:
            span.set_tag(name, value)
        except Exception:
            pass


def _set_span_error(span: Any, error: BaseException) -> None:
    try:
        span.set_exc_info(type(error), error, error.__traceback__)
        return
    except Exception:
        pass

    try:
        span.set_tag("error", True)
        span.set_tag("error.type", type(error).__name__)
        span.set_tag("error.msg", str(error))
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
