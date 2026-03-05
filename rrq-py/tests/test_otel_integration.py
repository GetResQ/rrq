from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import ModuleType, SimpleNamespace
import sys
from typing import Any

import pytest

from rrq.integrations import otel as otel_integration
from rrq.runner import ExecutionContext, ExecutionRequest


class _FakeStatus:
    def __init__(self, code: object) -> None:
        self.code = code


class _FakeSpan:
    def __init__(self) -> None:
        self.attributes: dict[str, Any] = {}
        self.exceptions: list[BaseException] = []
        self.status: _FakeStatus | None = None

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def record_exception(self, error: BaseException) -> None:
        self.exceptions.append(error)

    def set_status(self, status: _FakeStatus) -> None:
        self.status = status


class _FakeSpanContextManager:
    def __init__(self, span: _FakeSpan) -> None:
        self._span = span

    def __enter__(self) -> _FakeSpan:
        return self._span

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> bool:
        return False


class _FakeTracer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def start_as_current_span(
        self,
        name: str,
        **kwargs: Any,
    ) -> _FakeSpanContextManager:
        span = _FakeSpan()
        manager = _FakeSpanContextManager(span)
        self.calls.append(
            {
                "name": name,
                "kwargs": kwargs,
                "span": span,
            }
        )
        return manager


def _build_request(
    *,
    trace_context: dict[str, str] | None = None,
    correlation_context: dict[str, str] | None = None,
    deadline: datetime | None = None,
) -> ExecutionRequest:
    return ExecutionRequest(
        request_id="req-1",
        job_id="job-1",
        function_name="echo",
        params={},
        context=ExecutionContext(
            job_id="job-1",
            attempt=2,
            enqueue_time=datetime.now(timezone.utc) - timedelta(seconds=5),
            queue_name="default",
            deadline=deadline,
            trace_context=trace_context,
            correlation_context=correlation_context,
            worker_id="worker-1",
        ),
    )


@pytest.fixture
def fake_opentelemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[_FakeTracer, list[dict[str, str]]]:
    tracer = _FakeTracer()
    extracted: list[dict[str, str]] = []

    def _extract(carrier: dict[str, str]) -> dict[str, dict[str, str]]:
        extracted.append(carrier)
        return {"carrier": carrier}

    opentelemetry_module = ModuleType("opentelemetry")
    opentelemetry_module.propagate = SimpleNamespace(extract=_extract)
    opentelemetry_module.trace = SimpleNamespace(get_tracer=lambda _name: tracer)

    trace_module = ModuleType("opentelemetry.trace")
    trace_module.SpanKind = SimpleNamespace(CONSUMER="consumer")
    trace_module.Status = _FakeStatus
    trace_module.StatusCode = SimpleNamespace(ERROR="error")

    monkeypatch.setitem(sys.modules, "opentelemetry", opentelemetry_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", trace_module)
    return tracer, extracted


def test_enable_configures_global_telemetry_backend(
    fake_opentelemetry: tuple[_FakeTracer, list[dict[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured: list[object] = []

    monkeypatch.setattr(
        otel_integration,
        "configure",
        lambda telemetry: configured.append(telemetry),
    )
    otel_integration.enable(service_name="svc")

    assert len(configured) == 1
    assert isinstance(configured[0], otel_integration.OtelTelemetry)


def test_runner_span_sets_attributes_and_outcomes(
    fake_opentelemetry: tuple[_FakeTracer, list[dict[str, str]]],
) -> None:
    tracer, extracted = fake_opentelemetry
    request = _build_request(
        trace_context={"traceparent": "00-abc-123-01"},
        correlation_context={"tenant_id": "t-1", "empty_value": "", "": "skip"},
        deadline=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    span_wrapper = otel_integration._OtelRunnerSpan(
        tracer=tracer,
        service_name="svc",
        request=request,
    )

    with span_wrapper as span:
        span.retry(duration_seconds=1.5, delay_seconds=2.5, reason="retrying")
        span.timeout(
            duration_seconds=2.0,
            timeout_seconds=9.0,
            error_message="took too long",
        )
        span.cancelled(duration_seconds=2.5, reason="manual")
        span.success(duration_seconds=3.0)

    assert extracted == [request.context.trace_context]
    assert len(tracer.calls) == 1
    call = tracer.calls[0]
    assert call["name"] == "rrq.runner"
    assert call["kwargs"]["context"] == {"carrier": request.context.trace_context}
    assert call["kwargs"]["kind"] == "consumer"

    span = call["span"]
    assert span.attributes["service.name"] == "svc"
    assert span.attributes["rrq.job_id"] == "job-1"
    assert span.attributes["rrq.function"] == "echo"
    assert span.attributes["rrq.queue"] == "default"
    assert span.attributes["rrq.attempt"] == 2
    assert span.attributes["rrq.worker_id"] == "worker-1"
    assert span.attributes["tenant_id"] == "t-1"
    assert "empty_value" not in span.attributes
    assert span.attributes["rrq.deadline"].endswith("+00:00")
    assert span.attributes["rrq.deadline_remaining_ms"] >= 0.0
    assert span.attributes["rrq.queue_wait_ms"] >= 0.0
    assert span.attributes["rrq.retry_delay_ms"] == 2500.0
    assert span.attributes["rrq.timeout_seconds"] == 9.0
    assert span.attributes["rrq.error_message"] == "took too long"
    assert span.attributes["rrq.outcome"] == "success"
    assert span.attributes["rrq.duration_ms"] == 3000.0


def test_runner_span_records_exception_on_exit(
    fake_opentelemetry: tuple[_FakeTracer, list[dict[str, str]]],
) -> None:
    tracer, _ = fake_opentelemetry
    span_wrapper = otel_integration._OtelRunnerSpan(
        tracer=tracer,
        service_name="svc",
        request=_build_request(),
    )

    with pytest.raises(RuntimeError, match="boom"):
        with span_wrapper:
            raise RuntimeError("boom")

    span = tracer.calls[0]["span"]
    assert len(span.exceptions) == 1
    assert str(span.exceptions[0]) == "boom"
    assert span.status is not None
    assert span.status.code == "error"


def test_runner_span_ignores_trace_context_extract_failures(
    fake_opentelemetry: tuple[_FakeTracer, list[dict[str, str]]],
) -> None:
    tracer, _ = fake_opentelemetry
    opentelemetry_module = sys.modules["opentelemetry"]

    def _raise_extract(_carrier: dict[str, str]) -> object:
        raise ValueError("invalid trace context")

    opentelemetry_module.propagate.extract = _raise_extract  # type: ignore[attr-defined]

    span_wrapper = otel_integration._OtelRunnerSpan(
        tracer=tracer,
        service_name="svc",
        request=_build_request(trace_context={"traceparent": "bad-value"}),
    )
    with span_wrapper:
        pass

    assert len(tracer.calls) == 1
    assert "context" not in tracer.calls[0]["kwargs"]


def test_otel_telemetry_builds_runner_span(
    fake_opentelemetry: tuple[_FakeTracer, list[dict[str, str]]],
) -> None:
    telemetry = otel_integration.OtelTelemetry(service_name="svc")
    span = telemetry.runner_span(_build_request())

    assert isinstance(span, otel_integration._OtelRunnerSpan)
