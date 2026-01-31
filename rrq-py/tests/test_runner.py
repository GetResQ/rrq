from __future__ import annotations

from datetime import datetime, timezone
import pytest

from rrq.runner import ExecutionContext, ExecutionRequest, PythonRunner
from rrq.registry import Registry


@pytest.mark.asyncio
async def test_python_runner_context_includes_trace_and_deadline() -> None:
    registry = Registry()
    captured: dict[str, object] = {}

    async def handler(request):  # type: ignore[no-untyped-def]
        captured["trace_context"] = request.context.trace_context
        captured["deadline"] = request.context.deadline
        captured["worker_id"] = request.context.worker_id
        return {"ok": True}

    registry.register("echo", handler)
    runner = PythonRunner(
        job_registry=registry,
        worker_id="worker-orchestrator",
    )
    deadline = datetime.now(timezone.utc)
    request = ExecutionRequest(
        request_id="req-ctx",
        job_id="job-ctx",
        function_name="echo",
        args=[],
        kwargs={},
        context=ExecutionContext(
            job_id="job-ctx",
            attempt=2,
            enqueue_time=datetime.now(timezone.utc),
            queue_name="default",
            deadline=deadline,
            trace_context={"traceparent": "00-abc-123-01"},
            worker_id="worker-123",
        ),
    )

    outcome = await runner.execute(request)

    assert outcome.status == "success"
    assert outcome.request_id == "req-ctx"
    assert captured["trace_context"] == {"traceparent": "00-abc-123-01"}
    assert captured["deadline"] == deadline
    assert captured["worker_id"] == "worker-123"
