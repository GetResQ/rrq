from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

import pytest

from rrq.client import RRQClient
from rrq.executor import ExecutionContext, ExecutionRequest, PythonExecutor
from rrq.registry import JobRegistry
from rrq.settings import RRQSettings


@pytest.mark.asyncio
async def test_python_executor_context_includes_trace_and_deadline() -> None:
    registry = JobRegistry()
    captured: dict[str, object] = {}

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured.update(ctx)
        return {"ok": True}

    registry.register("echo", handler)
    executor = PythonExecutor(
        job_registry=registry,
        settings=RRQSettings(),
        client=cast(RRQClient, object()),
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

    outcome = await executor.execute(request)

    assert outcome.status == "success"
    assert outcome.request_id == "req-ctx"
    assert captured["trace_context"] == {"traceparent": "00-abc-123-01"}
    assert captured["deadline"] == deadline
    assert captured["worker_id"] == "worker-123"
