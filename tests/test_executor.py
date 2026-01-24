from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import cast

import pytest

from rrq.client import RRQClient
from rrq.executor import (
    ExecutionContext,
    ExecutionOutcome,
    ExecutionRequest,
    PythonExecutor,
    QueueRoutingExecutor,
    resolve_executor_pool_sizes,
    StdioExecutor,
)
from rrq.registry import JobRegistry
from rrq.settings import ExecutorConfig, RRQSettings


@pytest.mark.asyncio
async def test_queue_routing_executor() -> None:
    class DummyExecutor:
        def __init__(self, name: str) -> None:
            self.name = name
            self.calls: list[str] = []
            self.closed = False

        async def execute(self, request: ExecutionRequest) -> ExecutionOutcome:
            self.calls.append(request.job_id)
            return ExecutionOutcome(status="success", result={"executor": self.name})

        async def cancel(self, job_id: str) -> None:
            self.calls.append(f"cancel:{job_id}")

        async def close(self) -> None:
            self.closed = True

    default_executor = DummyExecutor("default")
    rust_executor = DummyExecutor("rust")

    router = QueueRoutingExecutor(
        default=default_executor,
        routes={"rust_queue": rust_executor},
    )

    rust_request = ExecutionRequest(
        job_id="job-3",
        function_name="echo",
        args=[],
        kwargs={},
        context=ExecutionContext(
            job_id="job-3",
            attempt=1,
            enqueue_time=datetime.now(timezone.utc),
            queue_name="rust_queue",
        ),
    )
    outcome = await router.execute(rust_request)
    assert outcome.result == {"executor": "rust"}
    assert rust_executor.calls == ["job-3"]

    default_request = ExecutionRequest(
        job_id="job-4",
        function_name="echo",
        args=[],
        kwargs={},
        context=ExecutionContext(
            job_id="job-4",
            attempt=1,
            enqueue_time=datetime.now(timezone.utc),
            queue_name="default",
        ),
    )
    outcome = await router.execute(default_request)
    assert outcome.result == {"executor": "default"}
    assert default_executor.calls == ["job-4"]

    await router.close()
    assert default_executor.closed is True
    assert rust_executor.closed is True


@pytest.mark.asyncio
async def test_stdio_executor_success(tmp_path) -> None:
    script = (
        "import json,sys\n"
        "for line in sys.stdin:\n"
        "    req=json.loads(line)\n"
        "    out={'status':'success','result':{'job_id':req['job_id']}}\n"
        "    sys.stdout.write(json.dumps(out)+'\\n')\n"
        "    sys.stdout.flush()\n"
    )
    cmd = [sys.executable, "-u", "-c", script]
    executor = StdioExecutor(cmd=cmd, pool_size=1)
    request = ExecutionRequest(
        job_id="job-stdio",
        function_name="echo",
        args=[],
        kwargs={},
        context=ExecutionContext(
            job_id="job-stdio",
            attempt=1,
            enqueue_time=datetime.now(timezone.utc),
            queue_name="default",
        ),
    )
    try:
        outcome = await executor.execute(request)
    finally:
        await executor.close()

    assert outcome.status == "success"
    assert outcome.result == {"job_id": "job-stdio"}


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
    assert captured["trace_context"] == {"traceparent": "00-abc-123-01"}
    assert captured["deadline"] == deadline
    assert captured["worker_id"] == "worker-123"


def test_resolve_executor_pool_sizes_defaults_to_cpu_count(monkeypatch) -> None:
    monkeypatch.setattr("rrq.executor.os.cpu_count", lambda: 4)
    settings = RRQSettings(executors={"python": ExecutorConfig(cmd=["python"])})
    pool_sizes = resolve_executor_pool_sizes(settings)
    assert pool_sizes == {"python": 4}


def test_resolve_executor_pool_sizes_watch_mode_forces_one(monkeypatch) -> None:
    monkeypatch.setattr("rrq.executor.os.cpu_count", lambda: 8)
    settings = RRQSettings(
        executors={"python": ExecutorConfig(cmd=["python"], pool_size=5)}
    )
    pool_sizes = resolve_executor_pool_sizes(settings, watch_mode=True)
    assert pool_sizes == {"python": 1}
