from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
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
    SocketExecutor,
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
            return ExecutionOutcome(
                job_id=request.job_id,
                request_id=request.request_id,
                status="success",
                result={"executor": self.name},
            )

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
        request_id="req-rust",
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
        request_id="req-default",
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


def _make_socket_dir() -> tempfile.TemporaryDirectory[str]:
    base_dir = Path("/tmp")
    if base_dir.exists():
        return tempfile.TemporaryDirectory(dir=base_dir, prefix="rrq-socket-")
    return tempfile.TemporaryDirectory(prefix="rrq-socket-")


@pytest.mark.asyncio
async def test_socket_executor_success() -> None:
    script = (
        "import json,os,struct,socket\n"
        "def read_exact(conn, size):\n"
        "    data=b''\n"
        "    while len(data) < size:\n"
        "        chunk=conn.recv(size-len(data))\n"
        "        if not chunk:\n"
        "            return None\n"
        "        data+=chunk\n"
        "    return data\n"
        "path=os.environ['RRQ_EXECUTOR_SOCKET']\n"
        "server=socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)\n"
        "server.bind(path)\n"
        "server.listen(1)\n"
        "conn,_=server.accept()\n"
        "header=read_exact(conn,4)\n"
        "if header:\n"
        "    length=struct.unpack('>I', header)[0]\n"
        "    payload=read_exact(conn,length)\n"
        "    msg=json.loads(payload.decode('utf-8'))\n"
        "    req=msg['payload']\n"
        "    out={'job_id':req['job_id'],'request_id':req['request_id'],'status':'success','result':{'job_id':req['job_id']}}\n"
        "    resp={'type':'response','payload':out}\n"
        "    data=json.dumps(resp).encode('utf-8')\n"
        "    conn.sendall(struct.pack('>I', len(data)) + data)\n"
        "conn.close()\n"
        "server.close()\n"
    )
    cmd = [sys.executable, "-u", "-c", script]
    with _make_socket_dir() as socket_dir:
        executor = SocketExecutor(cmd=cmd, pool_size=1, socket_dir=socket_dir)
        request = ExecutionRequest(
            request_id="req-socket",
            job_id="job-socket",
            function_name="echo",
            args=[],
            kwargs={},
            context=ExecutionContext(
                job_id="job-socket",
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
    assert outcome.request_id == "req-socket"
    assert outcome.result == {"job_id": "job-socket"}


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
