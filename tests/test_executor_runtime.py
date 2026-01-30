from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import cast

import pytest

from rrq.client import RRQClient
from rrq.executor import ExecutionContext, ExecutionRequest, PythonExecutor
from rrq.executor_runtime import (
    _execute_and_respond,
    _execute_with_deadline,
    _handle_connection,
    _parse_tcp_socket,
    resolve_executor_socket,
)
from rrq.protocol import read_message, write_message
from rrq.registry import JobRegistry
from rrq.settings import RRQSettings


@pytest.mark.asyncio
async def test_execute_with_deadline_allows_future_deadline() -> None:
    registry = JobRegistry()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        await asyncio.sleep(0)
        return {"ok": True}

    registry.register("echo", handler)
    executor = PythonExecutor(
        job_registry=registry,
        settings=RRQSettings(),
        client=cast(RRQClient, object()),
        worker_id=None,
    )
    request = ExecutionRequest(
        request_id="req-deadline",
        job_id="job-deadline",
        function_name="echo",
        args=[],
        kwargs={},
        context=ExecutionContext(
            job_id="job-deadline",
            attempt=1,
            enqueue_time=datetime.now(timezone.utc),
            queue_name="default",
            deadline=datetime.now(timezone.utc) + timedelta(seconds=5),
        ),
    )

    outcome = await _execute_with_deadline(executor, request)

    assert outcome.status == "success"
    assert outcome.result == {"ok": True}


@pytest.mark.asyncio
async def test_execute_with_deadline_raises_for_past_deadline() -> None:
    registry = JobRegistry()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        return {"ok": True}

    registry.register("echo", handler)
    executor = PythonExecutor(
        job_registry=registry,
        settings=RRQSettings(),
        client=cast(RRQClient, object()),
        worker_id=None,
    )
    request = ExecutionRequest(
        request_id="req-expired",
        job_id="job-expired",
        function_name="echo",
        args=[],
        kwargs={},
        context=ExecutionContext(
            job_id="job-expired",
            attempt=1,
            enqueue_time=datetime.now(timezone.utc),
            queue_name="default",
            deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
        ),
    )

    with pytest.raises(asyncio.TimeoutError):
        await _execute_with_deadline(executor, request)


@pytest.mark.asyncio
async def test_handle_connection_waits_for_ready_event() -> None:
    registry = JobRegistry()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        return {"ok": True}

    registry.register("echo", handler)
    executor = PythonExecutor(
        job_registry=registry,
        settings=RRQSettings(),
        client=cast(RRQClient, object()),
        worker_id=None,
    )
    ready_event = asyncio.Event()
    in_flight: dict[str, asyncio.Task] = {}
    job_index: dict[str, set[str]] = {}
    inflight_lock = asyncio.Lock()
    server = await asyncio.start_server(
        lambda r, w: _handle_connection(
            r, w, executor, ready_event, in_flight, job_index, inflight_lock
        ),
        host="127.0.0.1",
        port=0,
    )
    writer: asyncio.StreamWriter | None = None
    try:
        sockets = server.sockets or []
        assert sockets
        host, port = sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        request = ExecutionRequest(
            request_id="req-ready",
            job_id="job-ready",
            function_name="echo",
            args=[],
            kwargs={},
            context=ExecutionContext(
                job_id="job-ready",
                attempt=1,
                enqueue_time=datetime.now(timezone.utc),
                queue_name="default",
            ),
        )
        await write_message(writer, "request", request.model_dump(mode="json"))
        await asyncio.sleep(0)
        ready_event.set()
        message = await asyncio.wait_for(read_message(reader), timeout=1)
        assert message is not None
        message_type, payload = message
        assert message_type == "response"
        assert payload["status"] == "success"
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_cancel_by_job_id_cancels_all_requests() -> None:
    registry = JobRegistry()
    blocker = asyncio.Event()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        await blocker.wait()
        return {"ok": True}

    registry.register("block", handler)
    executor = PythonExecutor(
        job_registry=registry,
        settings=RRQSettings(),
        client=cast(RRQClient, object()),
        worker_id=None,
    )
    ready_event = asyncio.Event()
    ready_event.set()
    in_flight: dict[str, asyncio.Task] = {}
    job_index: dict[str, set[str]] = {}
    inflight_lock = asyncio.Lock()
    server = await asyncio.start_server(
        lambda r, w: _handle_connection(
            r, w, executor, ready_event, in_flight, job_index, inflight_lock
        ),
        host="127.0.0.1",
        port=0,
    )
    writer: asyncio.StreamWriter | None = None
    try:
        sockets = server.sockets or []
        assert sockets
        host, port = sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        job_id = "job-cancel-all"
        req1 = ExecutionRequest(
            request_id="req-1",
            job_id=job_id,
            function_name="block",
            args=[],
            kwargs={},
            context=ExecutionContext(
                job_id=job_id,
                attempt=1,
                enqueue_time=datetime.now(timezone.utc),
                queue_name="default",
            ),
        )
        req2 = ExecutionRequest(
            request_id="req-2",
            job_id=job_id,
            function_name="block",
            args=[],
            kwargs={},
            context=ExecutionContext(
                job_id=job_id,
                attempt=2,
                enqueue_time=datetime.now(timezone.utc),
                queue_name="default",
            ),
        )
        await write_message(writer, "request", req1.model_dump(mode="json"))
        await write_message(writer, "request", req2.model_dump(mode="json"))

        for _ in range(50):
            async with inflight_lock:
                if len(in_flight) == 2:
                    break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("in_flight did not reach expected size")

        cancel_payload = {
            "protocol_version": "1",
            "job_id": job_id,
            "request_id": None,
            "hard_kill": False,
        }
        await write_message(writer, "cancel", cancel_payload)

        responses = []
        for _ in range(2):
            message = await asyncio.wait_for(read_message(reader), timeout=1)
            assert message is not None
            responses.append(message)

        for message_type, payload in responses:
            assert message_type == "response"
            assert payload["job_id"] == job_id
            assert payload["status"] == "error"
            assert payload["error"]["type"] == "cancelled"

        async def wait_for_cleanup() -> None:
            while True:
                async with inflight_lock:
                    if not in_flight and not job_index:
                        return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_cleanup(), timeout=1)
    finally:
        blocker.set()
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_execute_and_respond_cleans_inflight_on_write_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = JobRegistry()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        return {"ok": True}

    registry.register("echo", handler)
    executor = PythonExecutor(
        job_registry=registry,
        settings=RRQSettings(),
        client=cast(RRQClient, object()),
        worker_id=None,
    )
    request = ExecutionRequest(
        request_id="req-write-error",
        job_id="job-write-error",
        function_name="echo",
        args=[],
        kwargs={},
        context=ExecutionContext(
            job_id="job-write-error",
            attempt=1,
            enqueue_time=datetime.now(timezone.utc),
            queue_name="default",
        ),
    )

    async def boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("write failed")

    monkeypatch.setattr("rrq.executor_runtime.write_message", boom)

    dummy_task = asyncio.create_task(asyncio.sleep(0))
    in_flight = {request.request_id: dummy_task}
    job_index = {request.job_id: {request.request_id}}
    inflight_lock = asyncio.Lock()
    write_lock = asyncio.Lock()
    connection_requests = {request.request_id}
    connection_jobs = {request.request_id: request.job_id}
    connection_lock = asyncio.Lock()

    await _execute_and_respond(
        executor,
        request,
        cast(asyncio.StreamWriter, object()),
        write_lock,
        in_flight,
        job_index,
        inflight_lock,
        connection_requests,
        connection_jobs,
        connection_lock,
    )

    await dummy_task
    assert request.request_id not in in_flight
    assert request.job_id not in job_index
    assert request.request_id not in connection_requests
    assert request.request_id not in connection_jobs


def test_parse_tcp_socket_allows_localhost() -> None:
    host, port = _parse_tcp_socket("localhost:1234")
    assert host == "127.0.0.1"
    assert port == 1234


def test_parse_tcp_socket_rejects_non_localhost() -> None:
    with pytest.raises(ValueError):
        _parse_tcp_socket("example.com:1234")


def test_resolve_executor_socket_conflict() -> None:
    with pytest.raises(ValueError):
        resolve_executor_socket("/tmp/rrq.sock", "127.0.0.1:1234")
