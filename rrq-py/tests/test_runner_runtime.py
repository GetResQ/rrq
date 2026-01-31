from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import cast

import pytest

from rrq.runner import ExecutionContext, ExecutionRequest, PythonRunner
from rrq.runner_runtime import (
    _InflightTracker,
    _execute_and_respond,
    _execute_with_deadline,
    _handle_connection,
    _parse_tcp_socket,
    ENV_RUNNER_TCP_SOCKET,
    resolve_tcp_socket,
)
from rrq.protocol import read_message, write_message
from rrq.registry import JobRegistry


@pytest.mark.asyncio
async def test_execute_with_deadline_allows_future_deadline() -> None:
    registry = JobRegistry()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        await asyncio.sleep(0)
        return {"ok": True}

    registry.register("echo", handler)
    runner = PythonRunner(
        job_registry=registry,
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

    outcome = await _execute_with_deadline(runner, request)

    assert outcome.status == "success"
    assert outcome.result == {"ok": True}


@pytest.mark.asyncio
async def test_execute_with_deadline_raises_for_past_deadline() -> None:
    registry = JobRegistry()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        return {"ok": True}

    registry.register("echo", handler)
    runner = PythonRunner(
        job_registry=registry,
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
        await _execute_with_deadline(runner, request)


@pytest.mark.asyncio
async def test_handle_connection_waits_for_ready_event() -> None:
    registry = JobRegistry()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        return {"ok": True}

    registry.register("echo", handler)
    runner = PythonRunner(
        job_registry=registry,
        worker_id=None,
    )
    ready_event = asyncio.Event()
    tracker = _InflightTracker()
    await tracker.start()
    server = await asyncio.start_server(
        lambda r, w: _handle_connection(r, w, runner, ready_event, tracker),
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
        await tracker.close()


@pytest.mark.asyncio
async def test_handle_connection_returns_busy_when_inflight_limit_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("rrq.runner_runtime.MAX_IN_FLIGHT_PER_CONNECTION", 1)
    registry = JobRegistry()
    blocker = asyncio.Event()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        await blocker.wait()
        return {"ok": True}

    registry.register("block", handler)
    runner = PythonRunner(
        job_registry=registry,
        worker_id=None,
    )
    ready_event = asyncio.Event()
    ready_event.set()
    tracker = _InflightTracker()
    await tracker.start()
    server = await asyncio.start_server(
        lambda r, w: _handle_connection(r, w, runner, ready_event, tracker),
        host="127.0.0.1",
        port=0,
    )
    writer: asyncio.StreamWriter | None = None
    try:
        sockets = server.sockets or []
        assert sockets
        host, port = sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        job_id = "job-busy"
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
                attempt=1,
                enqueue_time=datetime.now(timezone.utc),
                queue_name="default",
            ),
        )

        await write_message(writer, "request", req1.model_dump(mode="json"))

        async def wait_for_inflight() -> None:
            while True:
                if len(tracker._in_flight) == 1:  # type: ignore[attr-defined]
                    return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_inflight(), timeout=1)
        await write_message(writer, "request", req2.model_dump(mode="json"))

        message = await asyncio.wait_for(read_message(reader), timeout=1)
        assert message is not None
        message_type, payload = message
        assert message_type == "response"
        assert payload["request_id"] == req2.request_id
        assert payload["status"] == "error"
        assert payload["error"]["message"].startswith("Runner busy")
    finally:
        blocker.set()
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        server.close()
        await server.wait_closed()
        await tracker.close()


@pytest.mark.asyncio
async def test_cancel_by_job_id_cancels_all_requests() -> None:
    registry = JobRegistry()
    blocker = asyncio.Event()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        await blocker.wait()
        return {"ok": True}

    registry.register("block", handler)
    runner = PythonRunner(
        job_registry=registry,
        worker_id=None,
    )
    ready_event = asyncio.Event()
    ready_event.set()
    tracker = _InflightTracker()
    await tracker.start()
    server = await asyncio.start_server(
        lambda r, w: _handle_connection(r, w, runner, ready_event, tracker),
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

        async def wait_for_tracking() -> None:
            while True:
                if len(tracker._in_flight) == 2:  # type: ignore[attr-defined]
                    return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_tracking(), timeout=1)

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
                if not tracker._in_flight and not tracker._job_index:  # type: ignore[attr-defined]
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
        await tracker.close()


@pytest.mark.asyncio
async def test_execute_and_respond_cleans_inflight_on_write_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = JobRegistry()

    async def handler(ctx, *args, **kwargs):  # type: ignore[no-untyped-def]
        return {"ok": True}

    registry.register("echo", handler)
    runner = PythonRunner(
        job_registry=registry,
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

    monkeypatch.setattr("rrq.runner_runtime.write_message", boom)

    tracker = _InflightTracker()
    await tracker.start()
    dummy_task = asyncio.create_task(asyncio.sleep(0))
    await tracker.track_start(request.request_id, request.job_id, dummy_task)
    write_lock = asyncio.Lock()
    connection_requests = {request.request_id}
    connection_jobs = {request.request_id: request.job_id}
    connection_lock = asyncio.Lock()

    await _execute_and_respond(
        runner,
        request,
        cast(asyncio.StreamWriter, object()),
        write_lock,
        tracker,
        connection_requests,
        connection_jobs,
        connection_lock,
    )

    await dummy_task
    assert not tracker._in_flight  # type: ignore[attr-defined]
    assert not tracker._job_index  # type: ignore[attr-defined]
    assert request.request_id not in connection_requests
    assert request.request_id not in connection_jobs
    await tracker.close()


@pytest.mark.asyncio
async def test_inflight_tracker_cancels_request() -> None:
    tracker = _InflightTracker()
    await tracker.start()
    task = asyncio.create_task(asyncio.sleep(10))
    await tracker.track_start("req-track", "job-track", task)
    await tracker.cancel_request("req-track")

    with pytest.raises(asyncio.CancelledError):
        await task

    assert not tracker._in_flight  # type: ignore[attr-defined]
    assert not tracker._job_index  # type: ignore[attr-defined]
    await tracker.close()


def test_parse_tcp_socket_allows_localhost() -> None:
    host, port = _parse_tcp_socket("localhost:1234")
    assert host == "127.0.0.1"
    assert port == 1234


def test_parse_tcp_socket_rejects_non_localhost() -> None:
    with pytest.raises(ValueError):
        _parse_tcp_socket("example.com:1234")


def test_resolve_tcp_socket_requires_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_RUNNER_TCP_SOCKET, raising=False)
    with pytest.raises(ValueError):
        resolve_tcp_socket(None)
