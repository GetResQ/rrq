"""Python socket executor runtime for RRQ."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import os
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import datetime, timezone
from ipaddress import ip_address
from pathlib import Path

from pydantic import BaseModel, Field

from .client import RRQClient
from .exc import HandlerNotFound
from .executor import ExecutionError, ExecutionOutcome, ExecutionRequest, PythonExecutor
from .executor_settings import PythonExecutorSettings
from .protocol import read_message, write_message
from .registry import JobRegistry
from .store import JobStore


ENV_EXECUTOR_SETTINGS = "RRQ_EXECUTOR_SETTINGS"
ENV_EXECUTOR_SOCKET = "RRQ_EXECUTOR_SOCKET"
ENV_EXECUTOR_TCP_SOCKET = "RRQ_EXECUTOR_TCP_SOCKET"
_LOCALHOST_ALIASES = {"localhost", "127.0.0.1", "::1"}


class CancelRequest(BaseModel):
    protocol_version: str = "1"
    job_id: str
    request_id: str | None = None
    hard_kill: bool = Field(default=False)


async def _write_outcome(
    writer: asyncio.StreamWriter,
    outcome: ExecutionOutcome,
    lock: asyncio.Lock,
) -> None:
    async with lock:
        await write_message(writer, "response", outcome.model_dump(mode="json"))


async def _execute_and_respond(
    executor: PythonExecutor,
    request: ExecutionRequest,
    writer: asyncio.StreamWriter,
    write_lock: asyncio.Lock,
    in_flight: dict[str, asyncio.Task],
    job_index: dict[str, str],
    inflight_lock: asyncio.Lock,
) -> None:
    try:
        try:
            outcome = await _execute_with_deadline(executor, request)
        except HandlerNotFound as exc:
            outcome = ExecutionOutcome(
                job_id=request.job_id,
                request_id=request.request_id,
                status="error",
                error=ExecutionError(
                    message=str(exc),
                    type="handler_not_found",
                ),
            )
        except asyncio.CancelledError:
            outcome = ExecutionOutcome(
                job_id=request.job_id,
                request_id=request.request_id,
                status="error",
                error=ExecutionError(message="Job cancelled", type="cancelled"),
            )
        except asyncio.TimeoutError as exc:
            outcome = ExecutionOutcome(
                job_id=request.job_id,
                request_id=request.request_id,
                status="timeout",
                error=ExecutionError(message=str(exc) or "Job execution timed out."),
            )
        except Exception as exc:
            outcome = ExecutionOutcome(
                job_id=request.job_id,
                request_id=request.request_id,
                status="error",
                error=ExecutionError(message=str(exc)),
            )
        await _write_outcome(writer, outcome, write_lock)
    finally:
        async with inflight_lock:
            in_flight.pop(request.request_id, None)
            job_index.pop(request.job_id, None)


async def _execute_with_deadline(
    executor: PythonExecutor,
    request: ExecutionRequest,
) -> ExecutionOutcome:
    deadline = request.context.deadline
    if deadline is None:
        return await executor.execute(request)
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    remaining = (deadline - now).total_seconds()
    if remaining <= 0:
        raise asyncio.TimeoutError("Job deadline exceeded")
    return await asyncio.wait_for(executor.execute(request), timeout=remaining)


def load_executor_settings(
    settings_object_path: str | None,
) -> PythonExecutorSettings:
    if settings_object_path is None:
        settings_object_path = os.getenv(ENV_EXECUTOR_SETTINGS)
    if settings_object_path is None:
        raise ValueError(
            "Python executor settings not provided. Use --settings or "
            f"{ENV_EXECUTOR_SETTINGS}."
        )

    parts = settings_object_path.split(".")
    if len(parts) < 2:
        raise ValueError(
            "settings_object_path must be in the form 'module.settings_object'"
        )
    settings_object_name = parts[-1]
    settings_object_module_path = ".".join(parts[:-1])
    settings_object_module = importlib.import_module(settings_object_module_path)
    settings_object = getattr(settings_object_module, settings_object_name)
    if not isinstance(settings_object, PythonExecutorSettings):
        raise ValueError("settings_object is not a PythonExecutorSettings instance")
    return settings_object


def _parse_tcp_socket(tcp_socket: str) -> tuple[str, int]:
    tcp_socket = tcp_socket.strip()
    if not tcp_socket:
        raise ValueError("TCP socket value cannot be empty")

    if tcp_socket.startswith("["):
        host_part, sep, port_part = tcp_socket.partition("]:")
        if not sep:
            raise ValueError("TCP socket must be in [host]:port format")
        host = host_part.lstrip("[")
    else:
        host, sep, port_part = tcp_socket.rpartition(":")
        if not sep:
            raise ValueError("TCP socket must be in host:port format")
        if not host:
            raise ValueError("TCP socket host cannot be empty")

    if host not in _LOCALHOST_ALIASES:
        try:
            ip = ip_address(host)
        except ValueError as exc:
            raise ValueError(f"Invalid TCP socket host: {host}") from exc
        if not ip.is_loopback:
            raise ValueError(f"TCP socket host must be localhost; got {host}")
        host = str(ip)

    try:
        port = int(port_part)
    except ValueError as exc:
        raise ValueError(f"Invalid TCP socket port: {port_part}") from exc
    if port <= 0 or port > 65535:
        raise ValueError(f"TCP socket port out of range: {port}")

    if host == "localhost":
        host = "127.0.0.1"
    return host, port


def resolve_executor_socket(
    socket_path: str | None,
    tcp_socket: str | None,
) -> tuple[str, str | tuple[str, int]]:
    if socket_path and tcp_socket:
        raise ValueError("Provide only one of --socket or --tcp-socket")

    if tcp_socket is None:
        tcp_socket = os.getenv(ENV_EXECUTOR_TCP_SOCKET)
    if socket_path is None:
        socket_path = os.getenv(ENV_EXECUTOR_SOCKET)

    if tcp_socket and socket_path:
        raise ValueError("Provide only one of --socket or --tcp-socket")

    if tcp_socket:
        return "tcp", _parse_tcp_socket(tcp_socket)
    if socket_path:
        return "unix", socket_path
    raise ValueError(
        "Executor socket not provided. Use --socket/--tcp-socket or set "
        f"{ENV_EXECUTOR_SOCKET}/{ENV_EXECUTOR_TCP_SOCKET}."
    )


async def _call_hook(
    hook: Callable[[], Awaitable[None] | None] | None,
) -> None:
    if hook is None:
        return
    result = hook()
    if inspect.isawaitable(result):
        await result


async def _handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    executor: PythonExecutor,
    ready_event: asyncio.Event,
    in_flight: dict[str, asyncio.Task],
    job_index: dict[str, str],
    inflight_lock: asyncio.Lock,
) -> None:
    write_lock = asyncio.Lock()
    try:
        while True:
            message = await read_message(reader)
            if message is None:
                break
            message_type, payload = message
            if message_type == "request":
                request = ExecutionRequest.model_validate(payload)

                if not ready_event.is_set():
                    await ready_event.wait()

                if request.protocol_version != "1":
                    outcome = ExecutionOutcome(
                        job_id=request.job_id,
                        request_id=request.request_id,
                        status="error",
                        error=ExecutionError(message="Unsupported protocol version"),
                    )
                    await _write_outcome(writer, outcome, write_lock)
                    continue

                task = asyncio.create_task(
                    _execute_and_respond(
                        executor,
                        request,
                        writer,
                        write_lock,
                        in_flight,
                        job_index,
                        inflight_lock,
                    ),
                    name=f"rrq-executor-{request.request_id}",
                )
                async with inflight_lock:
                    in_flight[request.request_id] = task
                    job_index[request.job_id] = request.request_id
                continue

            if message_type == "cancel":
                cancel_request = CancelRequest.model_validate(payload)
                if cancel_request.protocol_version != "1":
                    continue
                request_id = cancel_request.request_id
                if request_id is None:
                    async with inflight_lock:
                        request_id = job_index.get(cancel_request.job_id)
                if request_id is not None:
                    async with inflight_lock:
                        task = in_flight.get(request_id)
                    if task is not None:
                        task.cancel()
                continue

            raise ValueError(f"Unexpected message type: {message_type}")
    finally:
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()


async def run_python_executor(
    settings_object_path: str | None,
    socket_path: str | None = None,
    tcp_socket: str | None = None,
) -> None:
    executor_settings = load_executor_settings(settings_object_path)
    transport, target = resolve_executor_socket(socket_path, tcp_socket)
    settings = executor_settings.rrq_settings
    job_registry = executor_settings.job_registry
    if not isinstance(job_registry, JobRegistry):
        raise RuntimeError(
            "PythonExecutorSettings.job_registry must be a JobRegistry instance"
        )

    job_store = JobStore(settings=settings)
    client = RRQClient(settings=settings, job_store=job_store)
    executor = PythonExecutor(
        job_registry=job_registry,
        settings=settings,
        client=client,
        worker_id=None,
    )
    startup_completed = False
    ready_event = asyncio.Event()
    in_flight: dict[str, asyncio.Task] = {}
    job_index: dict[str, str] = {}
    inflight_lock = asyncio.Lock()
    server: asyncio.AbstractServer | None = None

    try:
        if transport == "unix":
            path = Path(str(target))
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                path.unlink()
            server = await asyncio.start_unix_server(
                lambda r, w: _handle_connection(
                    r, w, executor, ready_event, in_flight, job_index, inflight_lock
                ),
                path=str(path),
            )
        else:
            host, port = target
            server = await asyncio.start_server(
                lambda r, w: _handle_connection(
                    r, w, executor, ready_event, in_flight, job_index, inflight_lock
                ),
                host=host,
                port=port,
            )
        await _call_hook(executor_settings.on_startup)
        startup_completed = True
        ready_event.set()
        async with server:
            await server.serve_forever()
    finally:
        if server is not None:
            server.close()
            with suppress(Exception):
                await server.wait_closed()
        if startup_completed:
            await _call_hook(executor_settings.on_shutdown)
        await executor.close()
        await client.close()
        await job_store.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="RRQ Python executor runtime (Unix or TCP socket)"
    )
    parser.add_argument(
        "--settings",
        dest="settings_object_path",
        help=(
            "PythonExecutorSettings object path "
            "(e.g., myapp.executor_config.python_executor_settings). "
            f"Defaults to {ENV_EXECUTOR_SETTINGS} if unset."
        ),
    )
    parser.add_argument(
        "--socket",
        dest="socket_path",
        help=f"Unix socket path. Defaults to {ENV_EXECUTOR_SOCKET} if unset.",
    )
    parser.add_argument(
        "--tcp-socket",
        dest="tcp_socket",
        help=(
            "TCP socket in host:port form (localhost only). Defaults to "
            f"{ENV_EXECUTOR_TCP_SOCKET} if unset."
        ),
    )
    args = parser.parse_args()
    asyncio.run(
        run_python_executor(
            args.settings_object_path, args.socket_path, args.tcp_socket
        )
    )


__all__ = ["run_python_executor", "load_executor_settings", "main"]


if __name__ == "__main__":
    main()
