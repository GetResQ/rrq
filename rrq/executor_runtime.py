"""Python Unix socket executor runtime for RRQ."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import os
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path

from .client import RRQClient
from .exc import HandlerNotFound
from .executor import ExecutionError, ExecutionOutcome, ExecutionRequest, PythonExecutor
from .executor_settings import PythonExecutorSettings
from .protocol import read_message, write_message
from .registry import JobRegistry
from .store import JobStore


ENV_EXECUTOR_SETTINGS = "RRQ_EXECUTOR_SETTINGS"
ENV_EXECUTOR_SOCKET = "RRQ_EXECUTOR_SOCKET"


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


def resolve_socket_path(socket_path: str | None) -> str:
    if socket_path is None:
        socket_path = os.getenv(ENV_EXECUTOR_SOCKET)
    if socket_path is None:
        raise ValueError(
            f"Executor socket path not provided. Use --socket or {ENV_EXECUTOR_SOCKET}."
        )
    return socket_path


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
) -> None:
    try:
        while True:
            message = await read_message(reader)
            if message is None:
                break
            message_type, payload = message
            if message_type != "request":
                raise ValueError(f"Unexpected message type: {message_type}")
            request = ExecutionRequest.model_validate(payload)

            if not ready_event.is_set():
                outcome = ExecutionOutcome(
                    job_id=request.job_id,
                    request_id=request.request_id,
                    status="error",
                    error=ExecutionError(message="Executor not ready"),
                )
                await write_message(writer, "response", outcome.model_dump(mode="json"))
                continue

            if request.protocol_version != "1":
                outcome = ExecutionOutcome(
                    job_id=request.job_id,
                    request_id=request.request_id,
                    status="error",
                    error=ExecutionError(message="Unsupported protocol version"),
                )
                await write_message(writer, "response", outcome.model_dump(mode="json"))
                continue

            try:
                outcome = await executor.execute(request)
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
            except Exception as exc:
                outcome = ExecutionOutcome(
                    job_id=request.job_id,
                    request_id=request.request_id,
                    status="error",
                    error=ExecutionError(message=str(exc)),
                )

            await write_message(writer, "response", outcome.model_dump(mode="json"))
    finally:
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()


async def run_python_executor(
    settings_object_path: str | None,
    socket_path: str | None = None,
) -> None:
    executor_settings = load_executor_settings(settings_object_path)
    socket_path = resolve_socket_path(socket_path)
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
    server: asyncio.AbstractServer | None = None

    try:
        path = Path(socket_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        server = await asyncio.start_unix_server(
            lambda r, w: _handle_connection(r, w, executor, ready_event),
            path=str(path),
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
        description="RRQ Python executor runtime (Unix socket)"
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
    args = parser.parse_args()
    asyncio.run(run_python_executor(args.settings_object_path, args.socket_path))


__all__ = ["run_python_executor", "load_executor_settings", "main"]


if __name__ == "__main__":
    main()
