"""Python stdio executor runtime for RRQ."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import sys
from collections.abc import Awaitable, Callable

from .client import RRQClient
from .exc import HandlerNotFound
from .executor import ExecutionOutcome, ExecutionRequest, PythonExecutor
from .executor_settings import PythonExecutorSettings
from .registry import JobRegistry
from .store import JobStore


ENV_EXECUTOR_SETTINGS = "RRQ_EXECUTOR_SETTINGS"


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


async def _read_line() -> bytes:
    return await asyncio.to_thread(sys.stdin.buffer.readline)


async def _write_line(payload: str) -> None:
    await asyncio.to_thread(sys.stdout.write, payload)
    await asyncio.to_thread(sys.stdout.flush)


async def _call_hook(
    hook: Callable[[], Awaitable[None] | None] | None,
) -> None:
    if hook is None:
        return
    result = hook()
    if inspect.isawaitable(result):
        await result


async def run_python_executor(settings_object_path: str | None) -> None:
    executor_settings = load_executor_settings(settings_object_path)
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

    try:
        await _call_hook(executor_settings.on_startup)
        startup_completed = True
        while True:
            raw = await _read_line()
            if not raw:
                break
            raw = raw.strip()
            if not raw:
                continue
            try:
                request = ExecutionRequest.model_validate_json(raw)
            except Exception as exc:
                error = ExecutionOutcome(
                    job_id="unknown",
                    status="error",
                    error_message=f"Invalid request: {exc}",
                )
                await _write_line(error.model_dump_json() + "\n")
                continue

            if request.protocol_version != "1":
                outcome = ExecutionOutcome(
                    job_id=request.job_id,
                    status="error",
                    error_message="Unsupported protocol version",
                )
                await _write_line(outcome.model_dump_json() + "\n")
                continue

            try:
                outcome = await executor.execute(request)
            except HandlerNotFound as exc:
                outcome = ExecutionOutcome(
                    job_id=request.job_id,
                    status="error",
                    error_message=str(exc),
                    error_type="handler_not_found",
                )
            except Exception as exc:
                outcome = ExecutionOutcome(
                    job_id=request.job_id,
                    status="error",
                    error_message=str(exc),
                )

            await _write_line(outcome.model_dump_json() + "\n")
    finally:
        if startup_completed:
            await _call_hook(executor_settings.on_shutdown)
        await executor.close()
        await client.close()
        await job_store.aclose()
