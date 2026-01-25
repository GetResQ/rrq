"""Executor interfaces and Python implementation for RRQ job execution."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Mapping, Sequence
from asyncio.subprocess import PIPE, Process
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol
from pydantic import BaseModel, Field

from .client import RRQClient
from .exc import HandlerNotFound, RetryJob
from .registry import JobRegistry
from .settings import RRQSettings
from .telemetry import get_telemetry

logger = logging.getLogger(__name__)


class ExecutionContext(BaseModel):
    """Minimal execution context passed to runtimes."""

    job_id: str
    attempt: int
    enqueue_time: datetime
    queue_name: str
    deadline: datetime | None = None
    trace_context: dict[str, str] | None = None
    worker_id: str | None = None


class ExecutionRequest(BaseModel):
    """Job data sent to an executor."""

    protocol_version: str = "1"
    job_id: str
    function_name: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    context: ExecutionContext


class ExecutionOutcome(BaseModel):
    """Result returned by an executor."""

    job_id: str | None = None
    status: Literal["success", "retry", "timeout", "error"]
    result: Any | None = None
    error_message: str | None = None
    error_type: str | None = None
    retry_after_seconds: float | None = None


class Executor(Protocol):
    async def execute(self, request: ExecutionRequest) -> ExecutionOutcome:
        """Run a job and return an outcome."""

    async def cancel(self, job_id: str) -> None:
        """Best-effort cancellation for in-flight jobs."""

    async def close(self) -> None:
        """Release executor resources."""


class PythonExecutor:
    """Executes Python handlers registered in JobRegistry."""

    def __init__(
        self,
        *,
        job_registry: JobRegistry,
        settings: RRQSettings,
        client: RRQClient,
        worker_id: str | None,
    ) -> None:
        self.job_registry = job_registry
        self.settings = settings
        self.client = client
        self.worker_id = worker_id

    async def execute(self, request: ExecutionRequest) -> ExecutionOutcome:
        handler = self.job_registry.get_handler(request.function_name)
        if not handler:
            raise HandlerNotFound(
                f"No handler registered for function '{request.function_name}'"
            )

        worker_id = request.context.worker_id or self.worker_id
        context = {
            "job_id": request.context.job_id,
            "job_try": request.context.attempt,
            "enqueue_time": request.context.enqueue_time,
            "settings": self.settings,
            "worker_id": worker_id,
            "queue_name": request.context.queue_name,
            "rrq_client": self.client,
            "deadline": request.context.deadline,
            "trace_context": request.context.trace_context,
        }

        telemetry = get_telemetry()
        start_time = time.monotonic()
        span_cm = telemetry.executor_span(
            job_id=request.context.job_id,
            function_name=request.function_name,
            queue_name=request.context.queue_name,
            attempt=request.context.attempt,
            trace_context=request.context.trace_context,
            worker_id=worker_id,
        )

        with span_cm as span:
            try:
                logger.debug(
                    "Calling handler '%s' for job %s",
                    request.function_name,
                    request.job_id,
                )
                result = await handler(context, *request.args, **request.kwargs)
                logger.debug(
                    "Handler for job %s returned successfully.", request.job_id
                )
                span.success(duration_seconds=time.monotonic() - start_time)
                return ExecutionOutcome(
                    job_id=request.job_id, status="success", result=result
                )
            except RetryJob as exc:
                logger.info("Job %s requested retry: %s", request.job_id, exc)
                span.retry(
                    duration_seconds=time.monotonic() - start_time,
                    delay_seconds=exc.defer_seconds,
                    reason=str(exc) or None,
                )
                return ExecutionOutcome(
                    job_id=request.job_id,
                    status="retry",
                    error_message=str(exc) or None,
                    retry_after_seconds=exc.defer_seconds,
                )
            except (asyncio.TimeoutError, TimeoutError) as exc:
                error_message = str(exc) or "Job execution timed out."
                logger.warning(
                    "Job %s execution timed out: %s", request.job_id, error_message
                )
                span.timeout(
                    duration_seconds=time.monotonic() - start_time,
                    timeout_seconds=None,
                    error_message=error_message,
                )
                return ExecutionOutcome(
                    job_id=request.job_id,
                    status="timeout",
                    error_message=error_message,
                )
            except Exception as exc:
                logger.error(
                    "Job %s handler '%s' raised unhandled exception:",
                    request.job_id,
                    request.function_name,
                    exc_info=exc,
                )
                span.error(duration_seconds=time.monotonic() - start_time, error=exc)
                return ExecutionOutcome(
                    job_id=request.job_id,
                    status="error",
                    error_message=str(exc) or "Unhandled handler error",
                )

    async def cancel(self, job_id: str) -> None:
        return None

    async def close(self) -> None:
        return None


class QueueRoutingExecutor:
    """Routes jobs to executors based on queue name."""

    def __init__(
        self,
        *,
        default: Executor,
        routes: Mapping[str, Executor] | None = None,
    ) -> None:
        self._default = default
        self._routes = dict(routes or {})
        self._executors = self._dedupe_executors()

    def _dedupe_executors(self) -> list[Executor]:
        executors: list[Executor] = []
        seen: set[int] = set()
        for executor in [self._default, *self._routes.values()]:
            executor_id = id(executor)
            if executor_id in seen:
                continue
            seen.add(executor_id)
            executors.append(executor)
        return executors

    def _select_executor(self, request: ExecutionRequest) -> Executor:
        return self._routes.get(request.context.queue_name, self._default)

    async def execute(self, request: ExecutionRequest) -> ExecutionOutcome:
        executor = self._select_executor(request)
        return await executor.execute(request)

    async def cancel(self, job_id: str) -> None:
        for executor in self._executors:
            try:
                await executor.cancel(job_id)
            except Exception as exc:
                logger.debug("Executor cancel failed: %s", exc)

    async def close(self) -> None:
        for executor in self._executors:
            await executor.close()


@dataclass(frozen=True)
class _StdioProcess:
    process: Process
    stderr_task: asyncio.Task | None = None


@dataclass
class StdioReservation:
    pool: "StdioExecutorPool"
    stdio_proc: _StdioProcess
    _released: bool = False

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self.pool.release(self.stdio_proc)

    async def invalidate(self) -> None:
        if self._released:
            return
        self._released = True
        await self.pool.invalidate(self.stdio_proc)


class StdioExecutorPool:
    """Manage a pool of long-lived stdio executor processes."""

    def __init__(
        self,
        *,
        cmd: Sequence[str],
        pool_size: int,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        stderr_prefix: str | None = None,
    ) -> None:
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")
        if not cmd:
            raise ValueError("cmd must not be empty")
        self._cmd = list(cmd)
        self._pool_size = pool_size
        self._env = env
        self._cwd = cwd
        self._stderr_prefix = stderr_prefix or "executor"
        self._idle: asyncio.Queue[_StdioProcess] = asyncio.Queue()
        self._all: set[_StdioProcess] = set()
        self._closing = False
        self._closing_event = asyncio.Event()
        self._start_lock = asyncio.Lock()

    async def _spawn_process(self) -> _StdioProcess:
        env = None
        if self._env is not None:
            env = os.environ.copy()
            env.update(self._env)
        proc = await asyncio.create_subprocess_exec(
            *self._cmd,
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            env=env,
            cwd=self._cwd,
        )
        if proc.stdin is None or proc.stdout is None:
            raise RuntimeError("Executor process missing stdio pipes")
        stdio_proc = _StdioProcess(process=proc)
        if proc.stderr is not None:
            stderr_task = asyncio.create_task(
                self._pump_stderr(stdio_proc), name="rrq-executor-stderr"
            )
            stdio_proc = _StdioProcess(process=proc, stderr_task=stderr_task)
        return stdio_proc

    async def _pump_stderr(self, stdio_proc: _StdioProcess) -> None:
        stderr = stdio_proc.process.stderr
        if stderr is None:
            return
        prefix = self._stderr_prefix
        while True:
            try:
                line = await stderr.readline()
            except asyncio.CancelledError:
                break
            if not line:
                break
            message = line.decode("utf-8", errors="replace").rstrip()
            logger.warning("[%s] %s", prefix, message)

    async def _ensure_started(self) -> None:
        if self._all:
            return
        async with self._start_lock:
            if self._all:
                return
            for _ in range(self._pool_size):
                stdio_proc = await self._spawn_process()
                self._all.add(stdio_proc)
                await self._idle.put(stdio_proc)

    async def acquire(self) -> _StdioProcess:
        if self._closing:
            raise RuntimeError("Executor pool is closing")
        await self._ensure_started()
        while True:
            stdio_proc = await self._wait_for_idle()
            if stdio_proc.process.returncode is None:
                return stdio_proc
            await self._replace(stdio_proc)

    async def release(self, stdio_proc: _StdioProcess) -> None:
        if stdio_proc.process.returncode is not None:
            await self._replace(stdio_proc)
            return
        if self._closing:
            await self._terminate(stdio_proc)
            return
        await self._idle.put(stdio_proc)

    async def try_acquire(self) -> _StdioProcess | None:
        if self._closing:
            return None
        await self._ensure_started()
        while True:
            try:
                stdio_proc = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                return None
            if stdio_proc.process.returncode is None:
                return stdio_proc
            await self._replace(stdio_proc)

    async def _replace(self, stdio_proc: _StdioProcess) -> None:
        await self._terminate(stdio_proc)
        if self._closing:
            return
        replacement = await self._spawn_process()
        self._all.add(replacement)
        await self._idle.put(replacement)

    async def invalidate(self, stdio_proc: _StdioProcess) -> None:
        await self._replace(stdio_proc)

    async def _terminate(
        self, stdio_proc: _StdioProcess, *, grace_seconds: float = 1.0
    ) -> None:
        self._all.discard(stdio_proc)
        if stdio_proc.stderr_task is not None:
            stdio_proc.stderr_task.cancel()
            with suppress(asyncio.CancelledError):
                await stdio_proc.stderr_task
        proc = stdio_proc.process
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=grace_seconds)
            except asyncio.TimeoutError:
                proc.kill()
                with suppress(Exception):
                    await proc.wait()

    async def _wait_for_idle(self) -> _StdioProcess:
        while True:
            if self._closing:
                raise RuntimeError("Executor pool is closing")
            get_task = asyncio.create_task(self._idle.get())
            close_task = asyncio.create_task(self._closing_event.wait())
            done, pending = await asyncio.wait(
                {get_task, close_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if close_task in done:
                get_task.cancel()
                with suppress(asyncio.CancelledError):
                    await get_task
                raise RuntimeError("Executor pool is closing")
            close_task.cancel()
            with suppress(asyncio.CancelledError):
                await close_task
            return get_task.result()

    async def close(self, *, grace_seconds: float = 5.0) -> None:
        self._closing = True
        self._closing_event.set()
        while not self._idle.empty():
            stdio_proc = self._idle.get_nowait()
            await self._terminate(stdio_proc, grace_seconds=grace_seconds)
        for stdio_proc in list(self._all):
            await self._terminate(stdio_proc, grace_seconds=grace_seconds)


class StdioExecutor:
    """Executes jobs by sending requests to a stdio executor process pool."""

    def __init__(
        self,
        *,
        cmd: Sequence[str],
        pool_size: int,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        stderr_prefix: str | None = None,
        response_timeout_seconds: float | None = None,
    ) -> None:
        self._pool = StdioExecutorPool(
            cmd=cmd,
            pool_size=pool_size,
            env=env,
            cwd=cwd,
            stderr_prefix=stderr_prefix,
        )
        self._response_timeout_seconds = response_timeout_seconds

    async def execute(self, request: ExecutionRequest) -> ExecutionOutcome:
        stdio_proc = await self._pool.acquire()
        replaced = False
        try:
            payload = request.model_dump(mode="json")
            data = json.dumps(payload).encode("utf-8") + b"\n"
            stdin = stdio_proc.process.stdin
            stdout = stdio_proc.process.stdout
            if stdin is None or stdout is None:
                raise RuntimeError("Executor process missing stdio pipes")
            stdin.write(data)
            await stdin.drain()
            read = stdout.readline()
            if self._response_timeout_seconds is not None:
                line = await asyncio.wait_for(
                    read, timeout=self._response_timeout_seconds
                )
            else:
                line = await read
            if not line:
                raise RuntimeError("Executor process exited")
            outcome = ExecutionOutcome.model_validate_json(line)
            if outcome.job_id is None:
                raise RuntimeError("Executor outcome missing job_id")
            if outcome.job_id != request.job_id:
                raise RuntimeError(
                    f"Executor outcome job_id mismatch (expected {request.job_id}, got {outcome.job_id})"
                )
            return outcome
        except asyncio.CancelledError:
            await self._pool.invalidate(stdio_proc)
            replaced = True
            raise
        except Exception as exc:
            await self._pool.invalidate(stdio_proc)
            replaced = True
            return ExecutionOutcome(
                job_id=request.job_id, status="error", error_message=str(exc)
            )
        finally:
            if not replaced:
                await self._pool.release(stdio_proc)

    async def try_reserve(self) -> StdioReservation | None:
        stdio_proc = await self._pool.try_acquire()
        if stdio_proc is None:
            return None
        return StdioReservation(pool=self._pool, stdio_proc=stdio_proc)

    async def execute_with_reservation(
        self,
        reservation: StdioReservation,
        request: ExecutionRequest,
    ) -> ExecutionOutcome:
        stdio_proc = reservation.stdio_proc
        replaced = False
        try:
            payload = request.model_dump(mode="json")
            data = json.dumps(payload).encode("utf-8") + b"\n"
            stdin = stdio_proc.process.stdin
            stdout = stdio_proc.process.stdout
            if stdin is None or stdout is None:
                raise RuntimeError("Executor process missing stdio pipes")
            stdin.write(data)
            await stdin.drain()
            read = stdout.readline()
            if self._response_timeout_seconds is not None:
                line = await asyncio.wait_for(
                    read, timeout=self._response_timeout_seconds
                )
            else:
                line = await read
            if not line:
                raise RuntimeError("Executor process exited")
            outcome = ExecutionOutcome.model_validate_json(line)
            if outcome.job_id is None:
                raise RuntimeError("Executor outcome missing job_id")
            if outcome.job_id != request.job_id:
                raise RuntimeError(
                    f"Executor outcome job_id mismatch (expected {request.job_id}, got {outcome.job_id})"
                )
            return outcome
        except asyncio.CancelledError:
            await reservation.invalidate()
            replaced = True
            raise
        except Exception as exc:
            await reservation.invalidate()
            replaced = True
            return ExecutionOutcome(
                job_id=request.job_id, status="error", error_message=str(exc)
            )
        finally:
            if not replaced:
                await reservation.release()

    async def cancel(self, job_id: str) -> None:
        return None

    async def close(self) -> None:
        await self._pool.close()


def resolve_executor_pool_sizes(
    settings: RRQSettings,
    *,
    watch_mode: bool = False,
    default_pool_size: int | None = None,
) -> dict[str, int]:
    if default_pool_size is None:
        default_pool_size = 1 if watch_mode else (os.cpu_count() or 1)

    pool_sizes: dict[str, int] = {}
    for name, config in settings.executors.items():
        if watch_mode:
            pool_size = 1
        else:
            pool_size = (
                config.pool_size if config.pool_size is not None else default_pool_size
            )
        if pool_size <= 0:
            raise ValueError(f"pool_size must be positive for executor '{name}'")
        pool_sizes[name] = pool_size
    return pool_sizes


def build_executors_from_settings(
    settings: RRQSettings,
    *,
    pool_sizes: dict[str, int] | None = None,
    watch_mode: bool = False,
) -> dict[str, Executor]:
    executors: dict[str, Executor] = {}
    if pool_sizes is None:
        pool_sizes = resolve_executor_pool_sizes(settings, watch_mode=watch_mode)
    for name, config in settings.executors.items():
        if config.type != "stdio":
            raise ValueError(f"Unknown executor type '{config.type}' for '{name}'")
        if not config.cmd:
            raise ValueError(f"Executor '{name}' requires cmd for stdio mode")
        pool_size = pool_sizes.get(name)
        if pool_size is None:
            raise ValueError(f"Missing pool size for executor '{name}'")
        executors[name] = StdioExecutor(
            cmd=config.cmd,
            pool_size=pool_size,
            env=config.env,
            cwd=config.cwd,
            stderr_prefix=name,
            response_timeout_seconds=config.response_timeout_seconds,
        )
    return executors
