"""Executor interfaces and Python implementation for RRQ job execution."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from asyncio.subprocess import PIPE, DEVNULL, Process
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import uuid4
from pydantic import BaseModel, Field

from .client import RRQClient
from .exc import HandlerNotFound, RetryJob
from .protocol import read_message, write_message
from .registry import JobRegistry
from .settings import RRQSettings
from .telemetry import get_telemetry

logger = logging.getLogger(__name__)

ENV_EXECUTOR_SOCKET = "RRQ_EXECUTOR_SOCKET"
DEFAULT_SOCKET_CONNECT_TIMEOUT_SECONDS = 5.0
SOCKET_PATH_MAX_BYTES = (
    104
    if sys.platform == "darwin"
    else 108
    if sys.platform.startswith("linux")
    else None
)


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
    request_id: str
    job_id: str
    function_name: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    context: ExecutionContext


class ExecutionError(BaseModel):
    """Structured error information from an executor."""

    message: str
    type: str | None = None
    code: str | None = None
    details: dict[str, Any] | None = None


class ExecutionOutcome(BaseModel):
    """Result returned by an executor."""

    job_id: str | None = None
    request_id: str | None = None
    status: Literal["success", "retry", "timeout", "error"]
    result: Any | None = None
    error: ExecutionError | None = None
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
                    job_id=request.job_id,
                    request_id=request.request_id,
                    status="success",
                    result=result,
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
                    request_id=request.request_id,
                    status="retry",
                    error=ExecutionError(message=str(exc) or "Job requested retry"),
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
                    request_id=request.request_id,
                    status="timeout",
                    error=ExecutionError(message=error_message),
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
                    request_id=request.request_id,
                    status="error",
                    error=ExecutionError(message=str(exc) or "Unhandled handler error"),
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


@dataclass(eq=False)
class _SocketProcess:
    process: Process
    socket_path: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    stdout_task: asyncio.Task | None = None
    stderr_task: asyncio.Task | None = None


@dataclass
class SocketReservation:
    pool: "SocketExecutorPool"
    socket_proc: _SocketProcess
    _released: bool = False

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self.pool.release(self.socket_proc)

    async def invalidate(self) -> None:
        if self._released:
            return
        self._released = True
        await self.pool.invalidate(self.socket_proc)


class SocketExecutorPool:
    """Manage a pool of long-lived Unix socket executor processes."""

    def __init__(
        self,
        *,
        cmd: Sequence[str],
        pool_size: int,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        stderr_prefix: str | None = None,
        socket_dir: str | None = None,
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
        self._socket_dir = socket_dir
        self._owned_socket_dir: Path | None = None
        self._idle: asyncio.Queue[_SocketProcess] = asyncio.Queue()
        self._all: set[_SocketProcess] = set()
        self._closing = False
        self._closing_event = asyncio.Event()
        self._start_lock = asyncio.Lock()

    async def _spawn_process(self) -> _SocketProcess:
        socket_path = self._next_socket_path()
        env = None
        if self._env is not None:
            env = os.environ.copy()
            env.update(self._env)
        else:
            env = os.environ.copy()
        env[ENV_EXECUTOR_SOCKET] = socket_path
        proc = await asyncio.create_subprocess_exec(
            *self._cmd,
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            env=env,
            cwd=self._cwd,
        )
        stdout_task = None
        stderr_task = None
        if proc.stdout is not None:
            stdout_task = asyncio.create_task(
                self._pump_output(proc.stdout, "stdout"),
                name="rrq-executor-stdout",
            )
        if proc.stderr is not None:
            stderr_task = asyncio.create_task(
                self._pump_output(proc.stderr, "stderr"),
                name="rrq-executor-stderr",
            )

        try:
            reader, writer = await self._connect_socket(socket_path, proc)
        except Exception:
            if stdout_task is not None:
                stdout_task.cancel()
            if stderr_task is not None:
                stderr_task.cancel()
            proc.kill()
            with suppress(Exception):
                await proc.wait()
            raise

        return _SocketProcess(
            process=proc,
            socket_path=socket_path,
            reader=reader,
            writer=writer,
            stdout_task=stdout_task,
            stderr_task=stderr_task,
        )

    def _socket_dir_path(self) -> Path:
        if self._socket_dir:
            path = self._resolve_socket_dir()
            path.mkdir(parents=True, exist_ok=True)
            return path
        if self._owned_socket_dir is None:
            self._owned_socket_dir = self._make_temp_socket_dir()
        return self._owned_socket_dir

    def _next_socket_path(self) -> str:
        path = self._socket_dir_path() / f"exec-{uuid4().hex}.sock"
        self._validate_socket_path(path)
        if path.exists():
            path.unlink()
        return str(path)

    def _resolve_base_dir(self) -> Path:
        if self._cwd:
            base = Path(self._cwd).expanduser()
            if not base.is_absolute():
                base = (Path.cwd() / base).resolve()
            else:
                base = base.resolve()
            return base
        return Path.cwd()

    def _resolve_socket_dir(self) -> Path:
        path = Path(self._socket_dir).expanduser()
        if not path.is_absolute():
            path = self._resolve_base_dir() / path
        return path.resolve()

    def _make_temp_socket_dir(self) -> Path:
        base_dir = Path("/tmp")
        if not base_dir.is_dir():
            base_dir = Path(tempfile.gettempdir())
        return Path(tempfile.mkdtemp(prefix="rrq-executor-", dir=str(base_dir)))

    def _validate_socket_path(self, path: Path) -> None:
        if SOCKET_PATH_MAX_BYTES is None:
            return
        encoded_len = len(str(path).encode("utf-8"))
        if encoded_len > SOCKET_PATH_MAX_BYTES:
            raise RuntimeError(
                "Executor socket path is too long "
                f"({encoded_len} > {SOCKET_PATH_MAX_BYTES} bytes). "
                "Set socket_dir to a shorter absolute path, e.g. /tmp/rrq."
            )

    async def _connect_socket(
        self,
        socket_path: str,
        proc: Process,
        *,
        timeout_seconds: float = DEFAULT_SOCKET_CONNECT_TIMEOUT_SECONDS,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        while True:
            if proc.returncode is not None:
                raise RuntimeError("Executor process exited before socket ready")
            try:
                return await asyncio.open_unix_connection(socket_path)
            except (FileNotFoundError, ConnectionRefusedError, OSError) as exc:
                last_error = exc
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Executor socket not ready: {last_error}"
                ) from last_error
            await asyncio.sleep(0.05)

    async def _pump_output(self, stream: asyncio.StreamReader, label: str) -> None:
        prefix = self._stderr_prefix
        while True:
            try:
                line = await stream.readline()
            except asyncio.CancelledError:
                break
            if not line:
                break
            message = line.decode("utf-8", errors="replace").rstrip()
            logger.warning("[%s:%s] %s", prefix, label, message)

    async def _ensure_started(self) -> None:
        if self._all:
            return
        async with self._start_lock:
            if self._all:
                return
            for _ in range(self._pool_size):
                socket_proc = await self._spawn_process()
                self._all.add(socket_proc)
                await self._idle.put(socket_proc)

    async def acquire(self) -> _SocketProcess:
        if self._closing:
            raise RuntimeError("Executor pool is closing")
        await self._ensure_started()
        while True:
            socket_proc = await self._wait_for_idle()
            if socket_proc.process.returncode is None:
                return socket_proc
            await self._replace(socket_proc)

    async def release(self, socket_proc: _SocketProcess) -> None:
        if socket_proc.process.returncode is not None:
            await self._replace(socket_proc)
            return
        if self._closing:
            await self._terminate(socket_proc)
            return
        await self._idle.put(socket_proc)

    async def try_acquire(self) -> _SocketProcess | None:
        if self._closing:
            return None
        await self._ensure_started()
        while True:
            try:
                socket_proc = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                return None
            if socket_proc.process.returncode is None:
                return socket_proc
            await self._replace(socket_proc)

    async def _replace(self, socket_proc: _SocketProcess) -> None:
        await self._terminate(socket_proc)
        if self._closing:
            return
        replacement = await self._spawn_process()
        self._all.add(replacement)
        await self._idle.put(replacement)

    async def invalidate(self, socket_proc: _SocketProcess) -> None:
        await self._replace(socket_proc)

    async def _terminate(
        self, socket_proc: _SocketProcess, *, grace_seconds: float = 1.0
    ) -> None:
        self._all.discard(socket_proc)
        socket_proc.writer.close()
        with suppress(Exception):
            await socket_proc.writer.wait_closed()
        if socket_proc.stdout_task is not None:
            socket_proc.stdout_task.cancel()
            with suppress(asyncio.CancelledError):
                await socket_proc.stdout_task
        if socket_proc.stderr_task is not None:
            socket_proc.stderr_task.cancel()
            with suppress(asyncio.CancelledError):
                await socket_proc.stderr_task
        proc = socket_proc.process
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=grace_seconds)
            except asyncio.TimeoutError:
                proc.kill()
                with suppress(Exception):
                    await proc.wait()
        socket_path = Path(socket_proc.socket_path)
        if socket_path.exists():
            with suppress(OSError):
                socket_path.unlink()

    async def _wait_for_idle(self) -> _SocketProcess:
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
            socket_proc = self._idle.get_nowait()
            await self._terminate(socket_proc, grace_seconds=grace_seconds)
        for socket_proc in list(self._all):
            await self._terminate(socket_proc, grace_seconds=grace_seconds)
        if self._owned_socket_dir is not None:
            shutil.rmtree(self._owned_socket_dir, ignore_errors=True)


class SocketExecutor:
    """Executes jobs by sending requests to Unix socket executor processes."""

    def __init__(
        self,
        *,
        cmd: Sequence[str],
        pool_size: int,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        stderr_prefix: str | None = None,
        socket_dir: str | None = None,
        response_timeout_seconds: float | None = None,
    ) -> None:
        self._pool = SocketExecutorPool(
            cmd=cmd,
            pool_size=pool_size,
            env=env,
            cwd=cwd,
            stderr_prefix=stderr_prefix,
            socket_dir=socket_dir,
        )
        self._response_timeout_seconds = response_timeout_seconds

    async def execute(self, request: ExecutionRequest) -> ExecutionOutcome:
        socket_proc = await self._pool.acquire()
        replaced = False
        try:
            payload = request.model_dump(mode="json")
            await write_message(socket_proc.writer, "request", payload)
            outcome = await self._read_response(socket_proc.reader, request)
            return outcome
        except asyncio.CancelledError:
            await self._pool.invalidate(socket_proc)
            replaced = True
            raise
        except Exception as exc:
            await self._pool.invalidate(socket_proc)
            replaced = True
            return ExecutionOutcome(
                job_id=request.job_id,
                request_id=request.request_id,
                status="error",
                error=ExecutionError(message=str(exc)),
            )
        finally:
            if not replaced:
                await self._pool.release(socket_proc)

    async def try_reserve(self) -> SocketReservation | None:
        socket_proc = await self._pool.try_acquire()
        if socket_proc is None:
            return None
        return SocketReservation(pool=self._pool, socket_proc=socket_proc)

    async def execute_with_reservation(
        self,
        reservation: SocketReservation,
        request: ExecutionRequest,
    ) -> ExecutionOutcome:
        socket_proc = reservation.socket_proc
        replaced = False
        try:
            payload = request.model_dump(mode="json")
            await write_message(socket_proc.writer, "request", payload)
            outcome = await self._read_response(socket_proc.reader, request)
            return outcome
        except asyncio.CancelledError:
            await reservation.invalidate()
            replaced = True
            raise
        except Exception as exc:
            await reservation.invalidate()
            replaced = True
            return ExecutionOutcome(
                job_id=request.job_id,
                request_id=request.request_id,
                status="error",
                error=ExecutionError(message=str(exc)),
            )
        finally:
            if not replaced:
                await reservation.release()

    async def cancel(self, job_id: str) -> None:
        return None

    async def close(self) -> None:
        await self._pool.close()

    async def _read_response(
        self,
        reader: asyncio.StreamReader,
        request: ExecutionRequest,
    ) -> ExecutionOutcome:
        deadline = None
        if self._response_timeout_seconds is not None:
            deadline = time.monotonic() + self._response_timeout_seconds
        read = read_message(reader)
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Executor response timeout")
            message = await asyncio.wait_for(read, timeout=remaining)
        else:
            message = await read
        if message is None:
            raise RuntimeError("Executor process exited")
        message_type, payload = message
        if message_type != "response":
            raise RuntimeError(f"Unexpected executor message type: {message_type}")
        outcome = ExecutionOutcome.model_validate(payload)
        if outcome.job_id is None:
            raise RuntimeError("Executor outcome missing job_id")
        if outcome.request_id is None:
            raise RuntimeError("Executor outcome missing request_id")
        if outcome.job_id != request.job_id:
            raise RuntimeError(
                f"Executor outcome job_id mismatch (expected {request.job_id}, got {outcome.job_id})"
            )
        if outcome.request_id != request.request_id:
            raise RuntimeError(
                "Executor outcome request_id mismatch "
                f"(expected {request.request_id}, got {outcome.request_id})"
            )
        return outcome


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
        if config.type != "socket":
            raise ValueError(f"Unknown executor type '{config.type}' for '{name}'")
        if not config.cmd:
            raise ValueError(f"Executor '{name}' requires cmd for socket mode")
        pool_size = pool_sizes.get(name)
        if pool_size is None:
            raise ValueError(f"Missing pool size for executor '{name}'")
        executors[name] = SocketExecutor(
            cmd=config.cmd,
            pool_size=pool_size,
            env=config.env,
            cwd=config.cwd,
            stderr_prefix=name,
            socket_dir=config.socket_dir,
            response_timeout_seconds=config.response_timeout_seconds,
        )
    return executors
