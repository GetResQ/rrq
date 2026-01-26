from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable

import pytest
import pytest_asyncio

from rrq.client import RRQClient
from rrq.exc import RetryJob
from rrq.executor import ExecutionOutcome, ExecutionRequest, Executor
from rrq.job import Job, JobStatus
from rrq.settings import RRQSettings
from rrq.store import JobStore
from rrq.worker import RRQWorker

Handler = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class JobSpec:
    job_id: str | None
    function_name: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    max_retries: int | None = None
    job_timeout_seconds: int | None = None
    defer_by_seconds: float | None = None
    unique_key: str | None = None
    queue_name: str | None = None


@dataclass(frozen=True)
class JobSnapshot:
    status: JobStatus
    current_retries: int
    max_retries: int
    result: Any
    last_error: str | None
    in_dlq: bool
    queue_name: str | None
    unique_key: str | None
    deferred: bool


@dataclass(frozen=True)
class EngineRunResult:
    engine_name: str
    job_ids: list[str]
    jobs: dict[str, JobSnapshot]


class ScenarioExecutor(Executor):
    def __init__(
        self,
        handlers: dict[str, Handler],
        *,
        settings: RRQSettings,
        client: RRQClient | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.handlers = handlers
        self.settings = settings
        self.client = client
        self.worker_id = worker_id
        self.requests: list[ExecutionRequest] = []

    async def execute(self, request: ExecutionRequest) -> ExecutionOutcome:
        self.requests.append(request)
        handler = self.handlers.get(request.function_name)
        if handler is None:
            return ExecutionOutcome(
                job_id=request.job_id,
                status="error",
                error_message=(
                    f"No handler registered for function '{request.function_name}'"
                ),
                error_type="handler_not_found",
            )

        context = {
            "job_id": request.context.job_id,
            "job_try": request.context.attempt,
            "enqueue_time": request.context.enqueue_time,
            "settings": self.settings,
            "worker_id": self.worker_id,
            "queue_name": request.context.queue_name,
            "rrq_client": self.client,
        }

        try:
            result = await handler(context, *request.args, **request.kwargs)
            return ExecutionOutcome(
                job_id=request.job_id, status="success", result=result
            )
        except RetryJob as exc:
            return ExecutionOutcome(
                job_id=request.job_id,
                status="retry",
                error_message=str(exc) or None,
                retry_after_seconds=exc.defer_seconds,
            )
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, TimeoutError) as exc:
            return ExecutionOutcome(
                job_id=request.job_id,
                status="timeout",
                error_message=str(exc) or "Job execution timed out.",
            )
        except Exception as exc:  # noqa: BLE001 - tests need broad coverage
            return ExecutionOutcome(
                job_id=request.job_id,
                status="error",
                error_message=str(exc) or "Unhandled handler error",
            )

    async def cancel(self, job_id: str) -> None:
        return None

    async def close(self) -> None:
        return None


@pytest_asyncio.fixture(scope="function")
def redis_url() -> str:
    return "redis://localhost:6379/2"


@pytest_asyncio.fixture(scope="function")
def rrq_settings(redis_url: str) -> RRQSettings:
    return RRQSettings(
        redis_dsn=redis_url,
        default_job_timeout_seconds=1,
        default_result_ttl_seconds=10,
        default_max_retries=3,
        worker_concurrency=2,
        default_poll_delay_seconds=0.01,
        worker_shutdown_grace_period_seconds=0.2,
        base_retry_delay_seconds=0.01,
        max_retry_delay_seconds=0.05,
    )


def _snapshot_job(job: Job, *, in_dlq: bool) -> JobSnapshot:
    deferred = False
    if job.next_scheduled_run_time is not None:
        deferred = job.next_scheduled_run_time > (
            job.enqueue_time + timedelta(milliseconds=1)
        )
    return JobSnapshot(
        status=job.status,
        current_retries=job.current_retries,
        max_retries=job.max_retries,
        result=job.result,
        last_error=job.last_error,
        in_dlq=in_dlq,
        queue_name=job.queue_name,
        unique_key=job.job_unique_key,
        deferred=deferred,
    )


async def _wait_for_terminal(
    job_store: JobStore, job_ids: list[str], timeout_seconds: float
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        all_terminal = True
        for job_id in job_ids:
            job = await job_store.get_job_definition(job_id)
            if job is None:
                raise AssertionError(f"Missing job definition for {job_id}")
            if job.status not in {JobStatus.COMPLETED, JobStatus.FAILED}:
                all_terminal = False
                break
        if all_terminal:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Timed out waiting for jobs to reach terminal state")


async def _run_python_engine(
    settings: RRQSettings,
    job_specs: list[JobSpec],
    handlers: dict[str, Handler],
    *,
    timeout_seconds: float = 3.0,
    queues: list[str] | None = None,
    executor_handlers: dict[str, dict[str, Handler]] | None = None,
) -> EngineRunResult:
    job_store = JobStore(settings=settings)
    await job_store.redis.flushdb()
    client = RRQClient(settings=settings, job_store=job_store)
    if executor_handlers is None:
        executor_handlers = {"python": handlers}
    executors = {
        name: ScenarioExecutor(
            handler_map,
            settings=settings,
            client=client,
        )
        for name, handler_map in executor_handlers.items()
    }
    worker = RRQWorker(
        settings=settings,
        executors=executors,
        queues=queues,
    )
    worker.job_store = job_store
    worker.client.job_store = job_store
    for executor in executors.values():
        executor.worker_id = worker.worker_id
    worker._loop = asyncio.get_running_loop()

    job_ids: list[str] = []
    for spec in job_specs:
        defer_by = None
        if spec.defer_by_seconds is not None:
            defer_by = timedelta(seconds=spec.defer_by_seconds)
        job = await client.enqueue(
            spec.function_name,
            *spec.args,
            _queue_name=spec.queue_name,
            _job_id=spec.job_id,
            _unique_key=spec.unique_key,
            _max_retries=spec.max_retries,
            _job_timeout_seconds=spec.job_timeout_seconds,
            _defer_by=defer_by,
            **spec.kwargs,
        )
        assert job is not None
        job_ids.append(job.id)

    run_loop_task = asyncio.create_task(worker._run_loop())
    try:
        await _wait_for_terminal(job_store, job_ids, timeout_seconds)
    finally:
        worker._request_shutdown()
        try:
            await asyncio.wait_for(run_loop_task, timeout=timeout_seconds)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            run_loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await run_loop_task

    dlq_raw = await job_store.redis.lrange(settings.default_dlq_name, 0, -1)
    dlq_ids = {
        item.decode("utf-8") if isinstance(item, bytes) else str(item)
        for item in dlq_raw
    }

    snapshots: dict[str, JobSnapshot] = {}
    for job_id in job_ids:
        job = await job_store.get_job_definition(job_id)
        assert job is not None
        snapshots[job_id] = _snapshot_job(job, in_dlq=job_id in dlq_ids)

    await job_store.redis.flushdb()
    await job_store.aclose()

    return EngineRunResult(
        engine_name="python", job_ids=job_ids, jobs=snapshots
    )


def _normalize_snapshot(snapshot: JobSnapshot) -> dict[str, Any]:
    last_error = snapshot.last_error
    if snapshot.status == JobStatus.COMPLETED:
        last_error = None
    return {
        "status": snapshot.status.value,
        "current_retries": snapshot.current_retries,
        "max_retries": snapshot.max_retries,
        "result": snapshot.result,
        "last_error": last_error,
        "in_dlq": snapshot.in_dlq,
    }


def _normalize_snapshot_extended(snapshot: JobSnapshot) -> dict[str, Any]:
    payload = _normalize_snapshot(snapshot)
    payload.update(
        {
            "queue_name": snapshot.queue_name,
            "unique_key": snapshot.unique_key,
            "deferred": snapshot.deferred,
        }
    )
    return payload


def _normalize_run_result(
    result: EngineRunResult, *, scenario: str
) -> dict[str, Any]:
    return {
        "scenario": scenario,
        "engine": result.engine_name,
        "jobs": {
            job_id: _normalize_snapshot(result.jobs[job_id])
            for job_id in sorted(result.jobs)
        },
    }


def _normalize_run_result_extended(
    result: EngineRunResult, *, scenario: str
) -> dict[str, Any]:
    return {
        "scenario": scenario,
        "engine": result.engine_name,
        "jobs": {
            job_id: _normalize_snapshot_extended(result.jobs[job_id])
            for job_id in sorted(result.jobs)
        },
    }


async def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(await asyncio.to_thread(path.read_text))


async def _write_json(path: Path, payload: dict[str, Any]) -> None:
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread(
        path.write_text, json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )


async def _assert_golden(path: Path, payload: dict[str, Any]) -> None:
    if os.getenv("RRQ_UPDATE_GOLDENS") == "1":
        await _write_json(path, payload)
        return
    exists = await asyncio.to_thread(path.exists)
    if not exists:
        raise AssertionError(
            f"Golden file missing at {path}. Set RRQ_UPDATE_GOLDENS=1 to create it."
        )
    expected = await _read_json(path)
    assert payload == expected


@pytest.mark.asyncio
async def test_engine_conformance_success(rrq_settings: RRQSettings) -> None:
    async def handler(ctx: dict[str, Any], value: str) -> str:
        return f"ok:{value}"

    result = await _run_python_engine(
        rrq_settings,
        [
            JobSpec(
                job_id="job-success",
                function_name="success",
                args=["alpha"],
            )
        ],
        {"success": handler},
    )

    job_id = "job-success"
    snapshot = result.jobs[job_id]
    assert snapshot.status == JobStatus.COMPLETED
    assert snapshot.current_retries == 0
    assert snapshot.result == "ok:alpha"
    assert snapshot.in_dlq is False


@pytest.mark.asyncio
async def test_engine_conformance_retry_then_success(
    rrq_settings: RRQSettings,
) -> None:
    async def handler(ctx: dict[str, Any], value: str) -> str:
        if ctx["job_try"] == 1:
            raise RetryJob("retry", defer_seconds=0.01)
        return f"ok:{value}"

    result = await _run_python_engine(
        rrq_settings,
        [
            JobSpec(
                job_id="job-retry",
                function_name="retry_then_success",
                args=["beta"],
            )
        ],
        {"retry_then_success": handler},
    )

    snapshot = result.jobs["job-retry"]
    assert snapshot.status == JobStatus.COMPLETED
    assert snapshot.current_retries == 1
    assert snapshot.result == "ok:beta"
    assert snapshot.in_dlq is False


@pytest.mark.asyncio
async def test_engine_conformance_failure_to_dlq(
    rrq_settings: RRQSettings,
) -> None:
    async def handler(ctx: dict[str, Any], value: str) -> str:
        raise ValueError(f"boom: {value}")

    result = await _run_python_engine(
        rrq_settings,
        [
            JobSpec(
                job_id="job-fail",
                function_name="fail_always",
                args=["gamma"],
                max_retries=2,
            )
        ],
        {"fail_always": handler},
    )

    snapshot = result.jobs["job-fail"]
    assert snapshot.status == JobStatus.FAILED
    assert snapshot.current_retries == 2
    assert snapshot.last_error is not None
    assert "boom: gamma" in snapshot.last_error
    assert snapshot.in_dlq is True


@pytest.mark.asyncio
async def test_engine_conformance_timeout_to_dlq(
    rrq_settings: RRQSettings,
) -> None:
    async def handler(ctx: dict[str, Any]) -> str:
        await asyncio.sleep(2)
        return "late"

    result = await _run_python_engine(
        rrq_settings,
        [
            JobSpec(
                job_id="job-timeout",
                function_name="timeout_task",
                job_timeout_seconds=1,
            )
        ],
        {"timeout_task": handler},
        timeout_seconds=4.0,
    )

    snapshot = result.jobs["job-timeout"]
    assert snapshot.status == JobStatus.FAILED
    assert snapshot.current_retries == 1
    assert snapshot.last_error is not None
    assert "timed out" in snapshot.last_error
    assert snapshot.in_dlq is True


@pytest.mark.asyncio
async def test_engine_conformance_handler_not_found_to_dlq(
    rrq_settings: RRQSettings,
) -> None:
    result = await _run_python_engine(
        rrq_settings,
        [
            JobSpec(
                job_id="job-missing",
                function_name="missing_handler",
            )
        ],
        {},
    )

    snapshot = result.jobs["job-missing"]
    assert snapshot.status == JobStatus.FAILED
    assert snapshot.current_retries == 1
    assert snapshot.last_error is not None
    assert "No handler registered" in snapshot.last_error
    assert snapshot.in_dlq is True


@pytest.mark.asyncio
async def test_engine_conformance_golden_basic(
    rrq_settings: RRQSettings,
) -> None:
    async def success_handler(ctx: dict[str, Any], value: str) -> str:
        return f"ok:{value}"

    async def retry_handler(ctx: dict[str, Any], value: str) -> str:
        if ctx["job_try"] == 1:
            raise RetryJob("retry", defer_seconds=0.01)
        return f"ok:{value}"

    async def fail_handler(ctx: dict[str, Any], value: str) -> str:
        raise ValueError(f"boom: {value}")

    async def timeout_handler(ctx: dict[str, Any]) -> str:
        await asyncio.sleep(2)
        return "late"

    job_specs = [
        JobSpec(job_id="job-success", function_name="success", args=["alpha"]),
        JobSpec(job_id="job-retry", function_name="retry", args=["beta"]),
        JobSpec(
            job_id="job-fail",
            function_name="fail",
            args=["gamma"],
            max_retries=2,
        ),
        JobSpec(
            job_id="job-timeout",
            function_name="timeout",
            job_timeout_seconds=1,
        ),
        JobSpec(job_id="job-missing", function_name="missing_handler"),
    ]
    handlers = {
        "success": success_handler,
        "retry": retry_handler,
        "fail": fail_handler,
        "timeout": timeout_handler,
    }

    result = await _run_python_engine(
        rrq_settings,
        job_specs,
        handlers,
        timeout_seconds=4.0,
        queues=[rrq_settings.default_queue_name, "rrq:queue:custom"],
    )
    payload = _normalize_run_result(result, scenario="basic")
    golden_path = Path(__file__).parent / "data" / "engine_golden" / "basic.json"
    await _assert_golden(golden_path, payload)


@pytest.mark.asyncio
async def test_engine_conformance_golden_extended(
    rrq_settings: RRQSettings,
) -> None:
    async def success_handler(ctx: dict[str, Any], value: str) -> str:
        return f"ok:{value}"

    async def fail_handler(ctx: dict[str, Any], value: str) -> str:
        raise ValueError(f"boom: {value}")

    rrq_settings = rrq_settings.model_copy()
    rrq_settings.default_unique_job_lock_ttl_seconds = 1
    rrq_settings.default_poll_delay_seconds = 0.01
    rrq_settings.base_retry_delay_seconds = 0.01
    rrq_settings.max_retry_delay_seconds = 0.05
    rrq_settings.worker_shutdown_grace_period_seconds = 0.2
    rrq_settings.default_job_timeout_seconds = 1
    rrq_settings.default_result_ttl_seconds = 10

    job_specs = [
        JobSpec(
            job_id="job-unique-1",
            function_name="success",
            args=["alpha"],
            unique_key="unique-alpha",
        ),
        JobSpec(
            job_id="job-unique-2",
            function_name="success",
            args=["beta"],
            unique_key="unique-alpha",
        ),
        JobSpec(
            job_id="job-defer",
            function_name="success",
            args=["gamma"],
            defer_by_seconds=1.0,
        ),
        JobSpec(
            job_id="job-custom-queue",
            function_name="success",
            args=["delta"],
            queue_name="rrq:queue:custom",
        ),
        JobSpec(
            job_id="job-dlq",
            function_name="fail",
            args=["epsilon"],
            max_retries=1,
        ),
    ]
    handlers = {
        "success": success_handler,
        "fail": fail_handler,
    }

    result = await _run_python_engine(
        rrq_settings,
        job_specs,
        handlers,
        timeout_seconds=4.0,
        queues=[rrq_settings.default_queue_name, "rrq:queue:custom"],
    )
    payload = _normalize_run_result_extended(result, scenario="extended")
    golden_path = Path(__file__).parent / "data" / "engine_golden" / "extended.json"
    await _assert_golden(golden_path, payload)


@pytest.mark.asyncio
async def test_engine_conformance_golden_retry_backoff(
    rrq_settings: RRQSettings,
) -> None:
    async def fail_handler(ctx: dict[str, Any], value: str) -> str:
        raise ValueError(f"boom: {value}")

    rrq_settings = rrq_settings.model_copy()
    rrq_settings.default_poll_delay_seconds = 0.01
    rrq_settings.base_retry_delay_seconds = 0.01
    rrq_settings.max_retry_delay_seconds = 0.05
    rrq_settings.worker_shutdown_grace_period_seconds = 0.2
    rrq_settings.default_result_ttl_seconds = 10

    job_specs = [
        JobSpec(
            job_id="job-retry-backoff",
            function_name="fail",
            args=["backoff"],
            max_retries=3,
        )
    ]
    handlers = {"fail": fail_handler}

    result = await _run_python_engine(
        rrq_settings,
        job_specs,
        handlers,
        timeout_seconds=4.0,
    )
    payload = _normalize_run_result(result, scenario="retry_backoff")
    golden_path = (
        Path(__file__).parent / "data" / "engine_golden" / "retry_backoff.json"
    )
    await _assert_golden(golden_path, payload)


@pytest.mark.asyncio
async def test_engine_conformance_golden_routing(
    rrq_settings: RRQSettings,
) -> None:
    async def py_handler(ctx: dict[str, Any], value: str) -> str:
        return f"py:{value}"

    async def alt_handler(ctx: dict[str, Any], value: str) -> str:
        return f"alt:{value}"

    rrq_settings = rrq_settings.model_copy()
    rrq_settings.executor_routes = {"rrq:queue:routed": "alt"}
    rrq_settings.default_poll_delay_seconds = 0.01
    rrq_settings.worker_shutdown_grace_period_seconds = 0.2

    job_specs = [
        JobSpec(
            job_id="job-route-default",
            function_name="route",
            args=["alpha"],
        ),
        JobSpec(
            job_id="job-route-alt",
            function_name="route",
            args=["beta"],
            queue_name="rrq:queue:routed",
        ),
    ]

    result = await _run_python_engine(
        rrq_settings,
        job_specs,
        handlers={},
        executor_handlers={
            "python": {"route": py_handler},
            "alt": {"route": alt_handler},
        },
        timeout_seconds=3.0,
        queues=[rrq_settings.default_queue_name, "rrq:queue:routed"],
    )
    payload = _normalize_run_result_extended(result, scenario="routing")
    golden_path = (
        Path(__file__).parent / "data" / "engine_golden" / "routing.json"
    )
    await _assert_golden(golden_path, payload)
