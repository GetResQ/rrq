from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import cast

import pytest

from rrq.client import RRQClient
from rrq.executor import ExecutionContext, ExecutionRequest, PythonExecutor
from rrq.executor_runtime import _execute_with_deadline
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
