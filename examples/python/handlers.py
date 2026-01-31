"""Example handlers for RRQ Python runner."""

from __future__ import annotations

import asyncio
from typing import Any

from rrq.exc import RetryJob
from rrq.runner import ExecutionRequest


async def quick_task(request: ExecutionRequest) -> dict[str, Any]:
    await asyncio.sleep(0.05)
    message = request.args[0] if request.args else None
    source = request.kwargs.get("source") if request.kwargs else None
    return {
        "message": message,
        "job_id": request.context.job_id,
        "source": source,
    }


async def slow_task(request: ExecutionRequest) -> dict[str, Any]:
    seconds = 1.0
    if request.args:
        seconds = float(request.args[0])
    await asyncio.sleep(seconds)
    return {"slept": seconds, "job_id": request.context.job_id}


async def error_task(request: ExecutionRequest) -> None:
    raise ValueError(f"boom ({request.context.job_id})")


async def retry_task(request: ExecutionRequest) -> dict[str, Any]:
    until_attempt = 2
    if request.args:
        until_attempt = int(request.args[0])
    if request.context.attempt < until_attempt:
        raise RetryJob("retry requested", defer_seconds=0.2)
    return {
        "attempt": request.context.attempt,
        "job_id": request.context.job_id,
    }
