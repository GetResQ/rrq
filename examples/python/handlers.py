"""Example handlers for RRQ Python runner."""

from __future__ import annotations

import asyncio
from typing import Any

from rrq.exc import RetryJob


async def quick_task(
    ctx: dict[str, Any], message: str, source: str | None = None
) -> dict[str, Any]:
    await asyncio.sleep(0.05)
    return {"message": message, "job_id": ctx["job_id"], "source": source}


async def slow_task(ctx: dict[str, Any], seconds: float = 1.0) -> dict[str, Any]:
    await asyncio.sleep(seconds)
    return {"slept": seconds, "job_id": ctx["job_id"]}


async def error_task(ctx: dict[str, Any]) -> None:
    raise ValueError(f"boom ({ctx['job_id']})")


async def retry_task(ctx: dict[str, Any], until_attempt: int = 2) -> dict[str, Any]:
    if ctx["job_try"] < until_attempt:
        raise RetryJob("retry requested", defer_seconds=0.2)
    return {"attempt": ctx["job_try"], "job_id": ctx["job_id"]}
