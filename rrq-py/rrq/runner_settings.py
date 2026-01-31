"""Settings model for Python runner runtime."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from pydantic import BaseModel

from .registry import JobRegistry


class PythonRunnerSettings(BaseModel):
    """Configuration for the Python runner runtime."""

    job_registry: JobRegistry
    on_startup: Callable[[], Awaitable[None] | None] | None = None
    on_shutdown: Callable[[], Awaitable[None] | None] | None = None

    model_config = {
        "arbitrary_types_allowed": True,
    }
