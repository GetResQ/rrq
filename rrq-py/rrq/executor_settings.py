"""Settings model for Python executor runtime."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from pydantic import BaseModel

from .registry import JobRegistry


class PythonExecutorSettings(BaseModel):
    """Configuration for the Python executor runtime."""

    job_registry: JobRegistry
    on_startup: Callable[[], Awaitable[None] | None] | None = None
    on_shutdown: Callable[[], Awaitable[None] | None] | None = None

    model_config = {
        "arbitrary_types_allowed": True,
    }
