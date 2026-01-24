"""Settings model for Python executor runtime."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from pydantic import BaseModel, Field

from .registry import JobRegistry
from .settings import RRQSettings


class PythonExecutorSettings(BaseModel):
    """Configuration for the Python executor runtime."""

    rrq_settings: RRQSettings = Field(default_factory=RRQSettings)
    job_registry: JobRegistry
    on_startup: Callable[[], Awaitable[None] | None] | None = None
    on_shutdown: Callable[[], Awaitable[None] | None] | None = None

    model_config = {
        "arbitrary_types_allowed": True,
    }
