"""Settings model for Python runner runtime."""

from __future__ import annotations

from pydantic import BaseModel

from .registry import Registry


class PythonRunnerSettings(BaseModel):
    """Configuration for the Python runner runtime."""

    registry: Registry

    model_config = {
        "arbitrary_types_allowed": True,
    }
