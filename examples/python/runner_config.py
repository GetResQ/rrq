"""Python runner configuration for RRQ examples."""

from __future__ import annotations

from rrq.runner_settings import PythonRunnerSettings
from rrq.registry import Registry

from . import handlers

registry = Registry()
registry.register("quick_task", handlers.quick_task)
registry.register("slow_task", handlers.slow_task)
registry.register("error_task", handlers.error_task)
registry.register("retry_task", handlers.retry_task)

python_runner_settings = PythonRunnerSettings(
    registry=registry,
)

__all__ = ["python_runner_settings"]
