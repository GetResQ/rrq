"""Python runner configuration for RRQ examples."""

from __future__ import annotations

from rrq.runner_settings import PythonRunnerSettings
from rrq.registry import JobRegistry

from . import handlers

job_registry = JobRegistry()
job_registry.register("quick_task", handlers.quick_task)
job_registry.register("slow_task", handlers.slow_task)
job_registry.register("error_task", handlers.error_task)
job_registry.register("retry_task", handlers.retry_task)

python_runner_settings = PythonRunnerSettings(
    job_registry=job_registry,
)

__all__ = ["python_runner_settings"]
