"""Python executor configuration for RRQ examples."""

from __future__ import annotations

import os
from pathlib import Path

from rrq.config import load_toml_settings
from rrq.executor_settings import PythonExecutorSettings
from rrq.registry import JobRegistry

from . import handlers

CONFIG_PATH = Path(
    os.getenv("RRQ_EXECUTOR_CONFIG", Path(__file__).with_name("rrq.toml"))
)

job_registry = JobRegistry()
job_registry.register("quick_task", handlers.quick_task)
job_registry.register("slow_task", handlers.slow_task)
job_registry.register("error_task", handlers.error_task)
job_registry.register("retry_task", handlers.retry_task)

rrq_settings = load_toml_settings(str(CONFIG_PATH))

python_executor_settings = PythonExecutorSettings(
    rrq_settings=rrq_settings,
    job_registry=job_registry,
)

__all__ = ["python_executor_settings"]
