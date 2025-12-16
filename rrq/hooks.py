"""Lightweight hooks system for RRQ monitoring and integrations"""

import asyncio
import importlib
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, cast

from .job import Job
from .settings import RRQSettings


logger = logging.getLogger(__name__)


class RRQHook(ABC):
    """Base class for RRQ hooks"""

    def __init__(self, settings: RRQSettings):
        self.settings = settings

    async def on_job_enqueued(self, job: Job) -> None:
        """Called when a job is enqueued"""
        pass

    async def on_job_started(self, job: Job, worker_id: str) -> None:
        """Called when a job starts processing"""
        pass

    async def on_job_completed(self, job: Job, result: Any) -> None:
        """Called when a job completes successfully"""
        pass

    async def on_job_failed(self, job: Job, error: Exception) -> None:
        """Called when a job fails"""
        pass

    async def on_job_retrying(self, job: Job, attempt: int) -> None:
        """Called when a job is being retried"""
        pass

    async def on_worker_started(self, worker_id: str, queues: list[str]) -> None:
        """Called when a worker starts"""
        pass

    async def on_worker_stopped(self, worker_id: str) -> None:
        """Called when a worker stops"""
        pass

    async def on_worker_heartbeat(
        self, worker_id: str, health_data: dict[str, Any]
    ) -> None:
        """Called on worker heartbeat"""
        pass


class MetricsExporter(ABC):
    """Base class for metrics exporters"""

    def __init__(self, settings: RRQSettings):
        self.settings = settings

    @abstractmethod
    async def export_counter(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        """Export a counter metric"""
        pass

    @abstractmethod
    async def export_gauge(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        """Export a gauge metric"""
        pass

    @abstractmethod
    async def export_histogram(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        """Export a histogram metric"""
        pass

    async def close(self) -> None:
        """Close any exporter resources."""
        return None


class HookManager:
    """Manages hooks and exporters for RRQ"""

    def __init__(self, settings: RRQSettings):
        self.settings = settings
        self.hooks: list[RRQHook] = []
        self.exporters: dict[str, MetricsExporter] = {}
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize hooks and exporters from settings"""
        if self._initialized:
            return

        # Load event handlers
        for handler_path in self.settings.event_handlers:
            try:
                hook = self._load_hook(handler_path)
                self.hooks.append(hook)
                logger.info(f"Loaded hook: {handler_path}")
            except Exception as e:
                logger.error(f"Failed to load hook {handler_path}: {e}")

        # Load metrics exporter
        exporter_type = self.settings.metrics_exporter
        if exporter_type is not None:
            try:
                exporter = self._load_exporter(exporter_type)
                self.exporters[exporter_type] = exporter
                logger.info(f"Loaded metrics exporter: {exporter_type}")
            except Exception as e:
                logger.error(f"Failed to load exporter {exporter_type}: {e}")

        self._initialized = True

    def _load_hook(self, handler_path: str) -> RRQHook:
        """Load a hook from a module path"""
        module_path, class_name = handler_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        hook_class = getattr(module, class_name)

        if not isinstance(hook_class, type) or not issubclass(hook_class, RRQHook):
            raise ValueError(f"{handler_path} is not a subclass of RRQHook")

        return hook_class(self.settings)

    def _load_exporter(self, exporter_type: str) -> MetricsExporter:
        """Load a metrics exporter"""
        # Built-in exporters
        if exporter_type == "prometheus":
            from .exporters.prometheus import PrometheusExporter

            return PrometheusExporter(self.settings)
        elif exporter_type == "statsd":
            from .exporters.statsd import StatsdExporter

            return StatsdExporter(self.settings)
        return self._load_custom_exporter(exporter_type)

    def _load_custom_exporter(self, exporter_path: str) -> MetricsExporter:
        """Load a metrics exporter from a module path."""
        module_path, class_name = exporter_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        exporter_class = getattr(module, class_name)

        if not isinstance(exporter_class, type) or not issubclass(
            exporter_class, MetricsExporter
        ):
            raise ValueError(f"{exporter_path} is not a subclass of MetricsExporter")

        return exporter_class(self.settings)

    async def trigger_event(self, event_name: str, *args: Any, **kwargs: Any) -> None:
        """Trigger an event on all hooks"""
        if not self._initialized:
            await self.initialize()

        # Run hooks concurrently but catch exceptions
        tasks: list[asyncio.Task[object]] = []
        for hook in self.hooks:
            method = getattr(hook, event_name, None)
            if method is None:
                continue

            task = asyncio.create_task(
                self._safe_call(
                    cast(Callable[..., Awaitable[Any]], method), *args, **kwargs
                )
            )
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_call(
        self, method: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> None:
        """Safely call a hook method"""
        try:
            await method(*args, **kwargs)
        except Exception as e:
            method_name = getattr(
                method, "__qualname__", getattr(method, "__name__", "")
            )
            logger.error(f"Error in hook {method_name}: {e}")

    async def export_metric(
        self,
        metric_type: str,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Export a metric to all configured exporters"""
        if not self._initialized:
            await self.initialize()

        for exporter in self.exporters.values():
            try:
                if metric_type == "counter":
                    await exporter.export_counter(name, value, labels)
                elif metric_type == "gauge":
                    await exporter.export_gauge(name, value, labels)
                elif metric_type == "histogram":
                    await exporter.export_histogram(name, value, labels)
            except Exception as e:
                logger.error(f"Error exporting metric {name}: {e}")

    async def close(self) -> None:
        """Close all exporters"""
        for exporter in self.exporters.values():
            try:
                await exporter.close()
            except Exception as e:
                logger.error(f"Error closing exporter: {e}")


# Example hook implementation
class LoggingHook(RRQHook):
    """Example hook that logs all events"""

    async def on_job_enqueued(self, job: Job) -> None:
        logger.info(f"Job enqueued: {job.id} - {job.function_name}")

    async def on_job_started(self, job: Job, worker_id: str) -> None:
        logger.info(f"Job started: {job.id} on worker {worker_id}")

    async def on_job_completed(self, job: Job, result: Any) -> None:
        logger.info(f"Job completed: {job.id}")

    async def on_job_failed(self, job: Job, error: Exception) -> None:
        logger.error(f"Job failed: {job.id} - {error}")

    async def on_job_retrying(self, job: Job, attempt: int) -> None:
        logger.warning(f"Job retrying: {job.id} - attempt {attempt}")

    async def on_worker_started(self, worker_id: str, queues: list[str]) -> None:
        logger.info(f"Worker started: {worker_id} on queues {queues}")

    async def on_worker_stopped(self, worker_id: str) -> None:
        logger.info(f"Worker stopped: {worker_id}")
