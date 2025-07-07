"""Lightweight hooks system for RRQ monitoring and integrations"""

import asyncio
import importlib
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List

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

    async def on_worker_started(self, worker_id: str, queues: List[str]) -> None:
        """Called when a worker starts"""
        pass

    async def on_worker_stopped(self, worker_id: str) -> None:
        """Called when a worker stops"""
        pass

    async def on_worker_heartbeat(self, worker_id: str, health_data: Dict) -> None:
        """Called on worker heartbeat"""
        pass


class MetricsExporter(ABC):
    """Base class for metrics exporters"""

    @abstractmethod
    async def export_counter(
        self, name: str, value: float, labels: Dict[str, str] = None
    ) -> None:
        """Export a counter metric"""
        pass

    @abstractmethod
    async def export_gauge(
        self, name: str, value: float, labels: Dict[str, str] = None
    ) -> None:
        """Export a gauge metric"""
        pass

    @abstractmethod
    async def export_histogram(
        self, name: str, value: float, labels: Dict[str, str] = None
    ) -> None:
        """Export a histogram metric"""
        pass


class HookManager:
    """Manages hooks and exporters for RRQ"""

    def __init__(self, settings: RRQSettings):
        self.settings = settings
        self.hooks: List[RRQHook] = []
        self.exporters: Dict[str, MetricsExporter] = {}
        self._initialized = False

    async def initialize(self):
        """Initialize hooks and exporters from settings"""
        if self._initialized:
            return

        # Load event handlers
        if hasattr(self.settings, "event_handlers"):
            for handler_path in self.settings.event_handlers:
                try:
                    hook = self._load_hook(handler_path)
                    self.hooks.append(hook)
                    logger.info(f"Loaded hook: {handler_path}")
                except Exception as e:
                    logger.error(f"Failed to load hook {handler_path}: {e}")

        # Load metrics exporter
        if hasattr(self.settings, "metrics_exporter"):
            exporter_type = self.settings.metrics_exporter
            if exporter_type:
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

        if not issubclass(hook_class, RRQHook):
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
        else:
            # Try to load as custom exporter
            return self._load_hook(exporter_type)

    async def trigger_event(self, event_name: str, *args, **kwargs):
        """Trigger an event on all hooks"""
        if not self._initialized:
            await self.initialize()

        # Run hooks concurrently but catch exceptions
        tasks = []
        for hook in self.hooks:
            if hasattr(hook, event_name):
                method = getattr(hook, event_name)
                task = asyncio.create_task(self._safe_call(method, *args, **kwargs))
                tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_call(self, method: Callable, *args, **kwargs):
        """Safely call a hook method"""
        try:
            await method(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in hook {method.__qualname__}: {e}")

    async def export_metric(
        self, metric_type: str, name: str, value: float, labels: Dict[str, str] = None
    ):
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

    async def close(self):
        """Close all exporters"""
        for exporter in self.exporters.values():
            if hasattr(exporter, "close"):
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

    async def on_worker_started(self, worker_id: str, queues: List[str]) -> None:
        logger.info(f"Worker started: {worker_id} on queues {queues}")

    async def on_worker_stopped(self, worker_id: str) -> None:
        logger.info(f"Worker stopped: {worker_id}")
