"""Prometheus metrics exporter for RRQ hooks.

This exporter is optional and requires `prometheus_client` to be installed.
"""

from __future__ import annotations

from typing import Any

from ..hooks import MetricsExporter
from ..settings import RRQSettings


class PrometheusExporter(MetricsExporter):
    """Exports RRQ metrics via `prometheus_client`."""

    def __init__(self, settings: RRQSettings):
        super().__init__(settings)
        try:
            from prometheus_client import Counter, Gauge, Histogram  # type: ignore[import-not-found]
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "Prometheus exporter requires `prometheus_client` to be installed."
            ) from e

        self._counter_cls = Counter
        self._gauge_cls = Gauge
        self._histogram_cls = Histogram

        self._counters: dict[tuple[str, tuple[str, ...]], Any] = {}
        self._gauges: dict[tuple[str, tuple[str, ...]], Any] = {}
        self._histograms: dict[tuple[str, tuple[str, ...]], Any] = {}

    def _get_metric(
        self,
        store: dict[tuple[str, tuple[str, ...]], Any],
        metric_cls: Any,
        name: str,
        labelnames: tuple[str, ...],
    ) -> Any:
        key = (name, labelnames)
        metric = store.get(key)
        if metric is not None:
            return metric

        description = name
        if labelnames:
            metric = metric_cls(name, description, labelnames=labelnames)
        else:
            metric = metric_cls(name, description)
        store[key] = metric
        return metric

    @staticmethod
    def _sorted_labelnames(labels: dict[str, str] | None) -> tuple[str, ...]:
        if not labels:
            return ()
        return tuple(sorted(labels.keys()))

    async def export_counter(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        labelnames = self._sorted_labelnames(labels)
        counter = self._get_metric(self._counters, self._counter_cls, name, labelnames)
        if labelnames and labels:
            counter.labels(**{k: labels[k] for k in labelnames}).inc(value)
        else:
            counter.inc(value)

    async def export_gauge(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        labelnames = self._sorted_labelnames(labels)
        gauge = self._get_metric(self._gauges, self._gauge_cls, name, labelnames)
        if labelnames and labels:
            gauge.labels(**{k: labels[k] for k in labelnames}).set(value)
        else:
            gauge.set(value)

    async def export_histogram(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        labelnames = self._sorted_labelnames(labels)
        histogram = self._get_metric(
            self._histograms, self._histogram_cls, name, labelnames
        )
        if labelnames and labels:
            histogram.labels(**{k: labels[k] for k in labelnames}).observe(value)
        else:
            histogram.observe(value)
