"""StatsD metrics exporter for RRQ hooks.

This exporter is optional and requires `statsd` to be installed.

Labels are currently ignored because vanilla StatsD does not support tags.
"""

from __future__ import annotations

import os

from ..hooks import MetricsExporter
from ..settings import RRQSettings


class StatsdExporter(MetricsExporter):
    """Exports RRQ metrics via `statsd.StatsClient`."""

    def __init__(
        self,
        settings: RRQSettings,
        *,
        host: str | None = None,
        port: int | None = None,
        prefix: str | None = None,
    ):
        super().__init__(settings)
        try:
            from statsd import StatsClient  # type: ignore[import-not-found]
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "StatsD exporter requires `statsd` to be installed."
            ) from e

        resolved_host = host or os.getenv("RRQ_STATSD_HOST", "localhost")
        resolved_port = port or int(os.getenv("RRQ_STATSD_PORT", "8125"))
        resolved_prefix = prefix or os.getenv("RRQ_STATSD_PREFIX", "rrq")

        self._client = StatsClient(
            host=resolved_host, port=resolved_port, prefix=resolved_prefix
        )

    async def export_counter(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        _ = labels
        self._client.incr(name, int(value))

    async def export_gauge(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        _ = labels
        self._client.gauge(name, value)

    async def export_histogram(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        _ = labels
        # StatsD doesn't have a standard histogram type; use timing in milliseconds.
        self._client.timing(name, int(value * 1000))
