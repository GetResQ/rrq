"""Real-time monitoring dashboard for RRQ"""

import asyncio
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, List

import click
from rich.align import Align
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from rrq.constants import (
    HEALTH_KEY_PREFIX,
    JOB_KEY_PREFIX,
    QUEUE_KEY_PREFIX,
    DLQ_KEY_PREFIX,
)
from rrq.cli_commands.base import AsyncCommand, load_app_settings, get_job_store
from ..utils import (
    console,
    format_duration,
    format_queue_name,
    format_status,
    format_timestamp,
)

# Error truncation lengths for consistency with DLQ commands
ERROR_DISPLAY_LENGTH = 50  # For consistent display across DLQ and monitor


class MonitorCommands(AsyncCommand):
    """Real-time monitoring commands"""

    def register(self, cli_group: click.Group) -> None:
        """Register monitor commands"""

        @cli_group.command("monitor")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--refresh",
            type=float,
            default=1.0,
            help="Refresh interval in seconds",
        )
        @click.option(
            "--queues",
            multiple=True,
            help="Specific queues to monitor (default: all)",
        )
        def monitor(settings_object_path: str, refresh: float, queues: tuple):
            """Launch real-time monitoring dashboard"""
            self.make_async(self._monitor)(settings_object_path, refresh, queues)

    async def _monitor(
        self, settings_object_path: str, refresh: float, queues: tuple
    ) -> None:
        """Run the monitoring dashboard"""
        settings = load_app_settings(settings_object_path)
        dashboard = Dashboard(settings, refresh, queues)

        try:
            await dashboard.run()
        except KeyboardInterrupt:
            pass


class Dashboard:
    """Real-time monitoring dashboard"""

    def __init__(self, settings, refresh_interval: float, queue_filter: tuple):
        self.settings = settings
        self.refresh_interval = refresh_interval
        self.queue_filter = list(queue_filter) if queue_filter else None
        self.job_store = None

        # Metrics storage
        self.queue_sizes = defaultdict(lambda: deque(maxlen=60))  # 60 data points
        self.processing_rates = defaultdict(lambda: deque(maxlen=60))
        self.error_counts = defaultdict(int)
        self.dlq_stats = {"total_jobs": 0, "newest_error": None, "top_errors": {}}
        self.last_update = datetime.now()

        # Event streaming for real-time updates
        self._last_event_id = "0"
        self._event_buffer = deque(maxlen=100)

    async def run(self):
        """Run the dashboard"""
        self.job_store = await get_job_store(self.settings)

        try:
            layout = self.create_layout()

            with Live(
                layout, refresh_per_second=1 / self.refresh_interval, console=console
            ) as _:
                while True:
                    await self.update_metrics()
                    self.update_layout(layout)
                    await asyncio.sleep(self.refresh_interval)
        finally:
            await self.job_store.aclose()

    def create_layout(self) -> Layout:
        """Create the dashboard layout"""
        layout = Layout(name="root")

        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3),
        )

        layout["main"].split_row(
            Layout(name="queues", ratio=1),
            Layout(name="workers", ratio=1),
            Layout(name="dlq", ratio=1),
        )

        layout["queues"].split_column(
            Layout(name="queue_stats", ratio=2),
            Layout(name="queue_chart", ratio=1),
        )

        layout["workers"].split_column(
            Layout(name="worker_list", ratio=2),
            Layout(name="recent_jobs", ratio=1),
        )

        layout["dlq"].split_column(
            Layout(name="dlq_stats", ratio=2),
            Layout(name="dlq_errors", ratio=1),
        )

        return layout

    async def update_metrics(self):
        """Update all metrics using hybrid monitoring approach"""
        try:
            # Check for real-time events first
            await self._process_monitoring_events()

            # Use hybrid approach for queue/worker metrics
            queue_data = await self._update_queue_metrics_optimized()
            await self._update_worker_metrics_optimized()

            # Update queue size tracking
            for queue_name, size in queue_data.items():
                self.queue_sizes[queue_name].append(size)

            # Get recent job information (keep optimized scan)
            self.recent_jobs = await self._get_recent_jobs_optimized()

            # Get DLQ information
            await self._update_dlq_stats()

            self.last_update = datetime.now()
        except Exception as e:
            console.print(f"[red]Error updating metrics: {e}[/red]")
            # Continue with cached data if available

    async def _process_monitoring_events(self):
        """Process real-time monitoring events from Redis streams"""
        try:
            events = await self.job_store.consume_monitor_events(
                last_id=self._last_event_id,
                count=50,
                block=10,  # Short non-blocking read
            )

            for stream_name, event_list in events:
                for event_id, event_data in event_list:
                    # Update last processed event ID
                    self._last_event_id = (
                        event_id.decode() if isinstance(event_id, bytes) else event_id
                    )

                    # Process event based on type
                    event_type = event_data.get(b"event_type", b"").decode()
                    if event_type == "queue_activity":
                        queue_name = event_data.get(b"queue_name", b"").decode()
                        if queue_name:
                            # Trigger immediate refresh for this queue
                            await self._refresh_queue_size(queue_name)
                    elif event_type == "worker_heartbeat":
                        worker_id = event_data.get(b"worker_id", b"").decode()
                        if worker_id:
                            # Trigger immediate worker refresh
                            await self._refresh_worker_status(worker_id)

        except Exception:
            # Events are optional - continue without them if there's an issue
            pass

    async def _refresh_queue_size(self, queue_name: str):
        """Immediately refresh size for a specific queue"""
        try:
            queue_key = f"{QUEUE_KEY_PREFIX}{queue_name}"
            size = await self.job_store.redis.zcard(queue_key)
            self.queue_sizes[queue_name].append(size)
        except Exception:
            pass

    async def _refresh_worker_status(self, worker_id: str):
        """Immediately refresh status for a specific worker"""
        try:
            health_data, ttl = await self.job_store.get_worker_health(worker_id)
            if health_data:
                # Update worker in current list
                for i, worker in enumerate(self.workers):
                    if worker["id"] == worker_id:
                        self.workers[i].update(
                            {
                                "status": health_data.get("status", "unknown"),
                                "active_jobs": health_data.get("active_jobs", 0),
                                "queues": health_data.get("queues", []),
                                "last_heartbeat": health_data.get("timestamp"),
                                "ttl": ttl,
                            }
                        )
                        break
        except Exception:
            pass

    async def _get_recent_jobs(self, limit: int = 10) -> List[Dict]:
        """Get recently processed jobs"""
        jobs = []
        job_pattern = f"{JOB_KEY_PREFIX}*"

        # Sample recent jobs
        count = 0
        async for key in self.job_store.redis.scan_iter(match=job_pattern):
            if count >= limit * 2:  # Sample more to find recent ones
                break

            job_id = key.decode().replace(JOB_KEY_PREFIX, "")
            job_dict = await self.job_store.get_job_data_dict(job_id)
            if job_dict:
                # Only include recently updated jobs
                if "completed_at" in job_dict or "started_at" in job_dict:
                    jobs.append(
                        {
                            "id": job_id,
                            "function": job_dict.get("function_name", "unknown"),
                            "status": job_dict.get("status", "unknown"),
                            "started_at": float(job_dict.get("started_at", 0)),
                            "completed_at": float(job_dict.get("completed_at", 0)),
                        }
                    )

            count += 1

        # Sort by most recent activity
        jobs.sort(
            key=lambda x: x.get("completed_at") or x.get("started_at") or 0,
            reverse=True,
        )

        return jobs[:limit]

    async def _update_queue_metrics_optimized(self) -> Dict[str, int]:
        """Hybrid queue metrics collection using active registries and efficient batch operations"""
        # Use the hybrid monitoring approach: get active queues from registry
        try:
            # Get recently active queues from the registry (O(log N) operation)
            active_queue_names = await self.job_store.get_active_queues(
                max_age_seconds=300
            )

            # Apply filtering if specified
            if self.queue_filter:
                active_queue_names = [
                    q for q in active_queue_names if q in self.queue_filter
                ]

            # Use batch operation to get queue sizes efficiently
            if active_queue_names:
                queue_data = await self.job_store.batch_get_queue_sizes(
                    active_queue_names
                )
            else:
                queue_data = {}

            # Fallback to legacy scan for first run or if no active queues found
            if not queue_data:
                queue_data = await self._legacy_scan_queue_metrics()

        except Exception:
            # Fallback to legacy scan on any error
            queue_data = await self._legacy_scan_queue_metrics()

        return queue_data

    async def _legacy_scan_queue_metrics(self) -> Dict[str, int]:
        """Legacy scan-based queue metrics as fallback"""
        queue_keys = []
        queue_pattern = f"{QUEUE_KEY_PREFIX}*"

        # Perform limited scan (max 100 keys at a time)
        scan_count = 0
        try:
            async for key in self.job_store.redis.scan_iter(
                match=queue_pattern, count=50
            ):
                queue_keys.append(key)
                scan_count += 1
                if scan_count >= 100:  # Limit scan operations
                    break
        except TypeError:
            # Handle mocks that don't support count parameter
            async for key in self.job_store.redis.scan_iter(match=queue_pattern):
                queue_keys.append(key)
                scan_count += 1
                if scan_count >= 100:  # Limit scan operations
                    break

        # Apply filtering early and get sizes individually (compatible with tests)
        queue_data = {}
        for key in queue_keys:
            queue_name = key.decode().replace(QUEUE_KEY_PREFIX, "")
            if not self.queue_filter or queue_name in self.queue_filter:
                size = await self.job_store.redis.zcard(key)
                queue_data[queue_name] = size

        return queue_data

    async def _update_worker_metrics_optimized(self):
        """Hybrid worker metrics collection using active registries"""
        try:
            # Use the hybrid monitoring approach: get active workers from registry
            active_worker_ids = await self.job_store.get_active_workers(
                max_age_seconds=60
            )

            # Get worker health data efficiently
            workers = []
            for worker_id in active_worker_ids:
                health_data, ttl = await self.job_store.get_worker_health(worker_id)

                if health_data:
                    workers.append(
                        {
                            "id": worker_id,
                            "status": health_data.get("status", "unknown"),
                            "active_jobs": health_data.get("active_jobs", 0),
                            "queues": health_data.get("queues", []),
                            "last_heartbeat": health_data.get("timestamp"),
                            "ttl": ttl,
                        }
                    )

            # Fallback to legacy scan if no active workers found
            if not workers:
                workers = await self._legacy_scan_worker_metrics()

        except Exception:
            # Fallback to legacy scan on any error
            workers = await self._legacy_scan_worker_metrics()

        self.workers = workers

    async def _legacy_scan_worker_metrics(self) -> list:
        """Legacy scan-based worker metrics as fallback"""
        worker_keys = []
        health_pattern = f"{HEALTH_KEY_PREFIX}*"

        scan_count = 0
        try:
            async for key in self.job_store.redis.scan_iter(
                match=health_pattern, count=50
            ):
                worker_keys.append(key)
                scan_count += 1
                if scan_count >= 50:  # Limit worker scans
                    break
        except TypeError:
            # Handle mocks that don't support count parameter
            async for key in self.job_store.redis.scan_iter(match=health_pattern):
                worker_keys.append(key)
                scan_count += 1
                if scan_count >= 50:  # Limit worker scans
                    break

        # Get worker health individually (compatible with tests)
        workers = []
        for key in worker_keys:
            worker_id = key.decode().replace(HEALTH_KEY_PREFIX, "")
            health_data, ttl = await self.job_store.get_worker_health(worker_id)

            if health_data:
                workers.append(
                    {
                        "id": worker_id,
                        "status": health_data.get("status", "unknown"),
                        "active_jobs": health_data.get("active_jobs", 0),
                        "queues": health_data.get("queues", []),
                        "last_heartbeat": health_data.get("timestamp"),
                        "ttl": ttl,
                    }
                )

        return workers

    async def _get_recent_jobs_optimized(self, limit: int = 10) -> List[Dict]:
        """Optimized recent jobs collection with limited scanning"""
        jobs = []
        job_pattern = f"{JOB_KEY_PREFIX}*"

        # Limit scan iterations more aggressively for recent jobs
        job_keys = []
        scan_count = 0
        try:
            async for key in self.job_store.redis.scan_iter(
                match=job_pattern, count=20
            ):
                job_keys.append(key)
                scan_count += 1
                if scan_count >= limit * 3:  # Scan 3x the needed amount max
                    break
        except TypeError:
            # Handle mocks that don't support count parameter
            async for key in self.job_store.redis.scan_iter(match=job_pattern):
                job_keys.append(key)
                scan_count += 1
                if scan_count >= limit * 3:  # Scan 3x the needed amount max
                    break

        # Get job data individually (compatible with tests)
        if job_keys:
            recent_jobs = []
            for key in job_keys:
                job_id = key.decode().replace(JOB_KEY_PREFIX, "")
                job_dict = await self.job_store.get_job_data_dict(job_id)
                if job_dict:
                    try:
                        # Only include recently updated jobs
                        if "completed_at" in job_dict or "started_at" in job_dict:
                            recent_jobs.append(
                                {
                                    "id": job_id,
                                    "function": job_dict.get(
                                        "function_name", "unknown"
                                    ),
                                    "status": job_dict.get("status", "unknown"),
                                    "started_at": float(job_dict.get("started_at", 0)),
                                    "completed_at": float(
                                        job_dict.get("completed_at", 0)
                                    ),
                                }
                            )
                    except (ValueError, UnicodeDecodeError):
                        continue

            # Sort by most recent activity
            recent_jobs.sort(
                key=lambda x: x.get("completed_at") or x.get("started_at") or 0,
                reverse=True,
            )
            jobs = recent_jobs[:limit]

        return jobs

    def update_layout(self, layout: Layout):
        """Update the layout with current data"""
        # Header
        layout["header"].update(self._create_header())

        # Queue stats
        layout["queue_stats"].update(self._create_queue_stats())

        # Queue chart
        layout["queue_chart"].update(self._create_queue_chart())

        # Worker list
        layout["worker_list"].update(self._create_worker_list())

        # Recent jobs
        layout["recent_jobs"].update(self._create_recent_jobs())

        # DLQ stats
        layout["dlq_stats"].update(self._create_dlq_stats())

        # DLQ errors
        layout["dlq_errors"].update(self._create_dlq_errors())

        # Footer
        layout["footer"].update(self._create_footer())

    def _create_header(self) -> Panel:
        """Create header panel"""
        header_text = Text()
        header_text.append("RRQ Monitor", style="bold cyan")
        header_text.append(" | ", style="dim")
        header_text.append(
            f"Last Update: {self.last_update.strftime('%H:%M:%S')}", style="dim"
        )

        return Panel(
            Align.center(header_text),
            style="cyan",
        )

    def _create_queue_stats(self) -> Panel:
        """Create queue statistics table"""
        table = Table(show_header=True, header_style="bold magenta", expand=True)
        table.add_column("Queue", style="cyan")
        table.add_column("Size", justify="right")
        table.add_column("Trend", justify="center")
        table.add_column("Rate", justify="right")

        total_size = 0
        for queue_name in sorted(self.queue_sizes.keys()):
            sizes = list(self.queue_sizes[queue_name])
            current_size = sizes[-1] if sizes else 0
            total_size += current_size

            # Calculate trend
            trend = "→"
            trend_style = "dim"
            if len(sizes) >= 2:
                diff = sizes[-1] - sizes[-2]
                if diff > 0:
                    trend = "↑"
                    trend_style = "red"
                elif diff < 0:
                    trend = "↓"
                    trend_style = "green"

            # Calculate processing rate (jobs/min)
            rate = "N/A"
            if len(sizes) >= 10:
                # Average change over last 10 samples
                recent_changes = [sizes[i] - sizes[i - 1] for i in range(-9, 0)]
                avg_change = sum(recent_changes) / len(recent_changes)
                if avg_change < 0:  # Negative means jobs are being processed
                    rate = f"{abs(avg_change * 60 / self.refresh_interval):.1f}/min"

            table.add_row(
                format_queue_name(queue_name),
                str(current_size),
                Text(trend, style=trend_style),
                rate,
            )

        # Add total row
        table.add_row(
            Text("TOTAL", style="bold"),
            Text(str(total_size), style="bold"),
            "",
            "",
        )

        return Panel(table, title="Queue Statistics", border_style="blue")

    def _create_queue_chart(self) -> Panel:
        """Create queue size sparkline chart"""
        lines = []

        for queue_name in sorted(self.queue_sizes.keys()):
            sizes = list(self.queue_sizes[queue_name])
            if not sizes:
                continue

            # Create sparkline
            max_val = max(sizes) if sizes else 1
            min_val = min(sizes) if sizes else 0

            if max_val == min_val:
                sparkline = "─" * 20
            else:
                sparkline = ""
                for val in sizes[-20:]:  # Last 20 values
                    normalized = (val - min_val) / (max_val - min_val)
                    spark_chars = " ▁▂▃▄▅▆▇█"
                    idx = int(normalized * (len(spark_chars) - 1))
                    sparkline += spark_chars[idx]

            line = f"{queue_name:>12}: {sparkline} [{sizes[-1] if sizes else 0}]"
            lines.append(line)

        content = "\n".join(lines) if lines else "No queue data"
        return Panel(content, title="Queue Trends (60s)", border_style="green")

    def _create_worker_list(self) -> Panel:
        """Create worker status table"""
        table = Table(show_header=True, header_style="bold magenta", expand=True)
        table.add_column("Worker", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Jobs", justify="right")
        table.add_column("Heartbeat", style="dim")

        if not self.workers:
            table.add_row(
                "[dim italic]No active workers[/dim italic]",
                "",
                "",
                "",
            )
        else:
            for worker in sorted(self.workers, key=lambda x: x["id"]):
                # Status with color
                status_colors = {
                    "running": "green",
                    "idle": "yellow",
                    "stopped": "red",
                    "initializing": "blue",
                }
                status_color = status_colors.get(worker["status"], "white")
                status_text = Text(worker["status"].upper(), style=status_color)

                # Worker ID (truncated)
                worker_id = (
                    worker["id"][:12] + "..."
                    if len(worker["id"]) > 15
                    else worker["id"]
                )

                table.add_row(
                    worker_id,
                    status_text,
                    str(worker["active_jobs"]),
                    format_timestamp(worker["last_heartbeat"]),
                )

        return Panel(table, title="Active Workers", border_style="blue")

    def _create_recent_jobs(self) -> Panel:
        """Create recent jobs table"""
        table = Table(show_header=True, header_style="bold magenta", expand=True)
        table.add_column("Job", style="cyan")
        table.add_column("Function", style="yellow")
        table.add_column("Status", justify="center")
        table.add_column("Duration", justify="right")

        if not self.recent_jobs:
            table.add_row(
                "[dim italic]No recent jobs[/dim italic]",
                "",
                "",
                "",
            )
        else:
            for job in self.recent_jobs[:5]:  # Show top 5
                # Calculate duration
                duration = None
                if job.get("completed_at") and job.get("started_at"):
                    duration = job["completed_at"] - job["started_at"]

                # Truncate IDs
                job_id = job["id"][:8] + "..."
                function = (
                    job["function"][:20] + "..."
                    if len(job["function"]) > 20
                    else job["function"]
                )

                table.add_row(
                    job_id,
                    function,
                    format_status(job["status"]),
                    format_duration(duration) if duration else "N/A",
                )

        return Panel(table, title="Recent Jobs", border_style="green")

    def _create_footer(self) -> Panel:
        """Create footer panel"""
        footer_text = Text()
        footer_text.append("Press ", style="dim")
        footer_text.append("Ctrl+C", style="bold yellow")
        footer_text.append(" to exit", style="dim")

        return Panel(
            Align.center(footer_text),
            style="dim",
        )

    async def _update_dlq_stats(self):
        """Update DLQ statistics"""
        dlq_name = self.settings.default_dlq_name
        dlq_key = f"{DLQ_KEY_PREFIX}{dlq_name}"

        # Get total DLQ job count
        self.dlq_stats["total_jobs"] = await self.job_store.redis.llen(dlq_key)

        if self.dlq_stats["total_jobs"] > 0:
            # Get some recent DLQ jobs for error analysis
            job_ids = await self.job_store.redis.lrange(dlq_key, 0, 9)  # Get first 10
            job_ids = [job_id.decode("utf-8") for job_id in job_ids]

            errors = []
            newest_time = 0

            for job_id in job_ids:
                job_data = await self.job_store.get_job(job_id)
                if job_data:
                    error = job_data.get("last_error", "Unknown error")
                    completion_time = job_data.get("completion_time", 0)

                    if isinstance(completion_time, str):
                        try:
                            from datetime import datetime

                            completion_time = datetime.fromisoformat(
                                completion_time.replace("Z", "+00:00")
                            ).timestamp()
                        except (ValueError, TypeError):
                            completion_time = 0

                    if completion_time > newest_time:
                        newest_time = completion_time
                        self.dlq_stats["newest_error"] = (
                            error[:ERROR_DISPLAY_LENGTH] + "..."
                            if len(error) > ERROR_DISPLAY_LENGTH
                            else error
                        )

                    errors.append(
                        error[:ERROR_DISPLAY_LENGTH] + "..."
                        if len(error) > ERROR_DISPLAY_LENGTH
                        else error
                    )

            # Count error types
            error_counts = {}
            for error in errors:
                error_counts[error] = error_counts.get(error, 0) + 1

            self.dlq_stats["top_errors"] = dict(
                sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            )
        else:
            self.dlq_stats["newest_error"] = None
            self.dlq_stats["top_errors"] = {}

    def _create_dlq_stats(self) -> Panel:
        """Create DLQ statistics panel"""
        table = Table(show_header=True, header_style="bold red", expand=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")

        table.add_row("Total Jobs", str(self.dlq_stats["total_jobs"]))

        if self.dlq_stats["newest_error"]:
            table.add_row("Latest Error", self.dlq_stats["newest_error"])
        else:
            table.add_row("Latest Error", "None")

        return Panel(
            table,
            title=f"Dead Letter Queue ({self.settings.default_dlq_name})",
            border_style="red",
        )

    def _create_dlq_errors(self) -> Panel:
        """Create DLQ error patterns panel"""
        table = Table(show_header=True, header_style="bold red", expand=True)
        table.add_column("Error Pattern", style="red")
        table.add_column("Count", justify="right")

        if self.dlq_stats["top_errors"]:
            for error, count in self.dlq_stats["top_errors"].items():
                table.add_row(error, str(count))
        else:
            table.add_row("No errors", "0")

        return Panel(table, title="Top Error Patterns", border_style="red")
