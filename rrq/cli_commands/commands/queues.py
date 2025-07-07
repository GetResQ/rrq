"""Queue management and statistics commands"""

from datetime import datetime
from typing import Dict, List, Tuple

import click

from rrq.constants import JOB_KEY_PREFIX, QUEUE_KEY_PREFIX, DLQ_KEY_PREFIX
from rrq.store import JobStore
from rrq.cli_commands.base import AsyncCommand, load_app_settings, get_job_store
from ..utils import (
    console,
    create_progress,
    create_table,
    format_duration,
    format_queue_name,
    format_status,
    format_timestamp,
    print_error,
    print_info,
    print_warning,
)


class QueueCommands(AsyncCommand):
    """Commands for queue management and statistics"""

    def register(self, cli_group: click.Group) -> None:
        """Register queue commands"""

        @cli_group.group("queue")
        def queue_group():
            """Manage and inspect queues"""
            pass

        # List all queues
        @queue_group.command("list")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--show-empty",
            is_flag=True,
            help="Show queues with no pending jobs",
        )
        def list_queues(settings_object_path: str, show_empty: bool):
            """List all active queues with job counts"""
            self.make_async(self._list_queues)(settings_object_path, show_empty)

        # Show queue statistics
        @queue_group.command("stats")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--queue",
            "queue_names",
            multiple=True,
            help="Specific queue(s) to show stats for",
        )
        @click.option(
            "--max-scan",
            type=int,
            default=1000,
            help="Maximum jobs to scan for status breakdown (0 = unlimited, may be slow)",
        )
        def queue_stats(
            settings_object_path: str, queue_names: Tuple[str], max_scan: int
        ):
            """Show detailed statistics for queues"""
            self.make_async(self._queue_stats)(
                settings_object_path, queue_names, max_scan
            )

        # Inspect a specific queue
        @queue_group.command("inspect")
        @click.argument("queue_name")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--limit",
            type=int,
            default=20,
            help="Number of jobs to show",
        )
        @click.option(
            "--offset",
            type=int,
            default=0,
            help="Offset for pagination",
        )
        def inspect_queue(
            queue_name: str, settings_object_path: str, limit: int, offset: int
        ):
            """Inspect jobs in a specific queue"""
            self.make_async(self._inspect_queue)(
                queue_name, settings_object_path, limit, offset
            )

    async def _list_queues(self, settings_object_path: str, show_empty: bool) -> None:
        """List all active queues"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            queue_keys = await self._get_all_queue_keys(job_store)
            if not queue_keys:
                print_warning("No active queues found")
                return

            table = self._create_queue_list_table()
            queue_data = await self._fetch_queue_details(
                job_store, queue_keys, show_empty
            )

            if not queue_data:
                print_warning("No queues to display")
                return

            self._populate_queue_table(table, queue_data)
            self._display_queue_summary(table, queue_data)

        finally:
            await job_store.aclose()

    async def _get_all_queue_keys(self, job_store: JobStore) -> list[str]:
        """Get all queue keys from Redis"""
        queue_pattern = f"{QUEUE_KEY_PREFIX}*"
        queue_keys = []
        async for key in job_store.redis.scan_iter(match=queue_pattern):
            # Keep the key in the format returned by scan_iter (bytes or string depending on Redis client)
            if isinstance(key, bytes):
                queue_keys.append(key.decode())
            else:
                queue_keys.append(key)
        return queue_keys

    def _create_queue_list_table(self):
        """Create table for queue listing"""
        table = create_table("Active Queues")
        table.add_column("Queue Name", style="cyan")
        table.add_column("Pending Jobs", justify="right")
        table.add_column("Next Job", style="dim")
        table.add_column("Oldest Job", style="dim")
        return table

    async def _fetch_queue_details(
        self, job_store: JobStore, queue_keys: list[str], show_empty: bool
    ) -> list[tuple]:
        """Fetch detailed information for each queue"""
        with create_progress() as progress:
            task = progress.add_task(
                "Fetching queue information...", total=len(queue_keys)
            )
            queue_data = []

            for queue_key in queue_keys:
                queue_name = queue_key.replace(QUEUE_KEY_PREFIX, "")
                # Ensure queue_key is passed as the original format from scan_iter
                size = await job_store.redis.zcard(queue_key)

                if size == 0 and not show_empty:
                    progress.update(task, advance=1)
                    continue

                oldest_ts, newest_ts = await self._get_queue_timestamp_range(
                    job_store, queue_key, size
                )
                queue_data.append((queue_name, size, newest_ts, oldest_ts))
                progress.update(task, advance=1)

        # Sort by pending jobs count
        queue_data.sort(key=lambda x: x[1], reverse=True)
        return queue_data

    async def _get_queue_timestamp_range(
        self, job_store: JobStore, queue_key: str, size: int
    ) -> tuple:
        """Get oldest and newest job timestamps for a queue"""
        oldest_ts = None
        newest_ts = None

        if size > 0:
            # Get oldest (first) job
            oldest = await job_store.redis.zrange(queue_key, 0, 0, withscores=True)
            if oldest:
                oldest_ts = oldest[0][1]

            # Get newest (last) job
            newest = await job_store.redis.zrange(queue_key, -1, -1, withscores=True)
            if newest:
                newest_ts = newest[0][1]

        return oldest_ts, newest_ts

    def _populate_queue_table(self, table, queue_data: list[tuple]) -> None:
        """Add queue data rows to the table"""
        for queue_name, size, newest_ts, oldest_ts in queue_data:
            table.add_row(
                format_queue_name(queue_name),
                str(size),
                format_timestamp(newest_ts) if newest_ts else "N/A",
                format_timestamp(oldest_ts) if oldest_ts else "N/A",
            )

    def _display_queue_summary(self, table, queue_data: list[tuple]) -> None:
        """Display queue summary information"""
        total_jobs = sum(size for _, size, _, _ in queue_data)
        console.print(table)
        console.print(
            f"\nTotal: [bold]{len(queue_data)}[/bold] queues, [bold]{total_jobs}[/bold] pending jobs"
        )

    async def _queue_stats(
        self, settings_object_path: str, queue_names: Tuple[str], max_scan: int = 1000
    ) -> None:
        """Show detailed queue statistics"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            # If no specific queues specified, get all queues
            if not queue_names:
                queue_pattern = f"{QUEUE_KEY_PREFIX}*"
                queue_keys = []
                async for key in job_store.redis.scan_iter(match=queue_pattern):
                    queue_name = key.decode().replace(QUEUE_KEY_PREFIX, "")
                    queue_keys.append(queue_name)
                queue_names = tuple(queue_keys)

            if not queue_names:
                print_warning("No queues found")
                return

            # Create overall stats table
            stats_table = create_table("Queue Statistics")
            stats_table.add_column("Queue", style="cyan")
            stats_table.add_column("Total", justify="right")
            stats_table.add_column("Pending", justify="right", style="yellow")
            stats_table.add_column("Active", justify="right", style="blue")
            stats_table.add_column("Completed", justify="right", style="green")
            stats_table.add_column("Failed", justify="right", style="red")
            stats_table.add_column("DLQ", justify="right", style="magenta")
            stats_table.add_column("Avg Wait", justify="right")
            stats_table.add_column("Throughput", justify="right")

            with create_progress() as progress:
                task = progress.add_task("Analyzing queues...", total=len(queue_names))

                for queue_name in queue_names:
                    stats = await self._get_queue_statistics(
                        job_store, queue_name, max_scan
                    )

                    if stats["total"] == 0:
                        progress.update(task, advance=1)
                        continue

                    stats_table.add_row(
                        format_queue_name(queue_name),
                        str(stats["total"]),
                        str(stats["pending"]),
                        str(stats["active"]),
                        str(stats["completed"]),
                        str(stats["failed"]),
                        str(stats["dlq_jobs"]),
                        format_duration(stats["avg_wait_time"]),
                        f"{stats['throughput']:.1f}/min",
                    )

                    progress.update(task, advance=1)

            console.print(stats_table)

            # Show scan limitation note
            if max_scan > 0:
                console.print(
                    f"\n[dim]Note: Active/Completed/Failed counts based on scanning up to {max_scan:,} jobs.[/dim]"
                )
                console.print(
                    "[dim]Use --max-scan 0 for complete scan (may be slow for large datasets).[/dim]"
                )

        finally:
            await job_store.aclose()

    async def _inspect_queue(
        self, queue_name: str, settings_object_path: str, limit: int, offset: int
    ) -> None:
        """Inspect jobs in a specific queue"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            queue_key = f"{QUEUE_KEY_PREFIX}{queue_name}"

            # Check if queue exists
            if not await job_store.redis.exists(queue_key):
                print_error(f"Queue '{queue_name}' not found")
                return

            # Get queue size
            total_size = await job_store.redis.zcard(queue_key)

            if total_size == 0:
                print_info(f"Queue '{queue_name}' is empty")
                return

            # Get job IDs with scores
            job_entries = await job_store.redis.zrange(
                queue_key, offset, offset + limit - 1, withscores=True
            )

            # Create jobs table
            table = create_table(f"Jobs in Queue: {queue_name}")
            table.add_column("#", justify="right", style="dim")
            table.add_column("Job ID", style="cyan")
            table.add_column("Function", style="yellow")
            table.add_column("Status", justify="center")
            table.add_column("Scheduled", style="dim")
            table.add_column("Retries", justify="right")
            table.add_column("Priority", justify="right")

            # Fetch job details
            with create_progress() as progress:
                task = progress.add_task(
                    "Fetching job details...", total=len(job_entries)
                )

                for idx, (job_id_bytes, score) in enumerate(job_entries):
                    job_id = job_id_bytes.decode()

                    # Get job data using the new helper method
                    job_dict = await job_store.get_job_data_dict(job_id)

                    if not job_dict:
                        # Job key missing
                        table.add_row(
                            str(offset + idx + 1),
                            job_id,
                            "[red]<missing>[/red]",
                            format_status("missing"),
                            format_timestamp(score),
                            "N/A",
                            "N/A",
                        )
                    else:
                        # Parse job data
                        function_name = job_dict.get("function_name", "")
                        status = job_dict.get("status", "pending")
                        retries = job_dict.get("retries", "0")
                        priority = score  # Score is used as priority

                        table.add_row(
                            str(offset + idx + 1),
                            job_id[:8] + "...",  # Truncate job ID
                            function_name or "[unknown]",
                            format_status(status),
                            format_timestamp(score),
                            retries,
                            f"{priority:.0f}",
                        )

                    progress.update(task, advance=1)

            console.print(table)

            # Show pagination info
            showing_start = offset + 1
            showing_end = min(offset + limit, total_size)
            console.print(
                f"\nShowing [bold]{showing_start}-{showing_end}[/bold] of [bold]{total_size}[/bold] jobs"
            )

            if showing_end < total_size:
                console.print(f"[dim]Use --offset {showing_end} to see more[/dim]")

        finally:
            await job_store.aclose()

    async def _get_queue_statistics(
        self, job_store: JobStore, queue_name: str, max_scan: int = 1000
    ) -> Dict[str, any]:
        """Get detailed statistics for a queue"""
        stats = {
            "total": 0,
            "pending": 0,
            "active": 0,
            "completed": 0,
            "failed": 0,
            "dlq_jobs": 0,
            "avg_wait_time": None,
            "throughput": 0.0,
        }

        queue_key = f"{QUEUE_KEY_PREFIX}{queue_name}"

        # Get pending jobs count
        stats["pending"] = await job_store.redis.zcard(queue_key)

        # Get comprehensive job status breakdowns by scanning job records
        status_counts = await self._get_job_status_counts(
            job_store, queue_name, max_scan
        )
        stats.update(status_counts)

        # Calculate total from all status counts
        stats["total"] = (
            stats["pending"] + stats["active"] + stats["completed"] + stats["failed"]
        )

        # Calculate average wait time for pending jobs
        if stats["pending"] > 0:
            # Sample first 100 jobs
            job_entries = await job_store.redis.zrange(
                queue_key, 0, 99, withscores=True
            )
            if job_entries:
                now = datetime.now().timestamp()
                wait_times = [now - score for _, score in job_entries]
                stats["avg_wait_time"] = sum(wait_times) / len(wait_times)

        # Get DLQ jobs for this queue
        stats["dlq_jobs"] = await self._count_dlq_jobs_for_queue(job_store, queue_name)

        return stats

    async def _get_job_status_counts(
        self, job_store: JobStore, queue_name: str, max_scan: int = 1000
    ) -> Dict[str, int]:
        """Get job counts by status for a queue by scanning job records.

        Args:
            job_store: Redis job store
            queue_name: Queue to analyze
            max_scan: Maximum number of jobs to scan for performance (0 = unlimited)

        Note: This method scans job records in Redis which may be slow for large datasets.
        The max_scan parameter limits scanning for performance.
        """

        counts = {"active": 0, "completed": 0, "failed": 0}

        # Get a sampling of job keys to analyze
        pattern = f"{JOB_KEY_PREFIX}*"

        # Collect job keys in batches for pipeline processing
        job_keys_batch = []
        scanned_count = 0
        batch_size = 50  # Process jobs in batches of 50

        async for job_key in job_store.redis.scan_iter(match=pattern, count=100):
            if max_scan > 0 and scanned_count >= max_scan:
                break

            job_keys_batch.append(job_key)
            scanned_count += 1

            # Process batch when it's full or we've reached the end
            if len(job_keys_batch) >= batch_size:
                await self._process_job_batch(
                    job_store, job_keys_batch, queue_name, counts
                )
                job_keys_batch = []

        # Process remaining jobs in the last batch
        if job_keys_batch:
            await self._process_job_batch(job_store, job_keys_batch, queue_name, counts)

        return counts

    async def _process_job_batch(
        self,
        job_store: JobStore,
        job_keys: List[bytes],
        queue_name: str,
        counts: Dict[str, int],
    ) -> None:
        """Process a batch of job keys using Redis pipeline for efficiency"""
        from rrq.job import JobStatus

        if not job_keys:
            return

        # Use pipeline to fetch all job data in one round trip
        async with job_store.redis.pipeline(transaction=False) as pipe:
            for job_key in job_keys:
                pipe.hmget(job_key, ["queue_name", "status"])

            results = await pipe.execute()

        # Process results
        for result in results:
            if not result or len(result) < 2:
                continue

            job_queue = result[0].decode("utf-8") if result[0] else ""
            job_status = result[1].decode("utf-8") if result[1] else ""

            # Only count jobs that belong to this queue
            if job_queue != queue_name:
                continue

            # Count by status
            if job_status == JobStatus.ACTIVE.value:
                counts["active"] += 1
            elif job_status == JobStatus.COMPLETED.value:
                counts["completed"] += 1
            elif job_status == JobStatus.FAILED.value:
                counts["failed"] += 1

    async def _count_dlq_jobs_for_queue(
        self, job_store: JobStore, queue_name: str
    ) -> int:
        """Count DLQ jobs that originated from a specific queue"""
        dlq_name = job_store.settings.default_dlq_name
        dlq_key = f"{DLQ_KEY_PREFIX}{dlq_name}"

        # Get all job IDs from DLQ
        job_ids = await job_store.redis.lrange(dlq_key, 0, -1)
        job_ids = [job_id.decode("utf-8") for job_id in job_ids]

        count = 0
        for job_id in job_ids:
            job_data = await job_store.get_job(job_id)
            if job_data and job_data.get("queue_name") == queue_name:
                count += 1

        return count
