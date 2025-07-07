"""DLQ (Dead Letter Queue) management commands"""

import json
from datetime import datetime
from typing import Dict, List, Optional

import click
from rich.console import Console
from rich.table import Table

from rrq.cli_commands.base import AsyncCommand, get_job_store, load_app_settings
from rrq.cli_commands.utils import format_timestamp
from rrq.constants import DLQ_KEY_PREFIX, JOB_KEY_PREFIX

# Error truncation lengths for consistency
ERROR_DISPLAY_LENGTH = 50  # For tables and display
ERROR_GROUPING_LENGTH = 100  # For statistics and grouping


class DLQCommands(AsyncCommand):
    """DLQ management commands"""

    def register(self, cli_group: click.Group) -> None:
        """Register DLQ commands with the CLI"""

        @cli_group.group("dlq")
        def dlq_group():
            """Manage Dead Letter Queue (DLQ) jobs"""
            pass

        @dlq_group.command("list")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            required=False,
            default=None,
            help="Python path to settings object",
        )
        @click.option(
            "--queue",
            "original_queue",
            type=str,
            required=False,
            help="Filter by original queue name",
        )
        @click.option(
            "--function",
            "function_name",
            type=str,
            required=False,
            help="Filter by function name",
        )
        @click.option(
            "--limit", type=int, default=20, help="Maximum number of jobs to show"
        )
        @click.option("--offset", type=int, default=0, help="Offset for pagination")
        @click.option(
            "--dlq-name",
            type=str,
            required=False,
            help="Name of the DLQ to inspect (defaults to settings.default_dlq_name)",
        )
        @click.option("--raw", is_flag=True, help="Show raw job data as JSON")
        @click.option(
            "--batch-size",
            type=int,
            default=100,
            help="Batch size for fetching jobs (optimization for large DLQs)",
        )
        def list_dlq_jobs(
            settings_object_path: str,
            original_queue: Optional[str],
            function_name: Optional[str],
            limit: int,
            offset: int,
            dlq_name: Optional[str],
            raw: bool,
            batch_size: int,
        ):
            """List jobs in the Dead Letter Queue"""
            return self.make_async(self._list_dlq_jobs_async)(
                settings_object_path,
                original_queue,
                function_name,
                limit,
                offset,
                dlq_name,
                raw,
                batch_size,
            )

        @dlq_group.command("stats")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            required=False,
            default=None,
            help="Python path to settings object",
        )
        @click.option(
            "--dlq-name",
            type=str,
            required=False,
            help="Name of the DLQ to analyze (defaults to settings.default_dlq_name)",
        )
        def dlq_stats(settings_object_path: str, dlq_name: Optional[str]):
            """Show DLQ statistics and error patterns"""
            return self.make_async(self._dlq_stats_async)(
                settings_object_path, dlq_name
            )

        @dlq_group.command("requeue")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            required=False,
            default=None,
            help="Python path to settings object",
        )
        @click.option(
            "--dlq-name",
            type=str,
            required=False,
            help="Name of the DLQ (defaults to settings.default_dlq_name)",
        )
        @click.option(
            "--target-queue",
            type=str,
            required=False,
            help="Target queue name (defaults to settings.default_queue_name)",
        )
        @click.option(
            "--queue",
            "original_queue",
            type=str,
            required=False,
            help="Filter by original queue name",
        )
        @click.option(
            "--function",
            "function_name",
            type=str,
            required=False,
            help="Filter by function name",
        )
        @click.option(
            "--job-id", type=str, required=False, help="Requeue specific job by ID"
        )
        @click.option(
            "--limit",
            type=int,
            required=False,
            help="Maximum number of jobs to requeue",
        )
        @click.option(
            "--all",
            "requeue_all",
            is_flag=True,
            help="Requeue all jobs (required if no other filters specified)",
        )
        @click.option(
            "--dry-run",
            is_flag=True,
            help="Show what would be requeued without actually doing it",
        )
        def requeue_dlq_jobs(
            settings_object_path: str,
            dlq_name: Optional[str],
            target_queue: Optional[str],
            original_queue: Optional[str],
            function_name: Optional[str],
            job_id: Optional[str],
            limit: Optional[int],
            requeue_all: bool,
            dry_run: bool,
        ):
            """Requeue jobs from DLQ back to a live queue with filtering"""
            return self.make_async(self._requeue_dlq_jobs_async)(
                settings_object_path,
                dlq_name,
                target_queue,
                original_queue,
                function_name,
                job_id,
                limit,
                requeue_all,
                dry_run,
            )

        @dlq_group.command("inspect")
        @click.argument("job_id")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            required=False,
            default=None,
            help="Python path to settings object",
        )
        @click.option("--raw", is_flag=True, help="Show raw job data as JSON")
        def inspect_dlq_job(job_id: str, settings_object_path: str, raw: bool):
            """Inspect a specific job in the DLQ"""
            return self.make_async(self._inspect_dlq_job_async)(
                job_id, settings_object_path, raw
            )

    async def _get_dlq_jobs(
        self,
        job_store,
        dlq_name: str,
        original_queue: Optional[str] = None,
        function_name: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        batch_size: int = 100,
    ) -> List[Dict]:
        """Get jobs from DLQ with filtering and batch fetching optimization"""
        dlq_key = f"{DLQ_KEY_PREFIX}{dlq_name}"

        # Get job IDs from DLQ (Redis list)
        job_ids = await job_store.redis.lrange(dlq_key, 0, -1)
        job_ids = [job_id.decode("utf-8") for job_id in job_ids]

        # Batch fetch job data using pipelines to reduce round-trip overhead
        jobs = []
        for i in range(0, len(job_ids), batch_size):
            batch_job_ids = job_ids[i : i + batch_size]
            batch_jobs = await self._batch_get_jobs(job_store, batch_job_ids)

            for job_data in batch_jobs:
                if not job_data:
                    continue

                # Apply filters
                if original_queue and job_data.get("queue_name") != original_queue:
                    continue
                if function_name and job_data.get("function_name") != function_name:
                    continue

                jobs.append(job_data)

        # Sort by completion time (newest first)
        jobs.sort(key=lambda x: x.get("completion_time", 0), reverse=True)

        # Apply pagination
        return jobs[offset : offset + limit]

    async def _batch_get_jobs(
        self, job_store, job_ids: List[str]
    ) -> List[Optional[Dict]]:
        """Efficiently fetch multiple jobs using Redis pipeline"""
        from rrq.constants import JOB_KEY_PREFIX

        if not job_ids:
            return []

        async with job_store.redis.pipeline(transaction=False) as pipe:
            for job_id in job_ids:
                job_key = f"{JOB_KEY_PREFIX}{job_id}"
                pipe.hgetall(job_key)

            results = await pipe.execute()

        # Convert results to job data dicts
        jobs = []
        for result in results:
            if result:
                # Convert bytes to strings like the original get_job method
                job_data = {
                    k.decode("utf-8"): v.decode("utf-8") for k, v in result.items()
                }
                jobs.append(job_data)
            else:
                jobs.append(None)

        return jobs

    async def _get_dlq_job_count(self, job_store, dlq_name: str) -> int:
        """Get total count of jobs in DLQ"""
        dlq_key = f"{DLQ_KEY_PREFIX}{dlq_name}"
        return await job_store.redis.llen(dlq_key)

    async def _get_dlq_statistics(self, job_store, dlq_name: str) -> Dict:
        """Get comprehensive DLQ statistics"""
        dlq_key = f"{DLQ_KEY_PREFIX}{dlq_name}"

        # Get all job IDs from DLQ
        job_ids = await self._get_dlq_job_ids(job_store, dlq_key)

        if not job_ids:
            return self._create_empty_statistics()

        # Fetch job data
        jobs = await self._fetch_job_data_batch(job_store, job_ids)

        # Calculate all statistics
        return self._calculate_dlq_statistics(jobs)

    async def _get_dlq_job_ids(self, job_store, dlq_key: str) -> List[str]:
        """Get all job IDs from DLQ"""
        job_ids = await job_store.redis.lrange(dlq_key, 0, -1)
        return [job_id.decode("utf-8") for job_id in job_ids]

    def _create_empty_statistics(self) -> Dict:
        """Create empty statistics structure"""
        return {
            "total_jobs": 0,
            "oldest_job_time": None,
            "newest_job_time": None,
            "avg_retries": 0,
            "by_queue": {},
            "by_function": {},
            "top_errors": {},
        }

    async def _fetch_job_data_batch(self, job_store, job_ids: List[str]) -> List[Dict]:
        """Fetch job data for multiple job IDs"""
        jobs = []
        for job_id in job_ids:
            job_data = await job_store.get_job(job_id)
            if job_data:
                jobs.append(job_data)
        return jobs

    def _calculate_dlq_statistics(self, jobs: List[Dict]) -> Dict:
        """Calculate comprehensive statistics from job data"""
        completion_times = self._extract_completion_times(jobs)
        retries = self._extract_retries(jobs)

        by_queue = self._count_by_field(jobs, "queue_name")
        by_function = self._count_by_field(jobs, "function_name")
        top_errors = self._count_top_errors(jobs)

        return {
            "total_jobs": len(jobs),
            "oldest_job_time": min(completion_times) if completion_times else None,
            "newest_job_time": max(completion_times) if completion_times else None,
            "avg_retries": sum(retries) / len(retries) if retries else 0,
            "by_queue": dict(
                sorted(by_queue.items(), key=lambda x: x[1], reverse=True)
            ),
            "by_function": dict(
                sorted(by_function.items(), key=lambda x: x[1], reverse=True)
            ),
            "top_errors": top_errors,
        }

    def _extract_completion_times(self, jobs: List[Dict]) -> List[float]:
        """Extract completion times from jobs"""
        return [
            job.get("completion_time", 0) for job in jobs if job.get("completion_time")
        ]

    def _extract_retries(self, jobs: List[Dict]) -> List[int]:
        """Extract retry counts from jobs"""
        return [job.get("current_retries", 0) for job in jobs]

    def _count_by_field(self, jobs: List[Dict], field_name: str) -> Dict[str, int]:
        """Count jobs by a specific field"""
        counts = {}
        for job in jobs:
            value = job.get(field_name, "Unknown")
            counts[value] = counts.get(value, 0) + 1
        return counts

    def _count_top_errors(self, jobs: List[Dict]) -> Dict[str, int]:
        """Count and return top 10 error patterns"""
        error_counts = {}
        for job in jobs:
            error = job.get("last_error", "Unknown error")
            # Take first N chars of error for grouping
            error_key = error[:ERROR_GROUPING_LENGTH]
            error_counts[error_key] = error_counts.get(error_key, 0) + 1

        # Return top 10 errors
        return dict(sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:10])

    async def _get_dlq_jobs_by_id(
        self, job_store, dlq_name: str, job_ids: List[str]
    ) -> List[Dict]:
        """Get specific jobs from DLQ by their IDs"""
        jobs = []
        for job_id in job_ids:
            job_data = await job_store.get_job(job_id)
            if job_data and await self._is_job_in_dlq(job_store, dlq_name, job_id):
                jobs.append(job_data)
        return jobs

    async def _is_job_in_dlq(self, job_store, dlq_name: str, job_id: str) -> bool:
        """Check if a specific job is in the DLQ"""
        dlq_key = f"{DLQ_KEY_PREFIX}{dlq_name}"
        job_ids = await job_store.redis.lrange(dlq_key, 0, -1)
        job_ids = [jid.decode("utf-8") for jid in job_ids]
        return job_id in job_ids

    async def _requeue_specific_jobs(
        self, job_store, dlq_name: str, target_queue: str, jobs: List[Dict]
    ) -> int:
        """Requeue specific jobs from DLQ to target queue"""
        dlq_key = f"{DLQ_KEY_PREFIX}{dlq_name}"
        requeued_count = 0

        for job in jobs:
            job_id = job["id"]

            # Remove from DLQ list
            removed = await job_store.redis.lrem(dlq_key, 1, job_id.encode("utf-8"))
            if removed > 0:
                # Add to target queue with current timestamp
                now_ms = int(datetime.now().timestamp() * 1000)
                await job_store.add_job_to_queue(target_queue, job_id, now_ms)

                # Update job status back to PENDING
                job_key = f"{JOB_KEY_PREFIX}{job_id}"
                await job_store.redis.hset(
                    job_key,
                    mapping={
                        "status": "pending".encode("utf-8"),
                        "queue_name": target_queue.encode("utf-8"),
                    },
                )

                requeued_count += 1

        return requeued_count

    async def _list_dlq_jobs_async(
        self,
        settings_object_path: str,
        original_queue: Optional[str],
        function_name: Optional[str],
        limit: int,
        offset: int,
        dlq_name: Optional[str],
        raw: bool,
        batch_size: int = 100,
    ):
        """Async implementation for listing DLQ jobs"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            dlq_to_use = dlq_name or settings.default_dlq_name
            console = Console()

            jobs = await self._get_dlq_jobs(
                job_store,
                dlq_to_use,
                original_queue,
                function_name,
                limit,
                offset,
                batch_size,
            )

            if raw:
                self._display_raw_jobs(console, jobs)
                return

            if not jobs:
                self._display_no_jobs_message(
                    console, dlq_to_use, original_queue, function_name
                )
                return

            # Create and display table
            table = self._create_dlq_jobs_table(dlq_to_use, jobs)
            console.print(table)

            # Show pagination info
            await self._display_pagination_info(
                console, job_store, dlq_to_use, offset, limit, len(jobs)
            )

        finally:
            await job_store.aclose()

    def _display_raw_jobs(self, console: Console, jobs: List[Dict]):
        """Display jobs in raw JSON format"""
        for job in jobs:
            console.print(json.dumps(job, indent=2, default=str))

    def _display_no_jobs_message(
        self,
        console: Console,
        dlq_name: str,
        original_queue: Optional[str],
        function_name: Optional[str],
    ):
        """Display message when no jobs found"""
        console.print(f"[yellow]No jobs found in DLQ: {dlq_name}")
        if original_queue:
            console.print(f"[yellow]Filtered by queue: {original_queue}")
        if function_name:
            console.print(f"[yellow]Filtered by function: {function_name}")

    def _create_dlq_jobs_table(self, dlq_name: str, jobs: List[Dict]) -> Table:
        """Create table for displaying DLQ jobs"""
        table = Table(title=f"Jobs in DLQ: {dlq_name}")
        table.add_column("Job ID", style="cyan", no_wrap=True, max_width=20)
        table.add_column("Function", style="green", max_width=15)
        table.add_column("Queue", style="blue", max_width=12)
        table.add_column("Error", style="red", max_width=25)
        table.add_column("Failed At", style="yellow", max_width=16)
        table.add_column("Retries", style="magenta", justify="right", max_width=8)

        for job in jobs:
            error_text = self._truncate_error_text(
                job.get("last_error", "Unknown error")
            )

            table.add_row(
                self._truncate_job_id(job["id"]),
                job.get("function_name", "N/A"),
                job.get("queue_name", "N/A"),
                error_text,
                format_timestamp(job.get("completion_time")),
                str(job.get("current_retries", 0)),
            )

        return table

    def _truncate_error_text(self, error_text: str) -> str:
        """Truncate long error messages for display"""
        if len(error_text) <= ERROR_DISPLAY_LENGTH:
            return error_text
        return error_text[: ERROR_DISPLAY_LENGTH - 3] + "..."

    def _truncate_job_id(self, job_id: str) -> str:
        """Truncate long job IDs"""
        return job_id[:18] + "..." if len(job_id) > 18 else job_id

    async def _display_pagination_info(
        self,
        console: Console,
        job_store,
        dlq_name: str,
        offset: int,
        limit: int,
        jobs_shown: int,
    ):
        """Display pagination information"""
        total_jobs = await self._get_dlq_job_count(job_store, dlq_name)
        start_idx = offset + 1
        end_idx = min(offset + jobs_shown, total_jobs)

        console.print(f"\n[dim]Showing {start_idx}-{end_idx} of {total_jobs} jobs")
        if end_idx < total_jobs:
            console.print(f"[dim]Use --offset {offset + limit} to see more")

    async def _dlq_stats_async(
        self, settings_object_path: str, dlq_name: Optional[str]
    ):
        """Async implementation for DLQ statistics"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            dlq_to_use = dlq_name or settings.default_dlq_name
            console = Console()

            stats = await self._get_dlq_statistics(job_store, dlq_to_use)

            if stats["total_jobs"] == 0:
                console.print(f"[yellow]DLQ '{dlq_to_use}' is empty")
                return

            # Display all statistics tables
            self._display_overall_stats(console, dlq_to_use, stats)
            self._display_queue_breakdown(console, stats)
            self._display_function_breakdown(console, stats)
            self._display_error_patterns(console, stats)

        finally:
            await job_store.aclose()

    def _display_overall_stats(self, console: Console, dlq_name: str, stats: Dict):
        """Display overall DLQ statistics table"""
        stats_table = Table(title=f"DLQ Statistics: {dlq_name}")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="green")

        stats_table.add_row("Total Jobs", str(stats["total_jobs"]))
        stats_table.add_row("Oldest Job", format_timestamp(stats["oldest_job_time"]))
        stats_table.add_row("Newest Job", format_timestamp(stats["newest_job_time"]))
        stats_table.add_row("Average Retries", f"{stats['avg_retries']:.1f}")

        console.print(stats_table)

    def _display_queue_breakdown(self, console: Console, stats: Dict):
        """Display queue breakdown table"""
        if not stats["by_queue"]:
            return

        queue_table = Table(title="Jobs by Original Queue")
        queue_table.add_column("Queue", style="blue")
        queue_table.add_column("Count", style="green", justify="right")
        queue_table.add_column("Percentage", style="yellow", justify="right")

        for queue, count in stats["by_queue"].items():
            percentage = (count / stats["total_jobs"]) * 100
            queue_table.add_row(queue, str(count), f"{percentage:.1f}%")

        console.print(queue_table)

    def _display_function_breakdown(self, console: Console, stats: Dict):
        """Display function breakdown table"""
        if not stats["by_function"]:
            return

        func_table = Table(title="Jobs by Function")
        func_table.add_column("Function", style="green")
        func_table.add_column("Count", style="green", justify="right")
        func_table.add_column("Percentage", style="yellow", justify="right")

        for func, count in stats["by_function"].items():
            percentage = (count / stats["total_jobs"]) * 100
            func_table.add_row(func, str(count), f"{percentage:.1f}%")

        console.print(func_table)

    def _display_error_patterns(self, console: Console, stats: Dict):
        """Display error patterns table"""
        if not stats["top_errors"]:
            return

        error_table = Table(title="Top Error Patterns")
        error_table.add_column(
            f"Error (first {ERROR_DISPLAY_LENGTH} chars)", style="red"
        )
        error_table.add_column("Count", style="green", justify="right")

        for error, count in stats["top_errors"].items():
            error_display = self._truncate_error_text(error)
            error_table.add_row(error_display, str(count))

        console.print(error_table)

    async def _requeue_dlq_jobs_async(
        self,
        settings_object_path: str,
        dlq_name: Optional[str],
        target_queue: Optional[str],
        original_queue: Optional[str],
        function_name: Optional[str],
        job_id: Optional[str],
        limit: Optional[int],
        requeue_all: bool,
        dry_run: bool,
    ):
        """Async implementation for requeuing DLQ jobs"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            dlq_to_use = dlq_name or settings.default_dlq_name
            target_queue_to_use = target_queue or settings.default_queue_name
            console = Console()

            # Validate filters
            if not self._validate_requeue_filters(
                console, original_queue, function_name, job_id, requeue_all
            ):
                return

            # Get matching jobs
            matching_jobs = await self._get_matching_jobs_for_requeue(
                job_store, dlq_to_use, job_id, original_queue, function_name, limit
            )

            if not matching_jobs:
                console.print(f"[yellow]No matching jobs found in DLQ: {dlq_to_use}")
                return

            console.print(f"[cyan]Found {len(matching_jobs)} matching jobs to requeue")

            if dry_run:
                self._display_dry_run_results(
                    console, matching_jobs, target_queue_to_use
                )
                return

            # Actually requeue the jobs
            requeued_count = await self._requeue_specific_jobs(
                job_store, dlq_to_use, target_queue_to_use, matching_jobs
            )

            console.print(
                f"[green]Successfully requeued {requeued_count} jobs from DLQ '{dlq_to_use}' to queue '{target_queue_to_use}'"
            )

        finally:
            await job_store.aclose()

    def _validate_requeue_filters(
        self,
        console: Console,
        original_queue: Optional[str],
        function_name: Optional[str],
        job_id: Optional[str],
        requeue_all: bool,
    ) -> bool:
        """Validate that at least one filter or --all is specified"""
        has_filters = any([original_queue, function_name, job_id, requeue_all])
        if not has_filters:
            console.print(
                "[red]Error: Must specify --all or at least one filter (--queue, --function, --job-id)"
            )
            return False
        return True

    async def _get_matching_jobs_for_requeue(
        self,
        job_store,
        dlq_name: str,
        job_id: Optional[str],
        original_queue: Optional[str],
        function_name: Optional[str],
        limit: Optional[int],
    ) -> List[Dict]:
        """Get jobs matching the requeue criteria"""
        if job_id:
            # Single job requeue
            return await self._get_dlq_jobs_by_id(job_store, dlq_name, [job_id])
        else:
            # Filtered requeue
            matching_jobs = await self._get_dlq_jobs(
                job_store,
                dlq_name,
                original_queue,
                function_name,
                limit or 10000,
                0,  # Large limit for getting all matching
            )

            if limit:
                matching_jobs = matching_jobs[:limit]

            return matching_jobs

    def _display_dry_run_results(
        self, console: Console, matching_jobs: List[Dict], target_queue: str
    ):
        """Display dry run results"""
        console.print(f"[yellow]DRY RUN: Would requeue {len(matching_jobs)} jobs")

        # Show what would be requeued
        table = Table(title="Jobs to Requeue (Dry Run)")
        table.add_column("Job ID", style="cyan", max_width=20)
        table.add_column("Function", style="green", max_width=15)
        table.add_column("Original Queue", style="blue", max_width=12)
        table.add_column("Target Queue", style="magenta", max_width=12)

        for job in matching_jobs[:10]:  # Show first 10
            table.add_row(
                self._truncate_job_id(job["id"]),
                job.get("function_name", "N/A"),
                job.get("queue_name", "N/A"),
                target_queue,
            )

        console.print(table)
        if len(matching_jobs) > 10:
            console.print(f"[dim]... and {len(matching_jobs) - 10} more jobs")

    async def _inspect_dlq_job_async(
        self, job_id: str, settings_object_path: str, raw: bool
    ):
        """Async implementation for inspecting a DLQ job"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            console = Console()
            job_data = await job_store.get_job(job_id)

            if not job_data:
                console.print(f"[red]Job {job_id} not found")
                return

            if raw:
                console.print(json.dumps(job_data, indent=2, default=str))
                return

            # Display detailed job information
            self._display_job_details(console, job_id, job_data)
            self._display_timing_info(console, job_data)
            self._display_worker_info(console, job_data)
            self._display_job_arguments(console, job_data)
            self._display_error_info(console, job_data)
            self._display_unique_key_info(console, job_data)

        finally:
            await job_store.aclose()

    def _display_job_details(self, console: Console, job_id: str, job_data: Dict):
        """Display basic job details"""
        console.print(f"[bold cyan]Job Details: {job_id}")
        console.print(f"[bold]Status:[/] {job_data.get('status', 'Unknown')}")
        console.print(f"[bold]Function:[/] {job_data.get('function_name', 'N/A')}")
        console.print(f"[bold]Original Queue:[/] {job_data.get('queue_name', 'N/A')}")
        console.print(f"[bold]DLQ Name:[/] {job_data.get('dlq_name', 'N/A')}")
        console.print(
            f"[bold]Retries:[/] {job_data.get('current_retries', 0)}/{job_data.get('max_retries', 0)}"
        )

    def _display_timing_info(self, console: Console, job_data: Dict):
        """Display timing information"""
        console.print("\n[bold cyan]Timing Information")
        console.print(
            f"[bold]Enqueued At:[/] {format_timestamp(job_data.get('enqueue_time'))}"
        )
        console.print(
            f"[bold]Failed At:[/] {format_timestamp(job_data.get('completion_time'))}"
        )

    def _display_worker_info(self, console: Console, job_data: Dict):
        """Display worker information"""
        if job_data.get("worker_id"):
            console.print(f"[bold]Last Worker:[/] {job_data.get('worker_id')}")

    def _display_job_arguments(self, console: Console, job_data: Dict):
        """Display job arguments"""
        if job_data.get("job_args"):
            console.print("\n[bold cyan]Arguments")
            args = json.loads(job_data.get("job_args", "[]"))
            for i, arg in enumerate(args):
                console.print(f"[bold]Arg {i}:[/] {arg}")

        if job_data.get("job_kwargs"):
            console.print("\n[bold cyan]Keyword Arguments")
            kwargs = json.loads(job_data.get("job_kwargs", "{}"))
            for key, value in kwargs.items():
                console.print(f"[bold]{key}:[/] {value}")

    def _display_error_info(self, console: Console, job_data: Dict):
        """Display error information"""
        if job_data.get("last_error"):
            console.print("\n[bold red]Error Information")
            console.print(f"[bold]Error:[/] {job_data.get('last_error')}")

            if job_data.get("traceback"):
                console.print("\n[bold red]Traceback:")
                console.print(job_data.get("traceback"))

    def _display_unique_key_info(self, console: Console, job_data: Dict):
        """Display unique key information"""
        if job_data.get("job_unique_key"):
            console.print("\n[bold cyan]Unique Key")
            console.print(f"[bold]Key:[/] {job_data.get('job_unique_key')}")
