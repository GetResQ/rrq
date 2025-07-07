"""Job inspection and management commands"""

import json
from typing import Optional

import click
from rich.panel import Panel
from rich.syntax import Syntax

from rrq.constants import JOB_KEY_PREFIX, QUEUE_KEY_PREFIX
from rrq.cli_commands.base import AsyncCommand, load_app_settings, get_job_store
from ..utils import (
    console,
    create_progress,
    create_table,
    format_duration,
    format_status,
    format_timestamp,
    print_error,
    print_json,
    print_success,
    print_warning,
    truncate_string,
)


class JobCommands(AsyncCommand):
    """Commands for job inspection and management"""

    def register(self, cli_group: click.Group) -> None:
        """Register job commands"""

        @cli_group.group("job")
        def job_group():
            """Inspect and manage jobs"""
            pass

        # Show job details
        @job_group.command("show")
        @click.argument("job_id")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--raw",
            is_flag=True,
            help="Show raw job data as JSON",
        )
        def show_job(job_id: str, settings_object_path: str, raw: bool):
            """Show detailed information about a job"""
            self.make_async(self._show_job)(job_id, settings_object_path, raw)

        # List jobs
        @job_group.command("list")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--status",
            type=click.Choice(["pending", "active", "completed", "failed", "retrying"]),
            help="Filter by job status",
        )
        @click.option(
            "--queue",
            help="Filter by queue name",
        )
        @click.option(
            "--function",
            help="Filter by function name",
        )
        @click.option(
            "--limit",
            type=int,
            default=20,
            help="Number of jobs to show",
        )
        def list_jobs(
            settings_object_path: str,
            status: Optional[str],
            queue: Optional[str],
            function: Optional[str],
            limit: int,
        ):
            """List jobs with filters"""
            self.make_async(self._list_jobs)(
                settings_object_path, status, queue, function, limit
            )

        # Replay a job
        @job_group.command("replay")
        @click.argument("job_id")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--queue",
            help="Override target queue",
        )
        def replay_job(job_id: str, settings_object_path: str, queue: Optional[str]):
            """Replay a job with the same parameters"""
            self.make_async(self._replay_job)(job_id, settings_object_path, queue)

        # Cancel a job
        @job_group.command("cancel")
        @click.argument("job_id")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        def cancel_job(job_id: str, settings_object_path: str):
            """Cancel a pending job"""
            self.make_async(self._cancel_job)(job_id, settings_object_path)

        # Show job trace/timeline
        @job_group.command("trace")
        @click.argument("job_id")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        def trace_job(job_id: str, settings_object_path: str):
            """Show job execution timeline"""
            self.make_async(self._trace_job)(job_id, settings_object_path)

    async def _show_job(
        self, job_id: str, settings_object_path: str, raw: bool
    ) -> None:
        """Show detailed job information"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            # Get job data using the new helper method
            job_dict = await job_store.get_job_data_dict(job_id)

            if not job_dict:
                print_error(f"Job '{job_id}' not found")
                return

            if raw:
                # Show raw JSON data
                print_json(job_dict, title=f"Job {job_id}")
                return

            # Parse job data
            status = job_dict.get("status", "unknown")
            function_name = job_dict.get("function_name", "unknown")
            queue_name = job_dict.get("queue_name", "unknown")

            # Create info panel
            info_lines = [
                f"[bold]Job ID:[/bold] {job_id}",
                f"[bold]Status:[/bold] {format_status(status)}",
                f"[bold]Function:[/bold] {function_name}",
                f"[bold]Queue:[/bold] {queue_name}",
            ]

            # Add timestamps
            if "created_at" in job_dict:
                created_at = float(job_dict["created_at"])
                info_lines.append(
                    f"[bold]Created:[/bold] {format_timestamp(created_at)}"
                )

            if "started_at" in job_dict:
                started_at = float(job_dict["started_at"])
                info_lines.append(
                    f"[bold]Started:[/bold] {format_timestamp(started_at)}"
                )

            if "completed_at" in job_dict:
                completed_at = float(job_dict["completed_at"])
                info_lines.append(
                    f"[bold]Completed:[/bold] {format_timestamp(completed_at)}"
                )

                # Calculate duration
                if "started_at" in job_dict:
                    duration = completed_at - float(job_dict["started_at"])
                    info_lines.append(
                        f"[bold]Duration:[/bold] {format_duration(duration)}"
                    )

            # Add retry info
            retries = int(job_dict.get("retries", 0))
            max_retries = int(job_dict.get("max_retries", 3))
            info_lines.append(f"[bold]Retries:[/bold] {retries}/{max_retries}")

            # Show info panel
            console.print(
                Panel(
                    "\n".join(info_lines), title="Job Information", border_style="blue"
                )
            )

            # Show arguments
            if "args" in job_dict:
                args = json.loads(job_dict["args"])
                if args:
                    console.print("\n[bold]Arguments:[/bold]")
                    print_json(args)

            if "kwargs" in job_dict:
                kwargs = json.loads(job_dict["kwargs"])
                if kwargs:
                    console.print("\n[bold]Keyword Arguments:[/bold]")
                    print_json(kwargs)

            # Show result or error
            if status == "completed" and "result" in job_dict:
                console.print("\n[bold]Result:[/bold]")
                try:
                    result = json.loads(job_dict["result"])
                    print_json(result)
                except (json.JSONDecodeError, ValueError):
                    console.print(job_dict["result"])

            elif status in ["failed", "retrying"] and "error" in job_dict:
                console.print("\n[bold red]Error:[/bold red]")
                console.print(job_dict["error"])

                if "traceback" in job_dict:
                    console.print("\n[bold]Traceback:[/bold]")
                    syntax = Syntax(
                        job_dict["traceback"],
                        "python",
                        theme="monokai",
                        line_numbers=True,
                    )
                    console.print(syntax)

        finally:
            await job_store.aclose()

    async def _list_jobs(
        self,
        settings_object_path: str,
        status: Optional[str],
        queue: Optional[str],
        function: Optional[str],
        limit: int,
    ) -> None:
        """List jobs with filters"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            # Get all job keys
            job_pattern = f"{JOB_KEY_PREFIX}*"
            job_keys = []
            async for key in job_store.redis.scan_iter(match=job_pattern):
                job_keys.append(key.decode())

            if not job_keys:
                print_warning("No jobs found")
                return

            # Create table
            table = create_table("Jobs")
            table.add_column("Job ID", style="cyan")
            table.add_column("Function", style="yellow")
            table.add_column("Queue", style="blue")
            table.add_column("Status", justify="center")
            table.add_column("Created", style="dim")
            table.add_column("Duration", justify="right")

            # Fetch and filter jobs
            jobs = []
            with create_progress() as progress:
                task = progress.add_task("Fetching jobs...", total=len(job_keys))

                for job_key in job_keys:
                    job_id = job_key.replace(JOB_KEY_PREFIX, "")
                    job_dict = await job_store.get_job_data_dict(job_id)

                    if job_dict:
                        # Apply filters
                        if status and job_dict.get("status") != status:
                            progress.update(task, advance=1)
                            continue

                        if queue and job_dict.get("queue_name") != queue:
                            progress.update(task, advance=1)
                            continue

                        if function and job_dict.get("function_name") != function:
                            progress.update(task, advance=1)
                            continue
                        jobs.append((job_id, job_dict))

                    progress.update(task, advance=1)

            # Sort by created_at
            jobs.sort(key=lambda x: float(x[1].get("created_at", 0)), reverse=True)

            # Limit results
            jobs = jobs[:limit]

            # Add rows to table
            for job_id, job_dict in jobs:
                # Calculate duration
                duration = None
                if job_dict.get("completed_at") and job_dict.get("started_at"):
                    duration = float(job_dict["completed_at"]) - float(
                        job_dict["started_at"]
                    )
                elif job_dict.get("started_at") and job_dict.get("status") == "active":
                    import time

                    duration = time.time() - float(job_dict["started_at"])

                table.add_row(
                    job_id[:8] + "...",
                    truncate_string(job_dict.get("function_name", "unknown"), 30),
                    job_dict.get("queue_name", "unknown"),
                    format_status(job_dict.get("status", "unknown")),
                    format_timestamp(float(job_dict.get("created_at", 0))),
                    format_duration(duration) if duration else "N/A",
                )

            console.print(table)
            console.print(
                f"\n[dim]Showing {len(jobs)} of {len(job_keys)} total jobs[/dim]"
            )

        finally:
            await job_store.aclose()

    async def _replay_job(
        self, job_id: str, settings_object_path: str, queue: Optional[str]
    ) -> None:
        """Replay a job"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            # Get original job data using the new helper method
            job_dict = await job_store.get_job_data_dict(job_id)

            if not job_dict:
                print_error(f"Job '{job_id}' not found")
                return

            # Create new job with same parameters
            from rrq.client import RRQClient

            client = RRQClient(settings=settings)

            # Parse arguments
            args = json.loads(job_dict.get("args", "[]"))
            kwargs = json.loads(job_dict.get("kwargs", "{}"))

            # Enqueue new job
            new_job_id = await client.enqueue(
                function_name=job_dict["function_name"],
                args=args,
                kwargs=kwargs,
                queue_name=queue
                or job_dict.get("queue_name", settings.default_queue_name),
            )

            print_success(f"Job replayed with new ID: {new_job_id}")

        finally:
            await job_store.aclose()

    async def _cancel_job(self, job_id: str, settings_object_path: str) -> None:
        """Cancel a pending job"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            # Get job data using the new helper method
            job_dict = await job_store.get_job_data_dict(job_id)

            if not job_dict:
                print_error(f"Job '{job_id}' not found")
                return

            # Check status
            status = job_dict.get("status", "")
            if status != "pending":
                print_error(f"Can only cancel pending jobs. Job is currently: {status}")
                return

            # Remove from queue
            queue_name = job_dict.get("queue_name", "")
            if queue_name:
                queue_key = f"{QUEUE_KEY_PREFIX}{queue_name}"
                removed = await job_store.redis.zrem(queue_key, job_id)

                if removed:
                    # Update job status
                    job_key = f"{JOB_KEY_PREFIX}{job_id}"
                    await job_store.redis.hset(job_key, "status", "cancelled")
                    print_success(f"Job '{job_id}' cancelled successfully")
                else:
                    print_error("Failed to remove job from queue")
            else:
                print_error("Job has no associated queue")

        finally:
            await job_store.aclose()

    async def _trace_job(self, job_id: str, settings_object_path: str) -> None:
        """Show job execution timeline"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            # Get job data using the new helper method
            job_dict = await job_store.get_job_data_dict(job_id)

            if not job_dict:
                print_error(f"Job '{job_id}' not found")
                return

            # Create timeline table
            table = create_table(f"Job Timeline: {job_id[:8]}...")
            table.add_column("Event", style="cyan")
            table.add_column("Timestamp", style="dim")
            table.add_column("Details", style="yellow")

            # Add events
            events = []

            # Job created
            if "created_at" in job_dict:
                events.append(
                    (
                        "Created",
                        float(job_dict["created_at"]),
                        f"Function: {job_dict.get('function_name', 'unknown')}",
                    )
                )

            # Job started
            if "started_at" in job_dict:
                events.append(
                    (
                        "Started",
                        float(job_dict["started_at"]),
                        f"Worker: {job_dict.get('worker_id', 'unknown')}",
                    )
                )

            # Retries
            retries = int(job_dict.get("retries", 0))
            if retries > 0:
                for i in range(retries):
                    retry_key = f"retry_{i}_at"
                    if retry_key in job_dict:
                        events.append(
                            (
                                f"Retry {i + 1}",
                                float(job_dict[retry_key]),
                                f"Attempt {i + 1} of {job_dict.get('max_retries', 3)}",
                            )
                        )

            # Job completed/failed
            if "completed_at" in job_dict:
                status = job_dict.get("status", "unknown")
                if status == "completed":
                    events.append(
                        ("Completed", float(job_dict["completed_at"]), "Success")
                    )
                else:
                    error_msg = job_dict.get("error", "Unknown error")
                    events.append(
                        (
                            "Failed",
                            float(job_dict["completed_at"]),
                            truncate_string(error_msg, 50),
                        )
                    )

            # Sort events by timestamp
            events.sort(key=lambda x: x[1])

            # Add rows
            prev_timestamp = None
            for event, timestamp, details in events:
                # Calculate time since previous event
                time_diff = ""
                if prev_timestamp:
                    diff = timestamp - prev_timestamp
                    time_diff = f" (+{format_duration(diff)})"

                table.add_row(event, format_timestamp(timestamp) + time_diff, details)
                prev_timestamp = timestamp

            console.print(table)

            # Show total duration
            if events:
                total_duration = events[-1][1] - events[0][1]
                console.print(
                    f"\n[bold]Total Duration:[/bold] {format_duration(total_duration)}"
                )

        finally:
            await job_store.aclose()
