"""Debug and testing commands for RRQ"""

import asyncio
import json
import random
import time
from datetime import datetime, timedelta
from typing import List, Optional

import click

from rrq.constants import JOB_KEY_PREFIX, QUEUE_KEY_PREFIX
from rrq.store import JobStore
from rrq.cli_commands.base import AsyncCommand, load_app_settings, get_job_store
from ..utils import (
    console,
    create_progress,
    print_info,
    print_success,
    print_warning,
)


class DebugCommands(AsyncCommand):
    """Debug and testing commands"""

    def register(self, cli_group: click.Group) -> None:
        """Register debug commands"""

        @cli_group.group("debug")
        def debug_group():
            """Debug and testing tools"""
            pass

        # Generate fake jobs
        @debug_group.command("generate-jobs")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--count",
            type=int,
            default=100,
            help="Number of jobs to generate",
        )
        @click.option(
            "--queue",
            "queue_names",
            multiple=True,
            help="Queue names to use (default: test, urgent, low_priority)",
        )
        @click.option(
            "--status",
            "statuses",
            multiple=True,
            type=click.Choice(["pending", "active", "completed", "failed", "retrying"]),
            help="Job statuses to create (default: mix of all)",
        )
        @click.option(
            "--age-hours",
            type=int,
            default=24,
            help="Maximum age of jobs in hours",
        )
        @click.option(
            "--batch-size",
            type=int,
            default=10,
            help="Batch size for bulk operations",
        )
        def generate_jobs(
            settings_object_path: str,
            count: int,
            queue_names: tuple,
            statuses: tuple,
            age_hours: int,
            batch_size: int,
        ):
            """Generate fake jobs for testing"""
            self.make_async(self._generate_jobs)(
                settings_object_path,
                count,
                queue_names,
                statuses,
                age_hours,
                batch_size,
            )

        # Generate fake workers
        @debug_group.command("generate-workers")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--count",
            type=int,
            default=5,
            help="Number of workers to simulate",
        )
        @click.option(
            "--duration",
            type=int,
            default=60,
            help="Duration to simulate workers in seconds",
        )
        def generate_workers(settings_object_path: str, count: int, duration: int):
            """Generate fake worker heartbeats for testing"""
            self.make_async(self._generate_workers)(
                settings_object_path, count, duration
            )

        # Submit test job
        @debug_group.command("submit")
        @click.argument("function_name")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--args",
            help="JSON string of positional arguments",
        )
        @click.option(
            "--kwargs",
            help="JSON string of keyword arguments",
        )
        @click.option(
            "--queue",
            help="Queue name",
        )
        @click.option(
            "--delay",
            type=int,
            help="Delay in seconds",
        )
        def submit_job(
            function_name: str,
            settings_object_path: str,
            args: Optional[str],
            kwargs: Optional[str],
            queue: Optional[str],
            delay: Optional[int],
        ):
            """Submit a test job"""
            self.make_async(self._submit_job)(
                function_name, settings_object_path, args, kwargs, queue, delay
            )

        # Clear test data
        @debug_group.command("clear")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--confirm",
            is_flag=True,
            help="Confirm deletion without prompt",
        )
        @click.option(
            "--pattern",
            default="test_*",
            help="Pattern to match for deletion (default: test_*)",
        )
        def clear_data(settings_object_path: str, confirm: bool, pattern: str):
            """Clear test data from Redis"""
            self.make_async(self._clear_data)(settings_object_path, confirm, pattern)

        # Stress test
        @debug_group.command("stress-test")
        @click.option(
            "--settings",
            "settings_object_path",
            type=str,
            help="Python settings path (e.g., myapp.settings.rrq_settings)",
        )
        @click.option(
            "--jobs-per-second",
            type=int,
            default=10,
            help="Jobs to create per second",
        )
        @click.option(
            "--duration",
            type=int,
            default=60,
            help="Duration in seconds",
        )
        @click.option(
            "--queues",
            multiple=True,
            help="Queue names to use",
        )
        def stress_test(
            settings_object_path: str,
            jobs_per_second: int,
            duration: int,
            queues: tuple,
        ):
            """Run stress test by creating jobs continuously"""
            self.make_async(self._stress_test)(
                settings_object_path, jobs_per_second, duration, queues
            )

    async def _generate_jobs(
        self,
        settings_object_path: str,
        count: int,
        queue_names: tuple,
        statuses: tuple,
        age_hours: int,
        batch_size: int,
    ) -> None:
        """Generate fake jobs"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            # Default values
            if not queue_names:
                queue_names = ("test", "urgent", "low_priority", "default")

            if not statuses:
                statuses = ("pending", "completed", "failed", "retrying")

            # Sample function names
            function_names = [
                "process_data",
                "send_email",
                "generate_report",
                "cleanup_files",
                "sync_database",
                "resize_image",
                "calculate_metrics",
                "export_csv",
                "backup_data",
                "validate_input",
            ]

            # Generate timestamps
            now = datetime.now()
            start_time = now - timedelta(hours=age_hours)

            with create_progress() as progress:
                task = progress.add_task(
                    f"Generating {count} fake jobs...", total=count
                )

                created_jobs = []
                for i in range(count):
                    # Random attributes
                    job_id = f"test_job_{int(time.time() * 1000000)}_{i}"
                    function_name = random.choice(function_names)
                    queue_name = random.choice(queue_names)
                    status = random.choice(statuses)

                    # Random timestamps
                    created_at = start_time + timedelta(
                        seconds=random.randint(0, int(age_hours * 3600))
                    )

                    # Create job data
                    job_data = {
                        "id": job_id,
                        "function_name": function_name,
                        "queue_name": queue_name,
                        "status": status,
                        "args": json.dumps([f"arg_{i}", random.randint(1, 100)]),
                        "kwargs": json.dumps(
                            {
                                "user_id": random.randint(1, 1000),
                                "priority": random.choice(["high", "medium", "low"]),
                            }
                        ),
                        "created_at": created_at.timestamp(),
                        "retries": random.randint(0, 3),
                        "max_retries": 3,
                    }

                    # Add status-specific fields
                    if status in ("completed", "failed"):
                        started_at = created_at + timedelta(
                            seconds=random.randint(1, 60)
                        )
                        completed_at = started_at + timedelta(
                            seconds=random.randint(1, 300)
                        )
                        job_data.update(
                            {
                                "started_at": started_at.timestamp(),
                                "completed_at": completed_at.timestamp(),
                                "worker_id": f"worker_{random.randint(1, 10)}",
                            }
                        )

                        if status == "completed":
                            job_data["result"] = json.dumps(
                                {
                                    "success": True,
                                    "processed_items": random.randint(1, 100),
                                }
                            )
                        else:
                            job_data["error"] = random.choice(
                                [
                                    "Connection timeout",
                                    "Invalid input data",
                                    "Database error",
                                    "File not found",
                                    "Permission denied",
                                ]
                            )

                    elif status == "active":
                        started_at = created_at + timedelta(
                            seconds=random.randint(1, 60)
                        )
                        job_data.update(
                            {
                                "started_at": started_at.timestamp(),
                                "worker_id": f"worker_{random.randint(1, 10)}",
                            }
                        )

                    created_jobs.append((job_id, job_data, queue_name, status))

                    # Batch insert
                    if len(created_jobs) >= batch_size:
                        await self._insert_job_batch(job_store, created_jobs)
                        created_jobs = []
                        progress.update(task, advance=batch_size)

                # Insert remaining jobs
                if created_jobs:
                    await self._insert_job_batch(job_store, created_jobs)
                    progress.update(task, advance=len(created_jobs))

            print_success(
                f"Generated {count} fake jobs across {len(queue_names)} queues"
            )

        finally:
            await job_store.aclose()

    async def _insert_job_batch(self, job_store: JobStore, jobs: List[tuple]) -> None:
        """Insert a batch of jobs into Redis"""
        async with job_store.redis.pipeline() as pipe:
            for job_id, job_data, queue_name, status in jobs:
                # Insert job data
                job_key = f"{JOB_KEY_PREFIX}{job_id}"
                pipe.hset(job_key, mapping=job_data)

                # Add to queue if pending
                if status == "pending":
                    queue_key = f"{QUEUE_KEY_PREFIX}{queue_name}"
                    priority = job_data.get("created_at", time.time())
                    pipe.zadd(queue_key, {job_id: priority})

            await pipe.execute()

    async def _generate_workers(
        self, settings_object_path: str, count: int, duration: int
    ) -> None:
        """Generate fake worker heartbeats"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            worker_ids = [f"test_worker_{i}" for i in range(count)]

            print_info(f"Simulating {count} workers for {duration} seconds...")

            start_time = time.time()
            while time.time() - start_time < duration:
                # Update each worker
                for worker_id in worker_ids:
                    health_data = {
                        "worker_id": worker_id,
                        "status": random.choice(["running", "idle", "polling"]),
                        "active_jobs": random.randint(0, 5),
                        "concurrency_limit": random.randint(5, 20),
                        "queues": random.sample(
                            ["test", "urgent", "low_priority", "default"], 2
                        ),
                        "timestamp": time.time(),
                    }

                    await job_store.set_worker_health(worker_id, health_data, 60)

                # Wait before next update
                await asyncio.sleep(5)

                # Show progress
                elapsed = time.time() - start_time
                remaining = duration - elapsed
                console.print(
                    f"\rSimulating workers... {remaining:.0f}s remaining", end=""
                )

            console.print("\nWorker simulation complete")

        finally:
            await job_store.aclose()

    async def _submit_job(
        self,
        function_name: str,
        settings_object_path: str,
        args: Optional[str],
        kwargs: Optional[str],
        queue: Optional[str],
        delay: Optional[int],
    ) -> None:
        """Submit a test job"""
        settings = load_app_settings(settings_object_path)

        # Parse arguments
        parsed_args = json.loads(args) if args else []
        parsed_kwargs = json.loads(kwargs) if kwargs else {}

        # Create client
        from rrq.client import RRQClient

        client = RRQClient(settings=settings)

        try:
            # Submit job
            job_id = await client.enqueue(
                function_name=function_name,
                args=parsed_args,
                kwargs=parsed_kwargs,
                queue_name=queue or settings.default_queue_name,
                delay=delay,
            )

            print_success(f"Job submitted: {job_id}")
            console.print(f"Function: {function_name}")
            console.print(f"Args: {parsed_args}")
            console.print(f"Kwargs: {parsed_kwargs}")
            console.print(f"Queue: {queue or settings.default_queue_name}")
            if delay:
                console.print(f"Delay: {delay}s")

        finally:
            await client.aclose()

    async def _clear_data(
        self, settings_object_path: str, confirm: bool, pattern: str
    ) -> None:
        """Clear test data from Redis"""
        settings = load_app_settings(settings_object_path)
        job_store = await get_job_store(settings)

        try:
            # Find matching keys
            keys_to_delete = []
            async for key in job_store.redis.scan_iter(match=pattern):
                keys_to_delete.append(key.decode())

            if not keys_to_delete:
                print_info(f"No keys found matching pattern: {pattern}")
                return

            print_warning(
                f"Found {len(keys_to_delete)} keys matching pattern: {pattern}"
            )

            # Confirm deletion
            if not confirm:
                if not click.confirm(f"Delete {len(keys_to_delete)} keys?"):
                    print_info("Deletion cancelled")
                    return

            # Delete keys
            if keys_to_delete:
                deleted = await job_store.redis.delete(*keys_to_delete)
                print_success(f"Deleted {deleted} keys")

        finally:
            await job_store.aclose()

    async def _stress_test(
        self,
        settings_object_path: str,
        jobs_per_second: int,
        duration: int,
        queues: tuple,
    ) -> None:
        """Run stress test"""
        settings = load_app_settings(settings_object_path)

        # Default queues
        if not queues:
            queues = ("stress_test",)

        # Create client
        from rrq.client import RRQClient

        client = RRQClient(settings=settings)

        try:
            print_info(
                f"Starting stress test: {jobs_per_second} jobs/sec for {duration}s"
            )

            total_jobs = 0
            start_time = time.time()

            while time.time() - start_time < duration:
                batch_start = time.time()

                # Submit jobs for this second
                for i in range(jobs_per_second):
                    queue_name = random.choice(queues)

                    await client.enqueue(
                        function_name="stress_test_job",
                        args=[total_jobs + i],
                        kwargs={"batch": int(time.time())},
                        queue_name=queue_name,
                    )

                total_jobs += jobs_per_second

                # Wait for next second
                elapsed = time.time() - batch_start
                if elapsed < 1.0:
                    await asyncio.sleep(1.0 - elapsed)

                # Show progress
                test_elapsed = time.time() - start_time
                remaining = duration - test_elapsed
                console.print(
                    f"\rStress test: {total_jobs} jobs submitted, {remaining:.0f}s remaining",
                    end="",
                )

            console.print(f"\nStress test complete: {total_jobs} jobs submitted")

        finally:
            await client.aclose()
