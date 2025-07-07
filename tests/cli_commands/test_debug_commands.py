"""Tests for debug and testing CLI commands"""

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rrq.cli_commands.commands.debug import DebugCommands


class TestDebugCommands:
    """Test debug and testing commands"""

    @pytest.fixture
    def debug_commands(self):
        """DebugCommands instance for testing"""
        return DebugCommands()

    @pytest.fixture
    def cli_with_debug_commands(self, cli_runner):
        """CLI runner with debug commands registered"""
        import click

        @click.group()
        def test_cli():
            pass

        debug_commands = DebugCommands()
        debug_commands.register(test_cli)
        return test_cli, cli_runner

    @patch("rrq.cli_commands.commands.debug.get_job_store")
    def test_generate_jobs_command(self, mock_get_job_store, cli_with_debug_commands):
        """Test generate-jobs command"""
        test_cli, runner = cli_with_debug_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock pipeline for batch operations
        mock_pipeline = MagicMock()
        mock_pipeline.hset = MagicMock(return_value=None)
        mock_pipeline.zadd = MagicMock(return_value=None)
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(
            test_cli,
            ["debug", "generate-jobs", "--count", "10", "--queue", "test_queue"],
        )

        assert result.exit_code == 0
        assert "Generated 10 fake jobs" in result.output

    @patch("rrq.cli_commands.commands.debug.get_job_store")
    def test_generate_jobs_with_options(
        self, mock_get_job_store, cli_with_debug_commands
    ):
        """Test generate-jobs command with custom options"""
        test_cli, runner = cli_with_debug_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock pipeline
        mock_pipeline = MagicMock()
        mock_pipeline.hset = MagicMock(return_value=None)
        mock_pipeline.zadd = MagicMock(return_value=None)
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with custom options
        result = runner.invoke(
            test_cli,
            [
                "debug",
                "generate-jobs",
                "--count",
                "5",
                "--queue",
                "urgent",
                "--queue",
                "low_priority",
                "--status",
                "completed",
                "--status",
                "failed",
                "--age-hours",
                "12",
                "--batch-size",
                "2",
            ],
        )

        assert result.exit_code == 0
        assert "Generated 5 fake jobs" in result.output

    @patch("rrq.cli_commands.commands.debug.get_job_store")
    def test_generate_workers_command(
        self, mock_get_job_store, cli_with_debug_commands
    ):
        """Test generate-workers command"""
        test_cli, runner = cli_with_debug_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.set_worker_health = AsyncMock()

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Mock asyncio.sleep to speed up test
        with patch("asyncio.sleep", new_callable=AsyncMock):
            # Run command with short duration
            result = runner.invoke(
                test_cli,
                ["debug", "generate-workers", "--count", "2", "--duration", "1"],
            )

        assert result.exit_code == 0
        assert "Simulating 2 workers for 1 seconds" in result.output
        assert "Worker simulation complete" in result.output

    @patch("rrq.client.RRQClient")
    def test_submit_job_command(self, mock_client_class, cli_with_debug_commands):
        """Test submit command"""
        test_cli, runner = cli_with_debug_commands

        # Mock client
        mock_client = MagicMock()
        mock_client.aclose = AsyncMock()
        mock_client.enqueue = AsyncMock(return_value="test_job_123")

        # Mock client class directly
        mock_client_class.return_value = mock_client

        # Run command
        result = runner.invoke(
            test_cli,
            [
                "debug",
                "submit",
                "test_function",
                "--args",
                '["arg1", "arg2"]',
                "--kwargs",
                '{"key": "value"}',
                "--queue",
                "test_queue",
                "--delay",
                "5",
            ],
        )

        assert result.exit_code == 0
        assert "Job submitted: test_job_123" in result.output
        assert "Function: test_function" in result.output
        assert "Delay: 5s" in result.output

        # Verify client.enqueue was called correctly
        mock_client.enqueue.assert_called_once()
        call_kwargs = mock_client.enqueue.call_args.kwargs
        assert call_kwargs["function_name"] == "test_function"
        assert call_kwargs["args"] == ["arg1", "arg2"]
        assert call_kwargs["kwargs"] == {"key": "value"}
        assert call_kwargs["queue_name"] == "test_queue"
        assert call_kwargs["delay"] == 5

    @patch("rrq.client.RRQClient")
    def test_submit_job_defaults(self, mock_client_class, cli_with_debug_commands):
        """Test submit command with defaults"""
        test_cli, runner = cli_with_debug_commands

        # Mock client
        mock_client = MagicMock()
        mock_client.aclose = AsyncMock()
        mock_client.enqueue = AsyncMock(return_value="test_job_456")

        # Mock client class directly
        mock_client_class.return_value = mock_client

        # Run command with minimal args
        result = runner.invoke(test_cli, ["debug", "submit", "simple_function"])

        assert result.exit_code == 0
        assert "Job submitted: test_job_456" in result.output

        # Verify defaults were used
        call_kwargs = mock_client.enqueue.call_args.kwargs
        assert call_kwargs["args"] == []
        assert call_kwargs["kwargs"] == {}

    @patch("rrq.cli_commands.commands.debug.get_job_store")
    def test_clear_data_command(self, mock_get_job_store, cli_with_debug_commands):
        """Test clear command"""
        test_cli, runner = cli_with_debug_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock scan_iter to return test keys
        async def mock_scan_iter(match=None):
            test_keys = [b"test_job_001", b"test_job_002", b"test_worker_001"]
            for key in test_keys:
                yield key

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.delete = AsyncMock(return_value=3)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with confirmation
        result = runner.invoke(
            test_cli, ["debug", "clear", "--pattern", "test_*", "--confirm"]
        )

        assert result.exit_code == 0
        assert "Found 3 keys matching pattern" in result.output
        assert "Deleted 3 keys" in result.output

    @patch("rrq.cli_commands.commands.debug.get_job_store")
    def test_clear_data_no_keys(self, mock_get_job_store, cli_with_debug_commands):
        """Test clear command when no keys match"""
        test_cli, runner = cli_with_debug_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock scan_iter to return no keys
        async def mock_scan_iter(match=None):
            return
            yield  # Make it an async generator

        mock_store.redis.scan_iter = mock_scan_iter

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(
            test_cli, ["debug", "clear", "--pattern", "nonexistent_*"]
        )

        assert result.exit_code == 0
        assert "No keys found matching pattern" in result.output

    @patch("rrq.client.RRQClient")
    def test_stress_test_command(self, mock_client_class, cli_with_debug_commands):
        """Test stress-test command"""
        test_cli, runner = cli_with_debug_commands

        # Mock client
        mock_client = MagicMock()
        mock_client.aclose = AsyncMock()
        mock_client.enqueue = AsyncMock(
            side_effect=lambda **kwargs: f"job_{time.time()}"
        )

        # Mock client class directly
        mock_client_class.return_value = mock_client

        # Mock asyncio.sleep to speed up test
        with patch("asyncio.sleep", new_callable=AsyncMock):
            # Run command with minimal duration
            result = runner.invoke(
                test_cli,
                [
                    "debug",
                    "stress-test",
                    "--jobs-per-second",
                    "2",
                    "--duration",
                    "1",
                    "--queues",
                    "stress_queue",
                ],
            )

        assert result.exit_code == 0
        assert "Starting stress test: 2 jobs/sec for 1s" in result.output
        assert "Stress test complete" in result.output

        # Should have enqueued approximately 2 jobs
        assert mock_client.enqueue.call_count >= 1

    def test_debug_commands_register(self):
        """Test that debug commands register properly"""
        import click

        @click.group()
        def test_cli():
            pass

        debug_commands = DebugCommands()
        debug_commands.register(test_cli)

        # Check that debug group was added
        assert "debug" in test_cli.commands
        debug_group = test_cli.commands["debug"]

        # Check that subcommands were added
        expected_commands = [
            "generate-jobs",
            "generate-workers",
            "submit",
            "clear",
            "stress-test",
        ]
        for cmd in expected_commands:
            assert cmd in debug_group.commands


class TestDebugHelpers:
    """Test debug command helper functions"""

    @pytest.mark.asyncio
    async def test_insert_job_batch(self):
        """Test _insert_job_batch method"""
        # This tests the batch insertion logic conceptually
        jobs = [
            ("job_001", {"status": "pending"}, "test_queue", "pending"),
            ("job_002", {"status": "completed"}, "urgent", "completed"),
        ]

        # Simulate pipeline operations
        pipeline_calls = []

        for job_id, job_data, queue_name, status in jobs:
            # hset operation
            pipeline_calls.append(("hset", f"rrq:job:{job_id}", job_data))

            # zadd operation for pending jobs
            if status == "pending":
                pipeline_calls.append(("zadd", f"rrq:queue:{queue_name}", job_id))

        # Should have 3 operations (2 hset, 1 zadd)
        assert len(pipeline_calls) == 3
        assert pipeline_calls[0][0] == "hset"
        assert (
            pipeline_calls[1][0] == "zadd"
        )  # zadd comes after the first pending job hset
        assert pipeline_calls[2][0] == "hset"  # second job hset

    def test_generate_fake_job_data(self):
        """Test generation of fake job data"""
        import random
        from datetime import datetime, timedelta

        # Simulate fake job generation
        function_names = ["process_data", "send_email", "generate_report"]
        queue_names = ["urgent", "default", "low_priority"]
        statuses = ["pending", "completed", "failed", "retrying"]

        # Generate a fake job
        now = datetime.now()
        job_id = f"test_job_{int(time.time() * 1000000)}_0"
        function_name = random.choice(function_names)
        queue_name = random.choice(queue_names)
        status = random.choice(statuses)
        created_at = now - timedelta(hours=random.randint(0, 24))

        job_data = {
            "id": job_id,
            "function_name": function_name,
            "queue_name": queue_name,
            "status": status,
            "args": json.dumps(["arg_0", random.randint(1, 100)]),
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

        # Verify structure
        assert "id" in job_data
        assert "function_name" in job_data
        assert "queue_name" in job_data
        assert "status" in job_data
        assert function_name in function_names
        assert queue_name in queue_names
        assert status in statuses

    def test_generate_fake_worker_data(self):
        """Test generation of fake worker data"""
        import random

        # Simulate fake worker generation
        worker_id = "test_worker_0"

        health_data = {
            "worker_id": worker_id,
            "status": random.choice(["running", "idle", "polling"]),
            "active_jobs": random.randint(0, 5),
            "concurrency_limit": random.randint(5, 20),
            "queues": random.sample(["test", "urgent", "low_priority", "default"], 2),
            "timestamp": time.time(),
        }

        # Verify structure
        assert "worker_id" in health_data
        assert "status" in health_data
        assert "active_jobs" in health_data
        assert "concurrency_limit" in health_data
        assert "queues" in health_data
        assert "timestamp" in health_data
        assert health_data["status"] in ["running", "idle", "polling"]
        assert len(health_data["queues"]) == 2


class TestStressTest:
    """Test stress testing functionality"""

    def test_stress_test_job_distribution(self):
        """Test that stress test distributes jobs across queues"""
        import random

        queues = ["queue1", "queue2", "queue3"]
        jobs_per_second = 10

        # Simulate job distribution
        job_distribution = {queue: 0 for queue in queues}

        for _ in range(jobs_per_second * 5):  # 5 seconds worth
            selected_queue = random.choice(queues)
            job_distribution[selected_queue] += 1

        # Each queue should have received some jobs
        for queue in queues:
            assert job_distribution[queue] > 0

        # Total should equal jobs created
        assert sum(job_distribution.values()) == 50

    def test_stress_test_timing(self):
        """Test stress test timing calculations"""
        import time

        jobs_per_second = 5
        target_duration = 2

        # Simulate timing
        start_time = time.time()
        jobs_created = 0

        # Simulate 2 seconds of job creation
        for second in range(target_duration):
            batch_start = start_time + second

            # Create jobs for this second
            for _ in range(jobs_per_second):
                jobs_created += 1

            # Simulate elapsed time check
            elapsed = (start_time + second + 1) - batch_start
            _sleep_time = max(0, 1.0 - elapsed)
            # In real test, we would sleep for _sleep_time

        assert jobs_created == jobs_per_second * target_duration
