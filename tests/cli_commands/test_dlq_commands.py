"""Tests for DLQ management CLI commands"""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rrq.cli_commands.commands.dlq import DLQCommands


class TestDLQCommands:
    """Test DLQ management commands"""

    @pytest.fixture
    def dlq_commands(self):
        """DLQCommands instance for testing"""
        return DLQCommands()

    @pytest.fixture
    def cli_with_dlq_commands(self, cli_runner):
        """CLI runner with DLQ commands registered"""
        import click

        @click.group()
        def test_cli():
            pass

        dlq_commands = DLQCommands()
        dlq_commands.register(test_cli)
        return test_cli, cli_runner

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_list_command(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ list command"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock DLQ job IDs
        job_ids = [b"job_001", b"job_002", b"job_003"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)
        mock_store.redis.llen = AsyncMock(return_value=len(job_ids))

        # Mock Redis pipeline for batch_get_jobs
        mock_pipeline = MagicMock()
        mock_pipeline.hgetall = MagicMock()
        mock_pipeline.execute = AsyncMock(
            return_value=[
                {
                    b"id": b"job_001",
                    b"function_name": b"test_function",
                    b"queue_name": b"urgent",
                    b"status": b"failed",
                    b"last_error": b"Test error message",
                    b"completion_time": str(datetime.now().timestamp()).encode(),
                    b"current_retries": b"2",
                },
                {
                    b"id": b"job_002",
                    b"function_name": b"test_function",
                    b"queue_name": b"urgent",
                    b"status": b"failed",
                    b"last_error": b"Test error message",
                    b"completion_time": str(datetime.now().timestamp()).encode(),
                    b"current_retries": b"2",
                },
                {
                    b"id": b"job_003",
                    b"function_name": b"test_function",
                    b"queue_name": b"urgent",
                    b"status": b"failed",
                    b"last_error": b"Test error message",
                    b"completion_time": str(datetime.now().timestamp()).encode(),
                    b"current_retries": b"2",
                },
            ]
        )
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["dlq", "list"])

        assert result.exit_code == 0
        assert "Jobs in DLQ" in result.output
        assert "job_001" in result.output
        assert "test_function" in result.output
        assert "urgent" in result.output

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_list_with_filtering(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ list command with queue filtering"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock DLQ job IDs
        job_ids = [b"job_001", b"job_002"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock Redis pipeline for batch_get_jobs - one from urgent queue, one from default
        mock_pipeline = MagicMock()
        mock_pipeline.hgetall = MagicMock()
        mock_pipeline.execute = AsyncMock(
            return_value=[
                {
                    b"id": b"job_001",
                    b"function_name": b"test_function",
                    b"queue_name": b"urgent",
                    b"status": b"failed",
                    b"last_error": b"Test error",
                    b"completion_time": str(datetime.now().timestamp()).encode(),
                },
                {
                    b"id": b"job_002",
                    b"function_name": b"other_function",
                    b"queue_name": b"default",
                    b"status": b"failed",
                    b"last_error": b"Other error",
                    b"completion_time": str(datetime.now().timestamp()).encode(),
                },
            ]
        )
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with queue filter
        result = runner.invoke(test_cli, ["dlq", "list", "--queue", "urgent"])

        assert result.exit_code == 0
        assert "job_001" in result.output
        assert "job_002" not in result.output  # Should be filtered out

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_list_raw_output(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ list command with raw JSON output"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        job_ids = [b"job_001"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock Redis pipeline for batch_get_jobs
        mock_pipeline = MagicMock()
        mock_pipeline.hgetall = MagicMock()
        mock_pipeline.execute = AsyncMock(
            return_value=[
                {
                    b"id": b"job_001",
                    b"function_name": b"test_function",
                    b"status": b"failed",
                }
            ]
        )
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with raw flag
        result = runner.invoke(test_cli, ["dlq", "list", "--raw"])

        assert result.exit_code == 0
        # Should contain JSON output
        assert '"id": "job_001"' in result.output or "'id': 'job_001'" in result.output

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_stats_command(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ stats command"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock DLQ job IDs
        job_ids = [b"job_001", b"job_002", b"job_003"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock job data with different queues and functions
        def mock_get_job_side_effect(job_id):
            base_time = datetime.now().timestamp()
            jobs = {
                "job_001": {
                    "id": "job_001",
                    "function_name": "send_email",
                    "queue_name": "urgent",
                    "status": "failed",
                    "last_error": "SMTP connection failed",
                    "completion_time": base_time,
                    "current_retries": 3,
                },
                "job_002": {
                    "id": "job_002",
                    "function_name": "process_data",
                    "queue_name": "default",
                    "status": "failed",
                    "last_error": "Invalid data format",
                    "completion_time": base_time - 100,
                    "current_retries": 2,
                },
                "job_003": {
                    "id": "job_003",
                    "function_name": "send_email",
                    "queue_name": "urgent",
                    "status": "failed",
                    "last_error": "SMTP connection failed",
                    "completion_time": base_time - 200,
                    "current_retries": 1,
                },
            }
            return jobs.get(job_id)

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["dlq", "stats"])

        assert result.exit_code == 0
        assert "DLQ Statistics" in result.output
        assert "Total Jobs" in result.output
        assert "3" in result.output  # Total count
        assert "Jobs by Original Queue" in result.output
        assert "Jobs by Function" in result.output
        assert "Top Error Patterns" in result.output

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_stats_empty_dlq(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ stats command with empty DLQ"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock empty DLQ
        mock_store.redis.lrange = AsyncMock(return_value=[])

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["dlq", "stats"])

        assert result.exit_code == 0
        assert "DLQ" in result.output
        assert "empty" in result.output

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_inspect_command(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ inspect command"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock job data
        job_data = {
            "id": "job_001",
            "function_name": "test_function",
            "queue_name": "urgent",
            "status": "failed",
            "job_args": json.dumps(["arg1", "arg2"]),
            "job_kwargs": json.dumps({"key": "value"}),
            "last_error": "Test error message",
            "traceback": "Traceback (most recent call last):\n  ...",
            "enqueue_time": datetime.now().timestamp(),
            "completion_time": datetime.now().timestamp(),
            "current_retries": 2,
            "max_retries": 3,
            "worker_id": "worker_001",
            "job_unique_key": "unique_test_key",
        }
        mock_store.get_job = AsyncMock(return_value=job_data)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["dlq", "inspect", "job_001"])

        assert result.exit_code == 0
        assert "Job Details: job_001" in result.output
        assert "test_function" in result.output
        assert "urgent" in result.output
        assert "Test error message" in result.output
        assert "Arguments" in result.output
        assert "Keyword Arguments" in result.output
        assert "Error Information" in result.output
        assert "Traceback" in result.output

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_inspect_nonexistent_job(
        self, mock_get_job_store, cli_with_dlq_commands
    ):
        """Test DLQ inspect command with nonexistent job"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock job not found
        mock_store.get_job = AsyncMock(return_value=None)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["dlq", "inspect", "nonexistent_job"])

        assert result.exit_code == 0
        assert "not found" in result.output

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_requeue_command(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ requeue command with filtering"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock DLQ job IDs
        job_ids = [b"job_001", b"job_002"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock Redis pipeline for batch_get_jobs
        mock_pipeline = MagicMock()
        mock_pipeline.hgetall = MagicMock()
        mock_pipeline.execute = AsyncMock(
            return_value=[
                {
                    b"id": b"job_001",
                    b"function_name": b"test_function",
                    b"queue_name": b"urgent",
                    b"status": b"failed",
                },
                {
                    b"id": b"job_002",
                    b"function_name": b"other_function",
                    b"queue_name": b"default",
                    b"status": b"failed",
                },
            ]
        )
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)

        mock_store.add_job_to_queue = AsyncMock()
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with queue filter
        result = runner.invoke(test_cli, ["dlq", "requeue", "--queue", "urgent"])

        assert result.exit_code == 0
        assert "Found 1 matching jobs" in result.output
        assert "Successfully requeued 1 jobs" in result.output

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_requeue_dry_run(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ requeue command with dry run"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        job_ids = [b"job_001"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock Redis pipeline for batch_get_jobs
        mock_pipeline = MagicMock()
        mock_pipeline.hgetall = MagicMock()
        mock_pipeline.execute = AsyncMock(
            return_value=[
                {
                    b"id": b"job_001",
                    b"function_name": b"test_function",
                    b"queue_name": b"urgent",
                    b"status": b"failed",
                }
            ]
        )
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with dry run
        result = runner.invoke(test_cli, ["dlq", "requeue", "--all", "--dry-run"])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "Would requeue 1 jobs" in result.output
        assert "Jobs to Requeue" in result.output

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_requeue_no_filters(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ requeue command requires filters or --all"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command without filters or --all
        result = runner.invoke(test_cli, ["dlq", "requeue"])

        assert result.exit_code == 0
        assert "Must specify --all or at least one filter" in result.output

    @patch("rrq.cli_commands.commands.dlq.get_job_store")
    def test_dlq_requeue_specific_job(self, mock_get_job_store, cli_with_dlq_commands):
        """Test DLQ requeue command for specific job ID"""
        test_cli, runner = cli_with_dlq_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.llen = AsyncMock(return_value=0)
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock job in DLQ
        job_ids = [b"job_001", b"job_002"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        job_data = {
            "id": "job_001",
            "function_name": "test_function",
            "queue_name": "urgent",
            "status": "failed",
        }
        mock_store.get_job = AsyncMock(return_value=job_data)
        mock_store.add_job_to_queue = AsyncMock()
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with specific job ID
        result = runner.invoke(test_cli, ["dlq", "requeue", "--job-id", "job_001"])

        assert result.exit_code == 0
        assert "Found 1 matching jobs" in result.output
        assert "Successfully requeued 1 jobs" in result.output

    def test_dlq_commands_register(self):
        """Test that DLQ commands register properly"""
        import click

        @click.group()
        def test_cli():
            pass

        dlq_commands = DLQCommands()
        dlq_commands.register(test_cli)

        # Check that dlq group was added
        assert "dlq" in test_cli.commands
        dlq_group = test_cli.commands["dlq"]

        # Check that subcommands were added
        expected_commands = ["list", "stats", "inspect", "requeue"]
        for cmd in expected_commands:
            assert cmd in dlq_group.commands


class TestDLQHelperMethods:
    """Test DLQ helper methods"""

    @pytest.mark.asyncio
    async def test_get_dlq_jobs_filtering(self):
        """Test _get_dlq_jobs method with filtering"""
        from rrq.cli_commands.commands.dlq import DLQCommands

        dlq_commands = DLQCommands()

        # Mock job store
        mock_store = MagicMock()
        mock_store.redis.lrange = AsyncMock(return_value=[b"job_001", b"job_002"])

        # Mock Redis pipeline for batch_get_jobs
        mock_pipeline = MagicMock()
        mock_pipeline.hgetall = MagicMock()
        timestamp1 = str(datetime.now().timestamp()).encode()
        timestamp2 = str(datetime.now().timestamp() - 100).encode()
        mock_pipeline.execute = AsyncMock(
            return_value=[
                {
                    b"id": b"job_001",
                    b"function_name": b"send_email",
                    b"queue_name": b"urgent",
                    b"completion_time": timestamp1,
                },
                {
                    b"id": b"job_002",
                    b"function_name": b"process_data",
                    b"queue_name": b"default",
                    b"completion_time": timestamp2,
                },
            ]
        )
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)

        # Test filtering by queue
        jobs = await dlq_commands._get_dlq_jobs(
            mock_store, "test_dlq", original_queue="urgent"
        )

        assert len(jobs) == 1
        assert jobs[0]["id"] == "job_001"

        # Test filtering by function
        jobs = await dlq_commands._get_dlq_jobs(
            mock_store, "test_dlq", function_name="process_data"
        )

        assert len(jobs) == 1
        assert jobs[0]["id"] == "job_002"

    @pytest.mark.asyncio
    async def test_get_dlq_statistics(self):
        """Test _get_dlq_statistics method"""
        from rrq.cli_commands.commands.dlq import DLQCommands

        dlq_commands = DLQCommands()

        # Mock job store
        mock_store = MagicMock()
        mock_store.redis.lrange = AsyncMock(return_value=[b"job_001", b"job_002"])

        # Mock job data
        def mock_get_job_side_effect(job_id):
            base_time = datetime.now().timestamp()
            if job_id == "job_001":
                return {
                    "id": "job_001",
                    "function_name": "send_email",
                    "queue_name": "urgent",
                    "completion_time": base_time,
                    "current_retries": 3,
                    "last_error": "SMTP connection failed",
                }
            elif job_id == "job_002":
                return {
                    "id": "job_002",
                    "function_name": "send_email",
                    "queue_name": "default",
                    "completion_time": base_time - 100,
                    "current_retries": 1,
                    "last_error": "Authentication failed",
                }
            return None

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        stats = await dlq_commands._get_dlq_statistics(mock_store, "test_dlq")

        assert stats["total_jobs"] == 2
        assert stats["avg_retries"] == 2.0  # (3 + 1) / 2
        assert "urgent" in stats["by_queue"]
        assert "default" in stats["by_queue"]
        assert stats["by_queue"]["urgent"] == 1
        assert stats["by_queue"]["default"] == 1
        assert "send_email" in stats["by_function"]
        assert stats["by_function"]["send_email"] == 2
        assert len(stats["top_errors"]) == 2

    @pytest.mark.asyncio
    async def test_requeue_specific_jobs(self):
        """Test _requeue_specific_jobs method"""
        from rrq.cli_commands.commands.dlq import DLQCommands

        dlq_commands = DLQCommands()

        # Mock job store
        mock_store = MagicMock()
        mock_store.redis.lrem = AsyncMock(return_value=1)
        mock_store.redis.hset = AsyncMock()
        mock_store.add_job_to_queue = AsyncMock()

        jobs = [
            {"id": "job_001", "queue_name": "urgent"},
            {"id": "job_002", "queue_name": "default"},
        ]

        count = await dlq_commands._requeue_specific_jobs(
            mock_store, "test_dlq", "target_queue", jobs
        )

        assert count == 2
        assert mock_store.redis.lrem.call_count == 2
        assert mock_store.add_job_to_queue.call_count == 2
        assert mock_store.redis.hset.call_count == 2
