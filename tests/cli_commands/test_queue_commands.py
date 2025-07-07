"""Tests for queue management CLI commands"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rrq.cli_commands.commands.queues import QueueCommands


class TestQueueCommands:
    """Test queue management commands"""

    @pytest.fixture
    def queue_commands(self):
        """QueueCommands instance for testing"""
        return QueueCommands()

    @pytest.fixture
    def cli_with_queue_commands(self, cli_runner):
        """CLI runner with queue commands registered"""
        import click

        @click.group()
        def test_cli():
            pass

        queue_commands = QueueCommands()
        queue_commands.register(test_cli)
        return test_cli, cli_runner

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_list_command(self, mock_get_job_store, cli_with_queue_commands):
        """Test queue list command"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.get_job = AsyncMock(return_value=None)
        mock_store.settings = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock scan_iter to return queue keys
        async def mock_scan_iter(match=None, count=None):
            queue_keys = [
                b"rrq:queue:urgent",
                b"rrq:queue:default",
                b"rrq:queue:low_priority",
            ]
            for key in queue_keys:
                yield key

        mock_store.redis.scan_iter = mock_scan_iter

        # Mock queue sizes - handle both bytes and string keys
        queue_sizes = {
            b"rrq:queue:urgent": 5,
            b"rrq:queue:default": 2,
            b"rrq:queue:low_priority": 0,
            "rrq:queue:urgent": 5,
            "rrq:queue:default": 2,
            "rrq:queue:low_priority": 0,
        }
        mock_store.redis.zcard = AsyncMock(
            side_effect=lambda key: queue_sizes.get(key, 0)
        )

        # Mock queue range operations for timestamps
        mock_store.redis.zrange = AsyncMock(return_value=[])

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["queue", "list"])

        assert result.exit_code == 0
        assert "Active Queues" in result.output
        # Only queues with jobs should appear since --show-empty is False by default
        # urgent and default have jobs, low_priority has 0 jobs

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_list_show_empty(self, mock_get_job_store, cli_with_queue_commands):
        """Test queue list command with --show-empty flag"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.get_job = AsyncMock(return_value=None)
        mock_store.settings = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock scan_iter to return queue keys
        async def mock_scan_iter(match=None, count=None):
            queue_keys = [b"rrq:queue:empty_queue"]
            for key in queue_keys:
                yield key

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=0)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with --show-empty
        result = runner.invoke(test_cli, ["queue", "list", "--show-empty"])

        assert result.exit_code == 0
        assert "empty_queue" in result.output

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_stats_command(self, mock_get_job_store, cli_with_queue_commands):
        """Test queue stats command"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.get_job = AsyncMock(return_value=None)
        mock_store.settings = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock pipeline for batch operations
        mock_pipeline = MagicMock()
        mock_pipeline.hmget = MagicMock(return_value=None)
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)

        # Mock scan_iter to return queue keys
        async def mock_scan_iter(match=None, count=None):
            queue_keys = [b"rrq:queue:test_queue"]
            for key in queue_keys:
                yield key

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=3)
        mock_store.redis.zrange = AsyncMock(
            return_value=[(b"job1", 123456), (b"job2", 123457)]
        )

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["queue", "stats"])

        assert result.exit_code == 0
        assert "Queue Statistics" in result.output
        # Should show the queue name (possibly truncated)
        assert "test_" in result.output

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_inspect_command(self, mock_get_job_store, cli_with_queue_commands):
        """Test queue inspect command"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.get_job = AsyncMock(return_value=None)
        mock_store.settings = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock queue existence and size
        mock_store.redis.exists = AsyncMock(return_value=True)
        mock_store.redis.zcard = AsyncMock(return_value=2)

        # Mock job entries
        job_entries = [(b"job_001", 123456.0), (b"job_002", 123457.0)]
        mock_store.redis.zrange = AsyncMock(return_value=job_entries)

        # Mock job data
        job_data = {
            b"function_name": b"test_function",
            b"status": b"pending",
            b"retries": b"0",
        }
        job_data_dict = {
            "function_name": "test_function",
            "status": "pending",
            "retries": "0",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["queue", "inspect", "test_queue"])

        assert result.exit_code == 0
        assert "Jobs in Queue: test_queue" in result.output
        assert "job_001" in result.output
        assert "test_function" in result.output

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_inspect_nonexistent(
        self, mock_get_job_store, cli_with_queue_commands
    ):
        """Test queue inspect command with nonexistent queue"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.get_job = AsyncMock(return_value=None)
        mock_store.settings = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock queue doesn't exist
        mock_store.redis.exists = AsyncMock(return_value=False)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["queue", "inspect", "nonexistent_queue"])

        assert result.exit_code == 0
        assert "not found" in result.output

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_inspect_empty(self, mock_get_job_store, cli_with_queue_commands):
        """Test queue inspect command with empty queue"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.get_job = AsyncMock(return_value=None)
        mock_store.settings = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock empty queue
        mock_store.redis.exists = AsyncMock(return_value=True)
        mock_store.redis.zcard = AsyncMock(return_value=0)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["queue", "inspect", "empty_queue"])

        assert result.exit_code == 0
        assert "is empty" in result.output

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_inspect_pagination(
        self, mock_get_job_store, cli_with_queue_commands
    ):
        """Test queue inspect command with pagination"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.redis.ping = AsyncMock()
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.get_job = AsyncMock(return_value=None)
        mock_store.settings = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock large queue
        mock_store.redis.exists = AsyncMock(return_value=True)
        mock_store.redis.zcard = AsyncMock(return_value=50)

        # Mock job entries for pagination
        job_entries = [(b"job_010", 123466.0), (b"job_011", 123467.0)]
        mock_store.redis.zrange = AsyncMock(return_value=job_entries)

        # Mock job data
        job_data = {
            b"function_name": b"test_function",
            b"status": b"pending",
            b"retries": b"0",
        }
        job_data_dict = {
            "function_name": "test_function",
            "status": "pending",
            "retries": "0",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with offset
        result = runner.invoke(
            test_cli,
            ["queue", "inspect", "large_queue", "--offset", "10", "--limit", "2"],
        )

        assert result.exit_code == 0
        assert "Showing 11-12 of 50 jobs" in result.output
        assert "Use --offset 12 to see more" in result.output

    def test_queue_commands_register(self):
        """Test that queue commands register properly"""
        import click

        @click.group()
        def test_cli():
            pass

        queue_commands = QueueCommands()
        queue_commands.register(test_cli)

        # Check that queue group was added
        assert "queue" in test_cli.commands
        queue_group = test_cli.commands["queue"]

        # Check that subcommands were added
        expected_commands = ["list", "stats", "inspect"]
        for cmd in expected_commands:
            assert cmd in queue_group.commands


class TestQueueStatistics:
    """Test queue statistics calculation"""

    @pytest.mark.asyncio
    @patch("rrq.cli_commands.commands.queues.get_job_store")
    async def test_get_queue_statistics(self, mock_get_job_store):
        """Test _get_queue_statistics method"""
        from rrq.cli_commands.commands.queues import QueueCommands

        # Mock job store
        mock_store = MagicMock()
        mock_store.redis = MagicMock()
        mock_store.redis.zcard = AsyncMock(return_value=5)
        mock_store.redis.zrange = AsyncMock(
            return_value=[(b"job1", 123456), (b"job2", 123457)]
        )
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_store.get_job = AsyncMock(return_value=None)
        mock_store.settings = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        queue_commands = QueueCommands()
        stats = await queue_commands._get_queue_statistics(mock_store, "test_queue")

        assert stats["pending"] == 5
        assert stats["total"] == 5
        assert "avg_wait_time" in stats
        assert "throughput" in stats
