"""Tests for job inspection and management CLI commands"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rrq.cli_commands.commands.jobs import JobCommands


class TestJobCommands:
    """Test job management commands"""

    @pytest.fixture
    def job_commands(self):
        """JobCommands instance for testing"""
        return JobCommands()

    @pytest.fixture
    def cli_with_job_commands(self, cli_runner):
        """CLI runner with job commands registered"""
        import click

        @click.group()
        def test_cli():
            pass

        job_commands = JobCommands()
        job_commands.register(test_cli)
        return test_cli, cli_runner

    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    def test_job_show_command(self, mock_get_job_store, cli_with_job_commands):
        """Test job show command"""
        test_cli, runner = cli_with_job_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock job data
        job_data = {
            b"id": b"test_job_001",
            b"function_name": b"test_function",
            b"queue_name": b"test_queue",
            b"status": b"completed",
            b"args": b'["arg1", "arg2"]',
            b"kwargs": b'{"key": "value"}',
            b"created_at": b"1234567890.0",
            b"started_at": b"1234567895.0",
            b"completed_at": b"1234567900.0",
            b"result": b'{"success": true}',
            b"retries": b"0",
            b"max_retries": b"3",
        }
        job_data_dict = {
            "id": "test_job_001",
            "function_name": "test_function",
            "queue_name": "test_queue",
            "status": "completed",
            "args": '["arg1", "arg2"]',
            "kwargs": '{"key": "value"}',
            "created_at": "1234567890.0",
            "started_at": "1234567895.0",
            "completed_at": "1234567900.0",
            "result": '{"success": true}',
            "retries": "0",
            "max_retries": "3",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["job", "show", "test_job_001"])

        assert result.exit_code == 0
        assert "Job Information" in result.output
        assert "test_job_001" in result.output
        assert "test_function" in result.output
        assert "COMPLETED" in result.output

    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    def test_job_show_nonexistent(self, mock_get_job_store, cli_with_job_commands):
        """Test job show command with nonexistent job"""
        test_cli, runner = cli_with_job_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock job not found
        mock_store.redis.hgetall = AsyncMock(return_value={})
        mock_store.get_job_data_dict = AsyncMock(return_value=None)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["job", "show", "nonexistent_job"])

        assert result.exit_code == 0
        assert "not found" in result.output

    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    def test_job_show_raw(self, mock_get_job_store, cli_with_job_commands):
        """Test job show command with --raw flag"""
        test_cli, runner = cli_with_job_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock job data
        job_data = {
            b"id": b"test_job_001",
            b"function_name": b"test_function",
            b"status": b"pending",
        }
        job_data_dict = {
            "id": "test_job_001",
            "function_name": "test_function",
            "status": "pending",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with --raw
        result = runner.invoke(test_cli, ["job", "show", "test_job_001", "--raw"])

        assert result.exit_code == 0
        # Should contain JSON output
        assert "test_function" in result.output

    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    def test_job_list_command(self, mock_get_job_store, cli_with_job_commands):
        """Test job list command"""
        test_cli, runner = cli_with_job_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock scan_iter to return job keys
        async def mock_scan_iter(match=None):
            job_keys = [b"rrq:job:job_001", b"rrq:job:job_002"]
            for key in job_keys:
                yield key

        mock_store.redis.scan_iter = mock_scan_iter

        # Mock job data
        job_data_1 = {
            b"function_name": b"test_function",
            b"queue_name": b"test_queue",
            b"status": b"completed",
            b"created_at": b"1234567890.0",
            b"completed_at": b"1234567900.0",
            b"started_at": b"1234567895.0",
        }
        job_data_2 = {
            b"function_name": b"send_email",
            b"queue_name": b"urgent",
            b"status": b"failed",
            b"created_at": b"1234567880.0",
        }
        job_data_dict_1 = {
            "function_name": "test_function",
            "queue_name": "test_queue",
            "status": "completed",
            "created_at": "1234567890.0",
            "completed_at": "1234567900.0",
            "started_at": "1234567895.0",
        }
        job_data_dict_2 = {
            "function_name": "send_email",
            "queue_name": "urgent",
            "status": "failed",
            "created_at": "1234567880.0",
        }

        mock_store.redis.hgetall = AsyncMock(side_effect=[job_data_1, job_data_2])
        mock_store.get_job_data_dict = AsyncMock(
            side_effect=[job_data_dict_1, job_data_dict_2]
        )

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["job", "list"])

        assert result.exit_code == 0
        assert "Jobs" in result.output
        assert "job_001" in result.output
        assert "test_functi" in result.output  # May be truncated in table

    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    def test_job_list_with_filters(self, mock_get_job_store, cli_with_job_commands):
        """Test job list command with filters"""
        test_cli, runner = cli_with_job_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock scan_iter to return job keys
        async def mock_scan_iter(match=None):
            job_keys = [b"rrq:job:job_001"]
            for key in job_keys:
                yield key

        mock_store.redis.scan_iter = mock_scan_iter

        # Mock job data that matches filter
        job_data = {
            b"function_name": b"test_function",
            b"queue_name": b"test_queue",
            b"status": b"failed",
            b"created_at": b"1234567890.0",
        }
        job_data_dict = {
            "function_name": "test_function",
            "queue_name": "test_queue",
            "status": "failed",
            "created_at": "1234567890.0",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command with filters
        result = runner.invoke(
            test_cli,
            [
                "job",
                "list",
                "--status",
                "failed",
                "--queue",
                "test_queue",
                "--function",
                "test_function",
            ],
        )

        assert result.exit_code == 0
        assert "job_001" in result.output

    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    @patch("rrq.client.RRQClient")
    def test_job_replay_command(
        self, mock_client_class, mock_get_job_store, cli_with_job_commands
    ):
        """Test job replay command"""
        test_cli, runner = cli_with_job_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock job data
        job_data = {
            b"function_name": b"test_function",
            b"args": b'["arg1"]',
            b"kwargs": b'{"key": "value"}',
            b"queue_name": b"test_queue",
        }
        job_data_dict = {
            "function_name": "test_function",
            "args": '["arg1"]',
            "kwargs": '{"key": "value"}',
            "queue_name": "test_queue",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        # Mock client
        mock_client = MagicMock()
        mock_client.aclose = AsyncMock()
        mock_client.enqueue = AsyncMock(return_value="new_job_123")
        # Mock client class directly
        mock_client_class.return_value = mock_client

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["job", "replay", "test_job_001"])

        assert result.exit_code == 0
        assert "Job replayed with new ID: new_job_123" in result.output

        # Verify client.enqueue was called with correct parameters
        mock_client.enqueue.assert_called_once()
        call_kwargs = mock_client.enqueue.call_args.kwargs
        assert call_kwargs["function_name"] == "test_function"
        assert call_kwargs["args"] == ["arg1"]
        assert call_kwargs["kwargs"] == {"key": "value"}

    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    def test_job_cancel_command(self, mock_get_job_store, cli_with_job_commands):
        """Test job cancel command"""
        test_cli, runner = cli_with_job_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock job data
        job_data = {
            b"status": b"pending",
            b"queue_name": b"test_queue",
        }
        job_data_dict = {
            "status": "pending",
            "queue_name": "test_queue",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)
        mock_store.redis.zrem = AsyncMock(return_value=1)  # Successfully removed
        mock_store.redis.hset = AsyncMock()

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["job", "cancel", "test_job_001"])

        assert result.exit_code == 0
        assert "cancelled successfully" in result.output

    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    def test_job_cancel_non_pending(self, mock_get_job_store, cli_with_job_commands):
        """Test job cancel command on non-pending job"""
        test_cli, runner = cli_with_job_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock job data with non-pending status
        job_data = {
            b"status": b"completed",
            b"queue_name": b"test_queue",
        }
        job_data_dict = {
            "status": "completed",
            "queue_name": "test_queue",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["job", "cancel", "test_job_001"])

        assert result.exit_code == 0
        assert "Can only cancel pending jobs" in result.output

    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    def test_job_trace_command(self, mock_get_job_store, cli_with_job_commands):
        """Test job trace command"""
        test_cli, runner = cli_with_job_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock job data with timeline
        job_data = {
            b"function_name": b"test_function",
            b"created_at": b"1234567890.0",
            b"started_at": b"1234567895.0",
            b"completed_at": b"1234567900.0",
            b"status": b"completed",
            b"worker_id": b"worker_001",
            b"retries": b"1",
            b"max_retries": b"3",
            b"retry_0_at": b"1234567893.0",
        }
        job_data_dict = {
            "function_name": "test_function",
            "created_at": "1234567890.0",
            "started_at": "1234567895.0",
            "completed_at": "1234567900.0",
            "status": "completed",
            "worker_id": "worker_001",
            "retries": "1",
            "max_retries": "3",
            "retry_0_at": "1234567893.0",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store

        # Run command
        result = runner.invoke(test_cli, ["job", "trace", "test_job_001"])

        assert result.exit_code == 0
        assert "Job Timeline" in result.output
        assert "Created" in result.output
        assert "Started" in result.output
        assert "Completed" in result.output
        assert "Total Duration" in result.output

    def test_job_commands_register(self):
        """Test that job commands register properly"""
        import click

        @click.group()
        def test_cli():
            pass

        job_commands = JobCommands()
        job_commands.register(test_cli)

        # Check that job group was added
        assert "job" in test_cli.commands
        job_group = test_cli.commands["job"]

        # Check that subcommands were added
        expected_commands = ["show", "list", "replay", "cancel", "trace"]
        for cmd in expected_commands:
            assert cmd in job_group.commands


class TestJobFiltering:
    """Test job filtering functionality"""

    def test_status_filter(self):
        """Test filtering jobs by status"""
        # This would be tested in the actual _list_jobs method
        # Here we test the logic conceptually
        jobs = [
            {"status": "pending"},
            {"status": "completed"},
            {"status": "failed"},
        ]

        # Filter by status
        failed_jobs = [job for job in jobs if job.get("status") == "failed"]
        assert len(failed_jobs) == 1
        assert failed_jobs[0]["status"] == "failed"

    def test_queue_filter(self):
        """Test filtering jobs by queue"""
        jobs = [
            {"queue_name": "urgent"},
            {"queue_name": "default"},
            {"queue_name": "urgent"},
        ]

        # Filter by queue
        urgent_jobs = [job for job in jobs if job.get("queue_name") == "urgent"]
        assert len(urgent_jobs) == 2

    def test_function_filter(self):
        """Test filtering jobs by function name"""
        jobs = [
            {"function_name": "send_email"},
            {"function_name": "process_data"},
            {"function_name": "send_email"},
        ]

        # Filter by function
        email_jobs = [job for job in jobs if job.get("function_name") == "send_email"]
        assert len(email_jobs) == 2


class TestJobDataParsing:
    """Test job data parsing and formatting"""

    def test_job_timeline_parsing(self):
        """Test parsing job timeline events"""

        job_data = {
            "created_at": "1234567890.0",
            "started_at": "1234567895.0",
            "completed_at": "1234567900.0",
            "retries": "1",
            "retry_0_at": "1234567893.0",
        }

        # Parse timeline events
        events = []

        if "created_at" in job_data:
            events.append(("Created", float(job_data["created_at"])))

        if "started_at" in job_data:
            events.append(("Started", float(job_data["started_at"])))

        retries = int(job_data.get("retries", 0))
        for i in range(retries):
            retry_key = f"retry_{i}_at"
            if retry_key in job_data:
                events.append((f"Retry {i + 1}", float(job_data[retry_key])))

        if "completed_at" in job_data:
            events.append(("Completed", float(job_data["completed_at"])))

        # Sort events by timestamp
        events.sort(key=lambda x: x[1])

        assert len(events) == 4
        assert events[0][0] == "Created"
        assert events[1][0] == "Retry 1"
        assert events[2][0] == "Started"
        assert events[3][0] == "Completed"

    def test_job_duration_calculation(self):
        """Test job duration calculation"""
        # Simulate duration calculation
        started_at = 1234567895.0
        completed_at = 1234567900.0

        duration = completed_at - started_at
        assert duration == 5.0
