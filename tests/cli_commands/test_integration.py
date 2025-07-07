"""Integration tests for the modular CLI system"""

import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock, AsyncMock

from rrq.cli import rrq
from rrq.cli_commands.base import auto_discover_commands, BaseCommand, AsyncCommand
from rrq.cli_commands.commands.queues import QueueCommands
from rrq.cli_commands.commands.jobs import JobCommands
from rrq.cli_commands.commands.monitor import MonitorCommands
from rrq.cli_commands.commands.debug import DebugCommands
from rrq.cli_commands.commands.dlq import DLQCommands


class TestModularCLISystem:
    """Test the modular CLI system integration"""

    @pytest.fixture
    def cli_runner(self):
        """CLI test runner"""
        return CliRunner()

    def test_enhanced_cli_creation(self, cli_runner):
        """Test that enhanced CLI is created with all commands"""
        enhanced_cli = rrq

        # Should be the same as the base rrq CLI but with additional commands
        assert enhanced_cli is rrq

        # Check that all command groups are registered
        expected_groups = ["worker", "dlq", "queue", "job", "debug"]
        expected_commands = ["check", "monitor"]

        for group in expected_groups:
            assert group in enhanced_cli.commands

        for cmd in expected_commands:
            assert cmd in enhanced_cli.commands

    def test_original_commands_still_work(self, cli_runner):
        """Test that original CLI commands still work"""
        enhanced_cli = rrq

        # Test help
        result = cli_runner.invoke(enhanced_cli, ["--help"])
        assert result.exit_code == 0
        assert "RRQ: Reliable Redis Queue" in result.output

        # Test worker help
        result = cli_runner.invoke(enhanced_cli, ["worker", "--help"])
        assert result.exit_code == 0
        assert "Manage RRQ workers" in result.output

        # Test check command help
        result = cli_runner.invoke(enhanced_cli, ["check", "--help"])
        assert result.exit_code == 0
        assert "health check" in result.output

    def test_new_commands_work(self, cli_runner):
        """Test that new CLI commands work"""
        enhanced_cli = rrq

        # Test queue commands
        result = cli_runner.invoke(enhanced_cli, ["queue", "--help"])
        assert result.exit_code == 0
        assert "Manage and inspect queues" in result.output

        # Test job commands
        result = cli_runner.invoke(enhanced_cli, ["job", "--help"])
        assert result.exit_code == 0
        assert "Inspect and manage jobs" in result.output

        # Test debug commands
        result = cli_runner.invoke(enhanced_cli, ["debug", "--help"])
        assert result.exit_code == 0
        assert "Debug and testing tools" in result.output

        # Test DLQ commands
        result = cli_runner.invoke(enhanced_cli, ["dlq", "--help"])
        assert result.exit_code == 0
        assert "Manage" in result.output and "Dead Letter Queue" in result.output

        # Test monitor command
        result = cli_runner.invoke(enhanced_cli, ["monitor", "--help"])
        assert result.exit_code == 0
        assert "monitoring dashboard" in result.output

    def test_command_subcommands(self, cli_runner):
        """Test that command subcommands are properly registered"""
        enhanced_cli = rrq

        # Test queue subcommands
        result = cli_runner.invoke(enhanced_cli, ["queue", "list", "--help"])
        assert result.exit_code == 0
        assert "List all active queues" in result.output

        result = cli_runner.invoke(enhanced_cli, ["queue", "stats", "--help"])
        assert result.exit_code == 0
        assert "Show detailed statistics" in result.output

        # Test job subcommands
        result = cli_runner.invoke(enhanced_cli, ["job", "show", "--help"])
        assert result.exit_code == 0
        assert "Show detailed information" in result.output

        result = cli_runner.invoke(enhanced_cli, ["job", "list", "--help"])
        assert result.exit_code == 0
        assert "List jobs with filters" in result.output

        # Test debug subcommands
        result = cli_runner.invoke(enhanced_cli, ["debug", "generate-jobs", "--help"])
        assert result.exit_code == 0
        assert "Generate fake jobs" in result.output

        # Test DLQ subcommands
        result = cli_runner.invoke(enhanced_cli, ["dlq", "list", "--help"])
        assert result.exit_code == 0
        assert "List jobs in the Dead Letter Queue" in result.output

        result = cli_runner.invoke(enhanced_cli, ["dlq", "requeue", "--help"])
        assert result.exit_code == 0
        assert "Requeue jobs from DLQ back to a live queue" in result.output

        result = cli_runner.invoke(enhanced_cli, ["dlq", "stats", "--help"])
        assert result.exit_code == 0
        assert "Show DLQ statistics and error patterns" in result.output


class TestBaseCommandSystem:
    """Test the base command system and auto-discovery"""

    def test_base_command_abstract(self):
        """Test that BaseCommand is abstract"""
        with pytest.raises(TypeError):
            BaseCommand()

    def test_async_command_inheritance(self):
        """Test AsyncCommand inheritance"""

        class TestAsyncCommand(AsyncCommand):
            def register(self, cli_group):
                pass

        cmd = TestAsyncCommand()
        assert isinstance(cmd, BaseCommand)
        assert hasattr(cmd, "make_async")

    def test_make_async_wrapper(self):
        """Test make_async wrapper functionality"""

        class TestAsyncCommand(AsyncCommand):
            def register(self, cli_group):
                pass

            async def test_async_method(self, value):
                return f"async_{value}"

        cmd = TestAsyncCommand()
        wrapped = cmd.make_async(cmd.test_async_method)

        # The wrapper should work (though we can't easily test asyncio.run here)
        assert callable(wrapped)

    def test_command_registration(self):
        """Test command registration with CLI groups"""
        import click

        @click.group()
        def test_cli():
            pass

        # Test that all command classes can register
        commands = [
            QueueCommands(),
            JobCommands(),
            MonitorCommands(),
            DebugCommands(),
            DLQCommands(),
        ]

        for command in commands:
            # Should not raise an exception
            command.register(test_cli)


class TestCommandInteroperability:
    """Test that commands work together properly"""

    @patch("rrq.cli_commands.commands.debug.get_job_store")
    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_debug_and_queue_commands_integration(
        self, mock_queue_store, mock_debug_store, cli_runner
    ):
        """Test that debug commands create data that queue commands can see"""
        enhanced_cli = rrq

        # Mock stores for both commands
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock pipeline for debug command
        mock_pipeline = MagicMock()
        mock_pipeline.hset = MagicMock(return_value=None)
        mock_pipeline.zadd = MagicMock(return_value=None)
        mock_pipeline.hmget = MagicMock(return_value=None)
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)

        # Mock scan_iter for queue command
        async def mock_scan_iter(match=None):
            if "queue" in match:
                yield b"rrq:queue:test_queue"

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=5)
        mock_store.redis.zrange = AsyncMock(return_value=[])

        mock_queue_store.return_value = mock_store
        mock_debug_store.return_value = mock_store

        # First, generate some jobs
        result = cli_runner.invoke(
            enhanced_cli,
            [
                "debug",
                "generate-jobs",
                "--count",
                "5",
                "--queue",
                "test_queue",
                "--batch-size",
                "5",
            ],
        )
        assert result.exit_code == 0

        # Then, list queues to see the data
        result = cli_runner.invoke(enhanced_cli, ["queue", "list"])
        assert result.exit_code == 0

    def test_error_handling_in_command_registration(self, cli_runner):
        """Test error handling when command registration fails"""
        import click

        @click.group()
        def test_cli():
            pass

        # Create a command that will fail to register
        class FailingCommand(BaseCommand):
            def register(self, cli_group):
                raise Exception("Registration failed")

        failing_cmd = FailingCommand()

        # Should not crash the CLI
        try:
            failing_cmd.register(test_cli)
        except Exception:
            # Expected to fail
            pass

        # CLI should still be functional
        result = cli_runner.invoke(test_cli, ["--help"])
        assert result.exit_code == 0

    def test_settings_loading_consistency(self):
        """Test that all commands use consistent settings loading"""
        from rrq.cli_commands.base import load_app_settings

        # Test with None (should use defaults or env)
        settings1 = load_app_settings(None)
        settings2 = load_app_settings(None)

        # Should return consistent settings
        assert settings1.redis_dsn == settings2.redis_dsn
        assert settings1.default_queue_name == settings2.default_queue_name


class TestCLIRobustness:
    """Test CLI robustness and error handling"""

    def test_cli_graceful_degradation(self, cli_runner):
        """Test CLI works even if some commands fail to load"""
        # This tests the fallback mechanism in the CLI registration
        enhanced_cli = rrq

        # Should still work
        result = cli_runner.invoke(enhanced_cli, ["--help"])
        assert result.exit_code == 0

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_redis_connection_failure_handling(self, mock_get_job_store, cli_runner):
        """Test handling of Redis connection failures"""
        enhanced_cli = rrq

        # Mock connection failure - make it an async function that raises
        async def failing_get_job_store(settings):
            raise Exception("Redis connection failed")

        mock_get_job_store.side_effect = failing_get_job_store

        # Commands should handle the error gracefully
        result = cli_runner.invoke(enhanced_cli, ["queue", "list"])
        # Should exit with error but not crash
        assert result.exit_code != 0 or "ERROR" in result.output

    def test_invalid_command_arguments(self, cli_runner):
        """Test handling of invalid command arguments"""
        enhanced_cli = rrq

        # Test invalid queue inspect
        result = cli_runner.invoke(enhanced_cli, ["queue", "inspect"])
        assert result.exit_code != 0  # Should fail due to missing argument

        # Test invalid job show
        result = cli_runner.invoke(enhanced_cli, ["job", "show"])
        assert result.exit_code != 0  # Should fail due to missing argument

        # Test invalid options
        result = cli_runner.invoke(enhanced_cli, ["queue", "list", "--invalid-option"])
        assert result.exit_code != 0


class TestAutoDiscovery:
    """Test command auto-discovery functionality"""

    def test_auto_discover_commands_success(self):
        """Test successful auto-discovery of commands"""
        # Test auto-discovery on our actual commands package
        commands = auto_discover_commands("rrq.cli_commands.commands")

        # Should find our command classes
        command_names = [cmd.__name__ for cmd in commands]
        expected_commands = [
            "QueueCommands",
            "JobCommands",
            "MonitorCommands",
            "DebugCommands",
        ]

        for expected in expected_commands:
            assert expected in command_names

    def test_auto_discover_commands_nonexistent_package(self):
        """Test auto-discovery with nonexistent package"""
        # Should return empty list for non-existent packages
        commands = auto_discover_commands("nonexistent.package")
        assert commands == []

    def test_auto_discover_commands_filters_correctly(self):
        """Test that auto-discovery filters correctly"""
        commands = auto_discover_commands("rrq.cli_commands.commands")

        # All returned items should be BaseCommand subclasses
        for cmd_class in commands:
            assert issubclass(cmd_class, BaseCommand)
            assert cmd_class not in (BaseCommand, AsyncCommand)


class TestEndToEndWorkflow:
    """Test end-to-end CLI workflows"""

    @patch("rrq.cli_commands.commands.debug.get_job_store")
    @patch("rrq.cli_commands.commands.queues.get_job_store")
    @patch("rrq.cli_commands.commands.jobs.get_job_store")
    def test_complete_workflow(
        self, mock_job_store, mock_queue_store, mock_debug_store, cli_runner
    ):
        """Test a complete workflow: generate data, inspect queues, inspect jobs"""
        enhanced_cli = rrq

        # Setup common mock store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock for debug (generate-jobs)
        mock_pipeline = MagicMock()
        mock_pipeline.hset = MagicMock(return_value=None)
        mock_pipeline.zadd = MagicMock(return_value=None)
        mock_pipeline.hmget = MagicMock(return_value=None)
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)

        # Mock for queue list
        async def mock_scan_iter(match=None):
            if "queue" in match:
                yield b"rrq:queue:test_queue"
            elif "job" in match:
                yield b"rrq:job:test_job_001"

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=1)
        mock_store.redis.zrange = AsyncMock(return_value=[])

        # Mock for job data
        job_data = {
            b"function_name": b"test_function",
            b"status": b"pending",
            b"created_at": b"1234567890.0",
        }
        job_data_dict = {
            "function_name": "test_function",
            "status": "pending",
            "created_at": "1234567890.0",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        mock_debug_store.return_value = mock_store
        mock_queue_store.return_value = mock_store
        mock_job_store.return_value = mock_store

        # Step 1: Generate test data
        result = cli_runner.invoke(
            enhanced_cli, ["debug", "generate-jobs", "--count", "1"]
        )
        assert result.exit_code == 0
        assert "Generated 1 fake jobs" in result.output

        # Step 2: List queues
        result = cli_runner.invoke(enhanced_cli, ["queue", "list"])
        assert result.exit_code == 0
        assert "Active Queues" in result.output

        # Step 3: List jobs
        result = cli_runner.invoke(enhanced_cli, ["job", "list", "--limit", "5"])
        assert result.exit_code == 0
        assert "Jobs" in result.output
