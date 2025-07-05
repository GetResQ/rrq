"""Additional CLI tests for multi-worker functionality"""

import sys
import pytest
from click.testing import CliRunner

from rrq import cli
from rrq.settings import RRQSettings
from rrq.registry import JobRegistry


@pytest.fixture
def cli_runner():
    """Provides a Click CLI runner for testing."""
    return CliRunner()


@pytest.fixture
def mock_multi_worker_settings_path(tmp_path):
    """Creates a mock settings file for multi-worker testing."""
    settings_file = tmp_path / "test_settings.py"
    settings_file.write_text(
        """
from rrq.settings import RRQSettings
from rrq.registry import JobRegistry

# Create a job registry
test_registry = JobRegistry()

@test_registry.register
async def test_job(context):
    return "test_result"

# Create settings with the registry
rrq_settings = RRQSettings(
    redis_dsn="redis://localhost:6379/1",
    job_registry=test_registry,
    worker_concurrency=2,
)
"""
    )
    
    # Make the module importable
    import sys
    sys.path.insert(0, str(tmp_path))
    
    yield "test_settings.rrq_settings"
    
    # Cleanup
    sys.path.remove(str(tmp_path))


class TestBurstModeRestriction:
    """Test burst mode restriction with multiple workers."""
    
    def test_burst_mode_allowed_with_single_worker(self, cli_runner, mock_multi_worker_settings_path):
        """Test that burst mode is allowed with single worker."""
        # Mock the worker implementation to avoid actual execution
        import unittest.mock as mock
        
        with mock.patch("rrq.cli.RRQWorker") as mock_worker:
            mock_worker_instance = mock.MagicMock()
            mock_worker.return_value = mock_worker_instance
            
            result = cli_runner.invoke(
                cli.rrq,
                [
                    "worker", "run",
                    "--settings", mock_multi_worker_settings_path,
                    "--num-workers", "1",
                    "--burst"
                ]
            )
            
            assert result.exit_code == 0
            mock_worker.assert_called_once()
            # Check that burst=True was passed
            args, kwargs = mock_worker.call_args
            assert kwargs.get("burst") is True
    
    def test_burst_mode_restricted_with_multiple_workers(self, cli_runner, mock_multi_worker_settings_path):
        """Test that burst mode is restricted with multiple workers."""
        result = cli_runner.invoke(
            cli.rrq,
            [
                "worker", "run",
                "--settings", mock_multi_worker_settings_path,
                "--num-workers", "2",
                "--burst"
            ]
        )
        
        assert result.exit_code == 1
        assert "ERROR: --burst mode is not supported with multiple workers" in result.output
        assert "Burst mode cannot coordinate across multiple processes" in result.output
    
    def test_burst_mode_restricted_with_default_cpu_count(self, cli_runner, mock_multi_worker_settings_path):
        """Test that burst mode is restricted when CPU count > 1."""
        import unittest.mock as mock
        
        with mock.patch("rrq.cli.os.cpu_count", return_value=4):
            result = cli_runner.invoke(
                cli.rrq,
                [
                    "worker", "run",
                    "--settings", mock_multi_worker_settings_path,
                    "--burst"
                ]
            )
            
            assert result.exit_code == 1
            assert "ERROR: --burst mode is not supported with multiple workers" in result.output
    
    def test_burst_mode_allowed_with_single_cpu_default(self, cli_runner, mock_multi_worker_settings_path):
        """Test that burst mode is allowed when CPU count is 1."""
        import unittest.mock as mock
        
        with mock.patch("rrq.cli.os.cpu_count", return_value=1):
            with mock.patch("rrq.cli.RRQWorker") as mock_worker:
                mock_worker_instance = mock.MagicMock()
                mock_worker.return_value = mock_worker_instance
                
                result = cli_runner.invoke(
                    cli.rrq,
                    [
                        "worker", "run",
                        "--settings", mock_multi_worker_settings_path,
                        "--burst"
                    ]
                )
                
                assert result.exit_code == 0
                mock_worker.assert_called_once()
    
    def test_multiple_workers_without_burst_works(self, cli_runner, mock_multi_worker_settings_path):
        """Test that multiple workers work without burst mode."""
        import unittest.mock as mock
        
        with mock.patch("rrq.cli.subprocess.Popen") as mock_popen:
            mock_popen_instance = mock.MagicMock()
            mock_popen.return_value = mock_popen_instance
            
            result = cli_runner.invoke(
                cli.rrq,
                [
                    "worker", "run",
                    "--settings", mock_multi_worker_settings_path,
                    "--num-workers", "3"
                ]
            )
            
            assert result.exit_code == 0
            assert mock_popen.call_count == 3