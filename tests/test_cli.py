import sys
from typing import Generator
from unittest import mock

import pytest
from click.testing import CliRunner

from rrq import rrq
from rrq.registry import JobRegistry
from rrq.settings import RRQSettings

# A simple settings file content for testing
SIMPLE_SETTINGS_PY_CONTENT = """
from rrq.settings import RRQSettings
from rrq.registry import JobRegistry

job_registry_app = JobRegistry()
settings_instance = RRQSettings(redis_dsn="redis://localhost:6379/9", job_registry=job_registry_app)
"""

@pytest.fixture(scope="function")
def cli_runner() -> CliRunner:
    return CliRunner()

@pytest.fixture(scope="function")
def test_settings_file(tmp_path) -> Generator[str, None, None]:
    settings_file = tmp_path / "test_app_settings.py"
    settings_file.write_text(SIMPLE_SETTINGS_PY_CONTENT)
    # Add the directory of the settings file to sys.path so it can be imported
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(tmp_path))
    yield str(settings_file)
    # Clean up sys.path
    sys.path = original_sys_path

@pytest.fixture(scope="function")
def mock_app_settings_path(tmp_path):
    settings_dir = tmp_path / "app"
    settings_dir.mkdir()
    settings_file = settings_dir / "settings.py"
    
    # Create a unique registry for each test to avoid interference
    REGISTRY_INSTANCE_NAME = "test_registry_for_cli"
    SETTINGS_INSTANCE_NAME = "test_settings_for_cli"

    settings_content = f"""
from rrq.settings import RRQSettings
from rrq.registry import JobRegistry

{REGISTRY_INSTANCE_NAME} = JobRegistry()
{SETTINGS_INSTANCE_NAME} = RRQSettings(redis_dsn="redis://localhost:6379/9", job_registry={REGISTRY_INSTANCE_NAME})
"""
    settings_file.write_text(settings_content)
    
    # Add to sys.path
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(tmp_path))

    yield f"app.settings.{SETTINGS_INSTANCE_NAME}"

    sys.path = original_sys_path
    # Clean up compiled .pyc files if any, and the directory.
    # This is a bit more involved if directly manipulating sys.modules,
    # but for file-based imports, ensuring sys.path is clean is key.


def test_load_app_settings_success(mock_app_settings_path):
    """Test that _load_app_settings successfully loads a settings object."""
    # The _load_app_settings function is implicitly tested by commands requiring --settings.
    # We can directly test it for more granular feedback if needed.
    from rrq.rrq import _load_app_settings  # Import here to use the modified sys.path
    
    settings_object = _load_app_settings(mock_app_settings_path)
    assert isinstance(settings_object, RRQSettings)
    assert settings_object.redis_dsn == "redis://localhost:6379/9"
    assert isinstance(settings_object.job_registry, JobRegistry)


def test_load_app_settings_failure_module_not_found():
    """Test _load_app_settings with a non-existent module."""
    from rrq.rrq import _load_app_settings

    with pytest.raises(SystemExit) as e:
        _load_app_settings("non_existent_module.settings_object")
    assert e.value.code == 1
    # Ideally, capture click.echo output to verify the error message


def test_load_app_settings_failure_object_not_found(tmp_path):
    """Test _load_app_settings with an existing module but non-existent object."""
    import sys

    from rrq.rrq import _load_app_settings

    module_dir = tmp_path / "fakemodule"
    module_dir.mkdir()
    fake_module_file = module_dir / "config.py"
    fake_module_file.write_text("A = 1") # Contains an object 'A'

    original_sys_path = list(sys.path)
    sys.path.insert(0, str(tmp_path))

    with pytest.raises(SystemExit) as e:
        _load_app_settings("fakemodule.config.NON_EXISTENT_OBJECT")
    assert e.value.code == 1
    
    sys.path = original_sys_path


def test_load_app_settings_fallback_to_default():
    """Test _load_app_settings when no settings path is provided or in environment."""
    from rrq.rrq import _load_app_settings
    # Mock os.getenv to return None for RRQ_SETTINGS
    with mock.patch('os.getenv', return_value=None):
        settings_object = _load_app_settings(None)
        assert isinstance(settings_object, RRQSettings)
        # Verify it's a default settings object (e.g., check a default attribute)
        assert settings_object.redis_dsn == "redis://localhost:6379/0", "Default Redis DSN should be set"


@mock.patch("rrq.rrq.RRQWorker")
def test_worker_run_command_foreground(mock_worker_class, cli_runner, mock_app_settings_path):
    """Test 'rrq worker run' in foreground mode."""
    mock_worker_instance = mock.MagicMock()
    mock_worker_class.return_value = mock_worker_instance

    result = cli_runner.invoke(rrq.rrq, ["worker", "run", "--settings", mock_app_settings_path])

    assert result.exit_code == 0
    mock_worker_class.assert_called_once()
    # Check that the settings object passed to RRQWorker is correct
    args, kwargs = mock_worker_class.call_args
    assert isinstance(kwargs["settings"], RRQSettings)
    assert kwargs["settings"].redis_dsn == "redis://localhost:6379/9"
    assert isinstance(kwargs["settings"].job_registry, JobRegistry)
    assert kwargs["job_registry"] == kwargs["settings"].job_registry 

    mock_worker_instance.run.assert_called_once()


@mock.patch("rrq.rrq.RRQWorker")
def test_worker_run_command_burst_mode(mock_worker_class, cli_runner, mock_app_settings_path):
    """Test 'rrq worker run --burst'."""
    mock_worker_instance = mock.MagicMock()
    mock_worker_class.return_value = mock_worker_instance

    result = cli_runner.invoke(rrq.rrq, ["worker", "run", "--settings", mock_app_settings_path, "--burst"])

    assert result.exit_code != 0  # Burst mode is not implemented
    assert "Burst mode is not implemented yet" in str(result.output) or "Burst mode is not implemented yet" in str(result.exception)
    mock_worker_class.assert_not_called()


@mock.patch("rrq.rrq.start_rrq_worker_subprocess")
def test_worker_run_command_detach_mode(mock_start_subprocess, cli_runner, mock_app_settings_path):
    """Test 'rrq worker run --detach'."""
    result = cli_runner.invoke(rrq.rrq, ["worker", "run", "--settings", mock_app_settings_path, "--detach"])

    assert result.exit_code == 0
    mock_start_subprocess.assert_called_once_with(is_detached=True, settings_object_path=mock_app_settings_path)
    assert "Worker initiated in background" in result.output


def test_worker_run_command_missing_settings(cli_runner):
    """Test 'rrq worker run' without --settings."""
    result = cli_runner.invoke(rrq.rrq, ["worker", "run"])
    assert result.exit_code != 0 # Should fail because --settings is required
    assert "No 'job_registry_app' provided by settings" in str(result.output) or "No 'job_registry_app' provided by settings" in str(result.exception) # Custom error message from code


@mock.patch("rrq.rrq.watch_rrq_worker_impl")
def test_worker_watch_command(mock_watch_impl, cli_runner, mock_app_settings_path):
    """Test 'rrq worker watch' command."""
    # We need to ensure asyncio.run can be called, so mock its behavior
    # or ensure the mocked function doesn't rely on a running loop if not provided.
    async def dummy_watch_impl(*args, **kwargs):
        pass
    mock_watch_impl.side_effect = dummy_watch_impl

    result = cli_runner.invoke(rrq.rrq, ["worker", "watch", "--settings", mock_app_settings_path, "--path", "."])

    assert result.exit_code == 0
    mock_watch_impl.assert_called_once()
    args, kwargs = mock_watch_impl.call_args
    assert args[0] == "."  # Path argument
    assert kwargs["settings_object_path"] == mock_app_settings_path


def test_worker_watch_command_missing_settings(cli_runner):
    """Test 'rrq worker watch' without --settings."""
    result = cli_runner.invoke(rrq.rrq, ["worker", "watch", "--path", "."])
    assert result.exit_code != 0
    assert "requires --settings to be specified" in result.output


def test_worker_watch_command_invalid_path(cli_runner, mock_app_settings_path):
    """Test 'rrq worker watch' with a non-existent path."""
    # watchfiles checks path existence, Click also does for click.Path(exists=True)
    result = cli_runner.invoke(rrq.rrq, ["worker", "watch", "--settings", mock_app_settings_path, "--path", "./non_existent_path"])
    assert result.exit_code != 0 
    # Click itself will produce an error message for invalid path
    assert "Invalid value for '--path':" in result.output
    assert "does not exist" in result.output # Part of Click's error message for Path(exists=True) 


@mock.patch("rrq.rrq.check_health_async_impl")
def test_check_command_healthy(mock_check_health, cli_runner, mock_app_settings_path):
    """Test 'rrq check' command when health check is successful."""
    async def dummy_check_impl(*args, **kwargs):
        return True
    mock_check_health.side_effect = dummy_check_impl

    result = cli_runner.invoke(rrq.rrq, ["check", "--settings", mock_app_settings_path])

    assert result.exit_code == 0
    mock_check_health.assert_called_once_with(settings_object_path=mock_app_settings_path)
    assert "Health check PASSED" in result.output


@mock.patch("rrq.rrq.check_health_async_impl")
def test_check_command_unhealthy(mock_check_health, cli_runner, mock_app_settings_path):
    """Test 'rrq check' command when health check fails."""
    async def dummy_check_impl(*args, **kwargs):
        return False
    mock_check_health.side_effect = dummy_check_impl

    result = cli_runner.invoke(rrq.rrq, ["check", "--settings", mock_app_settings_path])

    assert result.exit_code == 1 # Should exit with 1 on failure
    mock_check_health.assert_called_once_with(settings_object_path=mock_app_settings_path)
    assert "Health check FAILED" in result.output


def test_check_command_missing_settings(cli_runner):
    """Test 'rrq check' without --settings."""
    result = cli_runner.invoke(rrq.rrq, ["check"])
    assert result.exit_code != 0
    assert "No active workers found" in result.output


def test_stats_command(cli_runner, mock_app_settings_path):
    """Test 'rrq stats' command."""
    # Test with no specific queue
    result_all = cli_runner.invoke(rrq.rrq, ["stats", "--settings", mock_app_settings_path])
    assert result_all.exit_code != 0  # Command doesn't exist yet
    assert "No such command 'stats'" in result_all.output

    # Test with a specific queue
    result_specific = cli_runner.invoke(rrq.rrq, ["stats", "--settings", mock_app_settings_path, "--queue", "my_queue"])
    assert result_specific.exit_code != 0
    assert "No such command 'stats'" in result_specific.output
    # We expect the command to fail as it's not implemented.


def test_stats_command_missing_settings(cli_runner):
    """Test 'rrq stats' without --settings."""
    result = cli_runner.invoke(rrq.rrq, ["stats"])
    assert result.exit_code != 0
    assert "No such command 'stats'" in result.output 