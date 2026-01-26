import json
import sys  # Python's sys module
from pathlib import Path
from typing import Generator
from unittest import mock

import pytest
from click.testing import CliRunner

from rrq import cli
from rrq.cli import _load_toml_settings, terminate_worker_process
from rrq.config import load_toml_settings
from rrq.settings import RRQSettings


def _write_test_config(path: Path, redis_dsn: str) -> None:
    cmd = json.dumps([sys.executable, "-c", "print('ok')"])
    path.write_text(
        "\n".join(
            [
                "[rrq]",
                f'redis_dsn = "{redis_dsn}"',
                'default_executor_name = "python"',
                "",
                "[rrq.executors.python]",
                'type = "stdio"',
                f"cmd = {cmd}",
                "",
            ]
        )
    )


def _write_multi_executor_config(path: Path) -> None:
    cmd = json.dumps([sys.executable, "-c", "print('ok')"])
    path.write_text(
        "\n".join(
            [
                "[rrq]",
                'redis_dsn = "redis://localhost:6379/9"',
                'default_executor_name = "python"',
                "",
                "[rrq.executors.python]",
                'type = "stdio"',
                f"cmd = {cmd}",
                "pool_size = 2",
                "",
                "[rrq.executors.rust]",
                'type = "stdio"',
                f"cmd = {cmd}",
                "pool_size = 3",
                "",
            ]
        )
    )


@pytest.fixture(scope="function")
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(scope="function")
def test_settings_file(tmp_path) -> Generator[str, None, None]:
    config_file = tmp_path / "rrq.toml"
    _write_test_config(config_file, "redis://localhost:6379/9")
    yield str(config_file)


@pytest.fixture(scope="function")
def mock_app_settings_path(tmp_path):
    config_path = tmp_path / "rrq.toml"
    _write_test_config(config_path, "redis://localhost:6379/9")
    return str(config_path)


def test_load_toml_settings_success(mock_app_settings_path):
    """Test that load_toml_settings successfully loads a config file."""
    settings_object = load_toml_settings(mock_app_settings_path)
    assert isinstance(settings_object, RRQSettings)
    assert settings_object.redis_dsn == "redis://localhost:6379/9"


def test_load_toml_settings_missing_file():
    """Test load_toml_settings with a missing file."""
    with pytest.raises(FileNotFoundError):
        load_toml_settings("non_existent_rrq.toml")


@mock.patch("rrq.cli.RRQWorker")
@mock.patch("rrq.cli.os.cpu_count", return_value=1)  # Force single worker path
def test_worker_run_command_foreground(
    mock_cpu_count_unused, mock_worker_class, cli_runner, mock_app_settings_path
):
    """Test 'rrq worker run' in foreground mode (single worker)."""
    mock_worker_instance = mock.MagicMock()
    mock_worker_instance.run = mock.AsyncMock()
    mock_worker_class.return_value = mock_worker_instance

    result = cli_runner.invoke(
        cli.rrq, ["worker", "run", "--config", mock_app_settings_path]
    )

    assert result.exit_code == 0
    mock_worker_class.assert_called_once()
    # Check that the settings object passed to RRQWorker is correct
    args, kwargs = mock_worker_class.call_args
    assert isinstance(kwargs["settings"], RRQSettings)
    assert kwargs["settings"].redis_dsn == "redis://localhost:6379/9"
    assert "executors" in kwargs
    assert "python" in kwargs["executors"]

    mock_worker_instance.run.assert_called_once()


@mock.patch("rrq.cli.RRQWorker")
@mock.patch("rrq.cli.os.cpu_count", return_value=1)  # Force single worker path
def test_worker_run_command_burst_mode(
    mock_cpu_count_unused, mock_worker_class, cli_runner, mock_app_settings_path
):
    """Test 'rrq worker run --burst' (single worker)."""
    mock_worker_instance = mock.MagicMock()
    mock_worker_instance.run = mock.AsyncMock()
    mock_worker_class.return_value = mock_worker_instance

    result = cli_runner.invoke(
        cli.rrq, ["worker", "run", "--config", mock_app_settings_path, "--burst"]
    )

    # Should run in burst mode and exit successfully
    assert result.exit_code == 0
    mock_worker_class.assert_called_once()
    args, kwargs = mock_worker_class.call_args
    # Burst flag should be passed to RRQWorker
    assert kwargs.get("burst", False) is True
    mock_worker_instance.run.assert_called_once()


@mock.patch("rrq.cli.RRQWorker")
@mock.patch("rrq.cli.os.cpu_count", return_value=1)  # Force single worker path
def test_worker_run_command_with_queues(
    mock_cpu_count_unused, mock_worker_class, cli_runner, mock_app_settings_path
):
    """Test 'rrq worker run' with --queue options (single worker)."""
    mock_worker_instance = mock.MagicMock()
    mock_worker_instance.run = mock.AsyncMock()
    mock_worker_class.return_value = mock_worker_instance

    result = cli_runner.invoke(
        cli.rrq,
        [
            "worker",
            "run",
            "--config",
            mock_app_settings_path,
            "--queue",
            "q1",
            "--queue",
            "q2",
        ],
    )
    assert result.exit_code == 0
    mock_worker_class.assert_called_once()
    args, kwargs = mock_worker_class.call_args
    assert kwargs.get("queues") == ["q1", "q2"]
    mock_worker_instance.run.assert_called_once()


@mock.patch("rrq.cli.os.cpu_count", return_value=1)  # Force single worker path
def test_worker_run_command_missing_settings(mock_cpu_count_unused, cli_runner):
    """Test 'rrq worker run' without --config (single worker)."""
    result = cli_runner.invoke(cli.rrq, ["worker", "run"])
    assert result.exit_code != 0
    assert "RRQ config not found" in result.output


@mock.patch("rrq.cli.RRQWorker")
def test_worker_run_derives_concurrency_from_executor_pools(
    mock_worker_class, cli_runner, tmp_path
):
    """Worker concurrency should equal the sum of executor pool sizes."""
    config_path = tmp_path / "rrq.toml"
    _write_multi_executor_config(config_path)
    mock_worker_instance = mock.MagicMock()
    mock_worker_instance.run = mock.AsyncMock()
    mock_worker_class.return_value = mock_worker_instance

    result = cli_runner.invoke(cli.rrq, ["worker", "run", "--config", str(config_path)])

    assert result.exit_code == 0
    mock_worker_class.assert_called_once()
    _args, kwargs = mock_worker_class.call_args
    assert kwargs["settings"].worker_concurrency == 5
    assert kwargs["executors"]["python"]._pool._pool_size == 2
    assert kwargs["executors"]["rust"]._pool._pool_size == 3


@mock.patch("rrq.cli.RRQWorker")
def test_worker_run_watch_mode_forces_pool_size_one(
    mock_worker_class, cli_runner, tmp_path
):
    """Watch mode should cap executor pool sizes at 1."""
    config_path = tmp_path / "rrq.toml"
    _write_multi_executor_config(config_path)
    mock_worker_instance = mock.MagicMock()
    mock_worker_instance.run = mock.AsyncMock()
    mock_worker_class.return_value = mock_worker_instance

    result = cli_runner.invoke(
        cli.rrq, ["worker", "run", "--config", str(config_path), "--watch-mode"]
    )

    assert result.exit_code == 0
    mock_worker_class.assert_called_once()
    _args, kwargs = mock_worker_class.call_args
    assert kwargs["settings"].worker_concurrency == 2
    assert kwargs["executors"]["python"]._pool._pool_size == 1
    assert kwargs["executors"]["rust"]._pool._pool_size == 1


@mock.patch("rrq.cli.watch_rrq_worker_impl")
def test_worker_watch_command(mock_watch_impl, cli_runner, mock_app_settings_path):
    """Test 'rrq worker watch' command."""

    # We need to ensure asyncio.run can be called, so mock its behavior
    # or ensure the mocked function doesn't rely on a running loop if not provided.
    async def dummy_watch_impl(*args, **kwargs):
        pass

    mock_watch_impl.side_effect = dummy_watch_impl

    result = cli_runner.invoke(
        cli.rrq,
        ["worker", "watch", "--config", mock_app_settings_path, "--path", "."],
    )

    assert result.exit_code == 0
    mock_watch_impl.assert_called_once()
    args, kwargs = mock_watch_impl.call_args
    assert args[0] == "."  # Path argument
    assert kwargs["config_path"] == mock_app_settings_path
    # Default queues if not provided
    assert kwargs.get("queues") is None


@mock.patch("rrq.cli.watch_rrq_worker_impl")
def test_worker_watch_command_with_queues(
    mock_watch_impl, cli_runner, mock_app_settings_path
):
    """Test 'rrq worker watch' with --queue options."""

    async def dummy_watch(path, config_path=None, queues=None, **kwargs):
        pass

    mock_watch_impl.side_effect = dummy_watch

    result = cli_runner.invoke(
        cli.rrq,
        [
            "worker",
            "watch",
            "--config",
            mock_app_settings_path,
            "--path",
            ".",
            "--queue",
            "alpha",
            "--queue",
            "beta",
        ],
    )
    assert result.exit_code == 0
    mock_watch_impl.assert_called_once()
    args, kwargs = mock_watch_impl.call_args
    assert args[0] == "."
    assert kwargs.get("config_path") == mock_app_settings_path
    assert kwargs.get("queues") == ["alpha", "beta"]


@mock.patch("rrq.cli.watch_rrq_worker_impl")
def test_worker_watch_command_with_patterns(
    mock_watch_impl, cli_runner, mock_app_settings_path
):
    """Test 'rrq worker watch' with include/exclude patterns."""

    async def dummy_watch(*args, **kwargs):
        pass

    mock_watch_impl.side_effect = dummy_watch

    result = cli_runner.invoke(
        cli.rrq,
        [
            "worker",
            "watch",
            "--config",
            mock_app_settings_path,
            "--path",
            ".",
            "--pattern",
            "*.py",
            "--pattern",
            "*.toml",
            "--ignore-pattern",
            "*.md",
        ],
    )

    assert result.exit_code == 0
    mock_watch_impl.assert_called_once()
    args, kwargs = mock_watch_impl.call_args
    assert args[0] == "."
    assert kwargs.get("config_path") == mock_app_settings_path
    assert kwargs.get("include_patterns") == ["*.py", "*.toml"]
    assert kwargs.get("ignore_patterns") == ["*.md"]


@mock.patch("rrq.cli.watch_rrq_worker_impl")
def test_worker_watch_command_missing_settings(mock_watch_impl, cli_runner):
    """Test 'rrq worker watch' without --config."""

    async def dummy_watch_impl(path, config_path=None, queues=None, **kwargs):
        pass

    mock_watch_impl.side_effect = dummy_watch_impl

    result = cli_runner.invoke(cli.rrq, ["worker", "watch", "--path", "."])
    assert result.exit_code == 0
    mock_watch_impl.assert_called_once()
    args, kwargs = mock_watch_impl.call_args
    assert args[0] == "."  # Path argument
    assert kwargs.get("config_path") is None
    assert kwargs.get("queues") is None


def test_worker_watch_command_invalid_path(cli_runner, mock_app_settings_path):
    """Test 'rrq worker watch' with a non-existent path."""
    # watchfiles checks path existence, Click also does for click.Path(exists=True)
    result = cli_runner.invoke(
        cli.rrq,
        [
            "worker",
            "watch",
            "--config",
            mock_app_settings_path,
            "--path",
            "./non_existent_path",
        ],
    )
    assert result.exit_code != 0
    # Click itself will produce an error message for invalid path
    assert "Invalid value for '--path':" in result.output
    assert (
        "does not exist" in result.output
    )  # Part of Click's error message for Path(exists=True)


@mock.patch("rrq.cli.check_health_async_impl")
def test_check_command_healthy(mock_check_health, cli_runner, mock_app_settings_path):
    """Test 'rrq check' command when health check is successful."""

    async def dummy_check_impl(*args, **kwargs):
        return True

    mock_check_health.side_effect = dummy_check_impl

    result = cli_runner.invoke(cli.rrq, ["check", "--config", mock_app_settings_path])

    assert result.exit_code == 0
    mock_check_health.assert_called_once_with(config_path=mock_app_settings_path)
    assert "Health check PASSED" in result.output


@mock.patch("rrq.cli.check_health_async_impl")
def test_check_command_unhealthy(mock_check_health, cli_runner, mock_app_settings_path):
    """Test 'rrq check' command when health check fails."""

    async def dummy_check_impl(*args, **kwargs):
        return False

    mock_check_health.side_effect = dummy_check_impl

    result = cli_runner.invoke(cli.rrq, ["check", "--config", mock_app_settings_path])

    assert result.exit_code == 1  # Should exit with 1 on failure
    mock_check_health.assert_called_once_with(config_path=mock_app_settings_path)
    assert "Health check FAILED" in result.output


def test_check_command_missing_settings(cli_runner):
    """Test 'rrq check' without --config."""
    result = cli_runner.invoke(cli.rrq, ["check"])
    assert result.exit_code != 0
    assert "RRQ config not found" in result.output


def test_stats_command(cli_runner, mock_app_settings_path):
    """Test 'rrq stats' command."""
    # Test with no specific queue
    result_all = cli_runner.invoke(
        cli.rrq, ["stats", "--config", mock_app_settings_path]
    )
    assert result_all.exit_code != 0  # Command doesn't exist yet
    assert "No such command 'stats'" in result_all.output

    # Test with a specific queue
    result_specific = cli_runner.invoke(
        cli.rrq, ["stats", "--config", mock_app_settings_path, "--queue", "my_queue"]
    )
    assert result_specific.exit_code != 0
    assert "No such command 'stats'" in result_specific.output
    # We expect the command to fail as it's not implemented.


def test_stats_command_missing_settings(cli_runner):
    """Test 'rrq stats' without --config."""
    result = cli_runner.invoke(cli.rrq, ["stats"])
    assert result.exit_code != 0
    assert "No such command 'stats'" in result.output


# DLQ tests removed - DLQ functionality is now implemented in the enhanced CLI
# and is thoroughly tested in tests/cli_commands/test_dlq_commands.py


def test_load_toml_settings_missing_config(monkeypatch):
    """Missing config should exit with an error in the CLI loader."""
    monkeypatch.delenv("RRQ_CONFIG", raising=False)
    with pytest.raises(SystemExit) as exc:
        _load_toml_settings(None)
    assert isinstance(exc.value, SystemExit)
    assert exc.value.code == 1


def test_load_toml_settings_from_env_var(tmp_path, monkeypatch):
    """Test loading settings via RRQ_CONFIG environment variable."""
    config_path = tmp_path / "rrq_env.toml"
    _write_test_config(config_path, "redis://envvar:333/7")
    monkeypatch.setenv("RRQ_CONFIG", str(config_path))
    settings_object = _load_toml_settings(None)
    assert settings_object.redis_dsn == "redis://envvar:333/7"


@pytest.mark.skipif(not cli.DOTENV_AVAILABLE, reason="python-dotenv not available")
def test_load_toml_settings_from_dotenv(tmp_path, monkeypatch):
    """Test loading settings path from a .env file."""
    config_path = tmp_path / "rrq_dotenv.toml"
    _write_test_config(config_path, "redis://dotenv:2222/2")
    env_file = tmp_path / ".env"
    env_file.write_text(f"RRQ_CONFIG={config_path}")
    monkeypatch.delenv("RRQ_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    settings_object = _load_toml_settings(None)
    assert settings_object.redis_dsn == "redis://dotenv:2222/2"


@pytest.mark.skipif(not cli.DOTENV_AVAILABLE, reason="python-dotenv not available")
def test_load_toml_settings_dotenv_not_override_system_env(tmp_path, monkeypatch):
    """System environment variables should override .env file values."""
    config_env = tmp_path / "rrq_env.toml"
    config_dotenv = tmp_path / "rrq_dotenv.toml"
    _write_test_config(config_env, "redis://env:1111/1")
    _write_test_config(config_dotenv, "redis://dotenv:2222/2")
    monkeypatch.setenv("RRQ_CONFIG", str(config_env))
    env_file = tmp_path / ".env"
    env_file.write_text(f"RRQ_CONFIG={config_dotenv}")
    monkeypatch.chdir(tmp_path)
    settings_object = _load_toml_settings(None)
    assert settings_object.redis_dsn == "redis://env:1111/1"


def test_terminate_worker_process_none(caplog):
    import logging

    logger = logging.getLogger("test_logger")
    caplog.set_level(logging.DEBUG, logger=logger.name)
    # No process to terminate
    terminate_worker_process(None, logger)
    assert "No active worker process to terminate." in caplog.text


class FakeProcess:
    def __init__(self, pid=None, returncode=None):
        self.pid = pid
        self.returncode = returncode

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return


def test_terminate_worker_already_terminated(caplog):
    import logging
    import subprocess
    from typing import Any, cast

    logger = logging.getLogger("test_logger2")
    caplog.set_level(logging.DEBUG, logger=logger.name)
    proc = FakeProcess(pid=1234, returncode=0)
    terminate_worker_process(cast(subprocess.Popen[Any], proc), logger)
    # should log that process already terminated
    assert "already terminated" in caplog.text
