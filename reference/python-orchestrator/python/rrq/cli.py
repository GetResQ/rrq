"""RRQ: Reliable Redis Queue Command Line Interface"""

import asyncio
import logging
import os
import signal
import subprocess
import sys
from typing import cast
from fnmatch import fnmatch

# import multiprocessing # No longer needed directly, os.cpu_count() is sufficient
from contextlib import suppress

import click
import redis.exceptions
from watchfiles import DefaultFilter, awatch

from .constants import HEALTH_KEY_PREFIX
from .config import load_toml_settings, resolve_config_source
from .store import JobStore
from .worker import RRQWorker
from .executor import build_executors_from_settings, resolve_executor_pool_sizes

# Attempt to import dotenv components for .env file loading
try:
    from dotenv import find_dotenv, load_dotenv

    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

logger = logging.getLogger(__name__)


def _load_toml_settings(config_path: str | None = None):
    if DOTENV_AVAILABLE:
        dotenv_path = find_dotenv(usecwd=True)
        if dotenv_path:
            logger.debug(f"Loading .env file at: {dotenv_path}...")
            load_dotenv(dotenv_path=dotenv_path, override=False)
    try:
        return load_toml_settings(config_path)
    except Exception as exc:
        click.echo(
            click.style(f"ERROR: {exc}", fg="red"),
            err=True,
        )
        sys.exit(1)


# --- Health Check ---
async def check_health_async_impl(config_path: str | None = None) -> bool:
    """Performs health check for RRQ workers."""
    rrq_settings = _load_toml_settings(config_path)

    logger.info("Performing RRQ worker health check...")
    job_store = None
    try:
        job_store = JobStore(settings=rrq_settings)
        await job_store.redis.ping()
        logger.debug(f"Successfully connected to Redis: {rrq_settings.redis_dsn}")

        health_key_pattern = f"{HEALTH_KEY_PREFIX}*"
        worker_keys = [
            key_bytes.decode("utf-8")
            async for key_bytes in job_store.redis.scan_iter(match=health_key_pattern)
        ]

        if not worker_keys:
            click.echo(
                click.style(
                    "Worker Health Check: FAIL (No active workers found)", fg="red"
                )
            )
            return False

        click.echo(
            click.style(
                f"Worker Health Check: Found {len(worker_keys)} active worker(s):",
                fg="green",
            )
        )
        for key in worker_keys:
            worker_id = key.split(HEALTH_KEY_PREFIX)[1]
            health_data, ttl = await job_store.get_worker_health(worker_id)
            if health_data:
                status = health_data.get("status", "N/A")
                active_jobs = health_data.get("active_jobs", "N/A")
                timestamp = health_data.get("timestamp", "N/A")
                click.echo(
                    f"  - Worker ID: {click.style(worker_id, bold=True)}\n"
                    f"    Status: {status}\n"
                    f"    Active Jobs: {active_jobs}\n"
                    f"    Last Heartbeat: {timestamp}\n"
                    f"    TTL: {ttl if ttl is not None else 'N/A'} seconds"
                )
            else:
                click.echo(
                    f"  - Worker ID: {click.style(worker_id, bold=True)} - Health data missing/invalid. TTL: {ttl if ttl is not None else 'N/A'}s"
                )
        return True
    except redis.exceptions.ConnectionError as e:
        click.echo(
            click.style(
                f"ERROR: Redis connection failed during health check: {e}", fg="red"
            ),
            err=True,
        )
        click.echo(
            click.style(
                f"Worker Health Check: FAIL - Redis connection error: {e}", fg="red"
            )
        )
        return False
    except Exception as e:
        click.echo(
            click.style(
                f"ERROR: An unexpected error occurred during health check: {e}",
                fg="red",
            ),
            err=True,
        )
        click.echo(
            click.style(f"Worker Health Check: FAIL - Unexpected error: {e}", fg="red")
        )
        return False
    finally:
        if job_store:
            await job_store.aclose()


# --- Process Management ---
def start_rrq_worker_subprocess(
    is_detached: bool = False,
    config_path: str | None = None,
    queues: list[str] | None = None,
    watch_mode: bool = False,
) -> subprocess.Popen | None:
    """Start an RRQ worker process, optionally for specific queues."""
    command = ["rrq", "worker", "run"]
    if watch_mode:
        command.append("--watch-mode")

    if config_path:
        command.extend(["--config", config_path])

    # Add queue filters if specified
    if queues:
        for q in queues:
            command.extend(["--queue", q])

    logger.info(f"Starting worker subprocess with command: {' '.join(command)}")
    env = os.environ.copy()
    if is_detached:
        process = subprocess.Popen(
            command,
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            env=env,
        )
        logger.info(f"RRQ worker started in background with PID: {process.pid}")
    else:
        process = subprocess.Popen(
            command,
            start_new_session=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )

    return process


def terminate_worker_process(
    process: subprocess.Popen | None, logger: logging.Logger
) -> None:
    if not process or process.pid is None:
        logger.debug("No active worker process to terminate.")
        return

    try:
        if process.poll() is not None:
            logger.debug(
                f"Worker process {process.pid} already terminated (poll returned exit code: {process.returncode})."
            )
            return

        pgid = os.getpgid(process.pid)
        logger.info(
            f"Terminating worker process group for PID {process.pid} (PGID {pgid})..."
        )
        os.killpg(pgid, signal.SIGTERM)
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        click.echo(
            click.style(
                f"WARNING: Worker process {process.pid} did not terminate gracefully (SIGTERM timeout), sending SIGKILL.",
                fg="yellow",
            ),
            err=True,
        )
        with suppress(ProcessLookupError):
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except Exception as e:
        click.echo(
            click.style(
                f"ERROR: Unexpected error checking worker process {process.pid}: {e}",
                fg="red",
            ),
            err=True,
        )


async def watch_rrq_worker_impl(
    watch_path: str,
    config_path: str | None = None,
    queues: list[str] | None = None,
    include_patterns: list[str] | None = None,
    ignore_patterns: list[str] | None = None,
) -> None:
    abs_watch_path = os.path.abspath(watch_path)
    click.echo(f"Watching for file changes in {abs_watch_path}...")

    # Load settings and display source
    click.echo("Loading RRQ Settings... ", nl=False)

    config_path, source = resolve_config_source(config_path)
    if config_path is None:
        click.echo("missing RRQ config (provide --config or RRQ_CONFIG).")
    else:
        click.echo(f"from {source} ({config_path}).")
    worker_process: subprocess.Popen | None = None
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def sig_handler(_signum, _frame):
        logger.info("Signal received, stopping watcher and worker...")
        if worker_process is not None:
            terminate_worker_process(worker_process, logger)
        loop.call_soon_threadsafe(shutdown_event.set)

    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    default_filter = DefaultFilter()
    default_watch_patterns = ["*.py", "*.toml"]
    if include_patterns is None:
        include_patterns = default_watch_patterns
    ignore_patterns = ignore_patterns or []

    def matches_pattern(path_value: str, patterns: list[str]) -> bool:
        if not patterns:
            return False
        normalized = path_value.replace(os.sep, "/")
        base_name = os.path.basename(normalized)
        return any(
            fnmatch(normalized, pattern) or fnmatch(base_name, pattern)
            for pattern in patterns
        )

    def watch_filter(change, path: str) -> bool:  # type: ignore[no-untyped-def]
        if not default_filter(change, path):
            return False
        rel_path = cast(str, os.path.relpath(os.fsdecode(path), abs_watch_path))
        if ignore_patterns and matches_pattern(rel_path, ignore_patterns):
            return False
        if include_patterns:
            return matches_pattern(rel_path, include_patterns)
        return True

    try:
        worker_process = start_rrq_worker_subprocess(
            is_detached=False,
            config_path=config_path,
            queues=queues,
            watch_mode=True,
        )
        async for changes in awatch(
            abs_watch_path, stop_event=shutdown_event, watch_filter=watch_filter
        ):
            if shutdown_event.is_set():
                break
            if not changes:
                continue

            logger.info(f"File changes detected: {changes}. Restarting RRQ worker...")
            if worker_process is not None:
                terminate_worker_process(worker_process, logger)
            await asyncio.sleep(1)
            if shutdown_event.is_set():
                break
            worker_process = start_rrq_worker_subprocess(
                is_detached=False,
                config_path=config_path,
                queues=queues,
                watch_mode=True,
            )
    except Exception as e:
        click.echo(
            click.style(f"ERROR: Error in watch_rrq_worker: {e}", fg="red"), err=True
        )
    finally:
        logger.info("Exiting watch mode. Ensuring worker process is terminated.")
        if not shutdown_event.is_set():
            shutdown_event.set()
        if worker_process is not None:
            terminate_worker_process(worker_process, logger)
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        logger.info("Watch worker cleanup complete.")


# --- Click CLI Definitions ---

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
def rrq():
    """RRQ: Reliable Redis Queue Command Line Interface.

    Provides tools for running RRQ workers, checking system health,
    and managing jobs. Requires an RRQ TOML config for most operations.
    """
    pass


# Register modular commands
try:
    # Import new command classes
    from .cli_commands.commands.queues import QueueCommands
    from .cli_commands.commands.jobs import JobCommands
    from .cli_commands.commands.monitor import MonitorCommands
    from .cli_commands.commands.debug import DebugCommands
    from .cli_commands.commands.dlq import DLQCommands

    # Register new commands with existing CLI
    command_classes = [
        QueueCommands(),
        JobCommands(),
        MonitorCommands(),
        DebugCommands(),
        DLQCommands(),
    ]

    for command_instance in command_classes:
        try:
            command_instance.register(rrq)
        except Exception as e:
            click.echo(
                f"Warning: Failed to register command {command_instance.__class__.__name__}: {e}",
                err=True,
            )

except ImportError as e:
    # Fall back to original CLI if new modules aren't available
    click.echo(f"Warning: Enhanced CLI features not available: {e}", err=True)


@rrq.group("worker")
def worker_cli():
    """Manage RRQ workers (run, watch)."""
    pass


@rrq.group("executor")
def executor_cli():
    """Run RRQ executor runtimes."""
    pass


@executor_cli.command("python")
@click.option(
    "--settings",
    "settings_object_path",
    type=str,
    required=False,
    default=None,
    help=(
        "Python executor settings object path "
        "(e.g., myapp.worker_config.python_executor_settings). "
        "Alternatively, this can be specified as RRQ_EXECUTOR_SETTINGS env variable."
    ),
)
def executor_python_command(settings_object_path: str) -> None:
    """Run the Python socket executor runtime."""
    from .executor_runtime import run_python_executor

    asyncio.run(run_python_executor(settings_object_path))


@worker_cli.command("run")
@click.option(
    "--burst",
    is_flag=True,
    help="Run worker in burst mode (process one job/batch then exit).",
)
@click.option(
    "--queue",
    "queues",
    type=str,
    multiple=True,
    help="Queue(s) to poll. Defaults to settings.default_queue_name.",
)
@click.option(
    "--config",
    "config_path",
    type=str,
    required=False,
    default=None,
    help=(
        "Path to RRQ TOML config (e.g., rrq.toml). "
        "Alternatively, this can be specified as RRQ_CONFIG env variable."
    ),
)
@click.option(
    "--watch-mode",
    is_flag=True,
    hidden=True,
    help="Internal flag used by the watch command to cap executor pool sizes.",
)
def worker_run_command(
    burst: bool,
    queues: tuple[str, ...],
    config_path: str | None,
    watch_mode: bool,
):
    """Run an RRQ worker process.
    Requires an RRQ TOML config.
    """
    # Display settings source
    click.echo("Loading RRQ Settings... ", nl=False)
    resolved_path, source = resolve_config_source(config_path)
    if resolved_path is None:
        click.echo("missing RRQ config (provide --config or RRQ_CONFIG).")
    else:
        click.echo(f"from {source} ({resolved_path}).")
    click.echo(f"Starting 1 RRQ worker process (Burst: {burst})")
    _run_single_worker(
        burst,
        list(queues) if queues else None,
        resolved_path,
        watch_mode=watch_mode,
    )


def _run_single_worker(
    burst: bool,
    queues_arg: list[str] | None,
    config_path: str | None,
    *,
    watch_mode: bool = False,
):
    """Helper function to run a single RRQ worker instance."""
    rrq_settings = _load_toml_settings(config_path)
    pool_sizes = resolve_executor_pool_sizes(rrq_settings, watch_mode=watch_mode)
    effective_concurrency = max(1, sum(pool_sizes.values()))
    rrq_settings = rrq_settings.model_copy(
        update={"worker_concurrency": effective_concurrency}
    )
    logger.debug(f"Effective RRQ settings for worker: {rrq_settings}")

    executors = build_executors_from_settings(
        rrq_settings,
        pool_sizes=pool_sizes,
    )
    worker_instance = RRQWorker(
        settings=rrq_settings,
        queues=queues_arg,
        burst=burst,
        executors=executors,
    )

    try:
        logger.info("Starting worker run loop for single worker...")
        asyncio.run(worker_instance.run())
    except KeyboardInterrupt:
        logger.info("RRQ Worker run interrupted by user (KeyboardInterrupt).")
    except Exception as e:
        click.echo(
            click.style(f"ERROR: Exception during RRQ Worker run: {e}", fg="red"),
            err=True,
        )
        # Consider re-raising or sys.exit(1) if the exception means failure
    finally:
        # asyncio.run handles loop cleanup.
        logger.info("RRQ Worker run finished or exited.")
        logger.info("RRQ Worker has shut down.")


@worker_cli.command("watch")
@click.option(
    "--path",
    default=".",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True),
    help="Directory path to watch for changes. Default is current directory.",
    show_default=True,
)
@click.option(
    "--config",
    "config_path",
    type=str,
    required=False,
    default=None,
    help=(
        "Path to RRQ TOML config (e.g., rrq.toml). "
        "Alternatively, this can be specified as RRQ_CONFIG env variable."
    ),
)
@click.option(
    "--queue",
    "queues",
    type=str,
    multiple=True,
    help="Queue(s) to poll when restarting worker. Defaults to settings.default_queue_name.",
)
@click.option(
    "--pattern",
    "include_patterns",
    type=str,
    multiple=True,
    help=(
        "Glob pattern(s) to include when watching for changes "
        "(e.g., '*.py'). Defaults to '*.py' and '*.toml'."
    ),
)
@click.option(
    "--ignore-pattern",
    "ignore_patterns",
    type=str,
    multiple=True,
    help="Glob pattern(s) to ignore when watching for changes (e.g., '*.md').",
)
def worker_watch_command(
    path: str,
    config_path: str | None,
    queues: tuple[str, ...],
    include_patterns: tuple[str, ...],
    ignore_patterns: tuple[str, ...],
):
    """Run the RRQ worker with auto-restart on file changes in PATH.
    Requires an RRQ TOML config.
    """
    # Run watch with optional queue filters
    include_list = list(include_patterns) if include_patterns else None
    ignore_list = list(ignore_patterns) if ignore_patterns else None

    asyncio.run(
        watch_rrq_worker_impl(
            path,
            config_path=config_path,
            queues=list(queues) if queues else None,
            include_patterns=include_list,
            ignore_patterns=ignore_list,
        )
    )


# --- DLQ Requeue CLI Command (delegates to JobStore) ---


@rrq.command("check")
@click.option(
    "--config",
    "config_path",
    type=str,
    required=False,
    default=None,
    help=(
        "Path to RRQ TOML config (e.g., rrq.toml). "
        "Alternatively, this can be specified as RRQ_CONFIG env variable."
    ),
)
def check_command(config_path: str | None):
    """Perform a health check on active RRQ worker(s).
    Requires an RRQ TOML config.
    """
    click.echo("Performing RRQ health check...")
    healthy = asyncio.run(check_health_async_impl(config_path=config_path))
    if healthy:
        click.echo(click.style("Health check PASSED.", fg="green"))
    else:
        click.echo(click.style("Health check FAILED.", fg="red"))
        sys.exit(1)
