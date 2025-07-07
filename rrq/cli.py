"""RRQ: Reliable Redis Queue Command Line Interface"""

import asyncio
import importlib
import logging
import os
import signal
import subprocess
import sys
import time

# import multiprocessing # No longer needed directly, os.cpu_count() is sufficient
from contextlib import suppress

import click
import redis.exceptions
from watchfiles import awatch

from .constants import HEALTH_KEY_PREFIX
from .settings import RRQSettings
from .store import JobStore
from .worker import RRQWorker

# Attempt to import dotenv components for .env file loading
try:
    from dotenv import find_dotenv, load_dotenv

    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

logger = logging.getLogger(__name__)


# Helper to load settings for commands
def _resolve_settings_source(
    settings_object_path: str | None = None,
) -> tuple[str | None, str]:
    """Resolve the settings path and its source.

    Returns:
        A tuple of (settings_path, source_description)
    """
    if settings_object_path is not None:
        return settings_object_path, "--settings parameter"

    env_setting = os.getenv("RRQ_SETTINGS")
    if env_setting is not None:
        # Check if a .env file exists to give more specific info
        if DOTENV_AVAILABLE and find_dotenv(usecwd=True):
            # We can't definitively know if it came from .env or system env,
            # but we can indicate both are possible
            return env_setting, "RRQ_SETTINGS env var (system or .env)"
        return env_setting, "RRQ_SETTINGS env var"

    return None, "built-in defaults"


def _load_app_settings(settings_object_path: str | None = None) -> RRQSettings:
    """Load the settings object from the given path.
    If not provided, the RRQ_SETTINGS environment variable will be used.
    If the environment variable is not set, will create a default settings object.
    RRQ Setting objects, automatically pick up ENVIRONMENT variables starting with RRQ_.

    This function will also attempt to load a .env file if python-dotenv is installed
    and a .env file is found. System environment variables take precedence over .env variables.

    Args:
        settings_object_path: A string representing the path to the settings object. (e.g. "myapp.worker_config.rrq_settings").

    Returns:
        The RRQSettings object.
    """
    if DOTENV_AVAILABLE:
        dotenv_path = find_dotenv(usecwd=True)
        if dotenv_path:
            logger.debug(f"Loading .env file at: {dotenv_path}...")
            load_dotenv(dotenv_path=dotenv_path, override=False)

    try:
        if settings_object_path is None:
            settings_object_path = os.getenv("RRQ_SETTINGS")

        if settings_object_path is None:
            return RRQSettings()

        # Split into module path and object name
        parts = settings_object_path.split(".")
        settings_object_name = parts[-1]
        settings_object_module_path = ".".join(parts[:-1])

        # Import the module
        settings_object_module = importlib.import_module(settings_object_module_path)

        # Get the object
        settings_object = getattr(settings_object_module, settings_object_name)

        return settings_object
    except ImportError:
        click.echo(
            click.style(
                f"ERROR: Could not import settings object '{settings_object_path}'. Make sure it is in PYTHONPATH.",
                fg="red",
            ),
            err=True,
        )
        sys.exit(1)
    except Exception as e:
        click.echo(
            click.style(
                f"ERROR: Unexpected error processing settings object '{settings_object_path}': {e}",
                fg="red",
            ),
            err=True,
        )
        sys.exit(1)


# --- Health Check ---
async def check_health_async_impl(settings_object_path: str | None = None) -> bool:
    """Performs health check for RRQ workers."""
    rrq_settings = _load_app_settings(settings_object_path)

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
    settings_object_path: str | None = None,
    queues: list[str] | None = None,
) -> subprocess.Popen | None:
    """Start an RRQ worker process, optionally for specific queues."""
    command = ["rrq", "worker", "run", "--num-workers", "1"]

    if settings_object_path:
        command.extend(["--settings", settings_object_path])

    # Add queue filters if specified
    if queues:
        for q in queues:
            command.extend(["--queue", q])

    logger.info(f"Starting worker subprocess with command: {' '.join(command)}")
    if is_detached:
        process = subprocess.Popen(
            command,
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )
        logger.info(f"RRQ worker started in background with PID: {process.pid}")
    else:
        process = subprocess.Popen(
            command,
            start_new_session=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
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
    settings_object_path: str | None = None,
    queues: list[str] | None = None,
) -> None:
    abs_watch_path = os.path.abspath(watch_path)
    click.echo(f"Watching for file changes in {abs_watch_path}...")

    # Load settings and display source
    click.echo("Loading RRQ Settings... ", nl=False)

    if settings_object_path:
        click.echo(f"from --settings parameter ({settings_object_path}).")
    elif os.getenv("RRQ_SETTINGS"):
        click.echo(f"from RRQ_SETTINGS env var ({os.getenv('RRQ_SETTINGS')}).")
    elif DOTENV_AVAILABLE and find_dotenv(usecwd=True):
        click.echo("found in .env file.")
    else:
        click.echo("using defaults.")
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

    try:
        worker_process = start_rrq_worker_subprocess(
            is_detached=False,
            settings_object_path=settings_object_path,
            queues=queues,
        )
        async for changes in awatch(abs_watch_path, stop_event=shutdown_event):
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
                settings_object_path=settings_object_path,
                queues=queues,
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
    and managing jobs. Requires an application-specific settings object
    for most operations.
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
    "--settings",
    "settings_object_path",
    type=str,
    required=False,
    default=None,
    help=(
        "Python settings path for application worker settings "
        "(e.g., myapp.worker_config.rrq_settings). "
        "Alternatively, this can be specified as RRQ_SETTINGS env variable. "
        "The specified settings object must include a `job_registry: JobRegistry`."
    ),
)
@click.option(
    "--num-workers",
    type=int,
    default=None,
    help="Number of parallel worker processes to start. Defaults to the number of CPU cores.",
)
def worker_run_command(
    burst: bool,
    queues: tuple[str, ...],
    settings_object_path: str,
    num_workers: int | None,
):
    """Run RRQ worker processes.
    Requires an application-specific settings object.
    """
    if num_workers is None:
        num_workers = (
            os.cpu_count() or 1
        )  # Default to CPU cores, or 1 if cpu_count() is None
        click.echo(
            f"No --num-workers specified, defaulting to {num_workers} (CPU cores)."
        )
    elif num_workers <= 0:
        click.echo(
            click.style("ERROR: --num-workers must be a positive integer.", fg="red"),
            err=True,
        )
        sys.exit(1)

    # Restrict burst mode with multiple workers
    if num_workers > 1 and burst:
        click.echo(
            click.style(
                "ERROR: --burst mode is not supported with multiple workers (--num-workers > 1). "
                "Burst mode cannot coordinate across multiple processes.",
                fg="red",
            ),
            err=True,
        )
        sys.exit(1)

    # Display settings source
    click.echo("Loading RRQ Settings... ", nl=False)
    if settings_object_path:
        click.echo(f"from --settings parameter ({settings_object_path}).")
    elif os.getenv("RRQ_SETTINGS"):
        click.echo(f"from RRQ_SETTINGS env var ({os.getenv('RRQ_SETTINGS')}).")
    elif DOTENV_AVAILABLE and find_dotenv(usecwd=True):
        click.echo("found in .env file.")
    else:
        click.echo("using defaults.")

    if num_workers == 1:
        # Run a single worker in the current process
        click.echo(f"Starting 1 RRQ worker process (Burst: {burst})")
        _run_single_worker(
            burst, list(queues) if queues else None, settings_object_path
        )
    else:
        # Run multiple worker subprocesses
        click.echo(f"Starting {num_workers} RRQ worker processes")
        # Burst is guaranteed to be False here
        _run_multiple_workers(
            num_workers, list(queues) if queues else None, settings_object_path
        )


def _run_single_worker(
    burst: bool,
    queues_arg: list[str] | None,
    settings_object_path: str | None,
):
    """Helper function to run a single RRQ worker instance."""
    rrq_settings = _load_app_settings(settings_object_path)

    if not rrq_settings.job_registry:
        click.echo(
            click.style(
                "ERROR: No 'job_registry'. You must provide a JobRegistry instance in settings.",
                fg="red",
            ),
            err=True,
        )
        sys.exit(1)

    logger.debug(
        f"Registered handlers (from effective registry): {rrq_settings.job_registry.get_registered_functions()}"
    )
    logger.debug(f"Effective RRQ settings for worker: {rrq_settings}")

    worker_instance = RRQWorker(
        settings=rrq_settings,
        job_registry=rrq_settings.job_registry,
        queues=queues_arg,
        burst=burst,
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


def _run_multiple_workers(
    num_workers: int,
    queues: list[str] | None,
    settings_object_path: str | None,
):
    """Manages multiple worker subprocesses."""
    processes: list[subprocess.Popen] = []
    # loop = asyncio.get_event_loop() # Not needed here, this function is synchronous

    original_sigint_handler = signal.getsignal(signal.SIGINT)
    original_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def sig_handler(signum, frame):
        click.echo(
            f"\nSignal {signal.Signals(signum).name} received. Terminating child workers..."
        )
        # Send SIGTERM to all processes
        for i, p in enumerate(processes):
            if p.poll() is None:  # Process is still running
                try:
                    pgid = os.getpgid(p.pid)
                    click.echo(f"Sending SIGTERM to worker {i + 1} (PID {p.pid})...")
                    os.killpg(pgid, signal.SIGTERM)
                except (ProcessLookupError, OSError):
                    pass  # Process already dead
        # Restore original handlers before exiting or re-raising
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGTERM, original_sigterm_handler)
        # Propagate signal to ensure parent exits if it was, e.g., a Ctrl+C
        # This is a bit tricky; for now, just exit.
        # A more robust way might involve re-raising the signal if not handled by click/asyncio.
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    try:
        for i in range(num_workers):
            # Construct the command for the subprocess.
            # Each subprocess runs 'rrq worker run' for a single worker.
            # We pass along relevant flags like --settings, --queue, and --burst.
            # Crucially, we do *not* pass --num-workers to the child,
            # or rather, we could conceptually pass --num-workers 1.
            # Use the rrq executable from the same venv
            venv_bin_dir = os.path.dirname(sys.executable)
            rrq_executable = os.path.join(venv_bin_dir, "rrq")
            cmd = [rrq_executable, "worker", "run", "--num-workers=1"]
            if settings_object_path:
                cmd.extend(["--settings", settings_object_path])
            elif os.getenv("RRQ_SETTINGS"):
                # Pass the RRQ_SETTINGS env var as explicit parameter to subprocess
                cmd.extend(["--settings", os.getenv("RRQ_SETTINGS")])
            else:
                # Error: No settings provided for multi-worker mode
                click.echo(
                    "Error: Multi-worker mode requires explicit settings. "
                    "Please provide either --settings option or set RRQ_SETTINGS environment variable.",
                    err=True,
                )
                sys.exit(1)
            if queues:
                for q_name in queues:
                    cmd.extend(["--queue", q_name])
            click.echo(f"Starting worker subprocess {i + 1}/{num_workers}...")

            # Set up environment - add current directory to PYTHONPATH
            env = os.environ.copy()
            current_pythonpath = env.get("PYTHONPATH", "")
            current_dir = os.getcwd()
            if current_pythonpath:
                env["PYTHONPATH"] = f"{current_dir}:{current_pythonpath}"
            else:
                env["PYTHONPATH"] = current_dir

            # Configure output redirection
            is_testing = "PYTEST_CURRENT_TEST" in os.environ
            stdout_dest = None if not is_testing else subprocess.DEVNULL
            stderr_dest = None if not is_testing else subprocess.DEVNULL

            process = subprocess.Popen(
                cmd,
                start_new_session=True,
                stdout=stdout_dest,
                stderr=stderr_dest,
                cwd=os.getcwd(),
                env=env,
            )
            processes.append(process)
            click.echo(f"Worker subprocess {i + 1} started with PID {process.pid}")

        # Wait for all processes to complete
        click.echo(f"All {num_workers} workers started. Press Ctrl+C to stop.")
        exit_codes = []

        try:
            for p in processes:
                exit_code = p.wait()
                exit_codes.append(exit_code)
        except KeyboardInterrupt:
            # Signal handler has already sent SIGTERM, now wait with timeout
            max_wait = 10
            check_interval = 0.1
            elapsed = 0

            while elapsed < max_wait:
                time.sleep(check_interval)
                elapsed += check_interval

                # Check if all processes have terminated
                all_terminated = all(p.poll() is not None for p in processes)
                if all_terminated:
                    click.echo("All workers terminated gracefully.")
                    break
            else:
                # Timeout reached, force kill remaining processes
                for i, p in enumerate(processes):
                    if p.poll() is None:
                        try:
                            click.echo(
                                click.style(
                                    f"WARNING: Worker {i + 1} did not terminate gracefully, sending SIGKILL.",
                                    fg="yellow",
                                ),
                                err=True,
                            )
                            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                        except (ProcessLookupError, OSError):
                            pass

            # Collect exit codes
            for p in processes:
                exit_codes.append(p.wait())

        # Report results
        for i, exit_code in enumerate(exit_codes):
            click.echo(f"Worker subprocess {i + 1} exited with code {exit_code}")
            if exit_code != 0:
                click.echo(
                    click.style(
                        f"Worker subprocess {i + 1} failed with exit code {exit_code}",
                        fg="red",
                    ),
                    err=True,
                )

    except Exception as e:
        click.echo(
            click.style(f"ERROR: Error managing worker subprocesses: {e}", fg="red"),
            err=True,
        )
        # Terminate any running processes if an error occurs in the manager
        for p in processes:
            if p.poll() is None:  # If process is still running
                terminate_worker_process(p, logger)
    finally:
        logger.info("All worker subprocesses terminated or completed.")
        # Restore original signal handlers
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGTERM, original_sigterm_handler)
        # Any other cleanup for the parent process
        # No loop to check or close here as this part is synchronous
        logger.info("Parent process for multi-worker management is exiting.")


@worker_cli.command("watch")
@click.option(
    "--path",
    default=".",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True),
    help="Directory path to watch for changes. Default is current directory.",
    show_default=True,
)
@click.option(
    "--settings",
    "settings_object_path",
    type=str,
    required=False,
    default=None,
    help=(
        "Python settings path for application worker settings "
        "(e.g., myapp.worker_config.rrq_settings). "
        "The specified settings object must define a `job_registry: JobRegistry`."
    ),
)
@click.option(
    "--queue",
    "queues",
    type=str,
    multiple=True,
    help="Queue(s) to poll when restarting worker. Defaults to settings.default_queue_name.",
)
def worker_watch_command(
    path: str,
    settings_object_path: str,
    queues: tuple[str, ...],
):
    """Run the RRQ worker with auto-restart on file changes in PATH.
    Requires an application-specific settings object.
    """
    # Run watch with optional queue filters
    asyncio.run(
        watch_rrq_worker_impl(
            path,
            settings_object_path=settings_object_path,
            queues=list(queues) if queues else None,
        )
    )


# --- DLQ Requeue CLI Command (delegates to JobStore) ---


@rrq.command("check")
@click.option(
    "--settings",
    "settings_object_path",
    type=str,
    required=False,
    default=None,
    help=(
        "Python settings path for application worker settings "
        "(e.g., myapp.worker_config.rrq_settings). "
        "Must include `job_registry: JobRegistry` to identify workers."
    ),
)
def check_command(settings_object_path: str):
    """Perform a health check on active RRQ worker(s).
    Requires an application-specific settings object.
    """
    click.echo("Performing RRQ health check...")
    healthy = asyncio.run(
        check_health_async_impl(settings_object_path=settings_object_path)
    )
    if healthy:
        click.echo(click.style("Health check PASSED.", fg="green"))
    else:
        click.echo(click.style("Health check FAILED.", fg="red"))
        sys.exit(1)
