"""RRQ: Reliable Redis Queue Command Line Interface"""

import asyncio
import importlib
import logging
import os
import signal
import subprocess
import sys
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
        logger.error(f"Redis connection failed during health check: {e}", exc_info=True)
        click.echo(
            click.style(
                f"Worker Health Check: FAIL - Redis connection error: {e}", fg="red"
            )
        )
        return False
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during health check: {e}", exc_info=True
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
    command = ["rrq", "worker", "run"]

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
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        logger.warning(
            f"Worker process {process.pid} did not terminate gracefully (SIGTERM timeout), sending SIGKILL."
        )
        with suppress(ProcessLookupError):
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except Exception as e:
        logger.error(f"Unexpected error checking worker process {process.pid}: {e}")


async def watch_rrq_worker_impl(
    watch_path: str,
    settings_object_path: str | None = None,
    queues: list[str] | None = None,
) -> None:
    abs_watch_path = os.path.abspath(watch_path)
    click.echo(
        f"Watching for file changes in {abs_watch_path} to restart RRQ worker (app settings: {settings_object_path})..."
    )
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
        logger.error(f"Error in watch_rrq_worker: {e}", exc_info=True)
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
def worker_run_command(
    burst: bool,
    queues: tuple[str, ...],
    settings_object_path: str,
):
    """Run an RRQ worker process.
    Requires an application-specific settings object.
    """
    rrq_settings = _load_app_settings(settings_object_path)

    # Determine queues to poll
    queues_arg = list(queues) if queues else None
    # Run worker in foreground (burst or continuous mode)

    logger.info(
        f"Starting RRQ Worker (Burst: {burst}, App Settings: {settings_object_path})"
    )

    if not rrq_settings.job_registry:
        click.echo(
            click.style(
                "ERROR: No 'job_registry_app'. You must provide a JobRegistry instance in settings.",
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

    loop = asyncio.get_event_loop()
    try:
        logger.info("Starting worker run loop...")
        loop.run_until_complete(worker_instance.run())
    except KeyboardInterrupt:
        logger.info("RRQ Worker run interrupted by user (KeyboardInterrupt).")
    except Exception as e:
        logger.error(f"Exception during RRQ Worker run: {e}", exc_info=True)
    finally:
        logger.info("RRQ Worker run finished or exited. Cleaning up event loop.")
        if loop.is_running():
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
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


@rrq.group("dlq")
def dlq_cli():
    """Manage the Dead Letter Queue (DLQ)."""
    pass


@dlq_cli.command("requeue")
@click.option(
    "--settings",
    "settings_object_path",
    type=str,
    required=False,
    default=None,
    help=(
        "Python settings path for application worker settings "
        "(e.g., myapp.worker_config.rrq_settings). "
        "Must include `job_registry: JobRegistry` if requeueing requires handler resolution."
    ),
)
@click.option(
    "--dlq-name",
    "dlq_name",
    type=str,
    required=False,
    default=None,
    help="Name of the DLQ (without prefix). Defaults to settings.default_dlq_name.",
)
@click.option(
    "--queue",
    "target_queue",
    type=str,
    required=False,
    default=None,
    help="Name of the target queue (without prefix). Defaults to settings.default_queue_name.",
)
@click.option(
    "--limit",
    type=int,
    required=False,
    default=None,
    help="Maximum number of DLQ jobs to requeue; all if not set.",
)
def dlq_requeue_command(
    settings_object_path: str,
    dlq_name: str,
    target_queue: str,
    limit: int,
):
    """Requeue jobs from the dead letter queue back into a live queue."""
    rrq_settings = _load_app_settings(settings_object_path)
    dlq_to_use = dlq_name or rrq_settings.default_dlq_name
    queue_to_use = target_queue or rrq_settings.default_queue_name
    job_store = JobStore(settings=rrq_settings)
    click.echo(
        f"Requeuing jobs from DLQ '{dlq_to_use}' to queue '{queue_to_use}' (limit: {limit or 'all'})..."
    )
    count = asyncio.run(job_store.requeue_dlq(dlq_to_use, queue_to_use, limit))
    click.echo(
        f"Requeued {count} job(s) from DLQ '{dlq_to_use}' to queue '{queue_to_use}'."
    )
