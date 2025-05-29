# RRQ: Reliable Redis Queue

RRQ is a Python library for creating reliable job queues using Redis and `asyncio`, inspired by [ARQ (Async Redis Queue)](https://github.com/samuelcolvin/arq). It focuses on providing at-least-once job processing semantics with features like automatic retries, job timeouts, dead-letter queues, and graceful worker shutdown.

## Key Features

*   **At-Least-Once Semantics**: Uses Redis locks to ensure a job is processed by only one worker at a time. If a worker crashes or shuts down mid-processing, the lock expires, and the job *should* be re-processed (though re-queueing on unclean shutdown isn't implemented here yet - graceful shutdown *does* re-queue).
*   **Automatic Retries with Backoff**: Jobs that fail with standard exceptions are automatically retried based on `max_retries` settings, using exponential backoff for delays.
*   **Explicit Retries**: Handlers can raise `RetryJob` to control retry attempts and delays.
*   **Job Timeouts**: Jobs exceeding their configured timeout (`job_timeout_seconds` or `default_job_timeout_seconds`) are terminated and moved to the DLQ.
*   **Dead Letter Queue (DLQ)**: Jobs that fail permanently (max retries reached, fatal error, timeout) are moved to a DLQ list in Redis for inspection.
*   **Job Uniqueness**: The `_unique_key` parameter in `enqueue` prevents duplicate jobs based on a custom key within a specified TTL.
*   **Graceful Shutdown**: Workers listen for SIGINT/SIGTERM and attempt to finish active jobs within a grace period before exiting. Interrupted jobs are re-queued.
*   **Worker Health Checks**: Workers periodically update a health key in Redis with a TTL, allowing monitoring systems to track active workers.
*   **Deferred Execution**: Jobs can be scheduled to run at a future time using `_defer_by` or `_defer_until`.
*   **Cron Jobs**: Periodic jobs can be defined in `RRQSettings.cron_jobs` using a simple cron syntax.

    - Using deferral with a specific `_job_id` will effectively reschedule the job associated with that ID to the new time, overwriting its previous definition and score. It does not create multiple distinct scheduled jobs with the same ID.

    - To batch multiple enqueue calls into a single deferred job (and prevent duplicates within the defer window), combine `_unique_key` with `_defer_by`. For example:

      ```python
      await client.enqueue(
          "process_updates",
          item_id=123,
          _unique_key="update:123",
          _defer_by=10,
      )
      ```

## Basic Usage

*(See [`rrq_example.py`](https://github.com/GetResQ/rrq/tree/master/example) in the project root for a runnable example)*

**1. Define Handlers:**

```python
# handlers.py
import asyncio
from rrq.exc import RetryJob

async def my_task(ctx, message: str):
    job_id = ctx['job_id']
    attempt = ctx['job_try']
    print(f"Processing job {job_id} (Attempt {attempt}): {message}")
    await asyncio.sleep(1)
    if attempt < 3 and message == "retry_me":
        raise RetryJob("Needs another go!")
    print(f"Finished job {job_id}")
    return {"result": f"Processed: {message}"}
```

**2. Register Handlers:**

```python
# main_setup.py (or wherever you initialize)
from rrq.registry import JobRegistry
from . import handlers # Assuming handlers.py is in the same directory

job_registry = JobRegistry()
job_registry.register("process_message", handlers.my_task)
```

**3. Configure Settings:**

```python
# config.py
from rrq.settings import RRQSettings

# Loads from environment variables (RRQ_REDIS_DSN, etc.) or uses defaults
rrq_settings = RRQSettings()
# Or override directly:
# rrq_settings = RRQSettings(redis_dsn="redis://localhost:6379/1")
```

**4. Enqueue Jobs:**

```python
# enqueue_script.py
import asyncio
from rrq.client import RRQClient
from config import rrq_settings # Import your settings

async def enqueue_jobs():
    client = RRQClient(settings=rrq_settings)
    await client.enqueue("process_message", "Hello RRQ!")
    await client.enqueue("process_message", "retry_me")
    await client.close()

if __name__ == "__main__":
    asyncio.run(enqueue_jobs())
```

**5. Run a Worker:**

Note: You don't need to run a worker as the Command Line Interface `rrq` is used for
this purpose.

```python
# worker_script.py
from rrq.worker import RRQWorker
from config import rrq_settings # Import your settings
from main_setup import job_registry # Import your registry

# Create worker instance
worker = RRQWorker(settings=rrq_settings, job_registry=job_registry)

# Run the worker (blocking)
if __name__ == "__main__":
    worker.run()
```

You can run multiple instances of `worker_script.py` for concurrent processing.

## Cron Jobs

Add instances of `CronJob` to `RRQSettings.cron_jobs` to run periodic jobs. The
`schedule` string follows the typical five-field cron format `minute hour day-of-month month day-of-week`.
It supports the most common features from Unix cron:

- numeric values
- ranges (e.g. `8-11`)
- lists separated by commas (e.g. `mon,wed,fri`)
- step values using `/` (e.g. `*/15`)
- names for months and days (`jan-dec`, `sun-sat`)

Jobs are evaluated in the server's timezone and run with minute resolution.

## Command Line Interface

RRQ provides a command-line interface (CLI) for managing workers and performing health checks:

- **`rrq worker run`** - Run an RRQ worker process.
  - `--settings` (optional): Specify the Python path to your settings object (e.g., `myapp.worker_config.rrq_settings`). If not provided, it will use the `RRQ_SETTINGS` environment variable or default to a basic `RRQSettings` object.
  - `--queue` (optional, multiple): Specify queue(s) to poll. Defaults to the `default_queue_name` in settings.
  - `--burst` (flag): Run the worker in burst mode to process one job or batch and then exit.
- **`rrq worker watch`** - Run an RRQ worker with auto-restart on file changes.
  - `--path` (optional): Directory path to watch for changes. Defaults to the current directory.
  - `--settings` (optional): Same as above.
  - `--queue` (optional, multiple): Same as above.
- **`rrq check`** - Perform a health check on active RRQ workers.
  - `--settings` (optional): Same as above.
- **`rrq dlq requeue`** - Requeue jobs from the dead letter queue back into a live queue.
  - `--settings` (optional): Same as above.
  - `--dlq-name` (optional): Name of the DLQ (without prefix). Defaults to `default_dlq_name` in settings.
  - `--queue` (optional): Target queue name (without prefix). Defaults to `default_queue_name` in settings.
  - `--limit` (optional): Maximum number of DLQ jobs to requeue; all if not set.

## Configuration

RRQ can be configured in several ways, with the following precedence:

1. **Command-Line Argument (`--settings`)**: Directly specify the settings object path via the CLI. This takes the highest precedence.
2. **Environment Variable (`RRQ_SETTINGS`)**: Set the `RRQ_SETTINGS` environment variable to point to your settings object path. Used if `--settings` is not provided.
3. **Default Settings**: If neither of the above is provided, RRQ will instantiate a default `RRQSettings` object, which can still be influenced by environment variables starting with `RRQ_`.
4. **Environment Variables (Prefix `RRQ_`)**: Individual settings can be overridden by environment variables starting with `RRQ_`, which are automatically picked up by the `RRQSettings` object.
5. **.env File**: If `python-dotenv` is installed, RRQ will attempt to load a `.env` file from the current working directory or parent directories. System environment variables take precedence over `.env` variables.

**Important Note on `job_registry`**: The `job_registry` attribute in your `RRQSettings` object is **critical** for RRQ to function. It must be an instance of `JobRegistry` and is used to register job handlers. Without a properly configured `job_registry`, workers will not know how to process jobs, and most operations will fail. Ensure it is set in your settings object to map job names to their respective handler functions.


## Core Components

*   **`RRQClient` (`client.py`)**: Used to enqueue jobs onto specific queues. Supports deferring jobs (by time delta or specific datetime), assigning custom job IDs, and enforcing job uniqueness via keys.
*   **`RRQWorker` (`worker.py`)**: The process that polls queues, fetches jobs, executes the corresponding handler functions, and manages the job lifecycle based on success, failure, retries, or timeouts. Handles graceful shutdown via signals (SIGINT, SIGTERM).
*   **`JobRegistry` (`registry.py`)**: A simple registry to map string function names (used when enqueuing) to the actual asynchronous handler functions the worker should execute.
*   **`JobStore` (`store.py`)**: An abstraction layer handling all direct interactions with Redis. It manages job definitions (Hashes), queues (Sorted Sets), processing locks (Strings with TTL), unique job locks, and worker health checks.
*   **`Job` (`job.py`)**: A Pydantic model representing a job, containing its ID, handler name, arguments, status, retry counts, timestamps, results, etc.
*   **`JobStatus` (`job.py`)**: An Enum defining the possible states of a job (`PENDING`, `ACTIVE`, `COMPLETED`, `FAILED`, `RETRYING`).
*   **`RRQSettings` (`settings.py`)**: A Pydantic `BaseSettings` model for configuring RRQ behavior (Redis DSN, queue names, timeouts, retry policies, concurrency, etc.). Loadable from environment variables (prefix `RRQ_`).
*   **`constants.py`**: Defines shared constants like Redis key prefixes and default configuration values.
*   **`exc.py`**: Defines custom exceptions, notably `RetryJob` which handlers can raise to explicitly request a retry, potentially with a custom delay.
