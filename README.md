# RRQ: Reliable Redis Queue

RRQ is a Python library for creating reliable job queues using Redis and `asyncio`, inspired by [ARQ (Async Redis Queue)](https://github.com/samuelcolvin/arq). It focuses on providing at-least-once job processing semantics with features like automatic retries, job timeouts, dead-letter queues, and graceful worker shutdown.

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
    *Note: Using deferral with a specific `_job_id` will effectively reschedule the job associated with that ID to the new time, overwriting its previous definition and score. It does not create multiple distinct scheduled jobs with the same ID.*
    *To batch multiple enqueue calls into a single deferred job (and prevent duplicates within the defer window), combine `_unique_key` with `_defer_by`. For example:*

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

## Configuration

RRQ behavior is configured via the `RRQSettings` object, which loads values from environment variables prefixed with `RRQ_` by default. Key settings include:

*   `RRQ_REDIS_DSN`: Connection string for Redis.
*   `RRQ_DEFAULT_QUEUE_NAME`: Default queue name.
*   `RRQ_DEFAULT_MAX_RETRIES`: Default retry limit.
*   `RRQ_DEFAULT_JOB_TIMEOUT_SECONDS`: Default job timeout.
*   `RRQ_WORKER_CONCURRENCY`: Max concurrent jobs per worker.
*   ... and others (see `settings.py`).

## RRQ CLI

RRQ provides a command-line interface (CLI) for interacting with the job queue system. The `rrq` CLI allows you to manage workers, check system health, and get statistics about queues and jobs.

### Usage

```bash
rrq <command> [options]
```

### Commands

- **`worker run`**: Run an RRQ worker process to process jobs from queues.
  ```bash
  rrq worker run [--burst] --settings <settings_path>
  ```
  - `--burst`: Run in burst mode (process one job/batch then exit).
  - `--settings`: Python settings path for application worker settings (e.g., `myapp.worker_config.rrq_settings`).

- **`worker watch`**: Run an RRQ worker with auto-restart on file changes in a specified directory.
  ```bash
  rrq worker watch [--path <directory>] --settings <settings_path>
  ```
  - `--path`: Directory to watch for changes (default: current directory).
  - `--settings`: Python settings path for application worker settings.

- **`check`**: Perform a health check on active RRQ workers.
  ```bash
  rrq check --settings <settings_path>
  ```
  - `--settings`: Python settings path for application settings.


### Configuration

The CLI uses the same `RRQSettings` as the library, loading configuration from environment variables prefixed with `RRQ_`. You can also specify the settings via the `--settings` option for commands.

```bash
rrq worker run --settings myapp.worker_config.rrq_settings
```

### Help

For detailed help on any command, use:
```bash
rrq <command> --help
```
