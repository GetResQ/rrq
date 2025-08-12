# RRQ: Reliable Redis Queue

RRQ is a Python library for creating reliable job queues using Redis and `asyncio`, inspired by [ARQ (Async Redis Queue)](https://github.com/samuelcolvin/arq). It focuses on providing at-least-once job processing semantics with features like automatic retries, job timeouts, dead-letter queues, and graceful worker shutdown.

## 🆕 What's New in v0.7.0

- **Comprehensive CLI Tools**: 15+ new commands for monitoring, debugging, and management
- **Real-time Monitoring Dashboard**: Interactive dashboard with `rrq monitor`
- **Enhanced DLQ Management**: Sophisticated filtering and requeuing capabilities
- **Python 3.10 Support**: Expanded compatibility from Python 3.11+ to 3.10+
- **Bug Fixes**: Critical fix for unique job enqueue failures with proper deferral

## Requirements

- Python 3.10 or higher
- Redis 5.0 or higher
- asyncio-compatible environment

## Key Features

*   **At-Least-Once Semantics**: Uses Redis locks to ensure a job is processed by only one worker at a time. If a worker crashes or shuts down mid-processing, the lock expires, and the job *should* be re-processed (though re-queueing on unclean shutdown isn't implemented here yet - graceful shutdown *does* re-queue).
*   **Automatic Retries with Backoff**: Jobs that fail with standard exceptions are automatically retried based on `max_retries` settings, using exponential backoff for delays.
*   **Explicit Retries**: Handlers can raise `RetryJob` to control retry attempts and delays.
*   **Job Timeouts**: Jobs exceeding their configured timeout (`job_timeout_seconds` or `default_job_timeout_seconds`) are terminated and moved to the DLQ.
*   **Dead Letter Queue (DLQ)**: Jobs that fail permanently (max retries reached, fatal error, timeout) are moved to a single global DLQ list in Redis. Each failed job retains its original queue information, allowing for filtered inspection and selective requeuing.
*   **Job Uniqueness**: The `_unique_key` parameter in `enqueue` prevents duplicate jobs based on a custom key within a specified TTL.
*   **Graceful Shutdown**: Workers listen for SIGINT/SIGTERM and attempt to finish active jobs within a grace period before exiting. Interrupted jobs are re-queued.
*   **Worker Health Checks**: Workers periodically update a health key in Redis with a TTL, allowing monitoring systems to track active workers.
*   **Deferred Execution**: Jobs can be scheduled to run at a future time using `_defer_by` or `_defer_until`.
*   **Cron Jobs**: Periodic jobs can be defined in `RRQSettings.cron_jobs` using a simple cron syntax.
*   **Comprehensive Monitoring**: Built-in CLI tools for monitoring queues, inspecting jobs, and debugging with real-time dashboards and beautiful table output.
*   **Development Tools**: Debug commands for generating test data, stress testing, and cleaning up development environments.

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

### Cron Schedule Examples

```python
# Every minute
"* * * * *"

# Every hour at minute 30
"30 * * * *"

# Every day at 2:30 AM
"30 2 * * *"

# Every Monday at 9:00 AM
"0 9 * * mon"

# Every 15 minutes
"*/15 * * * *"

# Every weekday at 6:00 PM
"0 18 * * mon-fri"

# First day of every month at midnight
"0 0 1 * *"

# Every 2 hours during business hours on weekdays
"0 9-17/2 * * mon-fri"
```

### Defining Cron Jobs

```python
from rrq.settings import RRQSettings
from rrq.cron import CronJob

# Define your cron jobs
cron_jobs = [
    # Daily cleanup at 2 AM
    CronJob(
        function_name="daily_cleanup",
        schedule="0 2 * * *",
        args=["temp_files"],
        kwargs={"max_age_days": 7}
    ),

    # Weekly report every Monday at 9 AM
    CronJob(
        function_name="generate_weekly_report",
        schedule="0 9 * * mon",
        unique=True  # Prevent duplicate reports if worker restarts
    ),

    # Health check every 15 minutes on a specific queue
    CronJob(
        function_name="system_health_check",
        schedule="*/15 * * * *",
        queue_name="monitoring"
    ),

    # Backup database every night at 1 AM
    CronJob(
        function_name="backup_database",
        schedule="0 1 * * *",
        kwargs={"backup_type": "incremental"}
    ),
]

# Add to your settings
rrq_settings = RRQSettings(
    redis_dsn="redis://localhost:6379/0",
    cron_jobs=cron_jobs
)
```

### Cron Job Handlers

Your cron job handlers are regular async functions, just like other job handlers:

```python
async def daily_cleanup(ctx, file_type: str, max_age_days: int = 7):
    """Clean up old files."""
    job_id = ctx['job_id']
    print(f"Job {job_id}: Cleaning up {file_type} files older than {max_age_days} days")
    # Your cleanup logic here
    return {"cleaned_files": 42, "status": "completed"}

async def generate_weekly_report(ctx):
    """Generate and send weekly report."""
    job_id = ctx['job_id']
    print(f"Job {job_id}: Generating weekly report")
    # Your report generation logic here
    return {"report_id": "weekly_2024_01", "status": "sent"}

# Register your handlers
from rrq.registry import JobRegistry

job_registry = JobRegistry()
job_registry.register("daily_cleanup", daily_cleanup)
job_registry.register("generate_weekly_report", generate_weekly_report)

# Add the registry to your settings
rrq_settings.job_registry = job_registry
```

**Note:** Cron jobs are automatically enqueued by the worker when they become due. The worker checks for due cron jobs every 30 seconds and enqueues them as regular jobs to be processed.

## Dead Letter Queue (DLQ) Management

RRQ uses a single global Dead Letter Queue to store jobs that have failed permanently. Jobs in the DLQ retain their original queue information, allowing for sophisticated filtering and management.

### DLQ Structure

- **Global DLQ**: One DLQ per RRQ instance (configurable via `default_dlq_name`)
- **Queue Preservation**: Each failed job remembers its original queue name
- **Filtering**: Jobs can be filtered by original queue, function name, error patterns, and time ranges
- **Inspection**: Full job details including arguments, errors, and execution timeline

### Common DLQ Workflows

#### Investigating Failures
```bash
# Get overall DLQ statistics
rrq dlq stats

# List recent failures from a specific queue
rrq dlq list --queue urgent --limit 10

# Group failures by function
rrq dlq list --function send_email

# Inspect a specific failed job
rrq dlq inspect job_abc123
```

#### Requeuing Failed Jobs
```bash
# Preview what would be requeued (dry run)
rrq dlq requeue --queue urgent --dry-run

# Requeue all failures from urgent queue
rrq dlq requeue --queue urgent --all

# Requeue specific function failures with limit
rrq dlq requeue --function send_email --limit 10

# Requeue single job to different queue
rrq dlq requeue --job-id abc123 --target-queue retry_queue
```

#### Monitoring DLQ in Real-time
```bash
# Monitor includes DLQ statistics panel
rrq monitor

# Queue stats show DLQ count per original queue
rrq queue stats
```

## Command Line Interface

RRQ provides a comprehensive command-line interface (CLI) for managing workers, monitoring queues, and debugging.

📖 **[Full CLI Reference Documentation](docs/CLI_REFERENCE.md)**

### Quick Examples
```bash
# Use default settings (localhost Redis)
rrq queue list

# Use custom settings
rrq queue list --settings myapp.config.rrq_settings

# Use environment variable
export RRQ_SETTINGS=myapp.config.rrq_settings
rrq monitor

# Debug workflow
rrq debug generate-jobs --count 100 --queue urgent
rrq queue inspect urgent --limit 10
rrq monitor --queues urgent --refresh 0.5

# DLQ management workflow
rrq dlq list --queue urgent --limit 10      # List failed jobs from urgent queue
rrq dlq stats                                # Show DLQ statistics and error patterns
rrq dlq inspect <job_id>                     # Inspect specific failed job
rrq dlq requeue --queue urgent --dry-run     # Preview requeue of urgent queue jobs
rrq dlq requeue --queue urgent --limit 5     # Requeue 5 jobs from urgent queue

# Advanced DLQ filtering and management
rrq dlq list --function send_email --limit 20          # List failed email jobs
rrq dlq list --queue urgent --function process_data    # Filter by queue AND function
rrq dlq requeue --function send_email --all            # Requeue all failed email jobs
rrq dlq requeue --job-id abc123 --target-queue retry   # Requeue specific job to retry queue
```

## Performance and Limitations

### Monitoring Performance Considerations

RRQ's monitoring and statistics commands are designed for operational visibility but have some performance considerations for large-scale deployments:

#### Queue Statistics (`rrq queue stats`)
- **Pending Job Counts**: Very fast, uses Redis `ZCARD` operation
- **Active/Completed/Failed Counts**: Requires scanning job records in Redis which can be slow for large datasets
- **Optimization**: Use `--max-scan` parameter to limit scanning (default: 1,000 jobs)
  ```bash
  # Fast scan for quick overview
  rrq queue stats --max-scan 500

  # Complete scan (may be slow)
  rrq queue stats --max-scan 0
  ```

#### DLQ Operations (`rrq dlq`)
- **Job Listing**: Uses batch fetching with Redis pipelines for efficiency
- **Optimization**: Use `--batch-size` parameter to control memory vs. performance trade-offs
  ```bash
  # Smaller batches for memory-constrained environments
  rrq dlq list --batch-size 50

  # Larger batches for better performance
  rrq dlq list --batch-size 200
  ```

#### Real-time Monitoring (`rrq monitor`)
- **Error Message Truncation**: Newest errors truncated to 50 characters, error patterns to 50 characters for display consistency
- **DLQ Statistics**: Updates in real-time but may impact Redis performance with very large DLQs

### Full Metrics Requirements

For comprehensive job lifecycle tracking and historical analytics, consider these architectural additions:

1. **Job History Tracking**:
   - Store completed/failed job summaries in a separate Redis structure or external database
   - Implement job completion event logging for time-series analytics

2. **Active Job Monitoring**:
   - Enhanced worker health tracking with job-level visibility
   - Real-time active job registry for immediate status reporting

3. **Throughput Calculation**:
   - Time-series data collection for accurate throughput metrics
   - Queue-specific performance trend tracking

4. **Scalable Statistics**:
   - Consider Redis Streams or time-series databases for high-frequency job event tracking
   - Implement sampling strategies for large-scale deployments

The current implementation prioritizes operational simplicity and immediate visibility over comprehensive historical analytics. For production monitoring at scale, complement RRQ's built-in tools with external monitoring systems.

## Configuration

RRQ can be configured in several ways, with the following precedence:

1. **Command-Line Argument (`--settings`)**: Directly specify the settings object path via the CLI. This takes the highest precedence.
2. **Environment Variable (`RRQ_SETTINGS`)**: Set the `RRQ_SETTINGS` environment variable to point to your settings object path. Used if `--settings` is not provided.
3. **Default Settings**: If neither of the above is provided, RRQ will instantiate a default `RRQSettings` object, which can still be influenced by environment variables starting with `RRQ_`.
4. **Environment Variables (Prefix `RRQ_`)**: Individual settings can be overridden by environment variables starting with `RRQ_`, which are automatically picked up by the `RRQSettings` object.
5. **.env File**: If `python-dotenv` is installed, RRQ will attempt to load a `.env` file from the current working directory or parent directories. System environment variables take precedence over `.env` variables.

**Important Note on `job_registry`**: The `job_registry` attribute in your `RRQSettings` object is **critical** for RRQ to function. It must be an instance of `JobRegistry` and is used to register job handlers. Without a properly configured `job_registry`, workers will not know how to process jobs, and most operations will fail. Ensure it is set in your settings object to map job names to their respective handler functions.

### Comprehensive CLI Command System
- **New modular CLI architecture** with dedicated command modules for better organization
- **Enhanced monitoring capabilities** with real-time dashboards and beautiful table output
- **Extensive DLQ management** commands for inspecting, filtering, and requeuing failed jobs
- **Job lifecycle management** with detailed inspection and control commands
- **Queue management** with statistics, purging, and migration capabilities
- **Debug utilities** for development and testing including stress testing and data generation

## 📚 New CLI Commands

### Monitor Commands
- `rrq monitor` - Real-time dashboard with queue stats, worker health, and DLQ monitoring
- `rrq monitor workers` - Detailed worker status and health monitoring
- `rrq monitor jobs` - Active job tracking and monitoring

### DLQ Commands
- `rrq dlq list` - List failed jobs with filtering by queue, function, and time
- `rrq dlq stats` - DLQ statistics including error patterns and queue distribution
- `rrq dlq inspect` - Detailed inspection of failed jobs
- `rrq dlq requeue` - Requeue failed jobs with dry-run support
- `rrq dlq purge` - Clean up old failed jobs

### Queue Commands
- `rrq queue list` - List all queues with job counts
- `rrq queue stats` - Detailed queue statistics and throughput metrics
- `rrq queue inspect` - Inspect pending jobs in queues
- `rrq queue purge` - Purge jobs from queues with safety confirmations
- `rrq queue migrate` - Move jobs between queues

### Job Commands
- `rrq job list` - List jobs with status filtering
- `rrq job inspect` - Detailed job information including timeline
- `rrq job result` - Retrieve job results
- `rrq job cancel` - Cancel active jobs
- `rrq job retry` - Manually retry failed jobs
- `rrq job delete` - Delete job records

### Debug Commands
- `rrq debug generate-jobs` - Generate test jobs for development
- `rrq debug stress-test` - Stress test the system
- `rrq debug cleanup` - Clean up test data
- `rrq debug redis-info` - Redis server information and diagnostics

## Core Components

*   **`RRQClient` (`client.py`)**: Used to enqueue jobs onto specific queues. Supports deferring jobs (by time delta or specific datetime), assigning custom job IDs, and enforcing job uniqueness via keys.
*   **`RRQWorker` (`worker.py`)**: The process that polls queues, fetches jobs, executes the corresponding handler functions, and manages the job lifecycle based on success, failure, retries, or timeouts. Handles graceful shutdown via signals (SIGINT, SIGTERM).
*   **`JobRegistry` (`registry.py`)**: A simple registry to map string function names (used when enqueuing) to the actual asynchronous handler functions the worker should execute.
*   **`JobStore` (`store.py`)**: An abstraction layer handling all direct interactions with Redis. It manages job definitions (Hashes), queues (Sorted Sets), processing locks (Strings with TTL), unique job locks, and worker health checks.
*   **`Job` (`job.py`)**: A Pydantic model representing a job, containing its ID, handler name, arguments, status, retry counts, timestamps, results, etc.
*   **`JobStatus` (`job.py`)**: An Enum defining the possible states of a job (`PENDING`, `ACTIVE`, `COMPLETED`, `FAILED`, `DEFERRED`). `
