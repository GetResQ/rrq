# RRQ: Reliable Redis Queue

RRQ is a Redis-backed job queue **system** with a Rust orchestrator and a
language-agnostic executor protocol. Producers can enqueue jobs from Python,
Rust, Go, or anything that can speak the job schema. Executors can be written
in any language that can read/write the stdio protocol. Python remains a
first-class runtime for executors and producers, but the core orchestration is
multi-language.

## 🆕 What's New in v0.9

- **New Architecture**: Separated worker scheduler from executor, allowing for jobs to be queued and processed from any system, not just Python

## What's New in v0.8

- **Distributed Tracing**: One-line integrations for Datadog, OpenTelemetry, and Logfire
- **Trace Context Propagation**: Automatic trace context from producer to worker
- **Comprehensive CLI Tools**: 15+ new commands for debugging and management
- **Enhanced DLQ Management**: Sophisticated filtering and requeuing capabilities

## Big Picture

RRQ is split into three layers:

1. **Orchestrator (Rust `rrq` binary)** — polls Redis, manages retries/timeouts/DLQ,
   and routes jobs to executor pools.
2. **Executors (stdio protocol)** — long-lived processes that execute jobs in any
   language (Python, Rust, Go, etc.).
3. **Producers (SDKs)** — enqueue jobs directly into Redis from any language.

The orchestrator reads `rrq.toml` and does **not** import your application
modules. Python handlers run in the Python executor runtime, not inside the
orchestrator process.

## Requirements

- Python 3.11 or higher (for the Python SDK and executor runtime)
- The Rust `rrq` orchestrator binary (bundled with the Python package or installed separately; override with `RRQ_RUST_BIN`)
- Redis 5.0 or higher

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
*   **Cron Jobs**: Periodic jobs can be defined in `rrq.toml` (`rrq.cron_jobs`) using a simple cron syntax.
*   **Operational Tooling**: CLI tools for inspecting queues, jobs, and debugging workflows.
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

*(See `examples/python/` and `examples/rust/` for runnable examples)*

## Architecture

The scheduler/orchestrator is a Rust binary that reads `rrq.toml` and spawns
executor runtimes (Python stdio, Rust, etc.). Python handlers run inside the
Python executor runtime (not inside the orchestrator process), keeping
orchestration and execution cleanly separated. The Python executor loads a
`PythonExecutorSettings` object (job registry + optional startup/shutdown hooks)
from `RRQ_EXECUTOR_SETTINGS`.

```
┌─────────────────────────┐       enqueue        ┌──────────────────────────┐
│  Producer SDKs          │  ───────────────▶    │  Redis                   │
│  (Python / Rust / Go)   │                      │  - queues (ZSET)         │
└─────────────────────────┘                      │  - job hashes            │
                                                 │  - results / DLQ         │
                                                 └──────────┬───────────────┘
                                                            │ poll/lock
                                                            ▼
                                                ┌──────────────────────────┐
                                                │ RRQ Orchestrator (Rust)  │
                                                │ (rrq worker run/watch)   │
                                                │ - reads rrq.toml         │
                                                │ - retries / timeouts     │
                                                │ - cancellation / DLQ     │
                                                │ - routing                │
                                                └──────────┬───────────────┘
                                                           │ stdio protocol
                              ┌────────────────────────────┴────────────────────────────┐
                              │                                                         │
                              ▼                                                         ▼
                 ┌──────────────────────────┐                              ┌──────────────────────────┐
                 │ Python executor runtime  │                              │ Rust / Go executor       │
                 │ (rrq-executor)           │                              │ (rrq_executor)           │
                 │ - loads PythonExecutor   │                              │ - stdio protocol         │
                 │   Settings + hooks       │                              │                          │
                 └──────────┬───────────────┘                              └──────────┬───────────────┘
                            │ ExecutionOutcome                                        │ ExecutionOutcome
                            └─────────────────────────────────────────────────────────┘
                                                           │
                                                           ▼
                                                ┌──────────────────────────┐
                                                │ Redis (job updates)      │
                                                └──────────────────────────┘
```

### resq-agent integration (Python handlers)

`resq-agent` provides a `PythonExecutorSettings` object at
`app.config.rrq.python_executor_settings`. This object registers all Python
handlers and supplies optional startup/shutdown hooks (DB + RRQ client
initialization). The orchestrator and executor both read `rrq.toml`:

- Orchestrator (Rust): `rrq worker run/watch --config rrq.toml`
- Python executor: `rrq-executor --settings app.config.rrq.python_executor_settings`

Reference protocol docs: `docs/EXECUTOR_PROTOCOL.md`  
Reference implementations: `reference/python` and `reference/rust`

## Examples

- Python producer/consumer/perf: `examples/python/`
- Rust producer: `examples/rust/producer`
- Rust producer crate (reference): `reference/rust/rrq-producer`
- Rust protocol crate (reference): `reference/rust/rrq-protocol`
- Rust stdio executor (reference): `reference/rust/rrq-executor`

### Integration Script

Run the end-to-end integration script (Python-only, Rust-only, mixed):

```
uv run python examples/integration_test.py --count 1000
```

Flags:
- `--redis-dsn` (default: `redis://localhost:6379/3`)
- `--no-flush` (do not flush Redis before each scenario)
- `--log-interval` (seconds between progress logs)
- `--no-build-rust` or `--rust-executor-cmd "..."` to control the Rust executor binary

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

**2. Register Handlers (Python executor):**

```python
# executor_config.py
import os
from pathlib import Path

from rrq.config import load_toml_settings
from rrq.executor_settings import PythonExecutorSettings
from rrq.registry import JobRegistry

from . import handlers  # Assuming handlers.py is in the same directory

job_registry = JobRegistry()
job_registry.register("process_message", handlers.my_task)

# Load the same TOML config as the orchestrator.
config_path = Path(os.getenv("RRQ_EXECUTOR_CONFIG", "rrq.toml"))
rrq_settings = load_toml_settings(str(config_path))

python_executor_settings = PythonExecutorSettings(
    rrq_settings=rrq_settings,
    job_registry=job_registry,
)
```

**3. Create `rrq.toml` (orchestrator config):**

```toml
[rrq]
redis_dsn = "redis://localhost:6379/1"
default_executor_name = "python"

[rrq.executors.python]
type = "stdio"
cmd = ["rrq-executor", "--settings", "myapp.executor_config.python_executor_settings"]
pool_size = 1
```

Set `RRQ_EXECUTOR_CONFIG` if the Python executor should load a different TOML
path than the default `rrq.toml`.

If `pool_size` is omitted, RRQ defaults it to the host CPU count. The worker
concurrency is derived from the sum of executor pool sizes at runtime.

**4. Enqueue Jobs (Python producer):**

```python
# enqueue_script.py
import asyncio
from rrq.client import RRQClient
from rrq.config import load_toml_settings

async def enqueue_jobs():
    rrq_settings = load_toml_settings("rrq.toml")
    client = RRQClient(settings=rrq_settings)
    await client.enqueue("process_message", "Hello RRQ!")
    await client.enqueue("process_message", "retry_me")
    await client.close()

if __name__ == "__main__":
    asyncio.run(enqueue_jobs())
```

**5. Run the Orchestrator (Rust):**

```bash
rrq worker run --config rrq.toml
```

The Rust orchestrator will spawn the configured executor processes as needed.

## Multi-runtime Executors (Python + Rust)

RRQ can route jobs to non-Python executors using a prefix in the function name:

```
executor#handler
```

Example: `rust#send_email`. If no prefix is provided, RRQ uses the default
executor (Python by default).
Executors communicate over stdio JSON Lines (v1).

**Configure executors (`rrq.toml`):**

```toml
[rrq]
default_executor_name = "python"
redis_dsn = "redis://localhost:6379/1"

[rrq.executors.python]
type = "stdio"
cmd = ["rrq-executor", "--settings", "myapp.executor_config.python_executor_settings"]

[rrq.executors.rust]
type = "stdio"
cmd = ["/opt/rrq-executor", "--stdio"]

# Optional queue-based routing
[rrq.routing]
media = "rust"
```

**Enqueue a Rust job:**

```python
await client.enqueue("rust#send_email", user_id=123)
```

**Run the Python stdio executor (optional):**

```bash
rrq-executor --settings myapp.executor_config.python_executor_settings
```

## Cron Jobs

Configure cron jobs in `rrq.toml` (`rrq.cron_jobs`) to run periodic jobs. The
`schedule` string follows the typical five-field cron format `minute hour day-of-month month day-of-week`.
It supports the most common features from Unix cron:

- numeric values
- ranges (e.g. `8-11`)
- lists separated by commas (e.g. `mon,wed,fri`)
- step values using `/` (e.g. `*/15`)
- names for months and days (`jan-dec`, `sun-sat`)

Jobs are evaluated in the server's timezone and run with minute resolution.

### Cron Schedule Examples

```
* * * * *       # Every minute
30 * * * *      # Every hour at minute 30
30 2 * * *      # Every day at 2:30 AM
0 9 * * mon     # Every Monday at 9:00 AM
*/15 * * * *    # Every 15 minutes
0 18 * * mon-fri # Every weekday at 6:00 PM
0 0 1 * *       # First day of every month at midnight
0 9-17/2 * * mon-fri # Every 2 hours during business hours on weekdays
```

### Defining Cron Jobs

```toml
[[rrq.cron_jobs]]
function_name = "daily_cleanup"
schedule = "0 2 * * *"
args = ["temp_files"]
kwargs = { max_age_days = 7 }

[[rrq.cron_jobs]]
function_name = "generate_weekly_report"
schedule = "0 9 * * mon"
unique = true

[[rrq.cron_jobs]]
function_name = "system_health_check"
schedule = "*/15 * * * *"
queue_name = "monitoring"

[[rrq.cron_jobs]]
function_name = "backup_database"
schedule = "0 1 * * *"
kwargs = { backup_type = "incremental" }
```

Cron jobs are configured in `rrq.toml`; the Rust orchestrator is the source of
truth for scheduling.

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
from rrq.executor_settings import PythonExecutorSettings

job_registry = JobRegistry()
job_registry.register("daily_cleanup", daily_cleanup)
job_registry.register("generate_weekly_report", generate_weekly_report)

python_executor_settings = PythonExecutorSettings(
    rrq_settings=rrq_settings,
    job_registry=job_registry,
)
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

## Command Line Interface

RRQ provides a comprehensive command-line interface (CLI) for managing workers, queues, and debugging.

📖 **[Full CLI Reference Documentation](docs/CLI_REFERENCE.md)**

### Quick Examples
```bash
# Use default settings (localhost Redis)
rrq queue list

# Use custom config
rrq queue list --config rrq.toml

# Debug workflow
rrq debug generate-jobs --count 100 --queue urgent
rrq queue inspect urgent --limit 10

# Run Python stdio executor (optional)
rrq-executor --settings myapp.executor_config.python_executor_settings

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

### Statistics Performance Considerations

RRQ's statistics commands are designed for operational visibility but have some performance considerations for large-scale deployments:

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

### Orchestrator (TOML)

The Rust orchestrator loads configuration from TOML with the following precedence:

1. **Command-Line Argument (`--config`)**: Directly specify the TOML path.
2. **Environment Variable (`RRQ_CONFIG`)**: Path to the TOML file.
3. **`rrq.toml` in the current working directory**.

If `python-dotenv` is installed, `.env` files are loaded to populate environment
variables (system env vars take precedence).

### Python Executor Runtime

The Python executor runtime loads a `PythonExecutorSettings` object that includes
the `JobRegistry` and the `RRQSettings` loaded from TOML:

```
rrq-executor --settings myapp.executor_config.python_executor_settings
```

You can also set `RRQ_EXECUTOR_SETTINGS` to that object path. The executor will
read `RRQ_EXECUTOR_CONFIG` (or `rrq.toml` by default) to load the same settings
as the orchestrator.

## Telemetry (Datadog / OTEL / Logfire)

RRQ supports optional distributed tracing for enqueue, orchestration, and executor
runtime spans. Enable the integration in the producer, orchestrator, and executor
processes to get end-to-end traces across the Redis queue. The Python executor
automatically emits `rrq.executor` spans when telemetry is enabled.

### Datadog (ddtrace)

```python
from rrq.integrations.ddtrace import enable as enable_rrq_ddtrace

enable_rrq_ddtrace(service="myapp-rrq")
```

This only instruments RRQ spans + propagation; it does **not** call
`ddtrace.patch_all()`. Configure `ddtrace` in your app as you already do.

#### Datadog + Rust executors (cross-language)

The Rust reference executor only extracts **W3C** `traceparent`/`tracestate`.
If your Python producer uses Datadog-only propagation, Rust spans will **not**
join the original trace. You have a few options:

- Standardize on W3C propagation end-to-end (e.g., use the RRQ OTEL integration
  in Python and point your OTLP exporter at Datadog).
- Configure your Datadog setup to emit W3C headers for propagation (if your
  Datadog version supports it).
- Extend the Rust executor to extract Datadog headers instead of (or in
  addition to) W3C.

### Logfire

```python
import logfire
from rrq.integrations.logfire import enable as enable_rrq_logfire

logfire.configure(service_name="myapp-rrq")
enable_rrq_logfire(service_name="myapp-rrq")
```

### OpenTelemetry (generic)

```python
from rrq.integrations.otel import enable as enable_rrq_otel

enable_rrq_otel(service_name="myapp-rrq")
```

Common OTEL env vars:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_SERVICE_NAME=myapp-rrq
```

### Rust executor (OpenTelemetry)

The Rust reference executor supports OTEL spans when built with the `otel`
feature. It will use `trace_context` from RRQ (W3C `traceparent`/`tracestate`)
as the parent context.

```
cd reference/rust/rrq-executor
cargo run --example stdio_executor --features otel
```

Set your OTLP endpoint via standard env vars (for example
`OTEL_EXPORTER_OTLP_ENDPOINT`).

### Propagation compatibility

Cross-language tracing requires the same propagation format everywhere.
The Rust reference executor extracts **W3C** `traceparent`/`tracestate`.
If your producer uses Datadog-only propagation, Rust spans will not join the
parent trace. For full end-to-end traces across Python and Rust, standardize on
W3C (e.g., use the RRQ OTEL integration in Python).

### CLI Command System
- **Modern Rust CLI** with structured subcommands for operational workflows
- **Extensive DLQ management** commands for inspecting, filtering, and requeuing failed jobs
- **Job lifecycle management** with detailed inspection and control commands
- **Queue management** with statistics and inspection capabilities
- **Debug utilities** for development and testing including stress testing and data generation

## 📚 New CLI Commands

### Worker Commands
- `rrq worker run` - Run the Rust orchestrator worker loop
- `rrq worker watch` - Run the worker with auto-restart on file changes

### Executor Commands
- `rrq executor python` - Launch the Python executor runtime (spawns `rrq-executor`)

### Health Commands
- `rrq check` - Check worker health and connectivity

### DLQ Commands
- `rrq dlq list` - List failed jobs with filtering by queue, function, and time
- `rrq dlq stats` - DLQ statistics including error patterns and queue distribution
- `rrq dlq inspect` - Detailed inspection of failed jobs
- `rrq dlq requeue` - Requeue failed jobs with dry-run support

### Queue Commands
- `rrq queue list` - List all queues with job counts
- `rrq queue stats` - Detailed queue statistics and throughput metrics
- `rrq queue inspect` - Inspect pending jobs in queues

### Job Commands
- `rrq job list` - List jobs with status filtering
- `rrq job show` - Detailed job information
- `rrq job trace` - Job lifecycle timeline
- `rrq job replay` - Requeue a job with original args
- `rrq job cancel` - Cancel pending jobs

### Debug Commands
- `rrq debug generate-jobs` - Generate test jobs for development
- `rrq debug generate-workers` - Simulate worker heartbeats
- `rrq debug submit` - Submit a one-off test job
- `rrq debug clear` - Clean up test data
- `rrq debug stress-test` - Stress test the system

## Core Components

*   **`RRQClient` (`client.py`)**: Enqueues jobs onto queues. Supports deferral, custom IDs, and unique keys.
*   **Rust orchestrator (`reference/rust/rrq-orchestrator`)**: Polls queues, enforces retries/timeouts/DLQ, and routes jobs to executors. Handles graceful shutdown via signals (SIGINT, SIGTERM).
*   **`StdioExecutor` (`executor.py`)**: Communicates with external runtimes (Python/Rust/Go) over the stdio protocol.
*   **`PythonExecutor` (`executor.py`)**: Executes Python handlers using a `JobRegistry` inside the Python executor runtime.
*   **`JobRegistry` (`registry.py`)**: Maps handler names to Python callables for the Python executor runtime.
*   **`JobStore` (`store.py`)**: Redis access layer for job hashes, queues, locks, DLQ, and worker health.
*   **`Job` (`job.py`)**: Pydantic model for job data (IDs, args, status, retries, timestamps).
*   **`JobStatus` (`job.py`)**: Job states (`PENDING`, `ACTIVE`, `COMPLETED`, `FAILED`, `RETRYING`, `CANCELLED`).
