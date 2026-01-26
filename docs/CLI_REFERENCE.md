# RRQ Command Line Interface Reference

RRQ provides a Rust-based command-line interface (CLI) for managing workers, queues, and debugging.

## Worker Management

### `rrq worker run`
Run an RRQ worker process.

**Options:**
- `--config` (optional): Path to RRQ TOML config (e.g., `rrq.toml`). If not provided, uses `RRQ_CONFIG` or `rrq.toml` in the current directory.
- `--queue` (optional, multiple): Specify queue(s) to poll. Defaults to the `default_queue_name` in settings.
- `--burst` (flag): Run the worker in burst mode to process one job or batch and then exit.

### `rrq worker watch`
Run an RRQ worker with auto-restart on file changes.

**Options:**
- `--path` (optional): Directory path to watch for changes. Defaults to the current directory.
- `--config` (optional): Same as above.
- `--queue` (optional, multiple): Same as above.
- `--pattern` (optional, multiple): Glob pattern(s) to include when watching for changes (e.g., `*.py`). Defaults to `*.py` and `*.toml`.
- `--ignore-pattern` (optional, multiple): Glob pattern(s) to ignore when watching for changes (e.g., `*.md`).

**Notes:**
- Watch mode forces executor `pool_size = 1` to keep restarts light-weight.

## Executor Management

### `rrq executor python`
Run the Python stdio executor runtime (for out-of-process Python handlers). This
subcommand spawns the `rrq-executor` entrypoint from the Python package.

**Options:**
- `--settings` (optional): Python executor settings object path (e.g., `myapp.executor_config.python_executor_settings`). If not provided, uses `RRQ_EXECUTOR_SETTINGS`.

## Health Monitoring

### `rrq check`
Perform a health check on active RRQ workers.

**Options:**
- `--config` (optional): Same as above.

## Queue Management

### `rrq queue list`
List all active queues with job counts and timestamps.

**Options:**
- `--config` (optional): Same as above.
- `--show-empty` (flag): Show queues with no pending jobs.

### `rrq queue stats`
Show detailed statistics for queues including throughput and wait times.

**Options:**
- `--config` (optional): Same as above.
- `--queue` (optional, multiple): Specific queue(s) to show stats for.

### `rrq queue inspect <queue_name>`
Inspect jobs in a specific queue with pagination.

**Options:**
- `--config` (optional): Same as above.
- `--limit` (optional): Number of jobs to show (default: 20).
- `--offset` (optional): Offset for pagination (default: 0).

## Job Management

### `rrq job show <job_id>`
Show detailed information about a specific job.

**Options:**
- `--config` (optional): Same as above.
- `--raw` (flag): Show raw job data as JSON.

### `rrq job list`
List jobs with filtering options.

**Options:**
- `--config` (optional): Same as above.
- `--status` (optional): Filter by job status (pending, active, completed, failed, retrying).
- `--queue` (optional): Filter by queue name.
- `--function` (optional): Filter by function name.
- `--limit` (optional): Number of jobs to show (default: 20).

### `rrq job replay <job_id>`
Replay a job with the same parameters.

**Options:**
- `--config` (optional): Same as above.
- `--queue` (optional): Override target queue.

### `rrq job cancel <job_id>`
Cancel a pending job.

**Options:**
- `--config` (optional): Same as above.

### `rrq job trace <job_id>`
Show job execution timeline with durations.

**Options:**
- `--config` (optional): Same as above.

## Dead Letter Queue Management

### `rrq dlq list`
List jobs in the Dead Letter Queue with filtering options.

**Options:**
- `--config` (optional): Same as above.
- `--dlq-name` (optional): Name of the DLQ to inspect (defaults to settings.default_dlq_name).
- `--queue` (optional): Filter by original queue name.
- `--function` (optional): Filter by function name.
- `--limit` (optional): Number of jobs to show (default: 20).
- `--offset` (optional): Offset for pagination (default: 0).
- `--raw` (flag): Show raw job data as JSON.

### `rrq dlq stats`
Show DLQ statistics and error patterns.

**Options:**
- `--config` (optional): Same as above.
- `--dlq-name` (optional): Name of the DLQ to analyze (defaults to settings.default_dlq_name).

### `rrq dlq inspect <job_id>`
Inspect a specific job in the DLQ.

**Options:**
- `--config` (optional): Same as above.
- `--raw` (flag): Show raw job data as JSON.

### `rrq dlq requeue`
Requeue jobs from DLQ back to a live queue with filtering.

**Options:**
- `--config` (optional): Same as above.
- `--dlq-name` (optional): Name of the DLQ (defaults to settings.default_dlq_name).
- `--target-queue` (optional): Target queue name (defaults to settings.default_queue_name).
- `--queue` (optional): Filter by original queue name.
- `--function` (optional): Filter by function name.
- `--job-id` (optional): Requeue specific job by ID.
- `--limit` (optional): Maximum number of jobs to requeue.
- `--all` (flag): Requeue all jobs (required if no other filters specified).
- `--dry-run` (flag): Show what would be requeued without actually doing it.

## Debug and Testing Tools

### `rrq debug generate-jobs`
Generate fake jobs for testing.

**Options:**
- `--config` (optional): Same as above.
- `--count` (optional): Number of jobs to generate (default: 100).
- `--queue` (optional, multiple): Queue names to use.
- `--status` (optional, multiple): Job statuses to create.
- `--age-hours` (optional): Maximum age of jobs in hours (default: 24).
- `--batch-size` (optional): Batch size for bulk operations (default: 10).

### `rrq debug generate-workers`
Generate fake worker heartbeats for testing.

**Options:**
- `--config` (optional): Same as above.
- `--count` (optional): Number of workers to simulate (default: 5).
- `--duration` (optional): Duration to simulate workers in seconds (default: 60).

### `rrq debug submit <function_name>`
Submit a test job.

**Options:**
- `--config` (optional): Same as above.
- `--args` (optional): JSON string of positional arguments.
- `--kwargs` (optional): JSON string of keyword arguments.
- `--queue` (optional): Queue name.
- `--delay` (optional): Delay in seconds.

### `rrq debug clear`
Clear test data from Redis.

**Options:**
- `--config` (optional): Same as above.
- `--confirm` (flag): Confirm deletion without prompt.
- `--pattern` (optional): Pattern to match for deletion (default: test_*).

### `rrq debug stress-test`
Run stress test by creating jobs continuously.

**Options:**
- `--config` (optional): Same as above.
- `--jobs-per-second` (optional): Jobs to create per second (default: 10).
- `--duration` (optional): Duration in seconds (default: 60).
- `--queues` (optional, multiple): Queue names to use.

## Settings Configuration

All CLI commands (except executor runtimes) accept the `--config` parameter to specify your RRQ TOML configuration. The config is resolved in the following order:

1. **`--config` parameter**: Direct path to TOML (e.g., `rrq.toml`)
2. **`RRQ_CONFIG` environment variable**: Path to TOML
3. **`rrq.toml` in the current working directory**

### Example Usage

```bash
# Use default settings (localhost Redis)
rrq queue list

# Use custom config
rrq queue list --config rrq.toml

# Use environment variable
export RRQ_CONFIG=rrq.toml
# Debug workflow
rrq debug generate-jobs --count 100 --queue urgent
rrq queue inspect urgent --limit 10

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
