# RRQ Command Line Interface Reference

RRQ provides a comprehensive command-line interface (CLI) for managing workers, monitoring queues, and debugging.

## Worker Management

### `rrq worker run`
Run an RRQ worker process.

**Options:**
- `--settings` (optional): Specify the Python path to your settings object (e.g., `myapp.worker_config.rrq_settings`). If not provided, it will use the `RRQ_SETTINGS` environment variable or default to a basic `RRQSettings` object.
- `--queue` (optional, multiple): Specify queue(s) to poll. Defaults to the `default_queue_name` in settings.
- `--burst` (flag): Run the worker in burst mode to process one job or batch and then exit. Cannot be used with `--num-workers > 1`.
- `--num-workers` (optional, integer): Number of parallel worker processes to start. Defaults to the number of CPU cores available on the machine. Cannot be used with `--burst` mode.

### `rrq worker watch`
Run an RRQ worker with auto-restart on file changes.

**Options:**
- `--path` (optional): Directory path to watch for changes. Defaults to the current directory.
- `--settings` (optional): Same as above.
- `--queue` (optional, multiple): Same as above.

## Health Monitoring

### `rrq check`
Perform a health check on active RRQ workers.

**Options:**
- `--settings` (optional): Same as above.

## Queue Management

### `rrq queue list`
List all active queues with job counts and timestamps.

**Options:**
- `--settings` (optional): Same as above.
- `--show-empty` (flag): Show queues with no pending jobs.

### `rrq queue stats`
Show detailed statistics for queues including throughput and wait times.

**Options:**
- `--settings` (optional): Same as above.
- `--queue` (optional, multiple): Specific queue(s) to show stats for.

### `rrq queue inspect <queue_name>`
Inspect jobs in a specific queue with pagination.

**Options:**
- `--settings` (optional): Same as above.
- `--limit` (optional): Number of jobs to show (default: 20).
- `--offset` (optional): Offset for pagination (default: 0).

## Job Management

### `rrq job show <job_id>`
Show detailed information about a specific job.

**Options:**
- `--settings` (optional): Same as above.
- `--raw` (flag): Show raw job data as JSON.

### `rrq job list`
List jobs with filtering options.

**Options:**
- `--settings` (optional): Same as above.
- `--status` (optional): Filter by job status (pending, active, completed, failed, retrying).
- `--queue` (optional): Filter by queue name.
- `--function` (optional): Filter by function name.
- `--limit` (optional): Number of jobs to show (default: 20).

### `rrq job replay <job_id>`
Replay a job with the same parameters.

**Options:**
- `--settings` (optional): Same as above.
- `--queue` (optional): Override target queue.

### `rrq job cancel <job_id>`
Cancel a pending job.

**Options:**
- `--settings` (optional): Same as above.

### `rrq job trace <job_id>`
Show job execution timeline with durations.

**Options:**
- `--settings` (optional): Same as above.

## Real-time Monitoring

### `rrq monitor`
Launch real-time monitoring dashboard with live statistics.

**Options:**
- `--settings` (optional): Same as above.
- `--refresh` (optional): Refresh interval in seconds (default: 1.0).
- `--queues` (optional, multiple): Specific queues to monitor.

**Refresh Rate Guidelines:**
- **Small deployments** (< 10 queues, < 5 workers): 0.5-2.0 seconds
- **Medium deployments** (10-100 queues, 5-20 workers): 2.0-5.0 seconds  
- **Large deployments** (> 100 queues, > 20 workers): 5.0-10.0 seconds
- **Very large deployments**: Consider using `--queues` filter to monitor specific queues

⚠️ **Warning**: Refresh rates below 1.0 second can overwhelm Redis with scan operations. Monitor Redis CPU usage and adjust accordingly.

## Dead Letter Queue Management

### `rrq dlq list`
List jobs in the Dead Letter Queue with filtering options.

**Options:**
- `--settings` (optional): Same as above.
- `--dlq-name` (optional): Name of the DLQ to inspect (defaults to settings.default_dlq_name).
- `--queue` (optional): Filter by original queue name.
- `--function` (optional): Filter by function name.
- `--limit` (optional): Number of jobs to show (default: 20).
- `--offset` (optional): Offset for pagination (default: 0).
- `--raw` (flag): Show raw job data as JSON.

### `rrq dlq stats`
Show DLQ statistics and error patterns.

**Options:**
- `--settings` (optional): Same as above.
- `--dlq-name` (optional): Name of the DLQ to analyze (defaults to settings.default_dlq_name).

### `rrq dlq inspect <job_id>`
Inspect a specific job in the DLQ.

**Options:**
- `--settings` (optional): Same as above.
- `--raw` (flag): Show raw job data as JSON.

### `rrq dlq requeue`
Requeue jobs from DLQ back to a live queue with filtering.

**Options:**
- `--settings` (optional): Same as above.
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
- `--settings` (optional): Same as above.
- `--count` (optional): Number of jobs to generate (default: 100).
- `--queue` (optional, multiple): Queue names to use.
- `--status` (optional, multiple): Job statuses to create.
- `--age-hours` (optional): Maximum age of jobs in hours (default: 24).
- `--batch-size` (optional): Batch size for bulk operations (default: 10).

### `rrq debug generate-workers`
Generate fake worker heartbeats for testing.

**Options:**
- `--settings` (optional): Same as above.
- `--count` (optional): Number of workers to simulate (default: 5).
- `--duration` (optional): Duration to simulate workers in seconds (default: 60).

### `rrq debug submit <function_name>`
Submit a test job.

**Options:**
- `--settings` (optional): Same as above.
- `--args` (optional): JSON string of positional arguments.
- `--kwargs` (optional): JSON string of keyword arguments.
- `--queue` (optional): Queue name.
- `--delay` (optional): Delay in seconds.

### `rrq debug clear`
Clear test data from Redis.

**Options:**
- `--settings` (optional): Same as above.
- `--confirm` (flag): Confirm deletion without prompt.
- `--pattern` (optional): Pattern to match for deletion (default: test_*).

### `rrq debug stress-test`
Run stress test by creating jobs continuously.

**Options:**
- `--settings` (optional): Same as above.
- `--jobs-per-second` (optional): Jobs to create per second (default: 10).
- `--duration` (optional): Duration in seconds (default: 60).
- `--queues` (optional, multiple): Queue names to use.

## Settings Configuration

All CLI commands accept the `--settings` parameter to specify your application's RRQ configuration. The settings are resolved in the following order:

1. **`--settings` parameter**: Direct path to settings object (e.g., `myapp.config.rrq_settings`)
2. **`RRQ_SETTINGS` environment variable**: Path to settings object
3. **Default settings**: Uses `redis://localhost:6379/0` and default configuration

### Example Usage

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