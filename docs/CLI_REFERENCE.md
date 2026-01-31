# RRQ CLI Reference (Rust)

The `rrq` binary is the Rust orchestrator CLI. It reads `rrq.toml` by default or
uses `--config` / `RRQ_CONFIG`.

## Worker Management

### `rrq worker run`
Run a worker process.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--queue` Queue(s) to poll (repeatable)
- `--burst` Process available jobs then exit

### `rrq worker watch`
Watch a directory and restart the worker on file changes.

Options:
- `--path` Directory to watch (defaults to `[rrq.watch].path` or `.`)
- `--config` Path to `rrq.toml` (optional)
- `--queue` Queue(s) to poll (repeatable)
- `--pattern` Include glob(s) (repeatable, default: `*.py`, `*.toml`)
- `--ignore-pattern` Ignore glob(s) (repeatable)
- `--no-gitignore` Disable `.gitignore` and `.git/info/exclude` filtering

Notes:
- Watch mode forces runner pool sizes and `max_in_flight` to 1.
- A change that matches `--pattern` and does not match `--ignore-pattern`
  triggers a worker restart.
- Runners can write to stdout/stderr; the orchestrator will capture and emit
  those lines with runner prefixes. Use `RUST_LOG` to control orchestrator
  verbosity.
- Watch defaults can be set in `rrq.toml` under `[rrq.watch]`. CLI flags take
  precedence over config values.

## Runner

### `rrq runner python`
Spawn the Python runner runtime (`rrq-runner`).

Options:
- `--settings` PythonRunnerSettings object path (optional; falls back to
  `RRQ_RUNNER_SETTINGS`)
- `--tcp-socket` Localhost TCP socket in `host:port` form (optional; falls back
  to `RRQ_RUNNER_TCP_SOCKET`)

Notes:
- The orchestrator sets `RRQ_RUNNER_TCP_SOCKET` automatically when it launches
  runners. For manual runs, pass `--tcp-socket` or set the env var.
- TCP sockets must bind to localhost.

## Health

### `rrq check`
Check worker health keys in Redis.

Options:
- `--config` Path to `rrq.toml` (optional)

## Queue Commands

### `rrq queue list`
List queues and pending counts.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--show-empty` Include empty queues

### `rrq queue stats`
Show pending/active/completed/failed counts and DLQ totals.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--queue` Restrict to specific queue(s) (repeatable)
- `--max-scan` Max job hashes to scan for status breakdowns (0 = full scan)

### `rrq queue inspect <queue>`
Show queued jobs with scheduling info.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--limit` Max rows (default: 20)
- `--offset` Offset (default: 0)

## Job Commands

### `rrq job show <job_id>`
Show job details.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--raw` Print raw JSON

### `rrq job list`
List jobs with filters.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--status` Filter by status
- `--queue` Filter by queue
- `--function` Filter by function
- `--limit` Max rows (default: 20)

### `rrq job replay <job_id>`
Re-enqueue with original args/kwargs.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--queue` Override target queue

### `rrq job cancel <job_id>`
Cancel a pending job.

Options:
- `--config` Path to `rrq.toml` (optional)

### `rrq job trace <job_id>`
Show a job timeline.

Options:
- `--config` Path to `rrq.toml` (optional)

## DLQ Commands

### `rrq dlq list`
List failed jobs with filtering.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--dlq-name` DLQ name (defaults to settings)
- `--queue` Filter by original queue
- `--function` Filter by function
- `--limit` Max rows (default: 20)
- `--offset` Offset (default: 0)
- `--raw` Print raw JSON
- `--batch-size` Batch size for job hash fetches

### `rrq dlq stats`
Show DLQ statistics.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--dlq-name` DLQ name (defaults to settings)

### `rrq dlq inspect <job_id>`
Inspect a single DLQ job.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--raw` Print raw JSON

### `rrq dlq requeue`
Requeue DLQ jobs.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--dlq-name` DLQ name
- `--target-queue` Target queue
- `--queue` Filter by original queue
- `--function` Filter by function
- `--job-id` Specific job id
- `--limit` Max jobs to requeue
- `--all` Requeue all (required if no filter is set)
- `--dry-run` Show what would be requeued

## Debug Commands

### `rrq debug generate-jobs`
Generate synthetic jobs.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--count` Number of jobs (default: 100)
- `--queue` Queue(s) (repeatable)
- `--status` Status values (repeatable)
- `--age-hours` Max job age
- `--batch-size` Batch size

### `rrq debug generate-workers`
Generate synthetic worker heartbeats.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--count` Worker count
- `--duration` Duration in seconds

### `rrq debug submit <function>`
Submit a one-off job.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--args` JSON args array
- `--kwargs` JSON kwargs object
- `--queue` Queue name
- `--delay` Delay in seconds

### `rrq debug clear`
Delete keys by pattern.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--confirm` Skip confirmation
- `--pattern` Redis key pattern (default: `test_*`)

### `rrq debug stress-test`
Submit jobs at a fixed rate.

Options:
- `--config` Path to `rrq.toml` (optional)
- `--jobs-per-second` Rate
- `--duration` Duration in seconds
- `--queues` Queue(s) (repeatable)

## Examples

```
rrq worker run --config rrq.toml
rrq worker watch --path . --pattern "*.py" --pattern "*.toml"
rrq queue list --config rrq.toml
rrq job show <job_id>
rrq dlq list --queue urgent --limit 10
rrq debug submit process_data --args "[1,2]" --kwargs "{\"flag\":true}"
```
