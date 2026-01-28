# RRQ TOML Reference

RRQ loads configuration from `rrq.toml` by default. You can point to a different
file with `--config` or `RRQ_CONFIG`. The file can either define a top-level
`[rrq]` table (recommended) or place the keys at the root. If `[rrq]` exists,
all other top-level keys are ignored.

All durations are in seconds unless noted otherwise.

## Minimal example

```toml
[rrq]
redis_dsn = "redis://localhost:6379/1"
default_executor_name = "python"

[rrq.executors.python]
type = "socket"
cmd = ["rrq-executor", "--settings", "myapp.executor_config.python_executor_settings"]
```

## [rrq]

Core settings shared by the orchestrator, producers, and executors. The Rust
orchestrator consumes all fields; the Python SDK ignores fields it does not
understand (for example, `cron_jobs` and `watch`).

| Key | Type | Default | Env override | Notes |
| --- | --- | --- | --- | --- |
| `redis_dsn` | string | `"redis://localhost:6379/0"` | `RRQ_REDIS_DSN` | Redis DSN. |
| `default_queue_name` | string | `"rrq:queue:default"` | `RRQ_DEFAULT_QUEUE_NAME` | Queue name or full Redis key. |
| `default_dlq_name` | string | `"rrq:dlq:default"` | `RRQ_DEFAULT_DLQ_NAME` | DLQ name or full Redis key. |
| `default_max_retries` | int | `5` | `RRQ_DEFAULT_MAX_RETRIES` | Default retry attempts per job. |
| `default_job_timeout_seconds` | int | `300` | `RRQ_DEFAULT_JOB_TIMEOUT_SECONDS` | Per-attempt timeout. |
| `default_lock_timeout_extension_seconds` | int | `60` | `RRQ_DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS` | Extra time added to job timeout for lock TTL. |
| `default_result_ttl_seconds` | int | `86400` | `RRQ_DEFAULT_RESULT_TTL_SECONDS` | TTL for successful job results. |
| `default_poll_delay_seconds` | float | `0.1` | `RRQ_DEFAULT_POLL_DELAY_SECONDS` | Worker sleep when queues are empty. |
| `default_unique_job_lock_ttl_seconds` | int | `21600` | `RRQ_DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS` | TTL for unique job locks. |
| `default_executor_name` | string | `"python"` | `RRQ_DEFAULT_EXECUTOR_NAME` | Must match a configured executor. |
| `executors` | table | `{}` | — | Map of executor configs. See below. |
| `executor_routes` | table | `{}` | — | Map of `queue_name = "executor"`. |
| `worker_health_check_interval_seconds` | float | `60` | `RRQ_WORKER_HEALTH_CHECK_INTERVAL_SECONDS` | Heartbeat interval. |
| `base_retry_delay_seconds` | float | `5.0` | `RRQ_BASE_RETRY_DELAY_SECONDS` | Initial retry delay for backoff. |
| `max_retry_delay_seconds` | float | `3600` | `RRQ_MAX_RETRY_DELAY_SECONDS` | Max retry delay. |
| `worker_shutdown_grace_period_seconds` | float | `10.0` | `RRQ_WORKER_SHUTDOWN_GRACE_PERIOD_SECONDS` | Grace period before forced shutdown. |
| `expected_job_ttl` | int | `30` | `RRQ_EXPECTED_JOB_TTL` | Buffer used in internal lock timing. |

Notes:
- Queue and DLQ names can be bare (for example, `"default"`). RRQ prefixes them
  with `rrq:queue:` or `rrq:dlq:` unless you provide a full key.
- The worker requires at least one executor, and `default_executor_name` must
  match one of them.
- You can select a specific executor per job by prefixing the function name as
  `executor#handler`. This overrides queue routing.

## [rrq.executors.<name>]

Executor configuration for Unix socket runtimes (Python, Rust, or other).

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `type` | string | `"socket"` | Only `socket` is supported. |
| `cmd` | array of strings | required | Command to start the executor. |
| `pool_size` | int | CPU count | Forced to `1` in watch mode. |
| `max_in_flight` | int | `1` | Max concurrent requests per executor process. |
| `env` | table | — | Extra environment variables for the executor process. |
| `cwd` | string | — | Working directory for the executor process. |
| `socket_dir` | string | temp dir | Directory where executor sockets are created. |
| `response_timeout_seconds` | float | — | Max wait for an executor response. |

Notes:
- `cmd` must be present for socket executors; RRQ will start one process per
  pool slot and pass `RRQ_EXECUTOR_SOCKET` for each process.
- `response_timeout_seconds` is separate from job timeouts. If it is hit, the
  executor process is discarded and the job is treated as failed.
- Relative `socket_dir` values are resolved against `cwd` if provided; otherwise
  they are resolved against the current working directory.
- On macOS, Unix socket paths have a short length limit. Use a short absolute
  `socket_dir` (for example, `/tmp/rrq`) if you see “socket path too long”
  errors.

## [rrq.routing]

Optional queue-to-executor routing map. This is normalized to `executor_routes`
internally.

```toml
[rrq.routing]
low_latency = "python"
bulk = "rust"
```

Resolution order:
1) `executor#handler` prefix in the job's function name
2) `[rrq.routing]` entry for the queue
3) `default_executor_name`

## [[rrq.cron_jobs]]

Schedule periodic jobs while a worker is running.

```toml
[[rrq.cron_jobs]]
function_name = "process_message"
schedule = "* * * * *"
args = ["cron payload"]
kwargs = { source = "cron" }
queue_name = "default"
unique = true
```

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `function_name` | string | required | Handler name to enqueue. |
| `schedule` | string | required | Standard 5-field cron expression (UTC). |
| `args` | array | `[]` | Positional args. |
| `kwargs` | table | `{}` | Keyword args. |
| `queue_name` | string | — | Override target queue. |
| `unique` | bool | `false` | Prevent duplicates with a per-function lock. |

## [rrq.watch]

Defaults for `rrq worker watch`. CLI flags override these values. Watch mode
also forces executor pool sizes and `max_in_flight` to `1` and sets
`worker_shutdown_grace_period_seconds` to `0`.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `path` | string | `"."` | Root path to watch. |
| `include_patterns` | array | `["*.py", "*.toml"]` | Restart on matching changes. |
| `ignore_patterns` | array | see below | Ignored globs. |
| `no_gitignore` | bool | `false` | Disable `.gitignore` and `.git/info/exclude`. |

Default `ignore_patterns` when not provided:

```toml
ignore_patterns = [
  ".git",
  ".git/**",
  ".venv",
  ".venv/**",
  "target",
  "target/**",
  "dist",
  "dist/**",
  "build",
  "build/**",
  "__pycache__",
  "**/__pycache__",
  "**/__pycache__/**",
  "*.pyc",
  "**/*.pyc",
  ".ruff_cache",
  ".ruff_cache/**",
  ".pytest_cache",
  ".pytest_cache/**",
]
```
