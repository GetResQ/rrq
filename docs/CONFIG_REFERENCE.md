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
default_runner_name = "worker"

[rrq.runners.worker]
type = "socket"
cmd = ["your-runner-binary", "--tcp-socket", "127.0.0.1:9000"]
tcp_socket = "127.0.0.1:9000"
```

## [rrq]

Core settings shared by the orchestrator, producers, and runners. All producers
(Rust, Python, TypeScript) use the shared Rust `rrq-config` loader, so TOML and
environment handling are consistent across languages. Producer bindings read
only the fields they need (for example, `redis_dsn` and default queue settings).

| Key | Type | Default | Env override | Notes |
| --- | --- | --- | --- | --- |
| `redis_dsn` | string | `"redis://localhost:6379/0"` | `RRQ_REDIS_DSN` | Redis DSN. |
| `default_queue_name` | string | `"rrq:queue:default"` | — | Queue name or full Redis key. |
| `default_dlq_name` | string | `"rrq:dlq:default"` | — | DLQ name or full Redis key. |
| `default_max_retries` | int | `5` | — | Default retry attempts per job. |
| `default_job_timeout_seconds` | int | `300` | — | Per-attempt timeout. |
| `default_lock_timeout_extension_seconds` | int | `60` | — | Extra time added to job timeout for lock TTL. |
| `default_result_ttl_seconds` | int | `86400` | — | TTL for successful job results. |
| `default_poll_delay_seconds` | float | `0.1` | — | Worker sleep when queues are empty. |
| `runner_connect_timeout_ms` | int | `15000` | — | Time (ms) to wait for runner sockets to come online. |
| `default_unique_job_lock_ttl_seconds` | int | `21600` | — | TTL for unique job locks. |
| `default_runner_name` | string | `"python"` | — | Must match a configured runner. |
| `runners` | table | `{}` | — | Map of runner configs. See below. |
| `runner_routes` | table | `{}` | — | Map of `queue_name = "runner"`. |
| `worker_health_check_interval_seconds` | float | `60` | — | Heartbeat interval. |
| `worker_health_check_ttl_buffer_seconds` | float | `10` | — | Extra TTL buffer added to worker health records. |
| `base_retry_delay_seconds` | float | `5.0` | — | Initial retry delay for backoff. |
| `max_retry_delay_seconds` | float | `3600` | — | Max retry delay. |
| `worker_shutdown_grace_period_seconds` | float | `10.0` | — | Grace period before forced shutdown. |
| `expected_job_ttl` | int | `30` | — | Buffer used in internal lock timing. |

Notes:
- Queue and DLQ names can be bare (for example, `"default"`). RRQ prefixes them
  with `rrq:queue:` or `rrq:dlq:` unless you provide a full key.
- The worker requires at least one runner, and `default_runner_name` must
  match one of them.
- You can select a specific runner per job by prefixing the function name as
  `runner#handler`. This overrides queue routing.

## [rrq.runners.<name>]

Runner configuration for localhost TCP socket runtimes (Python, Rust, or
other).

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `type` | string | `"socket"` | Only `socket` is supported. |
| `cmd` | array of strings | required | Command to start the runner. |
| `pool_size` | int | CPU count | Forced to `1` in watch mode. |
| `max_in_flight` | int | `1` | Max concurrent requests per runner process. |
| `env` | table | — | Extra environment variables for the runner process. |
| `cwd` | string | — | Working directory for the runner process. |
| `tcp_socket` | string | required | Localhost TCP socket in `host:port` or `[host]:port` form. |
| `response_timeout_seconds` | float | — | Max wait for a runner response. |

Notes:
- `cmd` must be present for runners; RRQ will start one process per
  pool slot and pass `--tcp-socket host:port` for each process.
- `tcp_socket` must point to a localhost address. When `pool_size > 1`, RRQ
  assigns one port per runner process starting at the configured port (for
  example, `9000`, `9001`, ...).
- `response_timeout_seconds` is separate from job timeouts. If it is hit, the
  runner process is discarded and the job is treated as failed.

## [rrq.runner_routes]

Optional queue-to-runner routing map. Legacy `[rrq.routing]` is still
accepted and normalized to `runner_routes`.

```toml
[rrq.runner_routes]
low_latency = "python"
bulk = "rust"
```

Resolution order:
1) `runner#handler` prefix in the job's function name
2) `[rrq.runner_routes]` entry for the queue
3) `default_runner_name`

## [[rrq.cron_jobs]]

Schedule periodic jobs while a worker is running.

```toml
[[rrq.cron_jobs]]
function_name = "process_message"
schedule = "0 * * * * *"
params = { payload = "cron payload", source = "cron" }
queue_name = "default"
unique = true
```

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `function_name` | string | required | Handler name to enqueue. |
| `schedule` | string | required | Cron expression with seconds (6 fields, UTC). |
| `params` | table | `{}` | Job parameters. |
| `queue_name` | string | — | Override target queue. |
| `unique` | bool | `false` | Prevent duplicates with a per-function lock. |

## [rrq.watch]

Defaults for `rrq worker watch`. CLI flags override these values. Watch mode
also forces runner pool sizes and `max_in_flight` to `1` and sets
`worker_shutdown_grace_period_seconds` to `0`.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `path` | string | `"."` | Root path to watch. |
| `include_patterns` | array | `["*.py", "*.toml"]` | Restart on matching changes. |
| `ignore_patterns` | array | see below | Ignored globs. |
| `no_gitignore` | bool | `false` | Disable `.gitignore` and `.git/info/exclude`. |
| `pre_restart_cmds` | array of arrays | `[]` | Commands to run before starting/restarting the worker in watch mode (for example, `cargo build`). Each entry is an argv array. A non-zero exit code keeps the worker stopped until the next change. |
| `pre_restart_cwd` | string | `path` | Working directory for `pre_restart_cmds`. |
| `pre_restart_timeout_seconds` | float | unset | Optional timeout applied per `pre_restart_cmds` entry. |

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
