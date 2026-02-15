# RRQ Parity Spec (Producers + Runners)

This document defines the cross-language contract for RRQ producers and runners.
All language bindings must follow this spec to maintain parity.

## Producer API

### Required operations
- `enqueue(function_name, params, options) -> job_id`
- `enqueue_with_rate_limit(function_name, options) -> job_id | null`
- `enqueue_with_debounce(function_name, options) -> job_id`
- `get_job_status(job_id) -> JobResult | null`

`params` is the job payload object. Language bindings may accept it as a
separate argument (Rust) or as `options.params` (Python/TypeScript), but it
must serialize to the same `job_params` payload in Redis.

### Producer config
All bindings must accept the same producer configuration values:
- `redis_dsn`
- `queue_name`
- `max_retries`
- `job_timeout_seconds`
- `result_ttl_seconds`
- `idempotency_ttl_seconds`
- `correlation_mappings`

Bindings must also support loading configuration from `rrq.toml` via the shared
Rust config loader (rrq-config). The default resolution is:
1) Explicit path
2) `RRQ_CONFIG` env var
3) `rrq.toml` in cwd

### Enqueue options
All bindings must support these options (snake_case in the wire format):
- `queue_name`
- `job_id`
- `unique_key`
- `unique_ttl_seconds`
- `max_retries`
- `job_timeout_seconds`
- `result_ttl_seconds`
- `trace_context`
- `defer_until` (RFC 3339)
- `defer_by_seconds`
- `rate_limit_key`
- `rate_limit_seconds`
- `debounce_key`
- `debounce_seconds`

## Runner API

### Handler signature
Canonical handler signature is ExecutionRequest-based:

```
handler(request) -> ExecutionOutcome | result
```

JavaScript runtimes may provide an optional second `AbortSignal` parameter.

### ExecutionOutcome
Outcome must include:
- `status`: `success | retry | timeout | error`
- `job_id`, `request_id`
- `result` (optional)
- `error` (optional)
- `retry_after_seconds` (optional)

### Error semantics
- handler not found → `status="error"`, `error.type="handler_not_found"`
- cancel → `status="error"`, `error.type="cancelled"`
- timeout → `status="timeout"`, `error.type="timeout"`

## Telemetry

OpenTelemetry is the only supported built-in integration. Other vendor-specific
integrations must not be included.

Runner spans must use:
- Span name: `rrq.runner`
- Attributes:
  - `rrq.job_id`, `rrq.function`, `rrq.queue`, `rrq.attempt`, `rrq.worker_id`
  - `rrq.outcome`, `rrq.duration_ms`, `rrq.retry_delay_ms`
  - `rrq.error_message`, `rrq.error_type`
  - optional timing attrs: `rrq.queue_wait_ms`, `rrq.deadline_remaining_ms`

Trace propagation uses `trace_context` (string map) on the job payload. RRQ does
not auto-inject producer trace context; callers must provide it explicitly.
