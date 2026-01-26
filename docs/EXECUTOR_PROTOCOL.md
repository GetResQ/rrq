# RRQ Executor Protocol (v1)

This protocol defines the contract between the RRQ orchestrator (Rust `rrq`
worker) and executor runtimes (Python, Rust, or any other language). The v1
transport is JSON Lines over stdio.

## Design Goals
- **Centralized scheduling** in the Rust orchestrator (timeouts, retries, DLQ).
- **Language-agnostic executors** that only execute jobs and return outcomes.
- **Stable wire format** for cross-language compatibility.

## Encoding
- **JSON**
- Timestamps are **RFC 3339** (UTC).
- Field names are **snake_case**.

## ExecutionRequest
```json
{
  "protocol_version": "1",
  "job_id": "uuid",
  "function_name": "my_handler",
  "args": [1, "two"],
  "kwargs": {"flag": true},
  "context": {
    "job_id": "uuid",
    "attempt": 1,
    "enqueue_time": "2025-01-01T12:00:00Z",
    "queue_name": "default",
    "deadline": "2025-01-01T12:05:00Z",
    "trace_context": {"traceparent": "..."},
    "worker_id": "rrq_worker_123"
  }
}
```

### Fields
- `protocol_version` (string): Always `"1"` for v1.
- `job_id` (string): Unique job ID.
- `function_name` (string): Handler name to execute.
- `args` (array): Positional arguments.
- `kwargs` (object): Keyword arguments.
- `context` (object):
  - `job_id` (string)
  - `attempt` (int): 1-based attempt number.
  - `enqueue_time` (string, RFC 3339)
  - `queue_name` (string)
  - `deadline` (string, RFC 3339, optional): Orchestrator-calculated deadline.
  - `trace_context` (object, optional): Propagation carrier for distributed
    tracing.
  - `worker_id` (string, optional)

### Tracing
`trace_context` carries the producer's trace propagation headers. Executors can
use it to continue traces in their own runtime. The Python executor runtime
will emit `rrq.executor` spans when telemetry is enabled.

The Rust reference executor supports OpenTelemetry with W3C
`traceparent`/`tracestate` headers when built with the `otel` feature.

For cross-language tracing, all runtimes must use the same propagation format.

## ExecutionOutcome
```json
{
  "job_id": "uuid",
  "status": "success",
  "result": {"ok": true},
  "error_message": null,
  "error_type": null,
  "retry_after_seconds": null
}
```

### status
One of:
- `success`: Execution completed. Orchestrator persists result.
- `retry`: Executor requests retry. Orchestrator applies retry policy.
- `timeout`: Executor timed itself out (orchestrator still enforces hard
  timeout).
- `error`: Execution failed. Orchestrator applies retry policy.

### job_id
The job ID this outcome corresponds to. Required for stdio executors so the
orchestrator can correlate responses and guard against stray or out-of-order
stdout lines.

### error_type (optional)
Reserved values:
- `handler_not_found`: Fatal; orchestrator moves job directly to DLQ.

### retry_after_seconds (optional)
Used with `status = retry` to request a specific delay. Orchestrator may
override per policy.

## Scheduling Semantics
- **Timeouts** are enforced by the orchestrator. Executors may return `timeout`
  if they implement their own local timeouts.
- **Retries** are controlled by the orchestrator. Executors return `retry` to
  request a retry.
- **DLQ** decisions are controlled by the orchestrator.
- **Cancellation** (v1) applies only to **pending** jobs. Once execution starts,
  the outcome is always applied.

## Executor Selection
RRQ may embed executor selection in the job function name using:

```
executor#handler
```

Example: `rust#send_email`. The orchestrator strips the prefix and sends only
`handler` as `function_name` in the `ExecutionRequest`.

## Transport (v1)
Stdio JSON Lines:
- stdin: one `ExecutionRequest` per line
- stdout: one `ExecutionOutcome` per line
- stderr: runtime logs (ignored by the orchestrator)

## Reference Implementations
- Python: `reference/python/stdio_executor.py`
- Rust protocol types: `reference/rust/rrq-protocol`
- Rust executor: `reference/rust/rrq-executor` (see
  `reference/rust/rrq-executor/examples/stdio_executor.rs`)
