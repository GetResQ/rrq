# RRQ Runner Protocol (v2)

This protocol defines the contract between the RRQ orchestrator (Rust `rrq`
worker) and runner runtimes (Python, Rust, or any other language). The v2
transport is a localhost TCP socket with length-delimited JSON frames.

> **Note**: Protocol v2 replaced `args`/`kwargs` with a single `params` object.
> The orchestrator and runners must use the same protocol version.

## Design Goals
- **Centralized scheduling** in the Rust orchestrator (timeouts, retries, DLQ).
- **Language-agnostic runners** that only execute jobs and return outcomes.
- **Stable wire format** for cross-language compatibility.

## Encoding
- **JSON**
- Timestamps are **RFC 3339** (UTC).
- Field names are **snake_case**.

## Transport (v2)
- The orchestrator sets `RRQ_RUNNER_TCP_SOCKET` to a `host:port` value.
- The runner **binds** a TCP socket on the provided **localhost-only**
  address and accepts connections.
- When using TCP runners with a pool, RRQ assigns a distinct port per
  runner process.
- Each connection is a stream of **length-delimited frames**:
  - 4-byte **big-endian** unsigned length prefix
  - followed by a UTF-8 JSON payload of that length
- Each frame is a JSON envelope:

```json
{
  "type": "request",
  "payload": { "..." : "..." }
}
```

Message types:
- `request`: `payload` is an `ExecutionRequest`.
- `response`: `payload` is an `ExecutionOutcome`.
- `cancel`: `payload` is a `CancelRequest`.

Runners should read requests and write responses **sequentially** on a single
connection. The orchestrator correlates responses by `request_id` and expects
exactly one response per request. The orchestrator may open **multiple
concurrent connections** to the same runner process (often one connection per
request). Cancellation frames may be sent on their own short-lived connection.
Protocol logging is not supported; runners should emit logs to stdout/stderr
as needed.

## ExecutionRequest (payload)
```json
{
  "protocol_version": "2",
  "request_id": "uuid",
  "job_id": "uuid",
  "function_name": "my_handler",
  "params": {"key": "value", "count": 42},
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
- `protocol_version` (string): Always `"2"` for v2.
- `request_id` (string): Unique ID for this request/response pair.
- `job_id` (string): Unique job ID.
- `function_name` (string): Handler name to execute.
- `params` (object): Job parameters as key-value pairs.
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
`trace_context` carries the producer's trace propagation headers. Runners can
use it to continue traces in their own runtime. The Rust, Python, and
TypeScript runner runtimes emit `rrq.runner` spans when OpenTelemetry is
enabled.

The Rust reference runner supports OpenTelemetry with W3C
`traceparent`/`tracestate` headers when built with the `otel` feature. The
Python and TypeScript runtimes rely on the OpenTelemetry propagator configured
in your application (typically W3C Trace Context).

For cross-language tracing, all runtimes must use the same propagation format
and producers must attach `trace_context` on enqueue.

## ExecutionOutcome (payload)
```json
{
  "job_id": "uuid",
  "request_id": "uuid",
  "status": "success",
  "result": {"ok": true},
  "error": null,
  "retry_after_seconds": null
}
```

### status
One of:
- `success`: Execution completed. Orchestrator persists result.
- `retry`: Runner requests retry. Orchestrator applies retry policy.
- `timeout`: Runner timed itself out (orchestrator still enforces hard
  timeout).
- `error`: Execution failed. Orchestrator applies retry policy.

### request_id
The request ID this outcome corresponds to. Required so the orchestrator can
correlate responses and guard against mismatched or out-of-order results.

### job_id
The job ID this outcome corresponds to.

### error (optional)
Structured error payload:

```json
{
  "message": "string",
  "type": "handler_not_found",
  "code": "optional_code",
  "details": { "any": "json" }
}
```

Reserved `type` values:
- `handler_not_found`: Fatal; orchestrator moves job directly to DLQ.

### retry_after_seconds (optional)
Used with `status = retry` to request a specific delay. Orchestrator may
override per policy.

## CancelRequest (payload)
```json
{
  "protocol_version": "2",
  "job_id": "uuid",
  "request_id": "uuid (optional)",
  "hard_kill": false
}
```

### Fields
- `protocol_version` (string): Always `"2"` for v2.
- `job_id` (string): Job ID to cancel.
- `request_id` (string, optional): If present, cancel only the matching
  in-flight request.
- `hard_kill` (bool): Reserved for orchestrator-level process management;
  runners may ignore it.

## Scheduling Semantics
- **Timeouts** are enforced by the orchestrator. Runners may return `timeout`
  if they implement their own local timeouts.
- **Retries** are controlled by the orchestrator. Runners return `retry` to
  request a retry.
- **DLQ** decisions are controlled by the orchestrator.
- **Cancellation** (v1) is best-effort. Runners should attempt to cancel the
  associated task and return an outcome (typically `error` with a `cancelled`
  type/message). If `request_id` is omitted, runners may cancel any matching
  in-flight task for the job ID or no-op if nothing is running.

## Runner Selection
RRQ may embed runner selection in the job function name using:

```
runner#handler
```

Example: `rust#send_email`. The orchestrator strips the prefix and sends only
`handler` as `function_name` in the `ExecutionRequest`.

## Reference Implementations
- Python: `rrq-py/rrq/runner_runtime.py` (see `examples/python/`)
- Rust protocol types: `rrq-rs/protocol`
- Rust runner: `rrq-rs/runner` (see
  `rrq-rs/runner/examples/socket_runner.rs`)
