# rrq-protocol

[![Crates.io](https://img.shields.io/crates/v/rrq-protocol.svg)](https://crates.io/crates/rrq-protocol)
[![Documentation](https://docs.rs/rrq-protocol/badge.svg)](https://docs.rs/rrq-protocol)
[![License](https://img.shields.io/crates/l/rrq-protocol.svg)](LICENSE)

**Wire protocol types for RRQ orchestrator-runner communication.**

## What is RRQ?

RRQ (Reliable Redis Queue) is a distributed job queue with a Rust orchestrator and language-flexible workers. This crate defines the protocol used for socket communication between the orchestrator and runner processes.

## When to Use This Crate

Use `rrq-protocol` if you're:
- Building a custom runner in a new language
- Extending the RRQ protocol
- Debugging orchestrator-runner communication

For most use cases, use [`rrq-runner`](https://crates.io/crates/rrq-runner) which wraps this protocol.

## Installation

```toml
[dependencies]
rrq-protocol = "0.9"
```

## Protocol Overview

Messages are length-prefixed JSON frames over TCP:

```
┌─────────────────┬──────────────────────────────┐
│  Length (4B)    │  JSON Payload (N bytes)      │
│  Big-endian u32 │  UTF-8 encoded               │
└─────────────────┴──────────────────────────────┘
```

## Message Types

### ExecutionRequest

Sent from orchestrator to runner when dispatching a job:

```rust
use rrq_protocol::{ExecutionRequest, ExecutionContext};

let request = ExecutionRequest {
    protocol_version: "2".to_string(),
    request_id: "req-uuid".to_string(),
    job_id: "job-uuid".to_string(),
    function_name: "send_email".to_string(),
    params: [("to".to_string(), json!("user@example.com"))].into(),
    context: ExecutionContext {
        job_id: "job-uuid".to_string(),
        attempt: 1,
        enqueue_time: chrono::Utc::now(),
        queue_name: "default".to_string(),
        deadline: None,
        trace_context: None,
        worker_id: Some("worker-1".to_string()),
    },
};
```

### ExecutionOutcome

Returned from runner after job execution:

```rust
use rrq_protocol::ExecutionOutcome;

// Success
let outcome = ExecutionOutcome::success("job-id", "req-id", json!({"sent": true}));

// Failure
let outcome = ExecutionOutcome::failure("job-id", "req-id", "Connection timeout");

// Retry after delay
let outcome = ExecutionOutcome::retry_after("job-id", "req-id", "Rate limited", 60);
```

### CancelRequest

Sent to cancel an in-flight job:

```rust
use rrq_protocol::CancelRequest;

let cancel = CancelRequest {
    protocol_version: "2".to_string(),
    job_id: "job-uuid".to_string(),
    request_id: Some("req-uuid".to_string()),
    hard_kill: false,
};
```

## Frame Encoding

```rust
use rrq_protocol::{encode_frame, RunnerMessage};

let message = RunnerMessage::Request { payload: request };
let frame: Vec<u8> = encode_frame(&message)?;
```

## Outcome Types

| Type | Description |
|------|-------------|
| `success` | Job completed |
| `failure` | Job failed (may retry) |
| `handler_not_found` | No handler for function |
| `timeout` | Exceeded deadline |
| `cancelled` | Job cancelled |
| `retry_after` | Retry after delay |

## Related Crates

| Crate | Purpose |
|-------|---------|
| [`rrq`](https://crates.io/crates/rrq) | Orchestrator |
| [`rrq-runner`](https://crates.io/crates/rrq-runner) | Runner runtime |
| [`rrq-producer`](https://crates.io/crates/rrq-producer) | Job producer |

## License

Apache-2.0
