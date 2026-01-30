# rrq-protocol

[![Crates.io](https://img.shields.io/crates/v/rrq-protocol.svg)](https://crates.io/crates/rrq-protocol)
[![Documentation](https://docs.rs/rrq-protocol/badge.svg)](https://docs.rs/rrq-protocol)
[![License](https://img.shields.io/crates/l/rrq-protocol.svg)](LICENSE)

Protocol definitions for communication between the [RRQ](https://crates.io/crates/rrq) orchestrator and executor processes.

## Overview

This crate defines the wire protocol used for socket communication between:
- **RRQ Orchestrator** - dispatches jobs to executor processes
- **RRQ Executor** - receives and executes job handlers

The protocol uses length-prefixed JSON frames over TCP connections.

## Installation

```toml
[dependencies]
rrq-protocol = "0.9"
```

## Protocol Messages

### ExecutionRequest

Sent from orchestrator to executor when dispatching a job:

```rust
use rrq_protocol::{ExecutionRequest, ExecutionContext};

let request = ExecutionRequest {
    protocol_version: "1".to_string(),
    request_id: "req-uuid".to_string(),
    job_id: "job-uuid".to_string(),
    function_name: "send_email".to_string(),
    args: vec![serde_json::json!("user@example.com")],
    kwargs: std::collections::HashMap::new(),
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

Returned from executor to orchestrator after job execution:

```rust
use rrq_protocol::ExecutionOutcome;

// Success
let outcome = ExecutionOutcome::success(
    "job-uuid".to_string(),
    "req-uuid".to_string(),
    serde_json::json!({"sent": true}),
);

// Failure
let outcome = ExecutionOutcome::failure(
    "job-uuid".to_string(),
    "req-uuid".to_string(),
    "Connection timeout".to_string(),
);

// Retry after delay
let outcome = ExecutionOutcome::retry_after(
    "job-uuid".to_string(),
    "req-uuid".to_string(),
    "Rate limited".to_string(),
    60, // retry after 60 seconds
);
```

### CancelRequest

Sent to executor to cancel an in-flight job:

```rust
use rrq_protocol::CancelRequest;

let cancel = CancelRequest {
    protocol_version: "1".to_string(),
    job_id: "job-uuid".to_string(),
    request_id: Some("req-uuid".to_string()),
    hard_kill: false,
};
```

## Frame Encoding

Messages are encoded as length-prefixed JSON:

```
┌─────────────────┬──────────────────────────────┐
│  Length (4B)    │  JSON Payload (N bytes)      │
│  Big-endian u32 │  UTF-8 encoded               │
└─────────────────┴──────────────────────────────┘
```

```rust
use rrq_protocol::{encode_frame, ExecutorMessage, ExecutionRequest};

let message = ExecutorMessage::Request {
    payload: request,
};
let frame: Vec<u8> = encode_frame(&message)?;
// frame = [length_bytes...][json_bytes...]
```

## Executor Message Envelope

All messages are wrapped in an `ExecutorMessage` enum:

```rust
use rrq_protocol::ExecutorMessage;

// Three variants:
let msg = ExecutorMessage::Request { payload: request };
let msg = ExecutorMessage::Response { payload: outcome };
let msg = ExecutorMessage::Cancel { payload: cancel };
```

## Outcome Types

The `outcome_type` field indicates how the job completed:

| Type | Description |
|------|-------------|
| `success` | Job completed successfully |
| `failure` | Job failed (may retry) |
| `handler_not_found` | No handler registered for function |
| `timeout` | Job exceeded deadline |
| `cancelled` | Job was cancelled |
| `retry_after` | Retry after specified delay |

## Related Crates

| Crate | Description |
|-------|-------------|
| [`rrq`](https://crates.io/crates/rrq) | Job queue orchestrator |
| [`rrq-producer`](https://crates.io/crates/rrq-producer) | Client for enqueuing jobs |
| [`rrq-executor`](https://crates.io/crates/rrq-executor) | Executor runtime implementation |

## License

Apache-2.0
