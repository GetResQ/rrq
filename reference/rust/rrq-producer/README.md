# rrq-producer

Minimal Rust producer for RRQ. This crate writes job payloads directly to Redis
using the RRQ job schema.

## Git dependency

```toml
[dependencies]
rrq-producer = { git = "https://github.com/YOUR_ORG/rrq", package = "rrq-producer", rev = "<sha>" }
```

## Example

```
cd reference/rust/rrq-producer
cargo run --example producer
```

Env vars:
- `RRQ_REDIS_DSN` (default: `redis://localhost:6379/3`)
- `RRQ_QUEUE` (default: `default`)
- `RRQ_FUNCTION` (default: `quick_task`)
- `RRQ_COUNT` (default: `5`)
