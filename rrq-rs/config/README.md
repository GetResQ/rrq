# rrq-config

Shared configuration loader and settings types for RRQ.

This crate is used by the Rust orchestrator and by the FFI producer bindings to
ensure consistent config parsing and defaults across languages.

## Usage

```rust
use rrq_config::load_toml_settings;

let settings = load_toml_settings(None)?;
println!("Redis DSN: {}", settings.redis_dsn);
```

## Producer settings

```rust
use rrq_config::load_producer_settings;

let producer_settings = load_producer_settings(None)?;
```

## License

Apache-2.0
