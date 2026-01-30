"""Configuration models for RRQ.

Settings are loaded from TOML files via rrq.config and validated with Pydantic.
"""

from pydantic import BaseModel, Field


class RRQSettings(BaseModel):
    """Configuration settings for the RRQ (Reliable Redis Queue) system.

    These settings cover the Python producer/client. Orchestrator settings are
    intentionally omitted; unknown keys in rrq.toml are ignored.
    """

    redis_dsn: str = Field(
        default="redis://localhost:6379/0",
        description="Redis Data Source Name (DSN) for connecting to the Redis server.",
    )
    default_queue_name: str | None = Field(
        default=None,
        description="Optional override for the default queue name when enqueuing.",
    )
    default_max_retries: int | None = Field(
        default=None,
        description="Optional override for max retries per job.",
    )
    default_job_timeout_seconds: int | None = Field(
        default=None,
        description="Optional override for per-job execution timeout (seconds).",
    )
    default_result_ttl_seconds: int | None = Field(
        default=None,
        description="Optional override for result TTL (seconds).",
    )
    default_unique_job_lock_ttl_seconds: int | None = Field(
        default=None,
        description="Optional override for idempotency TTL (seconds).",
    )
    model_config = {
        "extra": "ignore",
    }
