"""Configuration models for RRQ.

Settings are loaded from TOML files via rrq.config and validated with Pydantic.
"""

from pydantic import BaseModel, Field

from .constants import (
    DEFAULT_JOB_TIMEOUT_SECONDS,
    DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_QUEUE_NAME,
    DEFAULT_RESULT_TTL_SECONDS,
    DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
)


class RRQSettings(BaseModel):
    """Configuration settings for the RRQ (Reliable Redis Queue) system.

    These settings cover the Python producer/client and executor runtime. Orchestrator
    settings are intentionally omitted; unknown keys in rrq.toml are ignored.
    """

    redis_dsn: str = Field(
        default="redis://localhost:6379/0",
        description="Redis Data Source Name (DSN) for connecting to the Redis server.",
    )
    default_queue_name: str = Field(
        default=DEFAULT_QUEUE_NAME,
        description="Default queue name used if not specified when enqueuing or processing jobs.",
    )
    default_max_retries: int = Field(
        default=DEFAULT_MAX_RETRIES,
        description="Default maximum number of retries for a job before it's moved to the DLQ.",
    )
    default_job_timeout_seconds: int = Field(
        default=DEFAULT_JOB_TIMEOUT_SECONDS,
        description="Default timeout (in seconds) for a single job execution attempt.",
    )
    default_lock_timeout_extension_seconds: int = Field(
        default=DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS,
        description="Extra time (in seconds) added to a job's timeout to determine the Redis lock's TTL.",
    )
    default_result_ttl_seconds: int = Field(
        default=DEFAULT_RESULT_TTL_SECONDS,
        description="Default Time-To-Live (in seconds) for storing successful job results.",
    )
    default_unique_job_lock_ttl_seconds: int = Field(
        default=DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
        description="Default TTL (in seconds) for unique job locks if `_unique_key` is used during enqueue.",
    )
    expected_job_ttl: int = Field(
        default=30,
        description="Expected job processing time buffer for locks (in seconds).",
    )
    model_config = {
        "extra": "ignore",
    }
