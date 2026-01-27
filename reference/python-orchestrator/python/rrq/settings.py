"""Configuration models for the RRQ reference orchestrator."""

from typing import Literal

from pydantic import BaseModel, Field

from .constants import (
    DEFAULT_DLQ_NAME,
    DEFAULT_JOB_TIMEOUT_SECONDS,
    DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_POLL_DELAY_SECONDS,
    DEFAULT_QUEUE_NAME,
    DEFAULT_RESULT_TTL_SECONDS,
    DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
)
from .cron import CronJob


class ExecutorConfig(BaseModel):
    """Configuration for external executors (Unix socket)."""

    type: Literal["socket"] = "socket"
    cmd: list[str] | None = None
    pool_size: int | None = None
    env: dict[str, str] | None = None
    cwd: str | None = None
    socket_dir: str | None = None
    response_timeout_seconds: float | None = None


class RRQSettings(BaseModel):
    """Configuration settings for the RRQ (Reliable Redis Queue) system."""

    redis_dsn: str = Field(
        default="redis://localhost:6379/0",
        description="Redis Data Source Name (DSN) for connecting to the Redis server.",
    )
    default_queue_name: str = Field(
        default=DEFAULT_QUEUE_NAME,
        description="Default queue name used if not specified when enqueuing.",
    )
    default_dlq_name: str = Field(
        default=DEFAULT_DLQ_NAME,
        description="Default DLQ name for jobs that fail permanently.",
    )
    default_max_retries: int = Field(
        default=DEFAULT_MAX_RETRIES,
        description="Default maximum retries before a job is moved to the DLQ.",
    )
    default_job_timeout_seconds: int = Field(
        default=DEFAULT_JOB_TIMEOUT_SECONDS,
        description="Default timeout (in seconds) for a single job attempt.",
    )
    default_lock_timeout_extension_seconds: int = Field(
        default=DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS,
        description="Extra seconds added to determine lock TTL.",
    )
    default_result_ttl_seconds: int = Field(
        default=DEFAULT_RESULT_TTL_SECONDS,
        description="Default TTL (in seconds) for successful job results.",
    )
    default_poll_delay_seconds: float = Field(
        default=DEFAULT_POLL_DELAY_SECONDS,
        description="Default delay (in seconds) for worker polling when empty.",
    )
    default_unique_job_lock_ttl_seconds: int = Field(
        default=DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
        description="Default TTL (in seconds) for unique job locks.",
    )
    worker_concurrency: int = Field(
        default=10,
        description="Effective concurrent jobs a single worker can handle.",
    )
    default_executor_name: str = Field(
        default="python",
        description="Default executor name for jobs without explicit prefix.",
    )
    executors: dict[str, ExecutorConfig] = Field(
        default_factory=dict,
        description="External executor configurations keyed by executor name.",
    )
    executor_routes: dict[str, str] = Field(
        default_factory=dict,
        description="Optional routing map from queue name to executor name.",
    )
    worker_health_check_interval_seconds: float = Field(
        default=60,
        description="Interval (seconds) at which worker updates health status.",
    )
    base_retry_delay_seconds: float = Field(
        default=5.0,
        description="Initial delay (seconds) for the first retry attempt.",
    )
    max_retry_delay_seconds: float = Field(
        default=60 * 60,
        description="Maximum delay (seconds) for retry attempts.",
    )
    worker_shutdown_grace_period_seconds: float = Field(
        default=10.0,
        description="Grace period (seconds) for active jobs during shutdown.",
    )
    expected_job_ttl: int = Field(
        default=30,
        description="Expected job processing time buffer for locks (seconds).",
    )
    cron_jobs: list[CronJob] = Field(
        default_factory=list,
        description="Cron jobs to enqueue periodically while workers run.",
    )

    model_config = {
        "extra": "ignore",
    }
