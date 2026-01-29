"""Configuration models for RRQ.

Settings are loaded from TOML files via rrq.config and validated with Pydantic.
"""

from ipaddress import ip_address
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from .constants import (
    DEFAULT_DLQ_NAME,
    DEFAULT_JOB_TIMEOUT_SECONDS,
    DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_POLL_DELAY_SECONDS,
    DEFAULT_QUEUE_NAME,
    DEFAULT_RESULT_TTL_SECONDS,
    DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
    DEFAULT_EXECUTOR_CONNECT_TIMEOUT_MS,
)


class ExecutorConfig(BaseModel):
    """Configuration for external executors (Unix or TCP socket)."""

    type: Literal["socket"] = "socket"
    cmd: list[str] | None = None
    pool_size: int | None = None
    max_in_flight: int | None = None
    env: dict[str, str] | None = None
    cwd: str | None = None
    socket_dir: str | None = None
    tcp_socket: str | None = None
    response_timeout_seconds: float | None = None

    @model_validator(mode="after")
    def validate_socket_options(self) -> "ExecutorConfig":
        if self.socket_dir and self.tcp_socket:
            raise ValueError("tcp_socket cannot be used with socket_dir")
        if self.tcp_socket:
            self._validate_tcp_socket(self.tcp_socket)
        return self

    @staticmethod
    def _validate_tcp_socket(value: str) -> None:
        raw = value.strip()
        if not raw:
            raise ValueError("tcp_socket cannot be empty")

        if raw.startswith("["):
            host_part, sep, port_part = raw.partition("]:")
            if not sep:
                raise ValueError("tcp_socket must be in [host]:port format")
            host = host_part.lstrip("[")
        else:
            host, sep, port_part = raw.rpartition(":")
            if not sep:
                raise ValueError("tcp_socket must be in host:port format")
            if not host:
                raise ValueError("tcp_socket host cannot be empty")

        if host != "localhost":
            try:
                ip = ip_address(host)
            except ValueError as exc:
                raise ValueError(f"Invalid tcp_socket host: {host}") from exc
            if not ip.is_loopback:
                raise ValueError("tcp_socket host must be localhost")

        try:
            port = int(port_part)
        except ValueError as exc:
            raise ValueError(f"Invalid tcp_socket port: {port_part}") from exc
        if port <= 0 or port > 65535:
            raise ValueError(f"tcp_socket port out of range: {port}")


class RRQSettings(BaseModel):
    """Configuration settings for the RRQ (Reliable Redis Queue) system.

    These settings control various aspects of the client, executor runtime, and job store behavior,
    such as Redis connection, queue names, timeouts, retry policies, and concurrency hints.
    """

    redis_dsn: str = Field(
        default="redis://localhost:6379/0",
        description="Redis Data Source Name (DSN) for connecting to the Redis server.",
    )
    default_queue_name: str = Field(
        default=DEFAULT_QUEUE_NAME,
        description="Default queue name used if not specified when enqueuing or processing jobs.",
    )
    default_dlq_name: str = Field(
        default=DEFAULT_DLQ_NAME,
        description="Default Dead Letter Queue (DLQ) name for jobs that fail permanently.",
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
    default_poll_delay_seconds: float = Field(
        default=DEFAULT_POLL_DELAY_SECONDS,
        description="Default delay (in seconds) for worker polling when queues are empty.",
    )
    executor_connect_timeout_ms: int = Field(
        default=DEFAULT_EXECUTOR_CONNECT_TIMEOUT_MS,
        description=(
            "Timeout (in milliseconds) for executor sockets to become ready after spawn."
        ),
    )
    default_unique_job_lock_ttl_seconds: int = Field(
        default=DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
        description="Default TTL (in seconds) for unique job locks if `_unique_key` is used during enqueue.",
    )
    worker_concurrency: int = Field(
        default=10,
        description=(
            "Internal concurrency limit used by the worker runtime; "
            "computed from executor pool sizes and max_in_flight."
        ),
    )
    default_executor_name: str = Field(
        default="python",
        description="Default executor name for jobs without an explicit executor prefix.",
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
        description="Interval (in seconds) at which a worker updates its health check status in Redis.",
    )
    base_retry_delay_seconds: float = Field(
        default=5.0,
        description="Initial delay (in seconds) for the first retry attempt when using exponential backoff.",
    )
    max_retry_delay_seconds: float = Field(
        default=60 * 60,  # 1 hour
        description="Maximum delay (in seconds) for a retry attempt when using exponential backoff.",
    )
    worker_shutdown_grace_period_seconds: float = Field(
        default=10.0,
        description="Grace period (in seconds) for active job tasks to finish during worker shutdown.",
    )
    expected_job_ttl: int = Field(
        default=30,
        description="Expected job processing time buffer for locks (in seconds).",
    )
    model_config = {
        "extra": "ignore",
    }
