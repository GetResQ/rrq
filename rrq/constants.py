"""This module defines constants used throughout the RRQ (Reliable Redis Queue) system.

These constants include Redis key prefixes, default queue names, and default
configuration values for job processing and worker behavior.
"""

# RRQ Constants

# Default queue name if not specified
DEFAULT_QUEUE_NAME: str = "rrq:queue:default"

# Default Dead Letter Queue name
DEFAULT_DLQ_NAME: str = "rrq:dlq:default"

# Redis key prefixes
JOB_KEY_PREFIX: str = "rrq:job:"
QUEUE_KEY_PREFIX: str = "rrq:queue:"  # For ZSETs holding job IDs
DLQ_KEY_PREFIX: str = "rrq:dlq:"  # For lists holding Dead Letter Queue job IDs
ACTIVE_JOBS_PREFIX: str = (
    "rrq:active:"  # For lists of active jobs per worker (optional, for recovery)
)
LOCK_KEY_PREFIX: str = "rrq:lock:job:"  # For job processing locks
UNIQUE_JOB_LOCK_PREFIX: str = "rrq:lock:unique:"  # For user-defined unique job keys
HEALTH_KEY_PREFIX: str = "rrq:health:worker:"
RETRY_COUNTER_PREFIX: str = (
    "rrq:retry_count:"  # Potentially, if not stored directly in job hash
)

# Hybrid monitoring optimization keys
ACTIVE_QUEUES_SET: str = (
    "rrq:active:queues"  # ZSET: queue_name -> last_activity_timestamp
)
ACTIVE_WORKERS_SET: str = (
    "rrq:active:workers"  # ZSET: worker_id -> last_heartbeat_timestamp
)
MONITOR_EVENTS_STREAM: str = "rrq:monitor:events"  # Stream for real-time changes

# Default job settings (can be overridden by RRQSettings or per job)
DEFAULT_MAX_RETRIES: int = 5
DEFAULT_JOB_TIMEOUT_SECONDS: int = 300  # 5 minutes
DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS: int = (
    60  # How much longer lock should live than job_timeout
)
DEFAULT_RESULT_TTL_SECONDS: int = 3600 * 24  # 1 day
DEFAULT_DLQ_RESULT_TTL_SECONDS: int = 3600 * 24 * 7  # 7 days for DLQ job details
DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS: int = 3600 * 6  # 6 hours for unique job lock

# Poll delay for worker
DEFAULT_POLL_DELAY_SECONDS: float = 0.1

# Default worker ID if not specified
DEFAULT_WORKER_ID_PREFIX: str = "rrq_worker_"
CONNECTION_POOL_MAX_CONNECTIONS: int = 20
