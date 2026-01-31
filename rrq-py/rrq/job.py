"""This module defines the core data structures for jobs in the RRQ system,
including the Job model and JobStatus enumeration.
"""

import uuid
from datetime import timezone, datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Represents the lifecycle status of a job within the RRQ system."""

    PENDING = "PENDING"  # Job enqueued, awaiting processing by a worker.
    ACTIVE = "ACTIVE"  # Job picked up by a worker and is currently being processed.
    COMPLETED = "COMPLETED"  # Job processed successfully.
    FAILED = (
        "FAILED"  # Job failed after all retry attempts or was a non-retryable failure.
    )
    RETRYING = "RETRYING"  # Job failed, an attempt will be made to re-process it after a delay.
    CANCELLED = "CANCELLED"  # Job cancelled before completion.
    UNKNOWN = "UNKNOWN"  # Unrecognized status or missing status field.
    # NOT_FOUND might be a status for queries, but not stored on the job itself typically


def new_job_id() -> str:
    """Generates a new unique job ID (UUID4)."""
    return str(uuid.uuid4())


class JobResult(BaseModel):
    """Result status for a job without fetching the full job record."""

    status: JobStatus
    result: Any | None = None
    last_error: str | None = None


class Job(BaseModel):
    """Represents a job to be processed by an RRQ worker.

    This model encapsulates all the information related to a job, including its
    identity, execution parameters, status, and results.
    """

    id: str = Field(
        default_factory=new_job_id, description="Unique identifier for the job."
    )
    function_name: str = Field(
        description="Name of the handler function to execute for this job."
    )
    job_args: list[Any] = Field(
        default_factory=list,
        description="Positional arguments for the handler function.",
    )
    job_kwargs: dict[str, Any] = Field(
        default_factory=dict, description="Keyword arguments for the handler function."
    )

    enqueue_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp (timezone.utc) when the job was initially enqueued.",
    )

    start_time: Optional[datetime] = Field(
        default=None,
        description="Timestamp (timezone.utc) when the job started processing.",
    )

    status: JobStatus = Field(
        default=JobStatus.PENDING, description="Current status of the job."
    )
    current_retries: int = Field(
        default=0, description="Number of retry attempts made so far."
    )
    next_scheduled_run_time: Optional[datetime] = Field(
        default=None,
        description="Timestamp (timezone.utc) when the job is next scheduled to run (for retries/deferrals).",
    )

    # Execution control parameters, can be overridden from worker defaults.
    max_retries: int = Field(
        default=3, description="Maximum number of retry attempts allowed for this job."
    )
    job_timeout_seconds: Optional[int] = Field(
        default=None,
        description="Optional per-job execution timeout in seconds. Overrides worker default if set.",
    )
    result_ttl_seconds: Optional[int] = Field(
        default=None,
        description="Optional Time-To-Live (in seconds) for the job's result. Overrides worker default if set.",
    )

    # Optional key for ensuring job uniqueness if provided during enqueue.
    job_unique_key: Optional[str] = Field(
        default=None, description="Optional key for ensuring job uniqueness."
    )

    # Fields populated upon job completion or failure.
    completion_time: Optional[datetime] = Field(
        default=None,
        description="Timestamp (timezone.utc) when the job finished (completed or failed permanently).",
    )
    result: Optional[Any] = Field(
        default=None,
        description="Result of the job if successful, or error details if failed.",
    )
    last_error: Optional[str] = Field(
        default=None,
        description="String representation of the last error encountered during processing.",
    )

    # Optional routing hints (currently informational, could be used for advanced routing).
    queue_name: Optional[str] = Field(
        default=None, description="The name of the queue this job was last enqueued on."
    )
    dlq_name: Optional[str] = Field(
        default=None,
        description="The name of the Dead Letter Queue this job will be moved to if it fails permanently.",
    )

    # Distributed tracing context carrier (serialized by the orchestrator).
    trace_context: Optional[dict[str, str]] = Field(
        default=None,
        description="Optional distributed tracing propagation carrier to continue traces from enqueue to execution.",
    )
