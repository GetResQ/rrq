"""This module defines the core data structures for jobs in the RRQ system,
including the Job model and JobStatus enumeration.
"""

import uuid
from datetime import UTC, datetime
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
    # NOT_FOUND might be a status for queries, but not stored on the job itself typically


def new_job_id() -> str:
    """Generates a new unique job ID (UUID4)."""
    return str(uuid.uuid4())


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
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp (UTC) when the job was initially enqueued.",
    )
    # score: Optional[float] = None # The score in the ZSET, derived from defer_until/defer_by
    # Not stored in the job hash directly, but used for queueing.

    status: JobStatus = Field(
        default=JobStatus.PENDING, description="Current status of the job."
    )
    current_retries: int = Field(
        default=0, description="Number of retry attempts made so far."
    )
    next_scheduled_run_time: Optional[datetime] = Field(
        default=None,
        description="Timestamp (UTC) when the job is next scheduled to run (for retries/deferrals).",
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
        description="Timestamp (UTC) when the job finished (completed or failed permanently).",
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

    # For model_config to allow arbitrary types if result is complex and not Pydantic model
    # class Config:
    #     arbitrary_types_allowed = True

    # def to_redis_hash(self) -> dict[str, Any]:
    #     """Prepares the job model for storage as a Redis hash.
    #     Pydantic's model_dump is good, but we might want to ensure all values are easily
    #     storable as strings or simple types for Redis, or handle serialization here.
    #     For now, model_dump with json_encoders should suffice with a good serializer.
    #     """
    #     # Using model_dump ensures that Pydantic models are properly serialized (e.g., datetimes to ISO strings)
    #     # We will use a JSON serializer in JobStore that handles Pydantic models correctly.
    #     return self.model_dump(exclude_none=True)

    # @classmethod
    # def from_redis_hash(cls, data: dict[str, Any]) -> "Job":
    #     """Reconstructs a Job instance from data retrieved from a Redis hash."""""""""
    #     # Pydantic will handle parsing basic types. Datetimes are expected to be ISO strings.
    #     # Handle potential None values for args/kwargs if they were excluded from dump
    #     # data.setdefault("args", None) # Removed
    #     # data.setdefault("kwargs", None) # Removed
    #     return cls(**data)
    pass  # Add pass if class body becomes empty after removing methods, or remove if not needed
