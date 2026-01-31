"""This module defines custom exceptions for the RRQ (Reliable Redis Queue) system."""

from typing import Optional


class RRQError(Exception):
    """Base class for all RRQ specific errors."""

    pass


class RetryJob(RRQError):
    """Exception raised by a job handler to signal that the job should be retried.

    This allows a handler to explicitly request a retry, potentially with a custom delay,
    rather than relying on automatic retries for general exceptions.
    """

    def __init__(
        self,
        message: str = "Job requested retry",
        defer_seconds: Optional[float] = None,
    ):
        """
        Args:
            message: Optional message describing why the retry is requested.
            defer_seconds: Optional custom delay in seconds before the job is re-queued.
                         If None, the worker will use its default backoff strategy.
        """
        super().__init__(message)
        self.defer_seconds = defer_seconds


class HandlerNotFound(RRQError):
    """Exception raised when a job handler cannot be found in the registry."""

    pass


class MaxRetriesExceeded(Exception):
    """Raised when a job fails after reaching its maximum retry limit."""

    pass


# Add other RRQ-specific exceptions here as needed
