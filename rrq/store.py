"""This module defines the JobStore class, responsible for all interactions
with the Redis backend for storing and managing RRQ job data and queues.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any, Optional

from redis.asyncio import Redis as AsyncRedis

from .constants import (
    DEFAULT_DLQ_RESULT_TTL_SECONDS,
    JOB_KEY_PREFIX,
    LOCK_KEY_PREFIX,
    QUEUE_KEY_PREFIX,
    UNIQUE_JOB_LOCK_PREFIX,
)
from .job import Job, JobStatus
from .settings import RRQSettings

logger = logging.getLogger(__name__)

class JobStore:
    """Provides an abstraction layer for interacting with Redis for RRQ operations.

    Handles serialization/deserialization, key management, and atomic operations
    related to jobs, queues, locks, and worker health.
    """

    def __init__(self, settings: RRQSettings):
        """Initializes the JobStore with a Redis connection.

        Args:
            redis_dsn: The Redis Data Source Name (DSN) string.
        """
        self.settings = settings
        self.redis = AsyncRedis.from_url(
            settings.redis_dsn, decode_responses=False
        )  # Work with bytes initially
        
    def _format_queue_key(self, queue_name: str) -> str:
        """Normalize a queue name or key into a Redis key for ZSET queues."""

        # If already a full key, use it directly
        if queue_name.startswith(QUEUE_KEY_PREFIX):
            return queue_name
        return f"{QUEUE_KEY_PREFIX}{queue_name}"

    def _format_dlq_key(self, dlq_name: str) -> str:
        """Normalize a DLQ name or key into a Redis key for DLQ lists."""
        from .constants import DLQ_KEY_PREFIX

        # If already a full key, use it directly
        if dlq_name.startswith(DLQ_KEY_PREFIX):
            return dlq_name
        return f"{DLQ_KEY_PREFIX}{dlq_name}"

    async def aclose(self):
        """Closes the Redis connection pool associated with this store."""
        await self.redis.aclose()

    async def _serialize_job_field(self, value: Any) -> bytes:
        """Serializes a single field value for storing in a Redis hash."""
        # Pydantic models are dumped to dict, then JSON string, then bytes.
        # Basic types are JSON dumped directly.
        if hasattr(value, "model_dump_json"):  # For Pydantic sub-models if any
            return value.model_dump_json().encode("utf-8")
        if isinstance(value, dict | list) or (
            hasattr(value, "__dict__") and not callable(value)
        ):
            # Fallback for other dict-like or list-like objects, and simple custom objects
            try:
                # Use Pydantic-aware JSON dumping if possible
                if hasattr(value, "model_dump"):
                    value = value.model_dump(mode="json")
                return json.dumps(value, default=str).encode(
                    "utf-8"
                )  # default=str for datetimes etc.
            except TypeError:
                return str(value).encode("utf-8")  # Last resort
        return str(value).encode("utf-8")  # For simple types like int, str, bool

    async def _deserialize_job_field(self, value_bytes: bytes) -> Any:
        """Deserializes a single field value from Redis bytes."""
        try:
            # Attempt to parse as JSON first, as most complex types will be stored this way.
            return json.loads(value_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # If it fails, it might be a simple string that wasn't JSON encoded (e.g. status enums)
            # or a raw byte representation that needs specific handling (not covered here yet)
            return value_bytes.decode("utf-8")  # Fallback to string

    async def save_job_definition(self, job: Job) -> None:
        """Saves the complete job definition as a Redis hash.

        Handles manual serialization of complex fields (args, kwargs, result).

        Args:
            job: The Job object to save.
        """
        job_key = f"{JOB_KEY_PREFIX}{job.id}"

        # Dump model excluding fields handled manually
        job_data_dict = job.model_dump(
            mode="json", exclude={"job_args", "job_kwargs", "result"}
        )

        # Manually serialize potentially complex fields to JSON strings
        job_args_json = json.dumps(job.job_args if job.job_args is not None else None)
        job_kwargs_json = json.dumps(
            job.job_kwargs if job.job_kwargs is not None else None
        )
        result_json = json.dumps(job.result if job.result is not None else None)

        # Combine base fields (converted to string) with manually serialized ones
        final_mapping_for_hset = {
            str(k): str(v) for k, v in job_data_dict.items() if v is not None
        }
        final_mapping_for_hset["job_args"] = job_args_json
        final_mapping_for_hset["job_kwargs"] = job_kwargs_json
        final_mapping_for_hset["result"] = result_json

        # Ensure ID is present
        if "id" not in final_mapping_for_hset:
            final_mapping_for_hset["id"] = job.id

        if final_mapping_for_hset:  # Avoid HSET with empty mapping
            await self.redis.hset(job_key, mapping=final_mapping_for_hset)
            logger.debug(f"Saved job definition for {job.id} to Redis hash {job_key}.")

    async def get_job_definition(self, job_id: str) -> Optional[Job]:
        """Retrieves a job definition from Redis and reconstructs the Job object.

        Handles manual deserialization of complex fields (args, kwargs, result).

        Args:
            job_id: The unique ID of the job to retrieve.

        Returns:
            The reconstructed Job object, or None if the job ID is not found or parsing fails.
        """
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        job_data_raw_bytes = await self.redis.hgetall(job_key)

        if not job_data_raw_bytes:
            logger.debug(f"Job definition not found for ID: {job_id}")
            return None

        # Decode all keys and values from bytes to str first
        job_data_dict_str = {
            k.decode("utf-8"): v.decode("utf-8") for k, v in job_data_raw_bytes.items()
        }

        # Manually extract and parse complex fields
        job_args_list = None
        job_kwargs_dict = None
        result_obj = None

        job_args_str = job_data_dict_str.pop("job_args", None)
        job_kwargs_str = job_data_dict_str.pop("job_kwargs", None)
        result_str = job_data_dict_str.pop("result", None)

        if job_args_str and job_args_str.lower() != "null":
            try:
                job_args_list = json.loads(job_args_str)
            except json.JSONDecodeError:
                logger.error(
                    f"Failed to JSON decode 'job_args' for job {job_id} from string: '{job_args_str}'",
                    exc_info=True,
                )

        if job_kwargs_str and job_kwargs_str.lower() != "null":
            try:
                job_kwargs_dict = json.loads(job_kwargs_str)
            except json.JSONDecodeError:
                logger.error(
                    f"Failed to JSON decode 'job_kwargs' for job {job_id} from string: '{job_kwargs_str}'",
                    exc_info=True,
                )

        if result_str and result_str.lower() != "null":
            try:
                # Always try to load result as JSON, as it's stored via json.dumps
                result_obj = json.loads(result_str)
            except json.JSONDecodeError:
                logger.error(
                    f"Failed to JSON decode 'result' for job {job_id} from string: '{result_str}'",
                    exc_info=True,
                )
                # Decide on fallback: None or the raw string?
                # If stored via json.dumps, failure here indicates corruption or non-JSON string stored previously.
                result_obj = None  # Safest fallback is likely None

        # Validate the remaining dictionary using Pydantic Job model
        try:
            # Pass only the remaining fields to the constructor
            base_job_data = dict(job_data_dict_str)
            validated_job = Job(**base_job_data)

            # Manually assign the parsed complex fields, ensuring correct types
            validated_job.job_args = job_args_list if job_args_list is not None else []
            validated_job.job_kwargs = (
                job_kwargs_dict if job_kwargs_dict is not None else {}
            )
            validated_job.result = result_obj

            logger.debug(f"Successfully retrieved and parsed job {validated_job.id}")
            return validated_job
        except Exception as e_val:
            logger.error(
                f"Pydantic validation error in get_job_definition for job {job_id}: {e_val} on data {base_job_data}",
                exc_info=True,
            )
            return None

    async def add_job_to_queue(
        self, queue_name: str, job_id: str, score: float
    ) -> None:
        """Adds a job ID to a specific queue (Redis Sorted Set) with a score.

        The score typically represents the time (e.g., milliseconds since epoch)
        when the job should become available for processing.

        Args:
            queue_name: The name of the queue (without the prefix).
            job_id: The ID of the job to add.
            score: The score (float) determining the job's position/priority in the queue.
        """
        queue_key = self._format_queue_key(queue_name)
        await self.redis.zadd(
            queue_key, {job_id.encode("utf-8"): score}
        )  # Store job_id as bytes
        logger.debug(f"Added job {job_id} to queue '{queue_key}' with score {score}")

    async def get_queued_job_ids(
        self, queue_name: str, start: int = 0, end: int = -1
    ) -> list[str]:
        """Retrieves a range of job IDs from a queue (Sorted Set) by index.

        Args:
            queue_name: The name of the queue (without the prefix).
            start: The starting index (0-based).
            end: The ending index (inclusive, -1 means to the end).

        Returns:
            A list of job IDs as strings.
        """
        queue_key = self._format_queue_key(queue_name)
        job_ids_bytes = await self.redis.zrange(queue_key, start, end)
        return [job_id.decode("utf-8") for job_id in job_ids_bytes]

    async def get_ready_job_ids(self, queue_name: str, count: int) -> list[str]:
        """Retrieves ready job IDs from the queue (score <= now) up to a specified count.

        Args:
            queue_name: The name of the queue (without the prefix).
            count: The maximum number of job IDs to retrieve.

        Returns:
            A list of ready job IDs as strings.
        """
        if count <= 0:
            return []
        queue_key = self._format_queue_key(queue_name)
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        # Fetch jobs with score from -inf up to current time, limit by count
        job_ids_bytes = await self.redis.zrangebyscore(
            queue_key, min=float("-inf"), max=float(now_ms), start=0, num=count
        )
        ids = [job_id.decode("utf-8") for job_id in job_ids_bytes]
        if ids:
            logger.debug(f"Found {len(ids)} ready jobs in queue '{queue_key}'.")
        return ids

    async def acquire_job_lock(
        self, job_id: str, worker_id: str, lock_timeout_ms: int
    ) -> bool:
        """Attempts to acquire an exclusive processing lock for a job using SET NX PX.

        Args:
            job_id: The ID of the job to lock.
            worker_id: The ID of the worker attempting to acquire the lock.
            lock_timeout_ms: The lock timeout/TTL in milliseconds.

        Returns:
            True if the lock was acquired successfully, False otherwise.
        """
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        result = await self.redis.set(
            lock_key, worker_id.encode("utf-8"), nx=True, px=lock_timeout_ms
        )
        if result:
            logger.debug(
                f"Worker {worker_id} acquired lock for job {job_id} ({lock_key})."
            )
        return result is True

    async def release_job_lock(self, job_id: str) -> None:
        """Releases the processing lock for a job.

        Args:
            job_id: The ID of the job whose lock should be released.
        """
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        deleted_count = await self.redis.delete(lock_key)
        if deleted_count > 0:
            logger.debug(f"Released lock for job {job_id} ({lock_key}).")
        # No need to log if lock didn't exist

    async def update_job_status(self, job_id: str, status: JobStatus) -> None:
        """Updates only the status field of a job in its Redis hash.

        Args:
            job_id: The ID of the job to update.
            status: The new JobStatus.
        """
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        # Status enum value needs to be accessed
        await self.redis.hset(job_key, "status", status.value.encode("utf-8"))
        logger.debug(f"Updated status of job {job_id} to {status.value}.")

    async def increment_job_retries(self, job_id: str) -> int:
        """Atomically increments the 'current_retries' field for a job.

        Args:
            job_id: The ID of the job whose retry count should be incremented.

        Returns:
            The new retry count after incrementing.
        """
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        new_retry_count = await self.redis.hincrby(job_key, "current_retries", 1)
        new_count = int(new_retry_count)  # hincrby might return bytes/str
        logger.debug(f"Incremented retries for job {job_id} to {new_count}.")
        return new_count

    async def move_job_to_dlq(
        self, job_id: str, dlq_name: str, error_message: str, completion_time: datetime
    ) -> None:
        """Moves a job to the Dead Letter Queue (DLQ).

        This involves updating the job's status to FAILED, storing the final error
        and completion time in its hash, adding the job ID to the DLQ list,
        and setting a TTL on the job hash itself.

        Args:
            job_id: The ID of the job to move.
            dlq_name: The name of the DLQ list (without prefix).
            error_message: The final error message to store.
            completion_time: The timestamp when the job failed permanently.
        """
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        dlq_redis_key = self._format_dlq_key(dlq_name)

        # Ensure complex fields are properly handled if needed (error could be complex)
        # For now, assuming simple string error message
        update_data = {
            "status": JobStatus.FAILED.value.encode("utf-8"),
            "last_error": error_message.encode("utf-8"),
            "completion_time": completion_time.isoformat().encode("utf-8"),
        }

        # Use pipeline for atomicity
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(job_key, mapping=update_data)
            pipe.lpush(dlq_redis_key, job_id.encode("utf-8"))
            pipe.expire(job_key, DEFAULT_DLQ_RESULT_TTL_SECONDS)
            results = await pipe.execute()
        logger.info(f"Moved job {job_id} to DLQ '{dlq_redis_key}'. Results: {results}")
    
    async def requeue_dlq(
        self,
        dlq_name: str,
        target_queue: str,
        limit: int | None = None,
    ) -> int:
        """Requeue jobs from the Dead Letter Queue back into a live queue.

        Pops jobs from the DLQ list and adds them to the target queue with current timestamp.

        Args:
            dlq_name: Name of the DLQ (without prefix).
            target_queue: Name of the target queue (without prefix).
            limit: Maximum number of jobs to requeue; all if None.

        Returns:
            Number of jobs requeued.
        """
        jobs_requeued = 0
        dlq_key = self._format_dlq_key(dlq_name)
        # Continue popping until limit is reached or DLQ is empty
        while limit is None or jobs_requeued < limit:
            job_id_bytes = await self.redis.rpop(dlq_key)
            if not job_id_bytes:
                break
            job_id = job_id_bytes.decode("utf-8")
            # Use current time for re-enqueue score
            now_ms = int(datetime.now(UTC).timestamp() * 1000)
            await self.add_job_to_queue(
                self._format_queue_key(target_queue),
                job_id,
                now_ms,
            )
            jobs_requeued += 1
        return jobs_requeued

    async def get_job_lock_owner(self, job_id: str) -> Optional[str]:
        """Gets the current owner (worker ID) of a job's processing lock, if held.

        Args:
            job_id: The ID of the job.

        Returns:
            The worker ID holding the lock, or None if the lock is not held.
        """
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        owner_bytes = await self.redis.get(lock_key)
        return owner_bytes.decode("utf-8") if owner_bytes else None

    async def remove_job_from_queue(self, queue_name: str, job_id: str) -> int:
        """Removes a specific job ID from a queue (Sorted Set).

        Args:
            queue_name: The name of the queue (without prefix).
            job_id: The ID of the job to remove.

        Returns:
            The number of elements removed (0 or 1).
        """
        queue_key = self._format_queue_key(queue_name)
        removed_count = await self.redis.zrem(queue_key, job_id.encode("utf-8"))
        count = int(removed_count)  # Ensure int
        if count > 0:
            logger.debug(f"Removed job {job_id} from queue '{queue_key}'.")
        return count

    async def acquire_unique_job_lock(
        self, unique_key: str, job_id: str, lock_ttl_seconds: int
    ) -> bool:
        """Acquires a lock for a unique job key in Redis if it doesn't already exist.

        This is used to prevent duplicate job submissions based on a user-defined unique key.
        The lock stores the job ID that acquired it and has a TTL.

        Args:
            unique_key: The user-defined key for ensuring job uniqueness.
            job_id: The ID of the job attempting to acquire the lock. This is stored
                    as the value of the lock for traceability.
            lock_ttl_seconds: The Time-To-Live (in seconds) for the lock.

        Returns:
            True if the lock was successfully acquired, False otherwise (e.g., lock already held).
        """
        lock_key = f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"
        # NX = only set if not exists. EX = set TTL in seconds.
        # The value stored is the job_id for traceability of who holds the lock.
        was_set = await self.redis.set(
            lock_key, job_id.encode("utf-8"), ex=lock_ttl_seconds, nx=True
        )
        if was_set:
            logger.info(
                f"Acquired unique job lock for key '{unique_key}' (job_id: {job_id}, TTL: {lock_ttl_seconds}s)"
            )
            return True
        else:
            locked_by_job_id_bytes = await self.redis.get(lock_key)
            locked_by_job_id = (
                locked_by_job_id_bytes.decode("utf-8")
                if locked_by_job_id_bytes
                else "unknown"
            )
            logger.debug(
                f"Failed to acquire unique job lock for key '{unique_key}'. Lock held by job_id: {locked_by_job_id}."
            )
            return False

    async def release_unique_job_lock(self, unique_key: str) -> None:
        """Deletes the lock associated with a unique job key from Redis.

        Args:
            unique_key: The user-defined key for which the lock should be released.
        """
        lock_key = f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"
        deleted_count = await self.redis.delete(lock_key)
        if deleted_count > 0:
            logger.info(
                f"Released unique job lock for key '{unique_key}' (lock: {lock_key})"
            )
        else:
            # This might happen if the lock expired naturally via TTL before explicit release.
            # Or if release is called multiple times, or on a key that never had a lock.
            logger.debug(
                f"No unique job lock found to release for key '{unique_key}' (lock: {lock_key}), or it already expired."
            )

    async def save_job_result(self, job_id: str, result: Any, ttl_seconds: int) -> None:
        """Saves the successful result and completion time for a job, sets TTL, and updates status.

        Args:
            job_id: The ID of the job.
            result: The result data to save (will be JSON serialized).
            ttl_seconds: The Time-To-Live in seconds for the job definition hash.
                         0 means persist indefinitely. < 0 means leave existing TTL.
        """
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        completion_time = datetime.now(UTC)

        # Serialize result to JSON string
        try:
            # Use pydantic JSON serialization if available, else standard JSON dump
            if hasattr(result, "model_dump_json"):
                result_str = result.model_dump_json()
            else:
                # Always JSON-encode the result, converting unknown types to strings
                result_str = json.dumps(result, default=str)
        except TypeError as e:
            logger.error(
                f"Failed to serialize result for job {job_id}: {e}", exc_info=True
            )
            result_str = json.dumps(f"<Unserializable Result: {type(result).__name__}>")

        update_data = {
            "result": result_str.encode("utf-8"),
            "completion_time": completion_time.isoformat().encode("utf-8"),
            "status": JobStatus.COMPLETED.value.encode("utf-8"),
        }

        # Use pipeline for atomicity of update + expire
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(job_key, mapping=update_data)
            if ttl_seconds > 0:
                pipe.expire(job_key, ttl_seconds)
            elif ttl_seconds == 0:
                pipe.persist(job_key)
            results = await pipe.execute()
        logger.debug(
            f"Saved result for job {job_id}. Status set to COMPLETED. TTL={ttl_seconds}. Results: {results}"
        )

    async def set_worker_health(
        self, worker_id: str, data: dict[str, Any], ttl_seconds: int
    ) -> None:
        """Sets the health check data (as a JSON string) for a worker with a TTL.

        Args:
            worker_id: The unique ID of the worker.
            data: The health data dictionary to store.
            ttl_seconds: The Time-To-Live for the health key in seconds.
        """
        health_key = f"rrq:health:worker:{worker_id}"
        try:
            payload = json.dumps(data, default=str).encode("utf-8")
            await self.redis.set(health_key, payload, ex=ttl_seconds)
            logger.debug(
                f"Set health data for worker {worker_id} with TTL {ttl_seconds}s."
            )
        except Exception as e:
            logger.error(
                f"Failed to set health data for worker {worker_id}: {e}", exc_info=True
            )

    async def get_worker_health(
        self, worker_id: str
    ) -> tuple[Optional[dict[str, Any]], Optional[int]]:
        """Retrieves the health check data and TTL for a worker.

        Returns:
            A tuple containing:
              - The parsed health data dictionary (or None if key doesn't exist or JSON is invalid).
              - The current TTL of the key in seconds (or None if key doesn't exist or has no TTL).
        """
        health_key = f"rrq:health:worker:{worker_id}"

        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.get(health_key)
            pipe.ttl(health_key)
            results = await pipe.execute()

        payload_bytes: Optional[bytes] = results[0]
        ttl_seconds: int = results[
            1
        ]  # TTL returns -2 if key not found, -1 if no expiry

        if payload_bytes is None:
            logger.debug(f"Health key not found for worker {worker_id}.")
            return None, None  # Key doesn't exist

        health_data = None
        try:
            health_data = json.loads(payload_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.error(
                f"Failed to parse health check JSON for worker {worker_id}",
                exc_info=True,
            )

        # Return TTL as None if it's -1 (no expiry) or -2 (key not found - though handled above)
        final_ttl = ttl_seconds if ttl_seconds >= 0 else None
        logger.debug(
            f"Retrieved health data for worker {worker_id}: TTL={final_ttl}, Data keys={list(health_data.keys()) if health_data else None}"
        )
        return health_data, final_ttl
