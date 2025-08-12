"""This module defines the JobStore class, responsible for all interactions
with the Redis backend for storing and managing RRQ job data and queues.
"""

import json
import logging
from datetime import timezone, datetime, timedelta
from typing import Any, Optional

from redis.asyncio import Redis as AsyncRedis
from redis.exceptions import RedisError

from .constants import (
    CONNECTION_POOL_MAX_CONNECTIONS,
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

    Transaction Usage Guidelines:
    - Use transaction=True for write operations that must be atomic (job updates, DLQ moves)
    - Use transaction=False for read-only batch operations (health checks, queue size queries)
    - All async context managers (async with) properly handle cleanup even on exceptions
    """

    def __init__(self, settings: RRQSettings):
        """Initializes the JobStore with a Redis connection.

        Args:
            redis_dsn: The Redis Data Source Name (DSN) string.
        """
        self.settings = settings
        self.redis = AsyncRedis.from_url(
            settings.redis_dsn,
            decode_responses=False,
            max_connections=CONNECTION_POOL_MAX_CONNECTIONS,
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_keepalive_options={},
        )

        # LUA scripts for atomic operations
        self._atomic_lock_and_remove_script = """
        -- KEYS: [1] = lock_key, [2] = queue_key
        -- ARGV: [1] = worker_id, [2] = lock_timeout_ms, [3] = job_id
        local lock_result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
        if lock_result then
            local removed_count = redis.call('ZREM', KEYS[2], ARGV[3])
            if removed_count == 0 then
                redis.call('DEL', KEYS[1])  -- Release lock if job wasn't in queue
                return {0, 0}  -- {lock_acquired, removed_count}
            end
            return {1, removed_count}
        else
            return {0, 0}
        end
        """

        self._atomic_retry_script = """
        -- KEYS: [1] = job_key, [2] = queue_key
        -- ARGV: [1] = job_id, [2] = retry_at_score, [3] = error_message, [4] = status
        local new_retry_count = redis.call('HINCRBY', KEYS[1], 'current_retries', 1)
        redis.call('HMSET', KEYS[1], 'status', ARGV[4], 'last_error', ARGV[3])
        redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
        return new_retry_count
        """

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

    async def get_job_data_dict(self, job_id: str) -> Optional[dict[str, str]]:
        """Retrieves raw job data from Redis as a decoded dictionary.

        This method provides a lightweight way to get job data for CLI commands
        without the overhead of full Job object reconstruction and validation.

        Args:
            job_id: The unique ID of the job to retrieve.

        Returns:
            Dict with decoded string keys and values, or None if job not found.
        """
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        job_data_raw_bytes = await self.redis.hgetall(job_key)

        if not job_data_raw_bytes:
            return None

        # Decode all keys and values from bytes to str
        return {
            k.decode("utf-8"): v.decode("utf-8") for k, v in job_data_raw_bytes.items()
        }

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
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
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

    async def atomic_lock_and_remove_job(
        self, job_id: str, queue_name: str, worker_id: str, lock_timeout_ms: int
    ) -> tuple[bool, int]:
        """Atomically acquires a job lock and removes the job from the queue.

        This is a critical operation that prevents race conditions between multiple
        workers trying to process the same job.

        Args:
            job_id: The ID of the job to lock and remove.
            queue_name: The name of the queue to remove the job from.
            worker_id: The ID of the worker attempting to acquire the lock.
            lock_timeout_ms: The lock timeout/TTL in milliseconds.

        Returns:
            A tuple of (lock_acquired: bool, removed_count: int).
            - lock_acquired: True if the lock was successfully acquired
            - removed_count: Number of jobs removed from the queue (0 or 1)
        """
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        queue_key = self._format_queue_key(queue_name)

        result = await self.redis.eval(
            self._atomic_lock_and_remove_script,
            2,  # Number of keys
            lock_key,
            queue_key,
            worker_id.encode("utf-8"),
            str(lock_timeout_ms),
            job_id.encode("utf-8"),
        )

        lock_acquired = bool(result[0])
        removed_count = int(result[1])

        if lock_acquired and removed_count > 0:
            logger.debug(
                f"Worker {worker_id} atomically acquired lock and removed job {job_id} from queue '{queue_name}'."
            )
        elif not lock_acquired:
            logger.debug(
                f"Worker {worker_id} failed to acquire lock for job {job_id} (already locked by another worker)."
            )
        else:
            logger.warning(
                f"Worker {worker_id} acquired lock for job {job_id} but job was already removed from queue '{queue_name}'."
            )

        return lock_acquired, removed_count

    async def atomic_retry_job(
        self,
        job_id: str,
        queue_name: str,
        retry_at_score: float,
        error_message: str,
        status: JobStatus,
    ) -> int:
        """Atomically increments job retry count, updates status/error, and re-queues the job.

        This prevents race conditions in the retry logic where multiple operations
        need to be performed atomically.

        Args:
            job_id: The ID of the job to retry.
            queue_name: The name of the queue to add the job back to.
            retry_at_score: The score (timestamp) when the job should be retried.
            error_message: The error message to store.
            status: The job status to set (usually RETRYING).

        Returns:
            The new retry count after incrementing.
        """
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        queue_key = self._format_queue_key(queue_name)

        new_retry_count = await self.redis.eval(
            self._atomic_retry_script,
            2,  # Number of keys
            job_key,
            queue_key,
            job_id.encode("utf-8"),
            str(retry_at_score),
            error_message.encode("utf-8"),
            status.value.encode("utf-8"),
        )

        new_count = int(new_retry_count)
        logger.debug(
            f"Atomically incremented retries for job {job_id} to {new_count} and re-queued for retry."
        )
        return new_count

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

        # Use pipeline with transaction=True for atomic write operations
        # This ensures all commands succeed or none do (ACID properties)
        async with self.redis.pipeline(transaction=True) as pipe:
            try:
                pipe.hset(job_key, mapping=update_data)
                pipe.lpush(dlq_redis_key, job_id.encode("utf-8"))
                pipe.expire(job_key, DEFAULT_DLQ_RESULT_TTL_SECONDS)
                results = await pipe.execute()
                logger.info(
                    f"Moved job {job_id} to DLQ '{dlq_redis_key}'. Results: {results}"
                )
            except RedisError as e:
                logger.error(
                    f"Failed to move job {job_id} to DLQ '{dlq_redis_key}': {e}"
                )
                raise

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
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
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
        completion_time = datetime.now(timezone.utc)

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

        # Use pipeline with transaction=True to atomically update and set TTL
        # This prevents partial updates where result is saved but TTL isn't set
        async with self.redis.pipeline(transaction=True) as pipe:
            try:
                pipe.hset(job_key, mapping=update_data)
                if ttl_seconds > 0:
                    pipe.expire(job_key, ttl_seconds)
                elif ttl_seconds == 0:
                    pipe.persist(job_key)
                results = await pipe.execute()
                logger.debug(
                    f"Saved result for job {job_id}. Status set to COMPLETED. TTL={ttl_seconds}. Results: {results}"
                )
            except RedisError as e:
                logger.error(f"Failed to save result for job {job_id}: {e}")
                raise

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

        # Use pipeline with transaction=False for read-only batch operations
        # No atomicity needed as we're only reading, this improves performance
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

    async def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        """Get simplified job data for monitoring/CLI purposes.

        Returns a dictionary with basic job information, or None if job not found.
        This is more lightweight than get_job_definition which returns full Job objects.
        """
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        job_data = await self.redis.hgetall(job_key)

        if not job_data:
            return None

        # Convert bytes to strings and return simplified dict
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in job_data.items()}

    # Hybrid monitoring optimization methods
    async def register_active_queue(self, queue_name: str) -> None:
        """Register a queue as active in the monitoring registry"""
        from .constants import ACTIVE_QUEUES_SET

        timestamp = datetime.now(timezone.utc).timestamp()
        await self.redis.zadd(ACTIVE_QUEUES_SET, {queue_name: timestamp})

    async def register_active_worker(self, worker_id: str) -> None:
        """Register a worker as active in the monitoring registry"""
        from .constants import ACTIVE_WORKERS_SET

        timestamp = datetime.now(timezone.utc).timestamp()
        await self.redis.zadd(ACTIVE_WORKERS_SET, {worker_id: timestamp})

    async def get_active_queues(self, max_age_seconds: int = 300) -> list[str]:
        """Get list of recently active queues"""
        from .constants import ACTIVE_QUEUES_SET

        cutoff_time = datetime.now(timezone.utc).timestamp() - max_age_seconds

        # Remove stale entries and get active ones
        await self.redis.zremrangebyscore(ACTIVE_QUEUES_SET, 0, cutoff_time)
        active_queues = await self.redis.zrange(ACTIVE_QUEUES_SET, 0, -1)

        return [q.decode("utf-8") if isinstance(q, bytes) else q for q in active_queues]

    async def get_active_workers(self, max_age_seconds: int = 60) -> list[str]:
        """Get list of recently active workers"""
        from .constants import ACTIVE_WORKERS_SET

        cutoff_time = datetime.now(timezone.utc).timestamp() - max_age_seconds

        # Remove stale entries and get active ones
        await self.redis.zremrangebyscore(ACTIVE_WORKERS_SET, 0, cutoff_time)
        active_workers = await self.redis.zrange(ACTIVE_WORKERS_SET, 0, -1)

        return [
            w.decode("utf-8") if isinstance(w, bytes) else w for w in active_workers
        ]

    async def publish_monitor_event(self, event_type: str, data: dict) -> None:
        """Publish a monitoring event to the Redis stream"""
        from .constants import MONITOR_EVENTS_STREAM

        event_data = {
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).timestamp(),
            **data,
        }

        # Add to stream with max length to prevent unbounded growth
        await self.redis.xadd(
            MONITOR_EVENTS_STREAM, event_data, maxlen=1000, approximate=True
        )

    async def consume_monitor_events(
        self, last_id: str = "0", count: int = 100, block: int = 50
    ) -> list:
        """Consume monitoring events from Redis stream"""
        from .constants import MONITOR_EVENTS_STREAM

        try:
            events = await self.redis.xread(
                {MONITOR_EVENTS_STREAM: last_id}, count=count, block=block
            )
            return events
        except Exception:
            # Handle timeout or other Redis errors gracefully
            return []

    async def get_lock_ttl(self, unique_key: str) -> int:
        lock_key = f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"
        ttl = await self.redis.ttl(lock_key)
        try:
            ttl_int = int(ttl)
        except (TypeError, ValueError):
            ttl_int = 0
        return ttl_int if ttl_int and ttl_int > 0 else 0

    async def get_last_process_time(self, unique_key: str) -> Optional[datetime]:
        key = f"last_process:{unique_key}"
        timestamp = await self.redis.get(key)
        return datetime.fromtimestamp(float(timestamp), timezone.utc) if timestamp else None

    async def set_last_process_time(self, unique_key: str, timestamp: datetime) -> None:
        key = f"last_process:{unique_key}"
        # Add TTL to auto-expire the marker; independent of app specifics
        ttl_seconds = max(60, int(self.settings.expected_job_ttl) * 2)
        await self.redis.set(key, timestamp.timestamp(), ex=ttl_seconds)

    async def get_unique_lock_holder(self, unique_key: str) -> Optional[str]:
        """Return the job_id currently holding the unique lock, if any."""
        lock_key = f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"
        value = await self.redis.get(lock_key)
        return value.decode("utf-8") if value else None

    async def defer_job(self, job: Job, defer_by: timedelta) -> None:
        target_queue = job.queue_name or self.settings.default_queue_name
        queue_key = self._format_queue_key(target_queue)
        # Use milliseconds since epoch to be consistent with queue scores
        score_ms = int((datetime.now(timezone.utc) + defer_by).timestamp() * 1000)
        await self.redis.zadd(queue_key, {job.id.encode("utf-8"): float(score_ms)})
        # Note: job was already removed from queue during acquisition.

    async def batch_get_queue_sizes(self, queue_names: list[str]) -> dict[str, int]:
        """Efficiently get sizes for multiple queues using pipeline"""
        from .constants import QUEUE_KEY_PREFIX

        if not queue_names:
            return {}

        # Use pipeline with transaction=False for read-only batch operations
        # No atomicity needed as we're only reading, this improves performance
        async with self.redis.pipeline(transaction=False) as pipe:
            for queue_name in queue_names:
                queue_key = f"{QUEUE_KEY_PREFIX}{queue_name}"
                pipe.zcard(queue_key)

            sizes = await pipe.execute()

        return dict(zip(queue_names, sizes))
