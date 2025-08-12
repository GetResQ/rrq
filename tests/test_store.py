import asyncio
import time
import uuid
from datetime import timezone, datetime, timedelta
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from redis.exceptions import ConnectionError, RedisError, ResponseError

from rrq.constants import (
    DEFAULT_DLQ_NAME,
    JOB_KEY_PREFIX,
    LOCK_KEY_PREFIX,
    UNIQUE_JOB_LOCK_PREFIX,
)
from rrq.job import Job, JobStatus
from rrq.settings import RRQSettings
from rrq.store import JobStore


@pytest.fixture(scope="session")
def redis_url_for_store() -> str:
    return "redis://localhost:6379/3"  # DB 3 for store tests


@pytest_asyncio.fixture(scope="function")
async def rrq_settings_for_store(redis_url_for_store: str) -> RRQSettings:
    return RRQSettings(redis_dsn=redis_url_for_store)


@pytest_asyncio.fixture(scope="function")
async def job_store(
    rrq_settings_for_store: RRQSettings,
) -> AsyncGenerator[JobStore, None]:
    """Provides a JobStore instance for store tests, function-scoped."""
    store = JobStore(settings=rrq_settings_for_store)
    if hasattr(store, "redis") and hasattr(store.redis, "flushdb"):
        await store.redis.flushdb()
    yield store
    if hasattr(store, "redis") and hasattr(store.redis, "flushdb"):
        await store.redis.flushdb()
    await store.aclose()


@pytest.mark.asyncio
async def test_save_and_get_job_definition(job_store: JobStore):
    job = Job(
        function_name="test_func", job_args=[1, "arg2"], job_kwargs={"key": "value"}
    )
    job_id = job.id

    await job_store.save_job_definition(job)
    retrieved_job = await job_store.get_job_definition(job_id)

    assert retrieved_job is not None
    assert retrieved_job.id == job_id
    assert retrieved_job.function_name == "test_func"
    assert retrieved_job.job_args == [1, "arg2"]
    assert retrieved_job.job_kwargs == {"key": "value"}
    assert retrieved_job.status == JobStatus.PENDING
    assert isinstance(retrieved_job.enqueue_time, datetime)
    if retrieved_job.enqueue_time.tzinfo is not None:
        assert retrieved_job.enqueue_time.tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_get_non_existent_job_definition(job_store: JobStore):
    retrieved_job = await job_store.get_job_definition("non_existent_job_id")
    assert retrieved_job is None


@pytest.mark.asyncio
async def test_add_job_to_queue_and_get(job_store: JobStore):
    queue_name = "test_queue_actual_store"
    job1 = Job(function_name="func1")
    job2 = Job(function_name="func2")

    await job_store.save_job_definition(job1)
    await job_store.save_job_definition(job2)

    score_1 = time.time()
    score_2 = time.time() + 0.01

    await job_store.add_job_to_queue(queue_name, job1.id, score_1)
    await job_store.add_job_to_queue(queue_name, job2.id, score_2)

    queued_jobs_ids = await job_store.get_queued_job_ids(queue_name)
    assert len(queued_jobs_ids) == 2
    assert job1.id in queued_jobs_ids
    assert job2.id in queued_jobs_ids
    assert queued_jobs_ids[0] == job1.id
    assert queued_jobs_ids[1] == job2.id


@pytest.mark.asyncio
async def test_acquire_and_release_job_lock(job_store: JobStore):
    job_id = f"lock_test_job_{uuid.uuid4()}"
    worker_id_1 = "worker_1"
    worker_id_2 = "worker_2"
    lock_timeout_ms = 1000

    acquired_1 = await job_store.acquire_job_lock(job_id, worker_id_1, lock_timeout_ms)
    assert acquired_1 is True

    acquired_2 = await job_store.acquire_job_lock(job_id, worker_id_2, lock_timeout_ms)
    assert acquired_2 is False

    lock_value_bytes = await job_store.redis.get(f"{LOCK_KEY_PREFIX}{job_id}")
    assert lock_value_bytes is not None
    assert lock_value_bytes.decode("utf-8") == worker_id_1

    await job_store.release_job_lock(job_id)

    lock_value_after_release = await job_store.redis.get(f"{LOCK_KEY_PREFIX}{job_id}")
    assert lock_value_after_release is None

    acquired_3 = await job_store.acquire_job_lock(job_id, worker_id_2, lock_timeout_ms)
    assert acquired_3 is True


@pytest.mark.asyncio
async def test_job_lock_expires(job_store: JobStore):
    job_id = f"lock_expiry_test_job_{uuid.uuid4()}"
    worker_id = "worker_expiry"
    lock_timeout_ms = 100

    acquired = await job_store.acquire_job_lock(job_id, worker_id, lock_timeout_ms)
    assert acquired is True

    await asyncio.sleep((lock_timeout_ms / 1000) + 0.05)

    another_worker_id = "worker_new"
    acquired_again = await job_store.acquire_job_lock(
        job_id, another_worker_id, lock_timeout_ms
    )
    assert acquired_again is True, (
        "Lock should have expired and be acquirable by another worker"
    )


@pytest.mark.asyncio
async def test_update_job_status(job_store: JobStore):
    job = Job(function_name="status_test_func")
    await job_store.save_job_definition(job)

    await job_store.update_job_status(job.id, JobStatus.ACTIVE)
    retrieved_job = await job_store.get_job_definition(job.id)
    assert retrieved_job is not None
    assert retrieved_job.status == JobStatus.ACTIVE

    await job_store.update_job_status(job.id, JobStatus.COMPLETED)
    retrieved_job_completed = await job_store.get_job_definition(job.id)
    assert retrieved_job_completed is not None
    assert retrieved_job_completed.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_increment_job_retries(job_store: JobStore):
    job = Job(function_name="retry_test_func")
    await job_store.save_job_definition(job)

    initial_retries = job.current_retries

    new_retry_count = await job_store.increment_job_retries(job.id)
    assert new_retry_count == initial_retries + 1
    retrieved_job = await job_store.get_job_definition(job.id)
    assert retrieved_job is not None
    assert retrieved_job.current_retries == initial_retries + 1

    new_retry_count_2 = await job_store.increment_job_retries(job.id)
    assert new_retry_count_2 == initial_retries + 2
    retrieved_job_2 = await job_store.get_job_definition(job.id)
    assert retrieved_job_2 is not None
    assert retrieved_job_2.current_retries == initial_retries + 2


@pytest.mark.asyncio
async def test_remove_job_from_queue(job_store: JobStore):
    queue_name = "removal_queue"
    job1 = Job(function_name="remove_func1")
    job2 = Job(function_name="remove_func2")

    await job_store.save_job_definition(job1)
    await job_store.save_job_definition(job2)

    await job_store.add_job_to_queue(queue_name, job1.id, time.time())
    await job_store.add_job_to_queue(queue_name, job2.id, time.time() + 0.01)

    queued_ids_before = await job_store.get_queued_job_ids(queue_name)
    assert job1.id in queued_ids_before
    assert job2.id in queued_ids_before
    assert len(queued_ids_before) == 2

    # Remove job1
    removed_count = await job_store.remove_job_from_queue(queue_name, job1.id)
    assert removed_count == 1  # ZREM returns number of elements removed

    queued_ids_after_remove1 = await job_store.get_queued_job_ids(queue_name)
    assert job1.id not in queued_ids_after_remove1
    assert job2.id in queued_ids_after_remove1
    assert len(queued_ids_after_remove1) == 1

    # Try removing job1 again (should remove 0)
    removed_count_again = await job_store.remove_job_from_queue(queue_name, job1.id)
    assert removed_count_again == 0

    # Remove job2
    removed_count_job2 = await job_store.remove_job_from_queue(queue_name, job2.id)
    assert removed_count_job2 == 1

    queued_ids_final = await job_store.get_queued_job_ids(queue_name)
    assert len(queued_ids_final) == 0


@pytest.mark.asyncio
async def test_save_and_get_job_result(job_store: JobStore):
    job = Job(function_name="result_test_func")
    await job_store.save_job_definition(job)

    result_data = {"output": "success", "value": 123}
    ttl_seconds = 60  # Example TTL

    await job_store.save_job_result(job.id, result_data, ttl_seconds)

    # Verify result field is updated in the job hash
    retrieved_job = await job_store.get_job_definition(job.id)
    assert retrieved_job is not None
    assert retrieved_job.result == result_data
    # Pydantic should deserialize the JSON string back to dict

    # Verify TTL is set on the main job key (simplest approach for now)
    actual_ttl = await job_store.redis.ttl(f"{JOB_KEY_PREFIX}{job.id}")
    assert actual_ttl > 0
    assert actual_ttl <= ttl_seconds
    # Note: Redis TTL is not exact, so check <= requested TTL


@pytest.mark.asyncio
async def test_save_job_result_no_ttl(job_store: JobStore):
    job_details = Job(function_name="result_no_ttl_func")
    await job_store.save_job_definition(job_details)

    result_data = "simple_result"
    # ttl_seconds = 0 or None means persist (or rely on default if set elsewhere)
    await job_store.save_job_result(job_details.id, result_data, ttl_seconds=0)

    retrieved_job_definition = await job_store.get_job_definition(job_details.id)
    assert retrieved_job_definition is not None
    assert retrieved_job_definition.result == result_data
    # Additionally, we might want to check that no TTL was set on the job key if that's the intent.
    # This would require checking Redis directly for TTL, e.g., await job_store.redis.ttl(f"{JOB_KEY_PREFIX}{job_details.id}")
    # For now, just checking the result is retrieved is the primary goal.


@pytest.mark.asyncio
async def test_move_job_to_dlq(job_store: JobStore):
    job = Job(function_name="dlq_test_func")
    await job_store.save_job_definition(job)

    dlq_name_to_use = DEFAULT_DLQ_NAME  # Use default from constants
    error_message = "Max retries exceeded"
    completion_time = datetime.now(timezone.utc)

    await job_store.move_job_to_dlq(
        job.id, dlq_name_to_use, error_message, completion_time
    )

    # Verify job status and error are updated
    retrieved_job = await job_store.get_job_definition(job.id)
    assert retrieved_job is not None
    assert retrieved_job.status == JobStatus.FAILED
    assert retrieved_job.last_error == error_message
    assert retrieved_job.completion_time is not None
    # Pydantic v2 should handle the ISO string parsing back to datetime
    assert isinstance(retrieved_job.completion_time, datetime)
    # Approximate check as direct comparison can have microsecond differences
    assert abs((retrieved_job.completion_time - completion_time).total_seconds()) < 1

    # Verify job ID is in the DLQ list
    dlq_key = dlq_name_to_use  # DLQ uses its own prefix
    dlq_content_bytes = await job_store.redis.lrange(dlq_key, 0, -1)
    dlq_content = [item.decode("utf-8") for item in dlq_content_bytes]
    assert job.id in dlq_content


@pytest.mark.asyncio
async def test_acquire_and_release_unique_job_lock(job_store: JobStore):
    unique_key = f"store_unique_lock_test_{uuid.uuid4()}"
    job_id_1 = "job_for_unique_lock_1"
    job_id_2 = "job_for_unique_lock_2"
    lock_ttl_seconds = 60  # Standard TTL for testing, expiry tested separately
    redis_lock_key = f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"

    # 1. Acquire lock for job1
    acquired_1 = await job_store.acquire_unique_job_lock(
        unique_key, job_id_1, lock_ttl_seconds
    )
    assert acquired_1 is True

    # Check Redis directly
    lock_value_bytes_1 = await job_store.redis.get(redis_lock_key)
    assert lock_value_bytes_1 is not None
    assert lock_value_bytes_1.decode("utf-8") == job_id_1

    # 2. Attempt to acquire with job2 - should fail
    acquired_2 = await job_store.acquire_unique_job_lock(
        unique_key, job_id_2, lock_ttl_seconds
    )
    assert acquired_2 is False

    # 3. Release lock
    await job_store.release_unique_job_lock(unique_key)

    # Check Redis directly - lock should be gone
    lock_value_after_release = await job_store.redis.get(redis_lock_key)
    assert lock_value_after_release is None

    # 4. Attempt to acquire with job2 again - should succeed
    acquired_3 = await job_store.acquire_unique_job_lock(
        unique_key, job_id_2, lock_ttl_seconds
    )
    assert acquired_3 is True
    lock_value_bytes_3 = await job_store.redis.get(redis_lock_key)
    assert lock_value_bytes_3 is not None
    assert lock_value_bytes_3.decode("utf-8") == job_id_2

    # Cleanup by releasing
    await job_store.release_unique_job_lock(unique_key)


@pytest.mark.asyncio
async def test_acquire_unique_job_lock_expires(job_store: JobStore):
    unique_key = f"store_unique_expiry_test_{uuid.uuid4()}"
    job_id_1 = "job_for_expiry_1"
    job_id_2 = "job_for_expiry_2"
    lock_ttl_seconds = 1  # Short TTL for expiry test
    redis_lock_key = f"{UNIQUE_JOB_LOCK_PREFIX}{unique_key}"

    # 1. Acquire lock with short TTL
    acquired_1 = await job_store.acquire_unique_job_lock(
        unique_key, job_id_1, lock_ttl_seconds
    )
    assert acquired_1 is True

    # Wait for lock to expire
    await asyncio.sleep(lock_ttl_seconds + 0.1)  # Wait a bit longer than TTL

    # 2. Attempt to acquire with another job ID - should succeed as lock expired
    acquired_2 = await job_store.acquire_unique_job_lock(
        unique_key, job_id_2, lock_ttl_seconds
    )
    assert acquired_2 is True, (
        "Lock should have expired and be acquirable by another job_id"
    )

    lock_value_bytes_2 = await job_store.redis.get(redis_lock_key)
    assert lock_value_bytes_2 is not None
    assert lock_value_bytes_2.decode("utf-8") == job_id_2

    # Cleanup
    await job_store.release_unique_job_lock(unique_key)


def test_format_keys():
    store = JobStore(RRQSettings())
    # queue key formatting
    assert store._format_queue_key("foo") == "rrq:queue:foo"
    already = "rrq:queue:bar"
    assert store._format_queue_key(already) == already
    # dlq key formatting
    from rrq.constants import DLQ_KEY_PREFIX

    assert store._format_dlq_key("baz") == f"{DLQ_KEY_PREFIX}baz"
    full = f"{DLQ_KEY_PREFIX}qux"
    assert store._format_dlq_key(full) == full


# --- Atomic Operations Tests ---


class TestAtomicOperations:
    """Test atomic LUA script operations in JobStore."""

    @pytest.mark.asyncio
    async def test_atomic_lock_and_remove_success(self, job_store: JobStore):
        """Test successful atomic lock and remove."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_id = "test_worker_1"
        lock_timeout_ms = 5000

        # Create a job and add it to the queue
        job = Job(id=job_id, function_name="test_func")
        await job_store.save_job_definition(job)

        # Add job to queue
        current_time_ms = int(time.time() * 1000)
        await job_store.add_job_to_queue(queue_name, job_id, current_time_ms)

        # Verify job is in queue
        ready_jobs = await job_store.get_ready_job_ids(queue_name, 10)
        assert job_id in ready_jobs

        # Test atomic lock and remove
        lock_acquired, removed_count = await job_store.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id, lock_timeout_ms
        )

        assert lock_acquired is True
        assert removed_count == 1

        # Verify job is no longer in queue
        ready_jobs_after = await job_store.get_ready_job_ids(queue_name, 10)
        assert job_id not in ready_jobs_after

        # Verify lock exists
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        lock_owner = await job_store.redis.get(lock_key)
        assert lock_owner.decode("utf-8") == worker_id

    @pytest.mark.asyncio
    async def test_atomic_lock_and_remove_already_locked(self, job_store: JobStore):
        """Test atomic lock and remove when job is already locked."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_id_1 = "test_worker_1"
        worker_id_2 = "test_worker_2"
        lock_timeout_ms = 5000

        # Create a job and add it to the queue
        job = Job(id=job_id, function_name="test_func")
        await job_store.save_job_definition(job)

        # Add job to queue
        current_time_ms = int(time.time() * 1000)
        await job_store.add_job_to_queue(queue_name, job_id, current_time_ms)

        # First worker acquires lock
        lock_acquired_1, removed_count_1 = await job_store.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id_1, lock_timeout_ms
        )

        assert lock_acquired_1 is True
        assert removed_count_1 == 1

        # Second worker tries to acquire lock
        lock_acquired_2, removed_count_2 = await job_store.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id_2, lock_timeout_ms
        )

        assert lock_acquired_2 is False
        assert removed_count_2 == 0

        # Verify first worker still owns the lock
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        lock_owner = await job_store.redis.get(lock_key)
        assert lock_owner.decode("utf-8") == worker_id_1

    @pytest.mark.asyncio
    async def test_atomic_lock_and_remove_job_not_in_queue(self, job_store: JobStore):
        """Test atomic lock and remove when job is not in queue."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_id = "test_worker"
        lock_timeout_ms = 5000

        # Create a job but don't add it to the queue
        job = Job(id=job_id, function_name="test_func")
        await job_store.save_job_definition(job)

        # Test atomic lock and remove
        lock_acquired, removed_count = await job_store.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id, lock_timeout_ms
        )

        assert lock_acquired is False
        assert removed_count == 0

        # Verify no lock was created
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        lock_owner = await job_store.redis.get(lock_key)
        assert lock_owner is None

    @pytest.mark.asyncio
    async def test_atomic_lock_and_remove_concurrent_workers(self, job_store: JobStore):
        """Test atomic lock and remove with concurrent workers."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_ids = [f"worker_{i}" for i in range(5)]
        lock_timeout_ms = 5000

        # Create a job and add it to the queue
        job = Job(id=job_id, function_name="test_func")
        await job_store.save_job_definition(job)

        # Add job to queue
        current_time_ms = int(time.time() * 1000)
        await job_store.add_job_to_queue(queue_name, job_id, current_time_ms)

        # All workers try to acquire lock concurrently
        tasks = [
            job_store.atomic_lock_and_remove_job(
                job_id, queue_name, worker_id, lock_timeout_ms
            )
            for worker_id in worker_ids
        ]

        results = await asyncio.gather(*tasks)

        # Exactly one worker should succeed
        successful_results = [r for r in results if r[0]]  # lock_acquired is True
        assert len(successful_results) == 1
        assert successful_results[0][1] == 1  # removed_count is 1

        # All other workers should fail
        failed_results = [r for r in results if not r[0]]  # lock_acquired is False
        assert len(failed_results) == 4
        for result in failed_results:
            assert result[1] == 0  # removed_count is 0

    @pytest.mark.asyncio
    async def test_atomic_retry_job_success(self, job_store: JobStore):
        """Test successful atomic retry job operation."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        error_message = "Test error message"
        retry_at_score = time.time() * 1000 + 5000  # 5 seconds from now

        # Create a job
        job = Job(id=job_id, function_name="test_func", current_retries=0)
        await job_store.save_job_definition(job)

        # Test atomic retry
        new_retry_count = await job_store.atomic_retry_job(
            job_id, queue_name, retry_at_score, error_message, JobStatus.RETRYING
        )

        assert new_retry_count == 1

        # Verify job was added to queue
        ready_jobs = await job_store.get_queued_job_ids(queue_name)
        assert job_id in ready_jobs

        # Verify job hash was updated
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        job_data = await job_store.redis.hgetall(job_key)

        assert job_data[b"current_retries"] == b"1"
        assert job_data[b"status"] == JobStatus.RETRYING.value.encode("utf-8")
        assert job_data[b"last_error"] == error_message.encode("utf-8")

    @pytest.mark.asyncio
    async def test_atomic_retry_job_concurrent_retries(self, job_store: JobStore):
        """Test atomic retry with concurrent retry attempts."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        error_message = "Test error message"
        retry_at_score = time.time() * 1000 + 5000

        # Create a job
        job = Job(id=job_id, function_name="test_func", current_retries=0)
        await job_store.save_job_definition(job)

        # Multiple concurrent retry attempts
        tasks = [
            job_store.atomic_retry_job(
                job_id,
                queue_name,
                retry_at_score + i,
                error_message,
                JobStatus.RETRYING,
            )
            for i in range(3)
        ]

        results = await asyncio.gather(*tasks)

        # Results should be consecutive increments
        assert sorted(results) == [1, 2, 3]

        # Verify final retry count
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        job_data = await job_store.redis.hgetall(job_key)
        assert job_data[b"current_retries"] == b"3"

        # Verify job was added to queue (only once, with the latest score)
        # Note: Redis sorted sets only allow one entry per member, so multiple retries
        # with different scores will update the existing entry, not create duplicates
        ready_jobs = await job_store.get_queued_job_ids(queue_name)
        assert ready_jobs.count(job_id) == 1

    @pytest.mark.asyncio
    async def test_complete_job_lifecycle_with_atomic_operations(
        self, job_store: JobStore
    ):
        """Test complete job lifecycle using atomic operations."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_id = "test_worker"
        lock_timeout_ms = 5000

        # Create and enqueue job
        job = Job(id=job_id, function_name="test_func")
        await job_store.save_job_definition(job)

        current_time_ms = int(time.time() * 1000)
        await job_store.add_job_to_queue(queue_name, job_id, current_time_ms)

        # Worker picks up job atomically
        lock_acquired, removed_count = await job_store.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id, lock_timeout_ms
        )

        assert lock_acquired is True
        assert removed_count == 1

        # Job fails and needs retry
        error_message = "Simulated failure"
        retry_at_score = time.time() * 1000 + 5000

        new_retry_count = await job_store.atomic_retry_job(
            job_id, queue_name, retry_at_score, error_message, JobStatus.RETRYING
        )

        assert new_retry_count == 1

        # Release the processing lock
        await job_store.release_job_lock(job_id)

        # Verify job is back in queue for retry
        ready_jobs = await job_store.get_queued_job_ids(queue_name)
        assert job_id in ready_jobs

        # Verify job state
        retrieved_job = await job_store.get_job_definition(job_id)
        assert retrieved_job is not None
        assert retrieved_job.current_retries == 1
        assert retrieved_job.status == JobStatus.RETRYING
        assert retrieved_job.last_error == error_message


# --- Pipeline Exception Handling Tests ---


class TestPipelineExceptionHandling:
    """Test exception handling in Redis pipeline operations."""

    @pytest.mark.asyncio
    async def test_move_to_dlq_pipeline_exception_cleanup(self, job_store: JobStore):
        """Test that pipeline resources are cleaned up even when exceptions occur."""
        job = Job(function_name="test_dlq_exception")
        await job_store.save_job_definition(job)

        # Mock pipeline to raise exception during execute
        with patch.object(job_store.redis, "pipeline") as mock_pipeline_factory:
            mock_pipe = AsyncMock()
            mock_pipe.__aenter__.return_value = mock_pipe
            mock_pipe.__aexit__.return_value = None
            mock_pipe.hset = MagicMock()
            mock_pipe.lpush = MagicMock()
            mock_pipe.expire = MagicMock()
            mock_pipe.execute = AsyncMock(side_effect=RedisError("Pipeline failed"))
            mock_pipeline_factory.return_value = mock_pipe

            # Call should raise the exception
            with pytest.raises(RedisError, match="Pipeline failed"):
                await job_store.move_job_to_dlq(
                    job.id, "test_dlq", "Test error", datetime.now(timezone.utc)
                )

            # Verify cleanup was called
            mock_pipe.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_result_pipeline_exception_cleanup(self, job_store: JobStore):
        """Test that save_job_result pipeline cleans up on exception."""
        job = Job(function_name="test_result_exception")
        await job_store.save_job_definition(job)

        # Mock pipeline to raise exception during execute
        with patch.object(job_store.redis, "pipeline") as mock_pipeline_factory:
            mock_pipe = AsyncMock()
            mock_pipe.__aenter__.return_value = mock_pipe
            mock_pipe.__aexit__.return_value = None
            mock_pipe.hset = MagicMock()
            mock_pipe.expire = MagicMock()
            mock_pipe.persist = MagicMock()
            mock_pipe.execute = AsyncMock(side_effect=RedisError("Save failed"))
            mock_pipeline_factory.return_value = mock_pipe

            # Call should raise the exception
            with pytest.raises(RedisError, match="Save failed"):
                await job_store.save_job_result(job.id, {"result": "data"}, 60)

            # Verify cleanup was called
            mock_pipe.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_pipeline_connection_error_handling(self, job_store: JobStore):
        """Test handling of connection errors in pipeline operations."""
        job = Job(function_name="test_connection_error")
        await job_store.save_job_definition(job)

        with patch.object(job_store.redis, "pipeline") as mock_pipeline_factory:
            mock_pipe = AsyncMock()
            mock_pipe.__aenter__.return_value = mock_pipe
            mock_pipe.__aexit__.return_value = None
            mock_pipe.hset = MagicMock()
            mock_pipe.lpush = MagicMock()
            mock_pipe.expire = MagicMock()
            mock_pipe.execute = AsyncMock(
                side_effect=ConnectionError("Connection lost")
            )
            mock_pipeline_factory.return_value = mock_pipe

            with pytest.raises(ConnectionError, match="Connection lost"):
                await job_store.move_job_to_dlq(
                    job.id, "test_dlq", "Test error", datetime.now(timezone.utc)
                )

            # Verify cleanup was called even for connection errors
            mock_pipe.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_pipeline_partial_execution_rollback(self, job_store: JobStore):
        """Test that transactional pipelines rollback on failure."""
        job = Job(function_name="test_rollback")
        await job_store.save_job_definition(job)

        # Get initial job state
        initial_job = await job_store.get_job_definition(job.id)
        assert initial_job.status == JobStatus.PENDING

        # Mock pipeline to fail after partial execution
        with patch.object(job_store.redis, "pipeline") as mock_pipeline_factory:
            mock_pipe = AsyncMock()
            mock_pipe.__aenter__.return_value = mock_pipe
            mock_pipe.__aexit__.return_value = None
            mock_pipe.hset = MagicMock()
            mock_pipe.lpush = MagicMock()
            mock_pipe.expire = MagicMock()
            # Simulate partial execution failure
            mock_pipe.execute = AsyncMock(
                side_effect=ResponseError("Transaction aborted")
            )
            mock_pipeline_factory.return_value = mock_pipe

            with pytest.raises(ResponseError):
                await job_store.move_job_to_dlq(
                    job.id, "test_dlq", "Test error", datetime.now(timezone.utc)
                )

        # Verify job state hasn't changed (transaction rollback)
        final_job = await job_store.get_job_definition(job.id)
        assert final_job.status == JobStatus.PENDING  # Should remain unchanged

    @pytest.mark.asyncio
    async def test_concurrent_pipeline_exception_handling(self, job_store: JobStore):
        """Test multiple concurrent pipelines with exceptions."""
        jobs = [Job(function_name=f"test_concurrent_{i}") for i in range(5)]
        for job in jobs:
            await job_store.save_job_definition(job)

        # Create mix of successful and failing operations
        async def move_with_random_failure(job: Job, index: int):
            if index % 2 == 0:
                # Mock failure for even indices
                with patch.object(job_store.redis, "pipeline") as mock_pipeline_factory:
                    mock_pipe = AsyncMock()
                    mock_pipe.__aenter__.return_value = mock_pipe
                    mock_pipe.__aexit__.return_value = None
                    mock_pipe.hset = MagicMock()
                    mock_pipe.lpush = MagicMock()
                    mock_pipe.expire = MagicMock()
                    mock_pipe.execute = AsyncMock(
                        side_effect=RedisError(f"Failed {index}")
                    )
                    mock_pipeline_factory.return_value = mock_pipe

                    with pytest.raises(RedisError):
                        await job_store.move_job_to_dlq(
                            job.id, "test_dlq", f"Error {index}", datetime.now(timezone.utc)
                        )
                    return f"failed_{index}"
            else:
                # Real operation for odd indices
                await job_store.move_job_to_dlq(
                    job.id, "test_dlq", f"Error {index}", datetime.now(timezone.utc)
                )
                return f"success_{index}"

        # Run concurrently
        tasks = [move_with_random_failure(job, i) for i, job in enumerate(jobs)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify mix of successes and failures
        success_count = sum(
            1 for r in results if isinstance(r, str) and r.startswith("success")
        )
        failure_count = sum(
            1 for r in results if isinstance(r, str) and r.startswith("failed")
        )

        assert success_count == 2  # Odd indices succeed (1, 3)
        assert failure_count == 3  # Even indices fail (0, 2, 4)

    @pytest.mark.asyncio
    async def test_pipeline_exception_with_ttl_operations(self, job_store: JobStore):
        """Test exception handling in save_job_result with different TTL paths."""
        job = Job(function_name="test_ttl_exception")
        await job_store.save_job_definition(job)

        # Test with TTL > 0 (expire path)
        with patch.object(job_store.redis, "pipeline") as mock_pipeline_factory:
            mock_pipe = AsyncMock()
            mock_pipe.__aenter__.return_value = mock_pipe
            mock_pipe.__aexit__.return_value = None
            mock_pipe.hset = MagicMock()
            mock_pipe.expire = MagicMock()
            # Fail on expire operation
            mock_pipe.execute = AsyncMock(side_effect=RedisError("Expire failed"))
            mock_pipeline_factory.return_value = mock_pipe

            with pytest.raises(RedisError, match="Expire failed"):
                await job_store.save_job_result(
                    job.id, {"result": "data"}, ttl_seconds=60
                )

            mock_pipe.__aexit__.assert_called_once()

        # Test with TTL = 0 (persist path)
        with patch.object(job_store.redis, "pipeline") as mock_pipeline_factory:
            mock_pipe = AsyncMock()
            mock_pipe.__aenter__.return_value = mock_pipe
            mock_pipe.__aexit__.return_value = None
            mock_pipe.hset = MagicMock()
            mock_pipe.persist = MagicMock()
            # Fail on persist operation
            mock_pipe.execute = AsyncMock(side_effect=RedisError("Persist failed"))
            mock_pipeline_factory.return_value = mock_pipe

            with pytest.raises(RedisError, match="Persist failed"):
                await job_store.save_job_result(
                    job.id, {"result": "data"}, ttl_seconds=0
                )

            mock_pipe.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_pipeline_logging_on_exception(self, job_store: JobStore, caplog):
        """Test that exceptions are properly logged with context."""
        job = Job(function_name="test_logging")
        await job_store.save_job_definition(job)

        # Test move_job_to_dlq logging
        with patch.object(job_store.redis, "pipeline") as mock_pipeline_factory:
            mock_pipe = AsyncMock()
            mock_pipe.__aenter__.return_value = mock_pipe
            mock_pipe.__aexit__.return_value = None
            mock_pipe.hset = MagicMock()
            mock_pipe.lpush = MagicMock()
            mock_pipe.expire = MagicMock()
            mock_pipe.execute = AsyncMock(
                side_effect=RedisError("DLQ operation failed")
            )
            mock_pipeline_factory.return_value = mock_pipe

            with pytest.raises(RedisError):
                await job_store.move_job_to_dlq(
                    job.id, "test_dlq", "Test error", datetime.now(timezone.utc)
                )

            # Check that error was logged with job context
            # The DLQ key will be formatted with prefix
            assert (
                f"Failed to move job {job.id} to DLQ 'rrq:dlq:test_dlq'" in caplog.text
            )
            assert "DLQ operation failed" in caplog.text

        caplog.clear()

        # Test save_job_result logging
        with patch.object(job_store.redis, "pipeline") as mock_pipeline_factory:
            mock_pipe = AsyncMock()
            mock_pipe.__aenter__.return_value = mock_pipe
            mock_pipe.__aexit__.return_value = None
            mock_pipe.hset = MagicMock()
            mock_pipe.expire = MagicMock()
            mock_pipe.execute = AsyncMock(side_effect=RedisError("Result save failed"))
            mock_pipeline_factory.return_value = mock_pipe

            with pytest.raises(RedisError):
                await job_store.save_job_result(job.id, {"result": "data"}, 60)

            # Check that error was logged with job context
            assert f"Failed to save result for job {job.id}" in caplog.text
            assert "Result save failed" in caplog.text

    @pytest.mark.asyncio
    async def test_real_pipeline_transaction_behavior(self, job_store: JobStore):
        """Test real Redis pipeline behavior with transactions."""
        job = Job(function_name="test_real_transaction")
        await job_store.save_job_definition(job)

        # Verify job is in initial state
        initial_job = await job_store.get_job_definition(job.id)
        assert initial_job.status == JobStatus.PENDING
        assert initial_job.last_error is None

        # Successfully move to DLQ
        completion_time = datetime.now(timezone.utc)
        await job_store.move_job_to_dlq(
            job.id, "test_dlq", "Test error message", completion_time
        )

        # Verify all changes were applied atomically
        updated_job = await job_store.get_job_definition(job.id)
        assert updated_job.status == JobStatus.FAILED
        assert updated_job.last_error == "Test error message"
        assert updated_job.completion_time is not None

        # Verify job is in DLQ (use formatted key)
        dlq_key = "rrq:dlq:test_dlq"
        dlq_jobs = await job_store.redis.lrange(dlq_key, 0, -1)
        assert job.id.encode("utf-8") in dlq_jobs

@pytest.mark.asyncio
async def test_get_lock_ttl(job_store: JobStore):
    unique_key = "ttl_test"
    ttl = 5
    await job_store.acquire_unique_job_lock(unique_key, "job1", ttl)
    remaining = await job_store.get_lock_ttl(unique_key)
    assert 0 < remaining <= ttl

@pytest.mark.asyncio
async def test_get_set_last_process_time(job_store: JobStore):
    unique_key = "process_time_test"
    ts = datetime.now(timezone.utc)
    await job_store.set_last_process_time(unique_key, ts)
    retrieved = await job_store.get_last_process_time(unique_key)
    assert retrieved == ts

@pytest.mark.asyncio
async def test_defer_job(job_store: JobStore):
    job = Job(function_name="defer_test", queue_name="test_queue")
    await job_store.save_job_definition(job)
    defer_by = timedelta(seconds=10)
    await job_store.defer_job(job, defer_by)
    score = await job_store.redis.zscore("rrq:queue:test_queue", job.id.encode("utf-8"))
    expected_ms = int((datetime.now(timezone.utc) + defer_by).timestamp() * 1000)
    assert abs(score - expected_ms) < 1500  # allow ~1.5s leeway
