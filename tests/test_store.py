import asyncio
import json
import time
import uuid
from datetime import UTC, datetime
from typing import AsyncGenerator

import pytest
import pytest_asyncio

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
        assert retrieved_job.enqueue_time.tzinfo == UTC


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
    completion_time = datetime.now(UTC)

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


@pytest.mark.asyncio
async def test_serialize_deserialize():
    store = JobStore(RRQSettings())
    # simple types
    b = await store._serialize_job_field(123)
    assert b == b"123"
    b2 = await store._serialize_job_field("abc")
    assert b2 == b"abc"
    # complex types
    obj = {"x": 1}
    b3 = await store._serialize_job_field(obj)
    assert json.loads(b3.decode()) == obj
    # deserialize valid JSON
    v = await store._deserialize_job_field(b'{"a":2}')
    assert v == {"a": 2}
    # deserialize non-JSON
    v2 = await store._deserialize_job_field(b"xyz")
    assert v2 == "xyz"


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
                job_id, queue_name, retry_at_score + i, error_message, JobStatus.RETRYING
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
        
        # Verify job was added to queue multiple times (with different scores)
        ready_jobs = await job_store.get_queued_job_ids(queue_name)
        assert ready_jobs.count(job_id) == 3

    @pytest.mark.asyncio
    async def test_complete_job_lifecycle_with_atomic_operations(self, job_store: JobStore):
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
