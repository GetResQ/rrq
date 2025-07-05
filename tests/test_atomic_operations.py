"""Tests for atomic LUA script operations in JobStore"""

import asyncio
import time
import uuid
from datetime import UTC, datetime

import pytest

from rrq.constants import JOB_KEY_PREFIX, LOCK_KEY_PREFIX
from rrq.job import Job, JobStatus
from rrq.settings import RRQSettings
from rrq.store import JobStore


@pytest.fixture(scope="session")
def redis_url_for_atomic_tests() -> str:
    return "redis://localhost:6379/4"  # Use a different DB for atomic tests


@pytest.fixture(scope="function")
async def job_store_atomic(redis_url_for_atomic_tests: str):
    """Provides a JobStore instance for atomic operation tests."""
    settings = RRQSettings(redis_dsn=redis_url_for_atomic_tests)
    store = JobStore(settings=settings)
    
    # Clean up before and after
    await store.redis.flushdb()
    
    yield store
    
    await store.redis.flushdb()
    await store.aclose()


class TestAtomicLockAndRemove:
    """Test atomic lock and remove operations."""
    
    @pytest.mark.asyncio
    async def test_atomic_lock_and_remove_success(self, job_store_atomic: JobStore):
        """Test successful atomic lock and remove."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_id = "test_worker_1"
        lock_timeout_ms = 5000
        
        # Create a job and add it to the queue
        job = Job(id=job_id, function_name="test_func")
        await job_store_atomic.save_job_definition(job)
        
        # Add job to queue
        current_time_ms = int(time.time() * 1000)
        await job_store_atomic.add_job_to_queue(queue_name, job_id, current_time_ms)
        
        # Verify job is in queue
        ready_jobs = await job_store_atomic.get_ready_job_ids(queue_name, 10)
        assert job_id in ready_jobs
        
        # Test atomic lock and remove
        lock_acquired, removed_count = await job_store_atomic.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id, lock_timeout_ms
        )
        
        assert lock_acquired is True
        assert removed_count == 1
        
        # Verify job is no longer in queue
        ready_jobs_after = await job_store_atomic.get_ready_job_ids(queue_name, 10)
        assert job_id not in ready_jobs_after
        
        # Verify lock exists
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        lock_owner = await job_store_atomic.redis.get(lock_key)
        assert lock_owner.decode("utf-8") == worker_id
    
    @pytest.mark.asyncio
    async def test_atomic_lock_and_remove_already_locked(self, job_store_atomic: JobStore):
        """Test atomic lock and remove when job is already locked."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_id_1 = "test_worker_1"
        worker_id_2 = "test_worker_2"
        lock_timeout_ms = 5000
        
        # Create a job and add it to the queue
        job = Job(id=job_id, function_name="test_func")
        await job_store_atomic.save_job_definition(job)
        
        # Add job to queue
        current_time_ms = int(time.time() * 1000)
        await job_store_atomic.add_job_to_queue(queue_name, job_id, current_time_ms)
        
        # First worker acquires lock
        lock_acquired_1, removed_count_1 = await job_store_atomic.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id_1, lock_timeout_ms
        )
        
        assert lock_acquired_1 is True
        assert removed_count_1 == 1
        
        # Second worker tries to acquire lock
        lock_acquired_2, removed_count_2 = await job_store_atomic.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id_2, lock_timeout_ms
        )
        
        assert lock_acquired_2 is False
        assert removed_count_2 == 0
        
        # Verify first worker still owns the lock
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        lock_owner = await job_store_atomic.redis.get(lock_key)
        assert lock_owner.decode("utf-8") == worker_id_1
    
    @pytest.mark.asyncio
    async def test_atomic_lock_and_remove_job_not_in_queue(self, job_store_atomic: JobStore):
        """Test atomic lock and remove when job is not in queue."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_id = "test_worker"
        lock_timeout_ms = 5000
        
        # Create a job but don't add it to the queue
        job = Job(id=job_id, function_name="test_func")
        await job_store_atomic.save_job_definition(job)
        
        # Test atomic lock and remove
        lock_acquired, removed_count = await job_store_atomic.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id, lock_timeout_ms
        )
        
        assert lock_acquired is False
        assert removed_count == 0
        
        # Verify no lock was created
        lock_key = f"{LOCK_KEY_PREFIX}{job_id}"
        lock_owner = await job_store_atomic.redis.get(lock_key)
        assert lock_owner is None
    
    @pytest.mark.asyncio
    async def test_atomic_lock_and_remove_concurrent_workers(self, job_store_atomic: JobStore):
        """Test atomic lock and remove with concurrent workers."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_ids = [f"worker_{i}" for i in range(5)]
        lock_timeout_ms = 5000
        
        # Create a job and add it to the queue
        job = Job(id=job_id, function_name="test_func")
        await job_store_atomic.save_job_definition(job)
        
        # Add job to queue
        current_time_ms = int(time.time() * 1000)
        await job_store_atomic.add_job_to_queue(queue_name, job_id, current_time_ms)
        
        # All workers try to acquire lock concurrently
        tasks = [
            job_store_atomic.atomic_lock_and_remove_job(
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


class TestAtomicRetry:
    """Test atomic retry operations."""
    
    @pytest.mark.asyncio
    async def test_atomic_retry_job_success(self, job_store_atomic: JobStore):
        """Test successful atomic retry job operation."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        error_message = "Test error message"
        retry_at_score = time.time() * 1000 + 5000  # 5 seconds from now
        
        # Create a job
        job = Job(id=job_id, function_name="test_func", current_retries=0)
        await job_store_atomic.save_job_definition(job)
        
        # Test atomic retry
        new_retry_count = await job_store_atomic.atomic_retry_job(
            job_id, queue_name, retry_at_score, error_message, JobStatus.RETRYING
        )
        
        assert new_retry_count == 1
        
        # Verify job was added to queue
        ready_jobs = await job_store_atomic.get_queued_job_ids(queue_name)
        assert job_id in ready_jobs
        
        # Verify job hash was updated
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        job_data = await job_store_atomic.redis.hgetall(job_key)
        
        assert job_data[b"current_retries"] == b"1"
        assert job_data[b"status"] == JobStatus.RETRYING.value.encode("utf-8")
        assert job_data[b"last_error"] == error_message.encode("utf-8")
    
    @pytest.mark.asyncio
    async def test_atomic_retry_job_increments_correctly(self, job_store_atomic: JobStore):
        """Test that atomic retry correctly increments retry count."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        error_message = "Test error message"
        retry_at_score = time.time() * 1000 + 5000
        
        # Create a job with existing retries
        job = Job(id=job_id, function_name="test_func", current_retries=2)
        await job_store_atomic.save_job_definition(job)
        
        # Test atomic retry
        new_retry_count = await job_store_atomic.atomic_retry_job(
            job_id, queue_name, retry_at_score, error_message, JobStatus.RETRYING
        )
        
        assert new_retry_count == 3
        
        # Verify job hash was updated
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        job_data = await job_store_atomic.redis.hgetall(job_key)
        
        assert job_data[b"current_retries"] == b"3"
    
    @pytest.mark.asyncio
    async def test_atomic_retry_job_concurrent_retries(self, job_store_atomic: JobStore):
        """Test atomic retry with concurrent retry attempts."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        error_message = "Test error message"
        retry_at_score = time.time() * 1000 + 5000
        
        # Create a job
        job = Job(id=job_id, function_name="test_func", current_retries=0)
        await job_store_atomic.save_job_definition(job)
        
        # Multiple concurrent retry attempts
        tasks = [
            job_store_atomic.atomic_retry_job(
                job_id, queue_name, retry_at_score + i, error_message, JobStatus.RETRYING
            )
            for i in range(3)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # Results should be consecutive increments
        assert sorted(results) == [1, 2, 3]
        
        # Verify final retry count
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        job_data = await job_store_atomic.redis.hgetall(job_key)
        assert job_data[b"current_retries"] == b"3"
        
        # Verify job was added to queue multiple times (with different scores)
        ready_jobs = await job_store_atomic.get_queued_job_ids(queue_name)
        assert ready_jobs.count(job_id) == 3


class TestAtomicOperationsIntegration:
    """Integration tests for atomic operations."""
    
    @pytest.mark.asyncio
    async def test_complete_job_lifecycle_with_atomic_operations(self, job_store_atomic: JobStore):
        """Test complete job lifecycle using atomic operations."""
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        worker_id = "test_worker"
        lock_timeout_ms = 5000
        
        # Create and enqueue job
        job = Job(id=job_id, function_name="test_func")
        await job_store_atomic.save_job_definition(job)
        
        current_time_ms = int(time.time() * 1000)
        await job_store_atomic.add_job_to_queue(queue_name, job_id, current_time_ms)
        
        # Worker picks up job atomically
        lock_acquired, removed_count = await job_store_atomic.atomic_lock_and_remove_job(
            job_id, queue_name, worker_id, lock_timeout_ms
        )
        
        assert lock_acquired is True
        assert removed_count == 1
        
        # Job fails and needs retry
        error_message = "Simulated failure"
        retry_at_score = time.time() * 1000 + 5000
        
        new_retry_count = await job_store_atomic.atomic_retry_job(
            job_id, queue_name, retry_at_score, error_message, JobStatus.RETRYING
        )
        
        assert new_retry_count == 1
        
        # Release the processing lock
        await job_store_atomic.release_job_lock(job_id)
        
        # Verify job is back in queue for retry
        ready_jobs = await job_store_atomic.get_queued_job_ids(queue_name)
        assert job_id in ready_jobs
        
        # Verify job state
        retrieved_job = await job_store_atomic.get_job_definition(job_id)
        assert retrieved_job is not None
        assert retrieved_job.current_retries == 1
        assert retrieved_job.status == JobStatus.RETRYING
        assert retrieved_job.last_error == error_message