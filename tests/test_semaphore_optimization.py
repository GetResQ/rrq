"""Tests for semaphore optimization in RRQWorker"""

import asyncio
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rrq.settings import RRQSettings
from rrq.registry import JobRegistry
from rrq.worker import RRQWorker
from rrq.job import Job, JobStatus
from rrq.store import JobStore


@pytest.fixture
def test_registry():
    """Create a test job registry."""
    registry = JobRegistry()
    
    async def test_job(context):
        return "test_result"
    
    registry.register("test_job", test_job)
    
    return registry


@pytest.fixture
def test_settings(test_registry):
    """Create test settings with job registry."""
    return RRQSettings(
        redis_dsn="redis://localhost:6379/6",
        job_registry=test_registry,
        worker_concurrency=3,  # Low concurrency for easier testing
    )


class TestSemaphoreOptimization:
    """Test semaphore optimization functionality."""
    
    @pytest.mark.asyncio
    async def test_semaphore_acquired_only_after_successful_lock(self, test_settings):
        """Test that semaphore is only acquired after successfully getting job lock."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        
        # Mock the job acquisition to fail (another worker got it)
        with patch.object(worker, '_try_acquire_job', return_value=None):
            # Mock semaphore to track if it was acquired
            semaphore_acquired = False
            original_acquire = worker._semaphore.acquire
            
            async def mock_acquire():
                nonlocal semaphore_acquired
                semaphore_acquired = True
                return await original_acquire()
            
            with patch.object(worker._semaphore, 'acquire', side_effect=mock_acquire):
                # Try to process job (should fail to acquire)
                job_started = await worker._try_process_job(job_id, queue_name)
                
                # Job should not have started and semaphore should not have been acquired
                assert job_started is False
                assert semaphore_acquired is False  # This is the key optimization test

    @pytest.mark.asyncio
    async def test_semaphore_acquired_after_successful_lock(self, test_settings):
        """Test that semaphore is acquired after successfully getting job lock."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        job = Job(id=job_id, function_name="test_job")
        
        # Mock successful job acquisition
        with patch.object(worker, '_try_acquire_job', return_value=job):
            # Mock _process_acquired_job to avoid actual execution
            with patch.object(worker, '_process_acquired_job', new_callable=AsyncMock):
                # Track semaphore acquisition
                semaphore_acquired = False
                original_acquire = worker._semaphore.acquire
                
                async def mock_acquire():
                    nonlocal semaphore_acquired
                    semaphore_acquired = True
                    return await original_acquire()
                
                with patch.object(worker._semaphore, 'acquire', side_effect=mock_acquire):
                    # Try to process job (should succeed)
                    job_started = await worker._try_process_job(job_id, queue_name)
                    
                    # Job should have started and semaphore should have been acquired
                    assert job_started is True
                    assert semaphore_acquired is True

    @pytest.mark.asyncio
    async def test_optimized_polling_loop_semaphore_usage(self, test_settings):
        """Test that the optimized polling loop uses semaphore efficiently."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        job_ids = [f"test_job_{i}" for i in range(5)]
        queue_name = "test_queue"
        
        # Track semaphore acquisitions and job acquisitions
        semaphore_acquisitions = []
        job_acquisitions = []
        
        def mock_try_acquire_job(job_id, queue):
            # Simulate that only job_0 and job_2 are successfully acquired
            if job_id.endswith('_0') or job_id.endswith('_2'):
                job_acquisitions.append(job_id)
                return Job(id=job_id, function_name="test_job")
            return None
        
        async def mock_acquire():
            semaphore_acquisitions.append(time.time())
            # Don't actually acquire to avoid blocking
        
        with patch.object(worker, '_try_acquire_job', side_effect=mock_try_acquire_job):
            with patch.object(worker._semaphore, 'acquire', side_effect=mock_acquire):
                with patch.object(worker, '_process_acquired_job', new_callable=AsyncMock):
                    with patch.object(worker.job_store, 'get_ready_job_ids', return_value=job_ids):
                        # Run one poll cycle
                        fetched_count = await worker._poll_for_jobs(5)
                        
                        # Should have tried to acquire 5 jobs but only succeeded with 2
                        assert len(job_acquisitions) == 2
                        assert job_acquisitions[0].endswith('_0')
                        assert job_acquisitions[1].endswith('_2')
                        
                        # Should have acquired semaphore only for successful job acquisitions
                        assert len(semaphore_acquisitions) == 2
                        assert fetched_count == 2

    @pytest.mark.asyncio
    async def test_concurrent_workers_semaphore_optimization(self, test_settings):
        """Test semaphore optimization with multiple workers competing for jobs."""
        workers = [
            RRQWorker(
                settings=test_settings,
                job_registry=test_settings.job_registry,
                worker_id=f"worker_{i}"
            )
            for i in range(3)
        ]
        
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        job = Job(id=job_id, function_name="test_job")
        
        # Track which worker successfully acquires the job and semaphore
        successful_acquisitions = []
        semaphore_acquisitions = []
        
        def mock_try_acquire_job(worker_id):
            def _inner(job_id_param, queue):
                # Only first worker succeeds
                if worker_id == "worker_0" and len(successful_acquisitions) == 0:
                    successful_acquisitions.append(worker_id)
                    return job
                return None
            return _inner
        
        def mock_acquire_semaphore(worker_id):
            async def _inner():
                semaphore_acquisitions.append(worker_id)
            return _inner
        
        # Set up mocks for each worker
        tasks = []
        for i, worker in enumerate(workers):
            worker_id = f"worker_{i}"
            
            with patch.object(worker, '_try_acquire_job', side_effect=mock_try_acquire_job(worker_id)):
                with patch.object(worker._semaphore, 'acquire', side_effect=mock_acquire_semaphore(worker_id)):
                    with patch.object(worker, '_process_acquired_job', new_callable=AsyncMock):
                        task = worker._try_process_job(job_id, queue_name)
                        tasks.append(task)
        
        # Run all workers concurrently
        results = await asyncio.gather(*tasks)
        
        # Only one worker should have succeeded
        successful_results = [r for r in results if r]
        assert len(successful_results) == 1
        
        # Only the successful worker should have acquired the semaphore
        assert len(successful_acquisitions) == 1
        assert len(semaphore_acquisitions) == 1
        assert successful_acquisitions[0] == semaphore_acquisitions[0]

    @pytest.mark.asyncio
    async def test_semaphore_released_on_processing_error(self, test_settings):
        """Test that semaphore is properly released when job processing fails."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        job = Job(id=job_id, function_name="test_job")
        
        # Track semaphore release
        semaphore_released = False
        original_release = worker._semaphore.release
        
        def mock_release():
            nonlocal semaphore_released
            semaphore_released = True
            return original_release()
        
        # Mock successful job acquisition but failed processing
        with patch.object(worker, '_try_acquire_job', return_value=job):
            with patch.object(worker, '_process_acquired_job', side_effect=Exception("Processing failed")):
                with patch.object(worker._semaphore, 'release', side_effect=mock_release):
                    # Try to process job (should fail in processing)
                    job_started = await worker._try_process_job(job_id, queue_name)
                    
                    # Job should not have started and semaphore should have been released
                    assert job_started is False
                    assert semaphore_released is True

    @pytest.mark.asyncio 
    async def test_backward_compatibility_with_old_interface(self, test_settings):
        """Test that the old _try_process_job interface still works correctly."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        job_id = f"test_job_{uuid.uuid4()}"
        queue_name = "test_queue"
        
        # Mock the underlying methods to verify the flow
        job = Job(id=job_id, function_name="test_job")
        
        with patch.object(worker, '_try_acquire_job', return_value=job) as mock_acquire:
            with patch.object(worker, '_process_acquired_job', new_callable=AsyncMock) as mock_process:
                # Test the old interface
                result = await worker._try_process_job(job_id, queue_name)
                
                # Verify the new methods were called
                mock_acquire.assert_called_once_with(job_id, queue_name)
                mock_process.assert_called_once_with(job, queue_name)
                assert result is True

class TestSemaphoreOptimizationIntegration:
    """Integration tests for semaphore optimization."""
    
    def test_semaphore_optimization_improves_efficiency(self, test_settings):
        """Test that semaphore optimization improves efficiency under contention."""
        # This is more of a documentation test showing the efficiency improvement
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        # Verify the new methods exist and are properly structured
        assert hasattr(worker, '_try_acquire_job')
        assert hasattr(worker, '_process_acquired_job')
        
        # Verify the optimization is in the polling loop
        import inspect
        poll_source = inspect.getsource(worker._poll_for_jobs)
        
        # Should use the optimized approach
        assert '_try_acquire_job' in poll_source
        assert '_process_acquired_job' in poll_source
        
        # Should acquire semaphore only after successful job acquisition
        lines = poll_source.split('\n')
        acquire_line = next((i for i, line in enumerate(lines) if '_try_acquire_job' in line), -1)
        semaphore_line = next((i for i, line in enumerate(lines) if 'semaphore.acquire' in line), -1)
        
        # Semaphore acquisition should come after job acquisition attempt
        assert acquire_line >= 0
        assert semaphore_line >= 0
        assert semaphore_line > acquire_line