"""Tests for jittered poll delays in RRQWorker"""

import pytest

from rrq.settings import RRQSettings
from rrq.registry import JobRegistry
from rrq.worker import RRQWorker


@pytest.fixture
def test_registry():
    """Create a test job registry."""
    registry = JobRegistry()
    
    @registry.register("test_job")
    async def test_job(context):
        return "test_result"
    
    return registry


@pytest.fixture
def test_settings(test_registry):
    """Create test settings with job registry."""
    return RRQSettings(
        redis_dsn="redis://localhost:6379/5",
        job_registry=test_registry,
        default_poll_delay_seconds=1.0,
    )


class TestJitteredDelays:
    """Test jittered delay calculations."""
    
    def test_calculate_jittered_delay_default_jitter(self, test_settings):
        """Test jittered delay with default jitter factor."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        base_delay = 2.0
        jitter_factor = 0.5  # Default
        
        # Test multiple calculations to verify jitter range
        delays = [worker._calculate_jittered_delay(base_delay) for _ in range(100)]
        
        # All delays should be within the expected range
        min_expected = base_delay * (1 - jitter_factor)  # 1.0
        max_expected = base_delay * (1 + jitter_factor)  # 3.0
        
        for delay in delays:
            assert min_expected <= delay <= max_expected
        
        # Should have variation (not all the same)
        assert len(set(delays)) > 1
        
        # Average should be close to base delay
        avg_delay = sum(delays) / len(delays)
        assert abs(avg_delay - base_delay) < 0.5  # Within reasonable tolerance
    
    def test_calculate_jittered_delay_custom_jitter(self, test_settings):
        """Test jittered delay with custom jitter factor."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        base_delay = 5.0
        jitter_factor = 0.2  # 20% jitter
        
        delays = [worker._calculate_jittered_delay(base_delay, jitter_factor) for _ in range(50)]
        
        # All delays should be within the expected range
        min_expected = base_delay * (1 - jitter_factor)  # 4.0
        max_expected = base_delay * (1 + jitter_factor)  # 6.0
        
        for delay in delays:
            assert min_expected <= delay <= max_expected
        
        # Should have variation
        assert len(set(delays)) > 1
    
    def test_calculate_jittered_delay_no_jitter(self, test_settings):
        """Test jittered delay with zero jitter factor."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        base_delay = 3.0
        jitter_factor = 0.0  # No jitter
        
        delays = [worker._calculate_jittered_delay(base_delay, jitter_factor) for _ in range(10)]
        
        # All delays should equal base delay
        for delay in delays:
            assert delay == base_delay
    
    def test_calculate_jittered_delay_maximum_jitter(self, test_settings):
        """Test jittered delay with maximum jitter factor."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        base_delay = 1.0
        jitter_factor = 1.0  # 100% jitter
        
        delays = [worker._calculate_jittered_delay(base_delay, jitter_factor) for _ in range(100)]
        
        # All delays should be within the expected range
        min_expected = base_delay * (1 - jitter_factor)  # 0.0
        max_expected = base_delay * (1 + jitter_factor)  # 2.0
        
        for delay in delays:
            assert min_expected <= delay <= max_expected
        
        # Should have significant variation
        assert len(set(delays)) > 10
    
    def test_calculate_jittered_delay_with_small_base(self, test_settings):
        """Test jittered delay with small base delay."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        base_delay = 0.1
        jitter_factor = 0.5
        
        delays = [worker._calculate_jittered_delay(base_delay, jitter_factor) for _ in range(50)]
        
        # All delays should be within the expected range
        min_expected = base_delay * (1 - jitter_factor)  # 0.05
        max_expected = base_delay * (1 + jitter_factor)  # 0.15
        
        for delay in delays:
            assert min_expected <= delay <= max_expected
            assert delay >= 0  # Delays should never be negative
    
    def test_jittered_delay_properties(self, test_settings):
        """Test mathematical properties of jittered delays."""
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker"
        )
        
        base_delay = 2.0
        jitter_factor = 0.3
        sample_size = 1000
        
        delays = [worker._calculate_jittered_delay(base_delay, jitter_factor) for _ in range(sample_size)]
        
        # Statistical properties
        min_delay = min(delays)
        max_delay = max(delays)
        avg_delay = sum(delays) / len(delays)
        
        # Check range bounds
        expected_min = base_delay * (1 - jitter_factor)
        expected_max = base_delay * (1 + jitter_factor)
        
        assert expected_min <= min_delay
        assert max_delay <= expected_max
        
        # Average should be close to base delay (within 5% for large sample)
        assert abs(avg_delay - base_delay) / base_delay < 0.05
        
        # Should use most of the available range
        range_used = max_delay - min_delay
        expected_range = expected_max - expected_min
        assert range_used > expected_range * 0.8  # At least 80% of range used


class TestJitteredDelaysIntegration:
    """Integration tests for jittered delays in worker operation."""
    
    @pytest.mark.asyncio
    async def test_worker_uses_jittered_delays_in_main_loop(self, test_settings):
        """Test that worker actually uses jittered delays (mock test)."""
        import unittest.mock as mock
        import asyncio
        
        worker = RRQWorker(
            settings=test_settings,
            job_registry=test_settings.job_registry,
            worker_id="test_worker",
            burst=True  # Use burst mode to exit quickly
        )
        
        # Mock the delay calculation to track its usage
        original_calculate_jittered_delay = worker._calculate_jittered_delay
        with mock.patch.object(worker, '_calculate_jittered_delay', side_effect=original_calculate_jittered_delay) as mock_jitter:
            # Mock other components to avoid actual Redis operations
            with mock.patch.object(worker.job_store, 'get_ready_job_ids', return_value=[]):
                with mock.patch.object(worker, '_call_startup_hook'):
                    with mock.patch.object(worker, '_call_shutdown_hook'):
                        # Run worker briefly
                        try:
                            await asyncio.wait_for(worker.run(), timeout=0.5)
                        except asyncio.TimeoutError:
                            pass  # Expected for this test
                        
                        # Verify jittered delay was called
                        assert mock_jitter.call_count > 0
                        
                        # Verify it was called with the expected base delay
                        calls = mock_jitter.call_args_list
                        for call in calls:
                            args, kwargs = call
                            base_delay = args[0]
                            # Should be either default poll delay or error delay (1.0)
                            assert base_delay in [test_settings.default_poll_delay_seconds, 1.0]
    
    def test_multiple_workers_have_different_jitter_patterns(self, test_settings):
        """Test that multiple workers generate different jitter patterns."""
        # Create multiple workers
        workers = [
            RRQWorker(
                settings=test_settings,
                job_registry=test_settings.job_registry,
                worker_id=f"test_worker_{i}"
            )
            for i in range(5)
        ]
        
        base_delay = 1.0
        samples_per_worker = 20
        
        # Collect delay patterns from each worker
        worker_delays = []
        for worker in workers:
            delays = [worker._calculate_jittered_delay(base_delay) for _ in range(samples_per_worker)]
            worker_delays.append(delays)
        
        # Verify that not all workers have identical patterns
        # (This is probabilistically almost certain with jitter)
        all_patterns_identical = all(
            worker_delays[0] == pattern for pattern in worker_delays[1:]
        )
        assert not all_patterns_identical
        
        # Verify each worker's delays are within expected range
        expected_min = base_delay * 0.5  # Default jitter factor
        expected_max = base_delay * 1.5
        
        for worker_pattern in worker_delays:
            for delay in worker_pattern:
                assert expected_min <= delay <= expected_max