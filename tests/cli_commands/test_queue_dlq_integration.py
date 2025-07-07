"""Tests for DLQ integration in queue statistics"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rrq.cli_commands.commands.queues import QueueCommands


class TestQueueDLQIntegration:
    """Test DLQ functionality in queue statistics"""

    @pytest.fixture
    def queue_commands(self):
        """QueueCommands instance for testing"""
        return QueueCommands()

    @pytest.fixture
    def cli_with_queue_commands(self, cli_runner):
        """CLI runner with queue commands registered"""
        import click

        @click.group()
        def test_cli():
            pass

        queue_commands = QueueCommands()
        queue_commands.register(test_cli)
        return test_cli, cli_runner

    @pytest.mark.asyncio
    async def test_count_dlq_jobs_for_queue(self, queue_commands):
        """Test counting DLQ jobs for a specific queue"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock DLQ job IDs
        job_ids = [b"job_001", b"job_002", b"job_003"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock job data - some from urgent queue, some from other queues
        def mock_get_job_side_effect(job_id):
            job_data = {
                "job_001": {"queue_name": "urgent", "status": "failed"},
                "job_002": {"queue_name": "default", "status": "failed"},
                "job_003": {"queue_name": "urgent", "status": "failed"},
            }
            return job_data.get(job_id)

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        # Count DLQ jobs for urgent queue
        count = await queue_commands._count_dlq_jobs_for_queue(mock_store, "urgent")

        assert count == 2  # job_001 and job_003

        # Count DLQ jobs for default queue
        count = await queue_commands._count_dlq_jobs_for_queue(mock_store, "default")

        assert count == 1  # job_002

        # Count DLQ jobs for non-existent queue
        count = await queue_commands._count_dlq_jobs_for_queue(
            mock_store, "nonexistent"
        )

        assert count == 0

    @pytest.mark.asyncio
    async def test_count_dlq_jobs_empty_dlq(self, queue_commands):
        """Test counting DLQ jobs when DLQ is empty"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock empty DLQ
        mock_store.redis.lrange = AsyncMock(return_value=[])

        count = await queue_commands._count_dlq_jobs_for_queue(mock_store, "urgent")

        assert count == 0

    @pytest.mark.asyncio
    async def test_count_dlq_jobs_with_missing_jobs(self, queue_commands):
        """Test counting DLQ jobs when some jobs can't be retrieved"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock DLQ job IDs
        job_ids = [b"job_001", b"job_002"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock job data where one job is missing
        def mock_get_job_side_effect(job_id):
            if job_id == "job_001":
                return {"queue_name": "urgent", "status": "failed"}
            elif job_id == "job_002":
                return None  # Job not found
            return None

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        count = await queue_commands._count_dlq_jobs_for_queue(mock_store, "urgent")

        assert count == 1  # Only job_001 counts

    @pytest.mark.asyncio
    async def test_get_queue_statistics_includes_dlq(self, queue_commands):
        """Test that queue statistics include DLQ job counts"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.redis.zcard = AsyncMock(return_value=5)  # 5 pending jobs
        mock_store.redis.zrange = AsyncMock(
            return_value=[]
        )  # No jobs for wait time calculation

        # Mock DLQ count method
        with patch.object(
            queue_commands, "_count_dlq_jobs_for_queue"
        ) as mock_count_dlq:
            mock_count_dlq.return_value = 3  # 3 DLQ jobs for this queue

            stats = await queue_commands._get_queue_statistics(mock_store, "urgent")

            assert stats["pending"] == 5
            assert stats["dlq_jobs"] == 3
            assert "dlq_jobs" in stats

            # Verify DLQ count was called
            mock_count_dlq.assert_called_once_with(mock_store, "urgent")

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_stats_command_shows_dlq_column(
        self, mock_get_job_store, cli_with_queue_commands
    ):
        """Test that queue stats command displays DLQ column"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()

        # Mock pipeline for batch operations
        mock_pipeline = MagicMock()
        mock_pipeline.hmget = MagicMock(return_value=None)
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)

        # Mock queue discovery
        async def mock_scan_iter(match=None, count=None):
            queue_keys = [b"rrq:queue:urgent", b"rrq:queue:default"]
            for key in queue_keys:
                yield key

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=10)  # Pending jobs
        mock_store.redis.zrange = AsyncMock(return_value=[])

        # Mock DLQ job counting
        mock_store.settings.default_dlq_name = "test_dlq"
        mock_store.redis.lrange = AsyncMock(return_value=[b"job_001", b"job_002"])

        def mock_get_job_side_effect(job_id):
            return {"queue_name": "urgent", "status": "failed"}

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)
        mock_get_job_store.return_value = mock_store

        # Run command
        result = runner.invoke(test_cli, ["queue", "stats"])

        assert result.exit_code == 0
        assert "Queue Statistics" in result.output
        assert "DLQ" in result.output  # DLQ column should be present

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_stats_command_dlq_counts(
        self, mock_get_job_store, cli_with_queue_commands
    ):
        """Test that queue stats command shows correct DLQ counts"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock pipeline for batch operations
        mock_pipeline = MagicMock()
        mock_pipeline.hmget = MagicMock(return_value=None)
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)

        # Mock queue discovery
        async def mock_scan_iter(match=None, count=None):
            yield b"rrq:queue:urgent"

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=5)  # 5 pending jobs
        mock_store.redis.zrange = AsyncMock(return_value=[])

        # Mock DLQ with jobs from different queues
        dlq_job_ids = [b"job_001", b"job_002", b"job_003"]
        mock_store.redis.lrange = AsyncMock(return_value=dlq_job_ids)

        def mock_get_job_side_effect(job_id):
            job_data = {
                "job_001": {"queue_name": "urgent", "status": "failed"},
                "job_002": {"queue_name": "default", "status": "failed"},
                "job_003": {"queue_name": "urgent", "status": "failed"},
            }
            return job_data.get(job_id)

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)
        mock_get_job_store.return_value = mock_store

        # Run command
        result = runner.invoke(test_cli, ["queue", "stats"])

        assert result.exit_code == 0
        # Should show 2 DLQ jobs for urgent queue (job_001 and job_003)
        assert "2" in result.output

    @pytest.mark.asyncio
    async def test_queue_statistics_dlq_performance(self, queue_commands):
        """Test that DLQ counting doesn't significantly impact performance"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.redis.zcard = AsyncMock(return_value=100)
        mock_store.redis.zrange = AsyncMock(return_value=[])
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock large DLQ
        large_dlq = [f"job_{i:03d}".encode() for i in range(1000)]
        mock_store.redis.lrange = AsyncMock(return_value=large_dlq)

        # Mock job retrieval that returns relevant jobs quickly
        call_count = 0

        def mock_get_job_side_effect(job_id):
            nonlocal call_count
            call_count += 1
            # Every 10th job is from our target queue
            if call_count % 10 == 0:
                return {"queue_name": "urgent", "status": "failed"}
            else:
                return {"queue_name": "other", "status": "failed"}

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        # Get statistics - this should complete reasonably quickly
        stats = await queue_commands._get_queue_statistics(mock_store, "urgent")

        # Verify we got a result and DLQ count is included
        assert "dlq_jobs" in stats
        assert stats["dlq_jobs"] == 100  # 1000 / 10

        # Verify all DLQ jobs were checked (this tests the implementation)
        assert mock_store.get_job.call_count == 1000

    @patch("rrq.cli_commands.commands.queues.get_job_store")
    def test_queue_stats_with_zero_dlq_jobs(
        self, mock_get_job_store, cli_with_queue_commands
    ):
        """Test queue stats display when queue has no DLQ jobs"""
        test_cli, runner = cli_with_queue_commands

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock pipeline for batch operations
        mock_pipeline = MagicMock()
        mock_pipeline.hmget = MagicMock(return_value=None)
        mock_pipeline.execute = AsyncMock(return_value=[])
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        mock_store.redis.pipeline = MagicMock(return_value=mock_pipeline)

        # Mock queue discovery
        async def mock_scan_iter(match=None, count=None):
            yield b"rrq:queue:clean_queue"

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=5)  # 5 pending jobs
        mock_store.redis.zrange = AsyncMock(return_value=[])

        # Mock empty DLQ or DLQ with no jobs from this queue
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_get_job_store.return_value = mock_store

        # Run command
        result = runner.invoke(test_cli, ["queue", "stats"])

        assert result.exit_code == 0
        # Should show 0 DLQ jobs for clean queue
        assert "0" in result.output

    @pytest.mark.asyncio
    async def test_dlq_count_with_malformed_job_data(self, queue_commands):
        """Test DLQ counting handles malformed job data gracefully"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock DLQ job IDs
        job_ids = [b"job_001", b"job_002", b"job_003"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock job data with various malformed entries
        def mock_get_job_side_effect(job_id):
            job_data = {
                "job_001": {"queue_name": "urgent", "status": "failed"},  # Valid
                "job_002": {"status": "failed"},  # Missing queue_name
                "job_003": None,  # Job not found
            }
            return job_data.get(job_id)

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        # Should handle malformed data gracefully
        count = await queue_commands._count_dlq_jobs_for_queue(mock_store, "urgent")

        assert count == 1  # Only job_001 is valid and matches

    def test_queue_commands_register_includes_dlq_column(self):
        """Test that registered queue commands include DLQ functionality"""
        import click

        @click.group()
        def test_cli():
            pass

        queue_commands = QueueCommands()
        queue_commands.register(test_cli)

        # Check that queue group was added with stats command
        assert "queue" in test_cli.commands
        queue_group = test_cli.commands["queue"]
        assert "stats" in queue_group.commands

        # The stats command should now include DLQ functionality
        # (We can't easily test the column addition without running the command,
        # but we can verify the method exists)
        assert hasattr(queue_commands, "_count_dlq_jobs_for_queue")


class TestQueueDLQIntegrationEdgeCases:
    """Test edge cases for queue DLQ integration"""

    @pytest.fixture
    def queue_commands(self):
        return QueueCommands()

    @pytest.mark.asyncio
    async def test_dlq_count_redis_error(self, queue_commands):
        """Test DLQ counting when Redis operations fail"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock Redis error
        mock_store.redis.lrange = AsyncMock(
            side_effect=Exception("Redis connection lost")
        )

        # Should handle Redis errors gracefully
        with pytest.raises(Exception):
            await queue_commands._count_dlq_jobs_for_queue(mock_store, "urgent")

    @pytest.mark.asyncio
    async def test_dlq_count_concurrent_modifications(self, queue_commands):
        """Test DLQ counting when DLQ is being modified concurrently"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.settings.default_dlq_name = "test_dlq"

        # Mock DLQ that changes during iteration
        job_ids = [b"job_001", b"job_002"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock job retrieval where some jobs disappear
        def mock_get_job_side_effect(job_id):
            if job_id == "job_001":
                return {"queue_name": "urgent", "status": "failed"}
            elif job_id == "job_002":
                return None  # Job was processed and removed during our iteration
            return None

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        # Should handle concurrent modifications gracefully
        count = await queue_commands._count_dlq_jobs_for_queue(mock_store, "urgent")

        assert count == 1  # Only count jobs that still exist

    @pytest.mark.asyncio
    async def test_queue_statistics_dlq_integration_complete(self, queue_commands):
        """Test complete integration of DLQ stats in queue statistics"""
        # Mock job store with complete setup
        mock_store = MagicMock()
        mock_store.redis.zcard = AsyncMock(return_value=25)  # 25 pending jobs
        mock_store.redis.zrange = AsyncMock(
            return_value=[
                (b"job_1", 1640995200.0),  # Mock job with timestamp
                (b"job_2", 1640995300.0),
            ]
        )
        mock_store.settings.default_dlq_name = "production_dlq"

        # Mock DLQ with realistic data
        mock_store.redis.lrange = AsyncMock(
            return_value=[b"failed_job_001", b"failed_job_002", b"failed_job_003"]
        )

        def mock_get_job_side_effect(job_id):
            # Simulate realistic DLQ job distribution
            if job_id in ["failed_job_001", "failed_job_003"]:
                return {"queue_name": "high_priority", "status": "failed"}
            elif job_id == "failed_job_002":
                return {"queue_name": "normal_priority", "status": "failed"}
            return None

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        # Get statistics for high_priority queue
        stats = await queue_commands._get_queue_statistics(mock_store, "high_priority")

        # Verify all expected statistics are present
        assert stats["total"] == 25
        assert stats["pending"] == 25
        assert stats["active"] == 0
        assert stats["completed"] == 0
        assert stats["failed"] == 0
        assert stats["dlq_jobs"] == 2  # failed_job_001 and failed_job_003
        assert stats["avg_wait_time"] is not None
        assert stats["throughput"] == 0.0

        # Verify all keys are present
        expected_keys = [
            "total",
            "pending",
            "active",
            "completed",
            "failed",
            "dlq_jobs",
            "avg_wait_time",
            "throughput",
        ]
        for key in expected_keys:
            assert key in stats
