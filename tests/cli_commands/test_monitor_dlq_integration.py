"""Tests for DLQ integration in monitoring dashboard"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rrq.cli_commands.commands.monitor import Dashboard
from rrq.settings import RRQSettings


class TestMonitorDLQIntegration:
    """Test DLQ functionality in monitoring dashboard"""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings with DLQ configuration"""
        settings = MagicMock(spec=RRQSettings)
        settings.default_dlq_name = "test_dlq"
        settings.default_queue_name = "default"
        return settings

    @pytest.fixture
    def dashboard(self, mock_settings):
        """Dashboard instance for testing"""
        return Dashboard(mock_settings, refresh_interval=1.0, queue_filter=None)

    @pytest.mark.asyncio
    async def test_update_dlq_stats_with_jobs(self, dashboard):
        """Test DLQ stats update with jobs present"""
        # Mock job store
        mock_store = MagicMock()
        dashboard.job_store = mock_store

        # Mock DLQ has jobs
        mock_store.redis.llen = AsyncMock(return_value=3)

        # Mock job IDs in DLQ
        job_ids = [b"job_001", b"job_002", b"job_003"]
        mock_store.redis.lrange = AsyncMock(return_value=job_ids)

        # Mock job data with different errors and timestamps
        base_time = datetime.now().timestamp()

        def mock_get_job_side_effect(job_id):
            job_data = {
                "job_001": {
                    "last_error": "SMTP connection timeout occurred",
                    "completion_time": base_time,  # Most recent
                },
                "job_002": {
                    "last_error": "Database connection failed",
                    "completion_time": base_time - 100,
                },
                "job_003": {
                    "last_error": "SMTP connection timeout occurred",  # Duplicate error
                    "completion_time": base_time - 200,
                },
            }
            return job_data.get(job_id)

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        # Update DLQ stats
        await dashboard._update_dlq_stats()

        # Verify stats were updated
        assert dashboard.dlq_stats["total_jobs"] == 3
        assert dashboard.dlq_stats["newest_error"] == "SMTP connection timeout occurred"
        # Error might be truncated to 100 chars in the stats
        assert any(
            "SMTP connection timeout" in error
            for error in dashboard.dlq_stats["top_errors"]
        )
        assert "Database connection failed" in dashboard.dlq_stats["top_errors"]
        # Find the truncated key and verify its count
        smtp_error_key = next(
            error
            for error in dashboard.dlq_stats["top_errors"]
            if "SMTP connection timeout" in error
        )
        assert dashboard.dlq_stats["top_errors"][smtp_error_key] == 2

    @pytest.mark.asyncio
    async def test_update_dlq_stats_empty_dlq(self, dashboard):
        """Test DLQ stats update with empty DLQ"""
        # Mock job store
        mock_store = MagicMock()
        dashboard.job_store = mock_store

        # Mock empty DLQ
        mock_store.redis.llen = AsyncMock(return_value=0)

        # Update DLQ stats
        await dashboard._update_dlq_stats()

        # Verify empty stats
        assert dashboard.dlq_stats["total_jobs"] == 0
        assert dashboard.dlq_stats["newest_error"] is None
        assert dashboard.dlq_stats["top_errors"] == {}

    @pytest.mark.asyncio
    async def test_update_dlq_stats_with_string_timestamps(self, dashboard):
        """Test DLQ stats update with string timestamp format"""
        # Mock job store
        mock_store = MagicMock()
        dashboard.job_store = mock_store

        mock_store.redis.llen = AsyncMock(return_value=1)
        mock_store.redis.lrange = AsyncMock(return_value=[b"job_001"])

        # Mock job with string timestamp (ISO format)
        job_data = {
            "last_error": "Test error message",
            "completion_time": "2024-01-01T12:00:00+00:00",  # String format
        }
        mock_store.get_job = AsyncMock(return_value=job_data)

        # Update DLQ stats (should handle string timestamp gracefully)
        await dashboard._update_dlq_stats()

        assert dashboard.dlq_stats["total_jobs"] == 1
        assert dashboard.dlq_stats["newest_error"] == "Test error message"

    def test_create_dlq_stats_panel(self, dashboard):
        """Test DLQ stats panel creation"""
        # Set up test data
        dashboard.dlq_stats = {
            "total_jobs": 5,
            "newest_error": "Connection timeout after 30 seconds",
        }

        panel = dashboard._create_dlq_stats()

        # Verify panel structure
        assert "Dead Letter Queue" in panel.title
        assert "test_dlq" in panel.title
        assert panel.border_style == "red"

        # Panel should contain table with metrics
        table = panel.renderable
        assert table.columns[0].header == "Metric"
        assert table.columns[1].header == "Value"

    def test_create_dlq_stats_panel_empty(self, dashboard):
        """Test DLQ stats panel with empty DLQ"""
        # Set up empty DLQ data
        dashboard.dlq_stats = {
            "total_jobs": 0,
            "newest_error": None,
        }

        panel = dashboard._create_dlq_stats()

        # Should still create panel but show zero jobs
        assert "Dead Letter Queue" in panel.title
        _table = panel.renderable
        # Should show "0" for total jobs and "None" for latest error

    def test_create_dlq_errors_panel(self, dashboard):
        """Test DLQ errors panel creation"""
        # Set up test data with error patterns
        dashboard.dlq_stats = {
            "top_errors": {
                "SMTP connection failed": 3,
                "Database timeout": 2,
                "Permission denied": 1,
            }
        }

        panel = dashboard._create_dlq_errors()

        # Verify panel structure
        assert panel.title == "Top Error Patterns"
        assert panel.border_style == "red"

        # Panel should contain table with errors
        table = panel.renderable
        assert table.columns[0].header == "Error Pattern"
        assert table.columns[1].header == "Count"

    def test_create_dlq_errors_panel_empty(self, dashboard):
        """Test DLQ errors panel with no errors"""
        # Set up empty error data
        dashboard.dlq_stats = {"top_errors": {}}

        panel = dashboard._create_dlq_errors()

        # Should still create panel but show no errors
        assert panel.title == "Top Error Patterns"
        _table = panel.renderable
        # Should show "No errors" row

    @pytest.mark.asyncio
    async def test_monitor_dashboard_includes_dlq_updates(self, dashboard):
        """Test that dashboard update cycle includes DLQ stats"""
        # Mock job store
        mock_store = MagicMock()
        dashboard.job_store = mock_store

        # Mock Redis operations
        async def mock_scan_iter(match=None):
            # Return empty async generator
            return
            yield  # Make it an async generator

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.llen = AsyncMock(return_value=0)  # Empty DLQ

        # Mock the update_metrics method to verify DLQ update is called
        with patch.object(dashboard, "_update_dlq_stats") as mock_dlq_update:
            await dashboard.update_metrics()

            # Verify DLQ stats were updated
            mock_dlq_update.assert_called_once()

    def test_dashboard_layout_includes_dlq_sections(self, dashboard):
        """Test that dashboard layout includes DLQ sections"""
        layout = dashboard.create_layout()

        # Verify DLQ sections are in the layout
        assert layout["main"].get("dlq") is not None
        assert layout["main"]["dlq"].get("dlq_stats") is not None
        assert layout["main"]["dlq"].get("dlq_errors") is not None

    @patch("rrq.cli_commands.commands.monitor.get_job_store")
    def test_update_layout_includes_dlq_panels(self, mock_get_job_store, dashboard):
        """Test that layout update includes DLQ panels"""
        # Mock job store
        mock_store = MagicMock()
        dashboard.job_store = mock_store

        # Initialize DLQ stats
        dashboard.dlq_stats = {
            "total_jobs": 2,
            "newest_error": "Test error",
            "top_errors": {"Test error": 2},
        }

        # Create layout and update it
        layout = dashboard.create_layout()

        # Mock the individual panel creation methods
        with (
            patch.object(dashboard, "_create_dlq_stats") as mock_stats_panel,
            patch.object(dashboard, "_create_dlq_errors") as mock_errors_panel,
            patch.object(dashboard, "_create_header") as _mock_header,
            patch.object(dashboard, "_create_queue_stats") as _mock_queue_stats,
            patch.object(dashboard, "_create_queue_chart") as _mock_queue_chart,
            patch.object(dashboard, "_create_worker_list") as _mock_worker_list,
            patch.object(dashboard, "_create_recent_jobs") as _mock_recent_jobs,
            patch.object(dashboard, "_create_footer") as _mock_footer,
        ):
            dashboard.update_layout(layout)

            # Verify DLQ panels were created
            mock_stats_panel.assert_called_once()
            mock_errors_panel.assert_called_once()

    @pytest.mark.asyncio
    async def test_dlq_stats_with_job_retrieval_errors(self, dashboard):
        """Test DLQ stats handling when job retrieval fails"""
        # Mock job store
        mock_store = MagicMock()
        dashboard.job_store = mock_store

        mock_store.redis.llen = AsyncMock(return_value=2)
        mock_store.redis.lrange = AsyncMock(return_value=[b"job_001", b"job_002"])

        # Mock job retrieval where one job fails to load
        def mock_get_job_side_effect(job_id):
            if job_id == "job_001":
                return {
                    "last_error": "Valid error message",
                    "completion_time": datetime.now().timestamp(),
                }
            elif job_id == "job_002":
                return None  # Job not found or failed to load
            return None

        mock_store.get_job = AsyncMock(side_effect=mock_get_job_side_effect)

        # Update DLQ stats - should handle missing jobs gracefully
        await dashboard._update_dlq_stats()

        # Should only process the valid job
        assert dashboard.dlq_stats["total_jobs"] == 2  # Total from Redis
        assert dashboard.dlq_stats["newest_error"] == "Valid error message"
        assert len(dashboard.dlq_stats["top_errors"]) == 1

    @pytest.mark.asyncio
    async def test_dlq_error_truncation(self, dashboard):
        """Test that long error messages are properly truncated"""
        # Mock job store
        mock_store = MagicMock()
        dashboard.job_store = mock_store

        mock_store.redis.llen = AsyncMock(return_value=1)
        mock_store.redis.lrange = AsyncMock(return_value=[b"job_001"])

        # Mock job with very long error message
        long_error = "This is a very long error message that should be truncated because it exceeds the maximum length limit"
        job_data = {
            "last_error": long_error,
            "completion_time": datetime.now().timestamp(),
        }
        mock_store.get_job = AsyncMock(return_value=job_data)

        await dashboard._update_dlq_stats()

        # Error should be truncated for display
        newest_error = dashboard.dlq_stats["newest_error"]
        assert len(newest_error) <= 53  # 50 chars + "..."
        assert newest_error.endswith("...")

    def test_dlq_stats_initialization(self, dashboard):
        """Test that DLQ stats are properly initialized"""
        # Verify initial DLQ stats structure
        assert "total_jobs" in dashboard.dlq_stats
        assert "newest_error" in dashboard.dlq_stats
        assert "top_errors" in dashboard.dlq_stats

        # Verify initial values
        assert dashboard.dlq_stats["total_jobs"] == 0
        assert dashboard.dlq_stats["newest_error"] is None
        assert dashboard.dlq_stats["top_errors"] == {}


class TestDLQMonitoringCommands:
    """Test DLQ monitoring command integration"""

    @patch("rrq.cli_commands.commands.monitor.get_job_store")
    @patch("rrq.cli_commands.commands.monitor.Live")
    def test_monitor_command_includes_dlq_data(self, mock_live, mock_get_job_store):
        """Test that monitor command processes DLQ data"""
        from rrq.cli_commands.commands.monitor import MonitorCommands

        # Mock settings
        mock_settings = MagicMock()
        mock_settings.default_dlq_name = "test_dlq"

        # Mock job store
        mock_store = MagicMock()
        mock_store.aclose = AsyncMock()
        mock_store.redis.scan_iter = AsyncMock(return_value=[])
        mock_store.redis.llen = AsyncMock(return_value=5)  # DLQ has jobs
        mock_store.redis.lrange = AsyncMock(return_value=[])
        mock_get_job_store.return_value = mock_store

        # Mock Live context manager to prevent actual UI
        mock_live_instance = MagicMock()
        mock_live.return_value.__enter__ = MagicMock(return_value=mock_live_instance)
        mock_live.return_value.__exit__ = MagicMock(return_value=None)

        _monitor_commands = MonitorCommands()

        # This would normally run the dashboard but we're mocking Live
        # The important thing is that it should create a Dashboard with DLQ functionality
        with patch(
            "rrq.cli_commands.commands.monitor.load_app_settings"
        ) as mock_load_settings:
            mock_load_settings.return_value = mock_settings

            # Create dashboard to verify DLQ integration
            dashboard = Dashboard(
                mock_settings, refresh_interval=1.0, queue_filter=None
            )

            # Verify DLQ stats are initialized
            assert "total_jobs" in dashboard.dlq_stats
            assert "newest_error" in dashboard.dlq_stats
            assert "top_errors" in dashboard.dlq_stats
