"""Tests for monitoring dashboard CLI commands"""

from collections import defaultdict, deque
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rrq.cli_commands.commands.monitor import Dashboard, MonitorCommands


class TestMonitorCommands:
    """Test monitoring dashboard commands"""

    @pytest.fixture
    def monitor_commands(self):
        """MonitorCommands instance for testing"""
        return MonitorCommands()

    @pytest.fixture
    def cli_with_monitor_commands(self, cli_runner):
        """CLI runner with monitor commands registered"""
        import click

        @click.group()
        def test_cli():
            pass

        monitor_commands = MonitorCommands()
        monitor_commands.register(test_cli)
        return test_cli, cli_runner

    @patch("rrq.cli_commands.commands.monitor.Dashboard")
    def test_monitor_command(self, mock_dashboard_class, cli_with_monitor_commands):
        """Test monitor command"""
        test_cli, runner = cli_with_monitor_commands

        # Mock dashboard
        mock_dashboard = MagicMock()
        mock_dashboard.run = AsyncMock()
        mock_dashboard_class.return_value = mock_dashboard

        # Run command (should exit quickly in test)
        result = runner.invoke(test_cli, ["monitor", "--refresh", "0.1"])

        assert result.exit_code == 0
        mock_dashboard_class.assert_called_once()
        mock_dashboard.run.assert_called_once()

    @patch("rrq.cli_commands.commands.monitor.Dashboard")
    def test_monitor_command_with_options(
        self, mock_dashboard_class, cli_with_monitor_commands
    ):
        """Test monitor command with custom options"""
        test_cli, runner = cli_with_monitor_commands

        # Mock dashboard
        mock_dashboard = MagicMock()
        mock_dashboard.run = AsyncMock()
        mock_dashboard_class.return_value = mock_dashboard

        # Run command with options
        result = runner.invoke(
            test_cli,
            [
                "monitor",
                "--refresh",
                "2.0",
                "--queues",
                "urgent",
                "--queues",
                "default",
            ],
        )

        assert result.exit_code == 0

        # Verify dashboard was created with correct parameters
        call_args = mock_dashboard_class.call_args
        _settings = call_args[0][0]  # First positional arg
        refresh_interval = call_args[0][1]  # Second positional arg
        queue_filter = call_args[0][2]  # Third positional arg

        assert refresh_interval == 2.0
        assert queue_filter == ("urgent", "default")

    def test_monitor_commands_register(self):
        """Test that monitor commands register properly"""
        import click

        @click.group()
        def test_cli():
            pass

        monitor_commands = MonitorCommands()
        monitor_commands.register(test_cli)

        # Check that monitor command was added
        assert "monitor" in test_cli.commands


class TestDashboard:
    """Test Dashboard class functionality"""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings for testing"""
        from rrq.settings import RRQSettings

        return RRQSettings(redis_dsn="redis://localhost:6379/9")

    @pytest.fixture
    def dashboard(self, mock_settings):
        """Dashboard instance for testing"""
        return Dashboard(mock_settings, 1.0, ())

    @pytest.fixture
    def dashboard_with_filter(self, mock_settings):
        """Dashboard instance with queue filter"""
        return Dashboard(mock_settings, 0.5, ("urgent", "default"))

    def test_dashboard_init(self, dashboard):
        """Test dashboard initialization"""
        assert dashboard.refresh_interval == 1.0
        assert dashboard.queue_filter is None
        assert dashboard.job_store is None
        assert isinstance(dashboard.queue_sizes, defaultdict)
        assert isinstance(dashboard.processing_rates, defaultdict)
        assert isinstance(dashboard.error_counts, defaultdict)

    def test_dashboard_init_with_filter(self, dashboard_with_filter):
        """Test dashboard initialization with queue filter"""
        assert dashboard_with_filter.refresh_interval == 0.5
        assert dashboard_with_filter.queue_filter == ["urgent", "default"]

    def test_create_layout(self, dashboard):
        """Test dashboard layout creation"""
        layout = dashboard.create_layout()

        assert layout.name == "root"
        assert layout.get("header") is not None
        assert layout.get("main") is not None
        assert layout.get("footer") is not None
        assert layout["main"].get("queues") is not None
        assert layout["main"].get("workers") is not None

    @pytest.mark.asyncio
    @patch("rrq.cli_commands.commands.monitor.get_job_store")
    async def test_update_metrics(self, mock_get_job_store, dashboard):
        """Test metrics updating"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.redis = MagicMock()

        # Mock scan_iter for queues
        async def mock_queue_scan(match=None):
            queue_keys = [b"rrq:queue:urgent", b"rrq:queue:default"]
            for key in queue_keys:
                yield key

        # Mock scan_iter for workers
        async def mock_worker_scan(match=None):
            worker_keys = [b"rrq:health:worker_001"]
            for key in worker_keys:
                yield key

        # Mock scan_iter for jobs
        async def mock_job_scan(match=None):
            job_keys = [b"rrq:job:job_001"]
            for key in job_keys:
                yield key

        # Mock different scan calls
        def mock_scan_iter(match=None):
            if "queue" in match:
                return mock_queue_scan(match)
            elif "health" in match:
                return mock_worker_scan(match)
            elif "job" in match:
                return mock_job_scan(match)
            return iter([])

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=5)
        mock_store.redis.llen = AsyncMock(return_value=0)

        # Mock job data for recent jobs
        job_data = {
            b"function_name": b"test_function",
            b"status": b"completed",
            b"created_at": b"1234567890.0",
        }
        job_data_dict = {
            "function_name": "test_function",
            "status": "completed",
            "created_at": "1234567890.0",
        }
        mock_store.redis.hgetall = AsyncMock(return_value=job_data)
        mock_store.get_job_data_dict = AsyncMock(return_value=job_data_dict)

        # Mock worker health
        worker_health = {
            "status": "running",
            "active_jobs": 2,
            "queues": ["urgent"],
            "timestamp": datetime.now().timestamp(),
        }
        mock_store.get_worker_health = AsyncMock(return_value=(worker_health, 60))

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store
        dashboard.job_store = mock_store

        # Update metrics
        await dashboard.update_metrics()

        # Verify data was collected
        assert "urgent" in dashboard.queue_sizes
        assert "default" in dashboard.queue_sizes
        assert len(dashboard.workers) == 1
        assert "worker_001" in dashboard.workers[0]["id"]  # May include prefix

    @pytest.mark.asyncio
    @patch("rrq.cli_commands.commands.monitor.get_job_store")
    async def test_update_metrics_with_filter(
        self, mock_get_job_store, dashboard_with_filter
    ):
        """Test metrics updating with queue filter"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.redis = MagicMock()

        # Mock scan_iter for queues
        async def mock_queue_scan(match=None):
            queue_keys = [
                b"rrq:queue:urgent",  # Should be included
                b"rrq:queue:default",  # Should be included
                b"rrq:queue:low_priority",  # Should be filtered out
            ]
            for key in queue_keys:
                yield key

        # Mock scan_iter for workers
        async def mock_worker_scan(match=None):
            return
            yield  # Make it an async generator

        # Mock scan_iter for jobs
        async def mock_job_scan(match=None):
            return
            yield  # Make it an async generator

        # Mock different scan calls
        def mock_scan_iter(match=None):
            if "queue" in match:
                return mock_queue_scan(match)
            elif "health" in match:
                return mock_worker_scan(match)
            elif "job" in match:
                return mock_job_scan(match)
            return iter([])

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=3)
        mock_store.redis.llen = AsyncMock(return_value=0)

        # Mock empty worker scan
        mock_store.get_worker_health = AsyncMock(return_value=(None, None))

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store
        dashboard_with_filter.job_store = mock_store

        # Update metrics
        await dashboard_with_filter.update_metrics()

        # Verify only filtered queues were included
        assert "urgent" in dashboard_with_filter.queue_sizes
        assert "default" in dashboard_with_filter.queue_sizes
        assert "low_priority" not in dashboard_with_filter.queue_sizes

    @pytest.mark.asyncio
    async def test_get_recent_jobs(self, dashboard):
        """Test getting recent jobs"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.redis = MagicMock()

        # Mock scan_iter for jobs
        async def mock_job_scan(match=None):
            job_keys = [b"rrq:job:job_001", b"rrq:job:job_002"]
            for key in job_keys:
                yield key

        mock_store.redis.scan_iter = mock_job_scan

        # Mock job data
        job_data_1 = {
            b"function_name": b"test_function",
            b"status": b"completed",
            b"started_at": b"1234567890.0",
            b"completed_at": b"1234567895.0",
        }
        job_data_2 = {
            b"function_name": b"send_email",
            b"status": b"failed",
            b"started_at": b"1234567880.0",
            b"completed_at": b"1234567885.0",
        }
        job_data_dict_1 = {
            "function_name": "test_function",
            "status": "completed",
            "started_at": "1234567890.0",
            "completed_at": "1234567895.0",
        }
        job_data_dict_2 = {
            "function_name": "send_email",
            "status": "failed",
            "started_at": "1234567880.0",
            "completed_at": "1234567885.0",
        }

        mock_store.redis.hgetall = AsyncMock(side_effect=[job_data_1, job_data_2])
        mock_store.get_job_data_dict = AsyncMock(
            side_effect=[job_data_dict_1, job_data_dict_2]
        )

        dashboard.job_store = mock_store

        # Get recent jobs
        recent_jobs = await dashboard._get_recent_jobs(limit=5)

        assert len(recent_jobs) == 2
        assert recent_jobs[0]["function"] == "test_function"  # Most recent first
        assert recent_jobs[1]["function"] == "send_email"

    def test_create_header(self, dashboard):
        """Test header panel creation"""
        from rich.panel import Panel

        header = dashboard._create_header()

        assert isinstance(header, Panel)

    def test_create_queue_stats(self, dashboard):
        """Test queue statistics panel creation"""
        from rich.panel import Panel

        # Add some test data
        dashboard.queue_sizes["urgent"].extend([5, 6, 4])
        dashboard.queue_sizes["default"].extend([2, 2, 3])

        stats_panel = dashboard._create_queue_stats()

        assert isinstance(stats_panel, Panel)

    def test_create_queue_chart(self, dashboard):
        """Test queue chart panel creation"""
        from rich.panel import Panel

        # Add some test data for sparklines
        dashboard.queue_sizes["urgent"].extend([1, 2, 3, 4, 5])
        dashboard.queue_sizes["default"].extend([10, 8, 6, 4, 2])

        chart_panel = dashboard._create_queue_chart()

        assert isinstance(chart_panel, Panel)

    def test_create_worker_list(self, dashboard):
        """Test worker list panel creation"""
        from rich.panel import Panel

        # Add test worker data
        dashboard.workers = [
            {
                "id": "worker_001",
                "status": "running",
                "active_jobs": 2,
                "last_heartbeat": datetime.now().timestamp(),
            },
            {
                "id": "worker_002",
                "status": "idle",
                "active_jobs": 0,
                "last_heartbeat": datetime.now().timestamp(),
            },
        ]

        worker_panel = dashboard._create_worker_list()

        assert isinstance(worker_panel, Panel)

    def test_create_worker_list_empty(self, dashboard):
        """Test worker list panel creation with no workers"""
        from rich.panel import Panel

        # No workers
        dashboard.workers = []

        worker_panel = dashboard._create_worker_list()

        assert isinstance(worker_panel, Panel)

    def test_create_recent_jobs(self, dashboard):
        """Test recent jobs panel creation"""
        from rich.panel import Panel

        # Add test job data
        dashboard.recent_jobs = [
            {
                "id": "job_001",
                "function": "test_function",
                "status": "completed",
                "started_at": 1234567890.0,
                "completed_at": 1234567895.0,
            },
            {
                "id": "job_002",
                "function": "send_email",
                "status": "failed",
                "started_at": 1234567880.0,
                "completed_at": 1234567885.0,
            },
        ]

        jobs_panel = dashboard._create_recent_jobs()

        assert isinstance(jobs_panel, Panel)

    def test_create_recent_jobs_empty(self, dashboard):
        """Test recent jobs panel creation with no jobs"""
        from rich.panel import Panel

        # No recent jobs
        dashboard.recent_jobs = []

        jobs_panel = dashboard._create_recent_jobs()

        assert isinstance(jobs_panel, Panel)

    def test_create_footer(self, dashboard):
        """Test footer panel creation"""
        from rich.panel import Panel

        footer = dashboard._create_footer()

        assert isinstance(footer, Panel)

    def test_update_layout(self, dashboard):
        """Test layout updating"""
        layout = dashboard.create_layout()

        # Mock the internal components since we're not actually running the UI
        dashboard.queue_sizes["test_queue"].append(5)  # Add to deque
        dashboard.workers = [
            {
                "id": "worker_001",
                "status": "running",
                "active_jobs": 1,
                "heartbeat": 30,
                "last_heartbeat": 1234567890.0,
            }
        ]
        dashboard.recent_jobs = [
            {"id": "job_001", "function": "test_func", "status": "completed"}
        ]
        dashboard.dlq_stats = {"total_jobs": 0, "newest_error": None, "top_errors": {}}
        dashboard.dlq_errors = {}

        # This should not raise an exception
        dashboard.update_layout(layout)


class TestHybridMonitoring:
    """Test hybrid monitoring functionality"""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings for testing"""
        from rrq.settings import RRQSettings

        return RRQSettings(redis_dsn="redis://localhost:6379/9")

    @pytest.fixture
    def dashboard(self, mock_settings):
        """Dashboard instance for hybrid monitoring tests"""
        return Dashboard(mock_settings, 1.0, ())

    @pytest.mark.asyncio
    @patch("rrq.cli_commands.commands.monitor.get_job_store")
    async def test_hybrid_queue_metrics_success(self, mock_get_job_store, dashboard):
        """Test hybrid queue metrics when registry data is available"""
        # Mock job store with hybrid monitoring methods
        mock_store = MagicMock()
        mock_store.get_active_queues = AsyncMock(return_value=["urgent", "default"])
        mock_store.batch_get_queue_sizes = AsyncMock(
            return_value={"urgent": 5, "default": 2}
        )

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store
        dashboard.job_store = mock_store

        # Test hybrid queue metrics
        queue_data = await dashboard._update_queue_metrics_optimized()

        # Verify hybrid methods were called
        mock_store.get_active_queues.assert_called_once_with(max_age_seconds=300)
        mock_store.batch_get_queue_sizes.assert_called_once_with(["urgent", "default"])

        # Verify results
        assert queue_data == {"urgent": 5, "default": 2}

    @pytest.mark.asyncio
    @patch("rrq.cli_commands.commands.monitor.get_job_store")
    async def test_hybrid_queue_metrics_fallback(self, mock_get_job_store, dashboard):
        """Test hybrid queue metrics fallback to legacy scan"""
        # Mock job store with failing hybrid methods
        mock_store = MagicMock()
        mock_store.get_active_queues = AsyncMock(side_effect=Exception("Redis error"))
        mock_store.redis = MagicMock()

        # Mock legacy scan fallback
        async def mock_scan_iter(match=None, count=None):
            queue_keys = [b"rrq:queue:urgent", b"rrq:queue:default"]
            for key in queue_keys:
                yield key

        mock_store.redis.scan_iter = mock_scan_iter
        mock_store.redis.zcard = AsyncMock(return_value=3)

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store
        dashboard.job_store = mock_store

        # Test hybrid queue metrics with fallback
        queue_data = await dashboard._update_queue_metrics_optimized()

        # Verify legacy scan was used
        assert queue_data == {"urgent": 3, "default": 3}

    @pytest.mark.asyncio
    @patch("rrq.cli_commands.commands.monitor.get_job_store")
    async def test_hybrid_worker_metrics_success(self, mock_get_job_store, dashboard):
        """Test hybrid worker metrics when registry data is available"""
        # Mock job store with hybrid monitoring methods
        mock_store = MagicMock()
        mock_store.get_active_workers = AsyncMock(
            return_value=["worker_001", "worker_002"]
        )

        # Mock worker health data
        health_data = {
            "status": "running",
            "active_jobs": 2,
            "queues": ["urgent"],
            "timestamp": datetime.now().timestamp(),
        }
        mock_store.get_worker_health = AsyncMock(return_value=(health_data, 60))

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store
        dashboard.job_store = mock_store

        # Test hybrid worker metrics
        await dashboard._update_worker_metrics_optimized()

        # Verify hybrid methods were called
        mock_store.get_active_workers.assert_called_once_with(max_age_seconds=60)
        assert mock_store.get_worker_health.call_count == 2  # Called for each worker

        # Verify results
        assert len(dashboard.workers) == 2
        assert dashboard.workers[0]["id"] == "worker_001"
        assert dashboard.workers[0]["status"] == "running"
        assert dashboard.workers[1]["id"] == "worker_002"

    @pytest.mark.asyncio
    @patch("rrq.cli_commands.commands.monitor.get_job_store")
    async def test_hybrid_worker_metrics_fallback(self, mock_get_job_store, dashboard):
        """Test hybrid worker metrics fallback to legacy scan"""
        # Mock job store with failing hybrid methods
        mock_store = MagicMock()
        mock_store.get_active_workers = AsyncMock(side_effect=Exception("Redis error"))
        mock_store.redis = MagicMock()

        # Mock legacy scan fallback
        async def mock_scan_iter(match=None, count=None):
            worker_keys = [b"rrq:health:worker:worker_001"]
            for key in worker_keys:
                yield key

        mock_store.redis.scan_iter = mock_scan_iter

        # Mock worker health data
        health_data = {
            "status": "running",
            "active_jobs": 1,
            "queues": ["default"],
            "timestamp": datetime.now().timestamp(),
        }
        mock_store.get_worker_health = AsyncMock(return_value=(health_data, 30))

        # Mock get_job_store as async function
        async def async_get_job_store(settings):
            return mock_store

        mock_get_job_store.side_effect = async_get_job_store
        dashboard.job_store = mock_store

        # Test hybrid worker metrics with fallback
        await dashboard._update_worker_metrics_optimized()

        # Verify legacy scan was used
        assert len(dashboard.workers) == 1
        assert dashboard.workers[0]["id"] == "worker_001"

    @pytest.mark.asyncio
    async def test_process_monitoring_events(self, dashboard):
        """Test processing of monitoring events from Redis streams"""
        # Mock job store with event consumption
        mock_store = MagicMock()
        mock_event_data = [
            (
                b"rrq:monitor:events",
                [
                    (
                        b"1234567890-0",
                        {
                            b"event_type": b"queue_activity",
                            b"queue_name": b"urgent",
                            b"timestamp": b"1234567890.0",
                        },
                    ),
                    (
                        b"1234567891-0",
                        {
                            b"event_type": b"worker_heartbeat",
                            b"worker_id": b"worker_001",
                            b"timestamp": b"1234567891.0",
                        },
                    ),
                ],
            )
        ]
        mock_store.consume_monitor_events = AsyncMock(return_value=mock_event_data)
        mock_store.redis = MagicMock()
        mock_store.redis.zcard = AsyncMock(return_value=5)

        # Mock worker health for refresh
        health_data = {"status": "running", "active_jobs": 2, "timestamp": 1234567891.0}
        mock_store.get_worker_health = AsyncMock(return_value=(health_data, 60))

        dashboard.job_store = mock_store
        dashboard.workers = [{"id": "worker_001", "status": "idle"}]  # Initial state

        # Process monitoring events
        await dashboard._process_monitoring_events()

        # Verify event consumption was called
        mock_store.consume_monitor_events.assert_called_once_with(
            last_id="0", count=50, block=10
        )

        # Verify last event ID was updated
        assert dashboard._last_event_id == "1234567891-0"

        # Verify worker was refreshed
        assert dashboard.workers[0]["status"] == "running"
        assert dashboard.workers[0]["active_jobs"] == 2

    @pytest.mark.asyncio
    async def test_refresh_queue_size(self, dashboard):
        """Test immediate queue size refresh"""
        # Mock job store
        mock_store = MagicMock()
        mock_store.redis = MagicMock()
        mock_store.redis.zcard = AsyncMock(return_value=7)

        dashboard.job_store = mock_store

        # Refresh queue size
        await dashboard._refresh_queue_size("urgent")

        # Verify queue size was updated
        mock_store.redis.zcard.assert_called_once_with("rrq:queue:urgent")
        assert list(dashboard.queue_sizes["urgent"]) == [7]

    @pytest.mark.asyncio
    async def test_refresh_worker_status(self, dashboard):
        """Test immediate worker status refresh"""
        # Mock job store
        mock_store = MagicMock()
        health_data = {
            "status": "running",
            "active_jobs": 3,
            "queues": ["urgent", "default"],
            "timestamp": datetime.now().timestamp(),
        }
        mock_store.get_worker_health = AsyncMock(return_value=(health_data, 45))

        dashboard.job_store = mock_store
        dashboard.workers = [
            {"id": "worker_001", "status": "idle", "active_jobs": 0},
            {"id": "worker_002", "status": "running", "active_jobs": 1},
        ]

        # Refresh worker status
        await dashboard._refresh_worker_status("worker_001")

        # Verify worker was updated
        mock_store.get_worker_health.assert_called_once_with("worker_001")
        assert dashboard.workers[0]["status"] == "running"
        assert dashboard.workers[0]["active_jobs"] == 3
        assert dashboard.workers[0]["ttl"] == 45

        # Verify other worker was not affected
        assert dashboard.workers[1]["active_jobs"] == 1


class TestDashboardDataProcessing:
    """Test dashboard data processing logic"""

    def test_queue_trend_calculation(self):
        """Test queue trend calculation logic"""
        sizes = deque([3, 5, 4, 6, 5], maxlen=60)

        # Calculate trend
        if len(sizes) >= 2:
            diff = sizes[-1] - sizes[-2]
            if diff > 0:
                trend = "↑"
                trend_style = "red"
            elif diff < 0:
                trend = "↓"
                trend_style = "green"
            else:
                trend = "→"
                trend_style = "dim"

        # Last change was 6 -> 5, so should be decreasing
        assert trend == "↓"
        assert trend_style == "green"

    def test_processing_rate_calculation(self):
        """Test processing rate calculation"""
        sizes = deque([10, 9, 8, 7, 6, 5, 4, 3, 2, 1], maxlen=60)
        refresh_interval = 1.0

        if len(sizes) >= 10:
            # Average change over last 10 samples
            recent_changes = [sizes[i] - sizes[i - 1] for i in range(-9, 0)]
            avg_change = sum(recent_changes) / len(recent_changes)

            if avg_change < 0:  # Negative means jobs are being processed
                rate = abs(avg_change * 60 / refresh_interval)
            else:
                rate = 0

        # Should calculate approximately 1 job/min
        assert rate == 60.0

    def test_sparkline_generation(self):
        """Test sparkline generation logic"""
        sizes = [1, 2, 3, 4, 5]
        max_val = max(sizes)
        min_val = min(sizes)

        sparkline = ""
        spark_chars = " ▁▂▃▄▅▆▇█"

        for val in sizes:
            normalized = (val - min_val) / (max_val - min_val)
            idx = int(normalized * (len(spark_chars) - 1))
            sparkline += spark_chars[idx]

        # Should generate increasing sparkline
        assert len(sparkline) == 5
        assert sparkline[0] == " "  # Minimum value
        assert sparkline[-1] == "█"  # Maximum value

    def test_worker_status_colors(self):
        """Test worker status color mapping"""
        status_colors = {
            "running": "green",
            "idle": "yellow",
            "stopped": "red",
            "initializing": "blue",
        }

        # Test all status mappings
        assert status_colors.get("running") == "green"
        assert status_colors.get("idle") == "yellow"
        assert status_colors.get("stopped") == "red"
        assert status_colors.get("initializing") == "blue"
        assert status_colors.get("unknown", "white") == "white"
