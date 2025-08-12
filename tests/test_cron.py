"""Tests for the cron functionality in RRQ."""

from datetime import timezone, datetime

import pytest

from rrq.cron import CronJob, CronSchedule


class TestCronSchedule:
    """Test the CronSchedule parsing and next_after functionality."""

    def test_parse_simple_expressions(self):
        """Test parsing of basic cron expressions."""
        # Every minute
        schedule = CronSchedule("* * * * *")
        assert schedule.minutes == list(range(0, 60))
        assert schedule.hours == list(range(0, 24))
        assert schedule.dom == list(range(1, 32))
        assert schedule.months == list(range(1, 13))
        assert schedule.dow == list(range(0, 7))

        # Specific values
        schedule = CronSchedule("30 14 1 6 1")
        assert schedule.minutes == [30]
        assert schedule.hours == [14]
        assert schedule.dom == [1]
        assert schedule.months == [6]
        assert schedule.dow == [1]

    def test_parse_ranges(self):
        """Test parsing of range expressions."""
        schedule = CronSchedule("0-5 8-17 1-15 * mon-fri")
        assert schedule.minutes == [0, 1, 2, 3, 4, 5]
        assert schedule.hours == [8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
        assert schedule.dom == list(range(1, 16))
        assert schedule.dow == [1, 2, 3, 4, 5]  # mon-fri

    def test_parse_lists(self):
        """Test parsing of comma-separated lists."""
        schedule = CronSchedule("0,15,30,45 9,17 * * mon,wed,fri")
        assert schedule.minutes == [0, 15, 30, 45]
        assert schedule.hours == [9, 17]
        assert schedule.dow == [1, 3, 5]  # mon, wed, fri

    def test_parse_step_values(self):
        """Test parsing of step values with /."""
        schedule = CronSchedule("*/15 */2 * * *")
        assert schedule.minutes == [0, 15, 30, 45]
        assert schedule.hours == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]

    def test_parse_month_names(self):
        """Test parsing of month names."""
        schedule = CronSchedule("0 0 1 jan,jun,dec *")
        assert schedule.months == [1, 6, 12]

    def test_parse_weekday_names(self):
        """Test parsing of weekday names."""
        schedule = CronSchedule("0 9 * * sun,tue,thu")
        assert schedule.dow == [0, 2, 4]  # sun, tue, thu

    def test_weekday_7_converts_to_0(self):
        """Test that weekday 7 (Sunday) converts to 0."""
        schedule = CronSchedule("0 0 * * 7")
        assert schedule.dow == [0]

    def test_invalid_expressions(self):
        """Test that invalid expressions raise ValueError."""
        with pytest.raises(ValueError, match="must have 5 fields"):
            CronSchedule("* * *")

        with pytest.raises(ValueError, match="out of range"):
            CronSchedule("60 * * * *")  # Invalid minute

        with pytest.raises(ValueError, match="out of range"):
            CronSchedule("* 25 * * *")  # Invalid hour

    def test_next_after_every_minute(self):
        """Test next_after for every minute schedule."""
        schedule = CronSchedule("* * * * *")
        now = datetime(2023, 6, 15, 10, 30, 45, tzinfo=timezone.utc)
        next_run = schedule.next_after(now)
        # Should be next minute boundary
        expected = datetime(2023, 6, 15, 10, 31, 0, tzinfo=timezone.utc)
        assert next_run == expected

    def test_next_after_specific_time(self):
        """Test next_after for specific time schedule."""
        schedule = CronSchedule("30 14 * * *")  # 2:30 PM daily
        now = datetime(2023, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
        next_run = schedule.next_after(now)
        expected = datetime(2023, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
        assert next_run == expected

    def test_next_after_crosses_day_boundary(self):
        """Test next_after when it needs to cross day boundary."""
        schedule = CronSchedule("30 9 * * *")  # 9:30 AM daily
        now = datetime(2023, 6, 15, 15, 0, 0, tzinfo=timezone.utc)  # 3 PM
        next_run = schedule.next_after(now)
        expected = datetime(2023, 6, 16, 9, 30, 0, tzinfo=timezone.utc)  # Next day
        assert next_run == expected

    def test_next_after_weekday_constraint(self):
        """Test next_after with weekday constraints."""
        schedule = CronSchedule("0 9 * * mon")  # 9 AM on Mondays
        # Start on a Friday
        now = datetime(2023, 6, 16, 10, 0, 0, tzinfo=timezone.utc)  # Friday
        next_run = schedule.next_after(now)
        # Should be next Monday
        expected = datetime(2023, 6, 19, 9, 0, 0, tzinfo=timezone.utc)  # Monday
        assert next_run == expected


class TestCronJob:
    """Test the CronJob model and functionality."""

    def test_cron_job_creation(self):
        """Test basic CronJob creation."""
        job = CronJob(
            function_name="test_task",
            schedule="0 9 * * *",
            args=["arg1", "arg2"],
            kwargs={"key": "value"},
            queue_name="custom_queue",
            unique=True,
        )

        assert job.function_name == "test_task"
        assert job.schedule == "0 9 * * *"
        assert job.args == ["arg1", "arg2"]
        assert job.kwargs == {"key": "value"}
        assert job.queue_name == "custom_queue"
        assert job.unique is True
        assert job._cron is not None  # Should be initialized in model_post_init

    def test_cron_job_defaults(self):
        """Test CronJob with default values."""
        job = CronJob(function_name="test_task", schedule="* * * * *")

        assert job.args == []
        assert job.kwargs == {}
        assert job.queue_name is None
        assert job.unique is False
        assert job.next_run_time is None

    def test_schedule_next(self):
        """Test the schedule_next method."""
        job = CronJob(function_name="test_task", schedule="30 14 * * *")
        now = datetime(2023, 6, 15, 10, 0, 0, tzinfo=timezone.utc)

        job.schedule_next(now)

        expected = datetime(2023, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
        assert job.next_run_time == expected

    def test_due_method_schedules_if_needed(self):
        """Test that due() schedules next run time if not set."""
        job = CronJob(function_name="test_task", schedule="* * * * *")
        now = datetime(2023, 6, 15, 10, 30, 0, tzinfo=timezone.utc)

        # next_run_time should be None initially
        assert job.next_run_time is None

        # Calling due() should schedule the next run
        is_due = job.due(now)

        # Should not be due immediately (next run is in the future)
        assert not is_due
        assert job.next_run_time is not None
        assert job.next_run_time > now

    def test_due_method_with_past_time(self):
        """Test due() method when next_run_time is in the past."""
        job = CronJob(function_name="test_task", schedule="* * * * *")
        past_time = datetime(2023, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
        now = datetime(2023, 6, 15, 10, 30, 0, tzinfo=timezone.utc)

        job.next_run_time = past_time

        is_due = job.due(now)
        assert is_due

    def test_due_method_with_future_time(self):
        """Test due() method when next_run_time is in the future."""
        job = CronJob(function_name="test_task", schedule="* * * * *")
        future_time = datetime(2023, 6, 15, 11, 0, 0, tzinfo=timezone.utc)
        now = datetime(2023, 6, 15, 10, 30, 0, tzinfo=timezone.utc)

        job.next_run_time = future_time

        is_due = job.due(now)
        assert not is_due

    def test_invalid_schedule_raises_error(self):
        """Test that invalid schedule raises error during creation."""
        with pytest.raises(ValueError):
            CronJob(function_name="test_task", schedule="invalid schedule")


class TestCronScheduleEdgeCases:
    """Test edge cases for cron schedule parsing and execution."""

    def test_february_29_leap_year(self):
        """Test handling of February 29 in leap years."""
        schedule = CronSchedule("0 0 29 2 *")  # Feb 29
        # In a non-leap year, should skip to next occurrence
        now = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)  # 2023 is not a leap year
        next_run = schedule.next_after(now)
        # Should be Feb 29, 2024 (next leap year)
        expected = datetime(2024, 2, 29, 0, 0, 0, tzinfo=timezone.utc)
        assert next_run == expected

    def test_day_of_month_and_weekday_interaction(self):
        """Test the interaction between day-of-month and day-of-week."""
        # Run on the 15th OR on Fridays (OR logic when both are specified)
        schedule = CronSchedule("0 9 15 * fri")

        # Test when 15th is not a Friday
        now = datetime(2023, 6, 10, 0, 0, 0, tzinfo=timezone.utc)  # June 10, 2023 (Saturday)
        next_run = schedule.next_after(now)

        # Should be June 15th (Thursday) since it comes before the next Friday
        expected = datetime(2023, 6, 15, 9, 0, 0, tzinfo=timezone.utc)
        assert next_run == expected

    def test_end_of_month_handling(self):
        """Test handling of end-of-month dates."""
        schedule = CronSchedule("0 0 31 * *")  # 31st of month

        # Start in February (which doesn't have 31 days)
        now = datetime(2023, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        next_run = schedule.next_after(now)

        # Should be March 31st
        expected = datetime(2023, 3, 31, 0, 0, 0, tzinfo=timezone.utc)
        assert next_run == expected

    def test_step_values_with_ranges(self):
        """Test step values combined with ranges."""
        schedule = CronSchedule("0 8-17/2 * * *")  # Every 2 hours from 8 AM to 5 PM
        expected_hours = [8, 10, 12, 14, 16]
        assert schedule.hours == expected_hours

    def test_complex_expression(self):
        """Test a complex real-world cron expression."""
        # Run at 9:30 AM on weekdays in Q1 and Q4
        schedule = CronSchedule("30 9 * jan,feb,mar,oct,nov,dec mon-fri")

        assert schedule.minutes == [30]
        assert schedule.hours == [9]
        assert schedule.months == [1, 2, 3, 10, 11, 12]
        assert schedule.dow == [1, 2, 3, 4, 5]  # mon-fri
