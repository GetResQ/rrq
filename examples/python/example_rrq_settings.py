"""example_rrq_settings.py: Example RRQ Application Settings"""

import logging

from rrq.cron import CronJob
from rrq.settings import RRQSettings

logger = logging.getLogger("rrq")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

redis_dsn = "redis://localhost:6379/0"


# RRQ Settings
rrq_settings = RRQSettings(
    redis_dsn=redis_dsn,
    # Example cron jobs - these would run periodically when a worker is running
    cron_jobs=[
        # Run a cleanup task every day at 2 AM
        CronJob(
            function_name="daily_cleanup",
            schedule="0 2 * * *",
            args=["cleanup_logs"],
            kwargs={"max_age_days": 30},
        ),
        # Send a status report every Monday at 9 AM
        CronJob(
            function_name="send_status_report",
            schedule="0 9 * * mon",
            unique=True,  # Prevent duplicate reports if worker restarts
        ),
        # Health check every 15 minutes
        CronJob(
            function_name="health_check",
            schedule="*/15 * * * *",
            queue_name="monitoring",  # Use a specific queue for monitoring tasks
        ),
    ],
)
