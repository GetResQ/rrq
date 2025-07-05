"""example_rrq_settings.py: Example RRQ Application Settings"""

import asyncio
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


async def on_startup_hook():
    logger.info("Executing 'on_startup_hook' (application-specific startup)...")
    await asyncio.sleep(0.1)
    logger.info("'on_startup_hook' complete.")


async def on_shutdown_hook():
    logger.info("Executing 'on_shutdown_hook' (application-specific shutdown)...")
    await asyncio.sleep(0.1)
    logger.info("'on_shutdown_hook' complete.")


# RRQ Settings
rrq_settings = RRQSettings(
    redis_dsn=redis_dsn,
    on_startup=on_startup_hook,
    on_shutdown=on_shutdown_hook,
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
