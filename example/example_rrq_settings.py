'''example_rrq_settings.py: Example RRQ Application Settings'''

import asyncio
import logging

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
)

