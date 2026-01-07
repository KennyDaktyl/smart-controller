from __future__ import annotations

import asyncio
import logging
import os
import signal

from smart_common.smart_logging import setup_logging

from app.scheduler.scheduler import Scheduler

logger = logging.getLogger(__name__)


async def run() -> None:
    setup_logging()
    settings = _load_settings()
    scheduler = Scheduler(**settings)

    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    scheduler_task = asyncio.create_task(scheduler.run(), name="scheduler")
    logger.info("smart-controller started")

    await shutdown_event.wait()
    logger.info("smart-controller shutdown requested")
    await scheduler.stop()
    await asyncio.gather(scheduler_task, return_exceptions=True)


def _load_settings() -> dict[str, float | int]:
    refresh_interval_sec = float(os.getenv("PROVIDER_REFRESH_INTERVAL_SEC", "60"))
    default_poll_interval_sec = float(
        os.getenv("PROVIDER_DEFAULT_POLL_INTERVAL_SEC", "60")
    )
    max_concurrency = int(os.getenv("PROVIDER_MAX_CONCURRENCY", "25"))

    return {
        "refresh_interval_sec": refresh_interval_sec,
        "default_poll_interval_sec": default_poll_interval_sec,
        "max_concurrency": max_concurrency,
    }
