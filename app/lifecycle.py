# app/lifecycle.py
from __future__ import annotations

import asyncio
import logging
import os
import signal

from smart_common.nats.client import nats_client
from smart_common.smart_logging import setup_logging

from app.scheduler.scheduler import Scheduler

logger = logging.getLogger(__name__)


async def run() -> None:
    setup_logging()
    settings = _load_settings()
    scheduler = Scheduler(**settings)

    logger.info("Lifecycle run started", extra={"taskName": "lifecycle"})
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    def _handle_shutdown(sig: signal.Signals) -> None:
        logger.info(
            "Shutdown signal received",
            extra={"taskName": "lifecycle", "signal": sig.name if hasattr(sig, "name") else sig},
        )
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda sig=sig: _handle_shutdown(sig))

    scheduler_task = asyncio.create_task(scheduler.run(), name="scheduler")
    logger.info("smart-controller started")
    logger.info("Scheduler task launched", extra={"taskName": "lifecycle"})

    await shutdown_event.wait()
    logger.info("smart-controller shutdown requested")
    logger.info("Lifecycle initiating shutdown", extra={"taskName": "lifecycle"})
    await scheduler.stop()
    await asyncio.gather(scheduler_task, return_exceptions=True)
    await nats_client.close()
    logger.info("Lifecycle shutdown complete", extra={"taskName": "lifecycle"})


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
