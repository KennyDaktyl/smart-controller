# app/lifecycle.py
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from smart_common.nats.client import nats_client
from smart_common.smart_logging import setup_logging
from app.scheduler.scheduler import Scheduler

logger = logging.getLogger("lifecycle")


async def run() -> None:
    setup_logging()

    logger.info("=== LIFECYCLE START ===")
    logger.info("Python=%s", sys.version)
    logger.info("Event loop=%s", asyncio.get_running_loop())

    settings = _load_settings()
    logger.info("Scheduler settings loaded: %s", settings)

    scheduler = Scheduler(**settings)

    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def _handle_shutdown(sig: signal.Signals) -> None:
        logger.warning(
            "Shutdown signal received",
            extra={
                "taskName": "lifecycle",
                "signal": sig.name if hasattr(sig, "name") else sig,
            },
        )
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda sig=sig: _handle_shutdown(sig))

    scheduler_task = asyncio.create_task(scheduler.run(), name="scheduler")

    def _scheduler_done(task: asyncio.Task) -> None:
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            logger.warning("Scheduler task cancelled")
            return

        if exc:
            logger.exception(
                "❌ Scheduler task CRASHED",
                exc_info=exc,
                extra={"taskName": "scheduler"},
            )
        else:
            logger.warning("⚠️ Scheduler task exited WITHOUT exception")

    scheduler_task.add_done_callback(_scheduler_done)

    logger.info("smart-controller started")
    logger.info("Scheduler task launched", extra={"taskName": "lifecycle"})

    # 🔎 watchdog – czy scheduler żyje
    async def _watchdog() -> None:
        while not shutdown_event.is_set():
            await asyncio.sleep(10)
            logger.info(
                "WATCHDOG: scheduler alive=%s state=%s",
                not scheduler_task.done(),
                scheduler_task._state,  # noqa
            )

    watchdog_task = asyncio.create_task(_watchdog(), name="scheduler-watchdog")

    await shutdown_event.wait()

    logger.warning("smart-controller shutdown requested")
    logger.info("Lifecycle initiating shutdown")

    await scheduler.stop()

    watchdog_task.cancel()
    await asyncio.gather(scheduler_task, return_exceptions=True)

    await nats_client.close()

    logger.info("Lifecycle shutdown complete")


def _load_settings() -> dict[str, float | int]:
    refresh_interval_sec = float(os.getenv("PROVIDER_REFRESH_INTERVAL_SEC", "60"))
    default_poll_interval_sec = float(
        os.getenv("PROVIDER_DEFAULT_POLL_INTERVAL_SEC", "60")
    )
    max_concurrency = int(os.getenv("PROVIDER_MAX_CONCURRENCY", "25"))

    logger.info(
        "ENV SETTINGS: refresh=%s default_poll=%s concurrency=%s",
        refresh_interval_sec,
        default_poll_interval_sec,
        max_concurrency,
    )

    return {
        "refresh_interval_sec": refresh_interval_sec,
        "default_poll_interval_sec": default_poll_interval_sec,
        "max_concurrency": max_concurrency,
    }
