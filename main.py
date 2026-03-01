# main.py
from __future__ import annotations

import asyncio
import logging
import os
import sys
import platform

import sentry_sdk

from smart_common.smart_logging.task_logging import install_task_logger
from app.lifecycle import run

install_task_logger()

logger = logging.getLogger("bootstrap")


def _init_sentry() -> None:
    sentry_dsn = os.getenv("SENTRY_DSN")
    if not sentry_dsn:
        logger.info("Sentry disabled (SENTRY_DSN is not set)")
        return

    sentry_sdk.init(
        dsn=sentry_dsn,
        send_default_pii=True,
        environment=os.getenv("ENV", "development"),
    )
    logger.info("Sentry enabled for ENV=%s", os.getenv("ENV", "development"))


def main() -> None:
    logger.info("=== SMART CONTROLLER BOOTSTRAP START ===")
    logger.info("PID=%s", os.getpid())
    logger.info("Python=%s", sys.version)
    logger.info("Platform=%s", platform.platform())
    logger.info("CWD=%s", os.getcwd())
    logger.info("PYTHONPATH=%s", sys.path)

    # 🔎 smart_common visibility
    try:
        import smart_common

        logger.info("smart_common imported OK from %s", smart_common.__file__)
    except Exception:
        logger.exception("❌ smart_common IMPORT FAILED")

    # 🔎 DB layer visibility
    try:
        from smart_common.core.db import get_db

        logger.info("get_db imported OK: %s", get_db)
    except Exception:
        logger.exception("❌ get_db IMPORT FAILED")

    # 🔎 critical envs
    for key in [
        "DATABASE_URL",
        "REDIS_HOST",
        "REDIS_PORT",
        "NATS_URL",
        "SUBJECT",
        "LOG_LEVEL",
    ]:
        logger.info("ENV %s=%s", key, os.getenv(key))

    _init_sentry()

    logger.info("=== STARTING ASYNCIO LOOP ===")
    asyncio.run(run())


if __name__ == "__main__":
    main()
