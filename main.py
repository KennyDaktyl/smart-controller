# main.py
from __future__ import annotations

import asyncio
import logging
import os
import sys
import platform

from smart_common.smart_logging.task_logging import install_task_logger
from app.lifecycle import run

install_task_logger()

logger = logging.getLogger("bootstrap")


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

    logger.info("=== STARTING ASYNCIO LOOP ===")
    asyncio.run(run())


if __name__ == "__main__":
    main()
