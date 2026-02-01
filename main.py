# main.py
from __future__ import annotations

import asyncio
from app.lifecycle import run

from smart_common.smart_logging.task_logging import install_task_logger

install_task_logger()

import logging


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
