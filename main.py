# main.py
from __future__ import annotations

import asyncio

from smart_common.smart_logging.task_logging import install_task_logger

install_task_logger()

from app.lifecycle import run


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
