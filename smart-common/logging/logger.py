import logging
import sys
from datetime import datetime, timezone

from smart_common.core.config import settings
from smart_common.logging.custom_rotating_handler import AdvancedRotatingFileHandler

LOG_DIR = settings.LOG_DIR
FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
DATE = "%Y-%m-%d %H:%M:%S"

formatter = logging.Formatter(FORMAT, datefmt=DATE)

file_handler = AdvancedRotatingFileHandler(
    base_log_dir=LOG_DIR,
    filename="service.log",
    retention_days=365
)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

root = logging.getLogger()
root.setLevel(logging.INFO)
root.handlers = [file_handler, console_handler]

root.info("🚀 Logging initialized")
root.info(f"Logs directory: {LOG_DIR}")
root.info(f"Logger start time UTC: {datetime.now(timezone.utc).isoformat()}")
