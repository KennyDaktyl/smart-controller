import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta

class AdvancedRotatingFileHandler(TimedRotatingFileHandler):

    def __init__(self, base_log_dir, filename="app.log", retention_days=365):
        self.base_log_dir = base_log_dir
        self.retention_days = retention_days

        os.makedirs(self.base_log_dir, exist_ok=True)

        log_path = os.path.join(self.base_log_dir, filename)

        super().__init__(
            log_path,
            when="midnight",
            interval=1,
            backupCount=0,
            encoding="utf-8",
            utc=False,
        )

    def rotate(self, source, dest):
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")

        target_dir = os.path.join(self.base_log_dir, year, month)
        os.makedirs(target_dir, exist_ok=True)

        dest_file = os.path.join(
            target_dir,
            f"{now.strftime('%Y-%m-%d')}.log"
        )

        if os.path.exists(source):
            os.rename(source, dest_file)

        self._cleanup_old_logs()

    def _cleanup_old_logs(self):
        cutoff = datetime.now() - timedelta(days=self.retention_days)

        for year_dir in os.listdir(self.base_log_dir):
            year_path = os.path.join(self.base_log_dir, year_dir)
            if not os.path.isdir(year_path):
                continue

            for month_dir in os.listdir(year_path):
                month_path = os.path.join(year_path, month_dir)
                if not os.path.isdir(month_path):
                    continue

                for filename in os.listdir(month_path):
                    file_path = os.path.join(month_path, filename)
                    try:
                        file_time = datetime.strptime(filename.replace(".log", ""), "%Y-%m-%d")
                    except:
                        continue

                    if file_time < cutoff:
                        os.remove(file_path)
