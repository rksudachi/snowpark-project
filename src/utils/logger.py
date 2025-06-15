import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from pydantic_settings import BaseSettings


class Logger(BaseSettings):
    log_dir: Path = Path("logs")
    log_max_bytes: int = 10**6  # 1MB
    log_backup_count: int = 3
    log_min_level: str = "INFO"
    log_format: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    log_extension: str = ".log"
    log_file: str = "app"
    log_encoding: str = "utf-8"

    def get_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.log_file)
        # ハンドラがなければ追加
        if not logger.handlers:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            handler = RotatingFileHandler(
                self.log_dir / f"{self.log_file}{self.log_extension}",
                maxBytes=self.log_max_bytes,
                backupCount=self.log_backup_count,
                encoding=self.log_encoding,
            )
            handler.setFormatter(logging.Formatter(self.log_format))
            logger.addHandler(handler)
        # 毎回レベルを設定（外部から変更も反映される）
        logger.setLevel(
            getattr(logging, self.log_min_level.upper(), logging.INFO)
        )
        return logger
