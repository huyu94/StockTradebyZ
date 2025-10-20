from __future__ import annotations

import sys
import os
from loguru import logger

LOG_FILE = os.environ.get('LOG_FILE')

def init_logging(stdout_level: str = "INFO", file_path: str | None = None):
    logger.remove()
    logger.add(sys.stdout, level=stdout_level, format="{time:YYYY-MM-DD HH:mm:ss} [{level}] {file.name}:{line} {message}")
    if file_path:
        logger.add(file_path, level=stdout_level, rotation="10 MB", encoding="utf-8", colorize=False,
                   format="{time:YYYY-MM-DD HH:mm:ss} [{level}] {file.name}:{line} {message}")


__all__ = ["logger", "init_logging"]
