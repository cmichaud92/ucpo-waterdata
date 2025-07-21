import logging
import logging.config
from pathlib import Path
import sys


def setup_logging(
        level: int = logging.INFO,
        log_to_file: bool = False,
        log_file_path: Path = Path("logs/app.log"),
        log_format: str = "%(asctime)s [%(levelname)s] %(message)s",
        date_format: str = "%Y-%m-%d %H:%M:%S"
        ) -> None:
    """
    Configures logging for the application.

    Parameters:
        level (int): Logging level (e.g., logging.INFO).
        log_to_file (bool): If True, also log to a file.
        log_file_path (Path): Path to log file.
        log_format (str): Format of log messages.
        date_format (str): Format for timestamps.
    """

    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "default"
        }
    }

    if log_to_file:
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        handlers["file"] = {
            "class": "logging.FileHandler",
            "filename": str(log_file_path),
            "formatter": "default",
            "mode": "a",
            "encoding": "utf-8"
        }

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": log_format,
                "datefmt": date_format
            }
        },
        "handlers": handlers,
        "root": {
            "level": level,
            "handlers": list(handlers.keys())
        }
    }

    logging.config.dictConfig(logging_config)
