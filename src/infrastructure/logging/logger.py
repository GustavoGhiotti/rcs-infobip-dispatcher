import logging
import os
import threading
from logging.handlers import TimedRotatingFileHandler


_logger_lock = threading.Lock()


def get_logger(name: str = "infobip-robo") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    with _logger_lock:
        if logger.handlers:
            return logger

        logger.setLevel(logging.INFO)

        log_dir = os.getenv("LOG_DIR", "logs")
        log_path = os.path.join(log_dir, f"{name}.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)

        file_handler = TimedRotatingFileHandler(
            log_path,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        logger.propagate = False

    return logger
