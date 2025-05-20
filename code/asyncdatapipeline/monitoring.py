from datetime import datetime
import logging
from typing import Dict, Any
from time import time


class LoggingFormatter(logging.Formatter):
    FORMAT = (
        "\033[38;5;244m%(asctime)s\033[0m"
        " | "
        "%(levelname)-7s"
        " | "
        "\033[38;5;214m%(name)s\033[0m"
        " : "
        "\033[38;5;111m%(message)s\033[0m"
    )

    LEVEL_COLORS = {
        "DEBUG": "\033[38;5;32m",
        "INFO": "\033[38;5;36m",
        "WARNING": "\033[38;5;221m",
        "ERROR": "\033[38;5;196m",
        "CRITICAL": "\033[48;5;196;38;5;231m",
    }

    def format(self, record: logging.LogRecord) -> str:
        """Config format"""
        levelname = record.levelname
        level_color = self.LEVEL_COLORS.get(levelname, "")
        record.levelname = f"{level_color}{levelname}\033[0m"
        record.asctime = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S.%f")
        return super().format(record)


class PipelineMonitor:
    """Observer for tracking pipeline metrics and logging events."""

    def __init__(self) -> None:
        self.metrics: Dict[str, Any] = {"throughput": 0, "latency": [], "errors": 0}
        self.logger = self.configure_logging("logger.log")

    def configure_logging(self, file_name: str, LOGGING_LEVEL: int = logging.INFO) -> logging.Logger:
        logger = logging.getLogger()
        logger.setLevel(LOGGING_LEVEL)
        file_handler = logging.FileHandler(file_name)
        file_handler.setLevel(LOGGING_LEVEL)

        # Create a console handler with a higher log level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(LOGGING_LEVEL)

        # Create a formatter and add it to the handlers
        default_formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] "
            "[%(funcName)s():%(lineno)s] [PID:%(process)d TID:%(thread)d] %(message)s",
            "%d/%m/%Y %H:%M:%S",
        )

        file_handler.setFormatter(default_formatter)
        console_handler.setFormatter(LoggingFormatter(LoggingFormatter.FORMAT))

        if logger.hasHandlers():
            logger.handlers.clear()

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger

    def log_debug(self, message: str) -> None:
        self.logger.debug(message)

    def log_event(self, message: str) -> None:
        self.logger.info(message)

    def log_warning(self, message: str) -> None:
        self.logger.warning(message)

    def log_error(self, message: str) -> None:
        self.logger.error(message)
        self.metrics["errors"] += 1

    def track_processing(self, start_time: float) -> None:
        latency = time() - start_time
        self.metrics["throughput"] += 1
        self.metrics["latency"].append(latency)

    def get_metrics(self) -> Dict[str, Any]:
        avg_latency = sum(self.metrics["latency"]) / len(self.metrics["latency"]) if self.metrics["latency"] else 0
        return {"throughput": self.metrics["throughput"], "avg_latency": avg_latency, "errors": self.metrics["errors"]}
