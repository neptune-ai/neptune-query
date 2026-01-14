"""
Logging utilities for the performance_e2e test backend.
Provides a consistent logging configuration across all modules.
"""

import logging
import os
from contextvars import ContextVar
from typing import (
    Dict,
    Optional,
)

import colorlog

from neptune_query.generated.neptune_api.types import Unset

scenario_name_ctx = ContextVar("scenario_name", default="-")
request_id_ctx = ContextVar("request_id", default="-")

# Define default log file path and environment variable name
DEFAULT_LOG_FILE = "performance_test.log"
LOG_FILE_ENV_VAR = "NEPTUNE_PERFORMANCE_LOG_FILE"

# Define color scheme for different log levels
LOG_COLORS = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}


class RequestMetadataFilter(logging.Filter):
    """Filter that adds request_id to log records."""

    def __init__(self, name: str = ""):
        super().__init__(name)
        self.request_id = "-"
        self.scenario_name = "-"

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "request_id"):
            record.request_id = request_id_ctx.get()
        if not hasattr(record, "scenario_name"):
            record.scenario_name = scenario_name_ctx.get()
        return True


# Create a singleton instance for the whole application
request_id_filter = RequestMetadataFilter()

# Store configured loggers to prevent duplicate setup
_CONFIGURED_LOGGERS: Dict[str, logging.Logger] = {}

# Flag to track if root logger has been configured
_ROOT_LOGGER_CONFIGURED = False


def configure_root_logger() -> None:
    """Configure the root logger to use a NullHandler to prevent duplicate logging.
    This prevents unconfigured loggers from propagating to the root logger.
    """
    global _ROOT_LOGGER_CONFIGURED

    if not _ROOT_LOGGER_CONFIGURED:
        # Configure root logger with NullHandler to prevent propagation of messages
        root_logger = logging.getLogger()

        # Clear any existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Add null handler to prevent "No handlers could be found" warnings
        root_logger.addHandler(logging.NullHandler())

        # Set to WARNING level by default
        root_logger.setLevel(logging.WARNING)

        _ROOT_LOGGER_CONFIGURED = True


def setup_logger(logger_name: str, level: Optional[int] = None) -> logging.Logger:
    # Configure root logger first
    configure_root_logger()

    # Check if this logger was already configured
    if logger_name in _CONFIGURED_LOGGERS:
        return _CONFIGURED_LOGGERS[logger_name]

    # Get or create the logger
    logger = logging.getLogger(logger_name)

    # Set propagate to False to prevent duplicate logs
    logger.propagate = False

    # Clear any existing handlers to be safe
    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    # Configure formatter with request_id
    formatter = colorlog.ColoredFormatter(
        fmt="%(asctime)s.%(msecs)03d | %(scenario_name)s "
        "| %(log_color)s%(levelname)s%(reset)s | %(name)s | %(request_id)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors=LOG_COLORS,
    )

    # Determine log file path from environment variable or use default
    log_file_path = os.environ.get(LOG_FILE_ENV_VAR, DEFAULT_LOG_FILE)

    # Create directory if it doesn't exist (for cases where path includes directories)
    log_file_dir = os.path.dirname(log_file_path)
    if log_file_dir and not os.path.exists(log_file_dir):
        os.makedirs(log_file_dir)

    # Configure file handler instead of stdout
    handler = logging.FileHandler(log_file_path)
    handler.setFormatter(formatter)
    handler.addFilter(request_id_filter)
    logger.addHandler(handler)

    # Set log level (default to INFO if not specified)
    logger.setLevel(level if level is not None else logging.INFO)

    # Store the configured logger
    _CONFIGURED_LOGGERS[logger_name] = logger

    return logger


def map_unset_to_none(value):
    return None if isinstance(value, Unset) else value


def repr_list(lst):
    if len(lst) < 5:
        return str(lst)
    else:
        return str(lst[:5] + ["..."]) + f" (total {len(lst)})"
