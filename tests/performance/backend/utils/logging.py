"""
Logging utilities for the performance test backend.
Provides a consistent logging configuration across all modules.
"""
import logging
import sys
from contextvars import ContextVar
from typing import (
    Dict,
    Optional,
)

from neptune_api.types import Unset

request_id_ctx = ContextVar("request_id", default="-")


class RequestIdFilter(logging.Filter):
    """Filter that adds request_id to log records."""

    def __init__(self, name: str = ""):
        super().__init__(name)
        self.request_id = "-"

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "request_id"):
            record.request_id = request_id_ctx.get()
        return True


# Create a singleton instance for the whole application
request_id_filter = RequestIdFilter()

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
    """Set up a logger with the standard configuration for the application.

    This function ensures that each logger is only configured once to prevent
    duplicate log messages.

    Args:
        logger_name: The name for the logger
        level: The logging level to use (defaults to INFO if not specified)

    Returns:
        A configured logger instance
    """
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
    formatter = logging.Formatter(
        fmt=("%(asctime)s.%(msecs)03d | %(levelname)s | %(name)s | [%(request_id)s] | " "%(message)s"),
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Configure handler
    handler = logging.StreamHandler(sys.stdout)
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
