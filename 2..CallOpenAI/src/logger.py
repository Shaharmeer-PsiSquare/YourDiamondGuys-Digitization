"""
Logging configuration module.

This module provides centralized logging configuration for the application,
with appropriate log levels and formatting.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logging(
    level: str = "INFO", log_file: Optional[str] = None, log_to_console: bool = True
) -> None:
    """
    Configure logging for the application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file. If provided, logs will be written to file
        log_to_console: Whether to log to console (default: True)
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    simple_formatter = logging.Formatter(fmt="%(levelname)s: %(message)s")

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Remove existing handlers
    root_logger.handlers.clear()

    # Add console handler if requested
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)

        # Use simple formatter for INFO and above, detailed for DEBUG
        if numeric_level <= logging.DEBUG:
            console_handler.setFormatter(detailed_formatter)
        else:
            console_handler.setFormatter(simple_formatter)

        root_logger.addHandler(console_handler)

    # Add file handler if log file is specified
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(detailed_formatter)

        root_logger.addHandler(file_handler)

    # Log initial message
    root_logger.info(f"Logging configured at {level} level")
    if log_file:
        root_logger.info(f"Logging to file: {log_file}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.

    Args:
        name: Name of the logger (typically __name__ of the module)

    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)


def create_log_filename(prefix: str = "app") -> str:
    """
    Create a timestamped log filename.

    Args:
        prefix: Prefix for the log filename

    Returns:
        str: Log filename with timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}.log"


def set_module_log_level(module_name: str, level: str) -> None:
    """
    Set log level for a specific module.

    Args:
        module_name: Name of the module
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger = logging.getLogger(module_name)
    logger.setLevel(numeric_level)


# Suppress verbose logging from third-party libraries
def suppress_third_party_logs() -> None:
    """
    Suppress verbose logging from third-party libraries.
    """
    # Suppress urllib3 debug logs
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Suppress requests debug logs
    logging.getLogger("requests").setLevel(logging.WARNING)

    # Suppress other common noisy loggers
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
