"""Logging configuration module for Guidewire Arrow.

This module provides centralized logging configuration for the Guidewire Arrow project.
It supports both file and stream logging with customizable formats and levels.
"""

import logging
from typing import Optional, List, Union
from pathlib import Path


def _logger_setup(
    log_level: int = logging.INFO,
    log_file: Optional[Union[str, Path]] = None,
    log_format: Optional[str] = None,
    logger_name: str = "guidewire",
) -> logging.Logger:
    """
    Configures logging for Guidewire Arrow modules.

    Args:
        log_level: Logging level (e.g., logging.INFO, logging.DEBUG).
        log_file: File path to save logs. Can be str or Path object.
        log_format: Custom format string for log messages.
        logger_name: Name of the logger to configure.

    Returns:
        logging.Logger: Configured logger instance.

    Raises:
        IOError: If log file cannot be created or written to.
    """
    handlers: List[logging.Handler] = []
    
    # Add file handler if log_file is specified
    if log_file:
        try:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(log_path))
        except IOError as e:
            raise IOError(f"Failed to create log file at {log_file}: {e}")

    # Always add stream handler
    handlers.append(logging.StreamHandler())

    # Use default format if none provided
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Configure the logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Add new handlers
    for handler in handlers:
        handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(handler)

    return logger


# Initialize default logger
logger = _logger_setup()
