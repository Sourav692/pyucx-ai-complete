"""
Logging Setup for the PyUCX-AI Multi-Agent Framework.

This module provides centralized logging configuration with support
for different output formats, levels, and destinations.
"""

import os
import sys
import logging
import logging.handlers
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime


class PyUCXFormatter(logging.Formatter):
    """Custom formatter for PyUCX-AI framework logs."""

    def __init__(self):
        """Initialize the formatter with colors and structure."""

        # Color codes for different log levels
        self.colors = {
            'DEBUG': '\033[36m',      # Cyan
            'INFO': '\033[32m',       # Green  
            'WARNING': '\033[33m',    # Yellow
            'ERROR': '\033[31m',      # Red
            'CRITICAL': '\033[35m'    # Magenta
        }
        self.reset_color = '\033[0m'

        # Check if we're outputting to a terminal
        self.use_colors = sys.stderr.isatty()

        super().__init__()

    def format(self, record):
        """Format the log record with colors and structure."""

        # Create the base format
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')

        # Add agent context if available
        agent_info = ""
        if hasattr(record, 'agent_name'):
            agent_info = f"[{record.agent_name}] "

        # Color the level name
        level_name = record.levelname
        if self.use_colors and level_name in self.colors:
            colored_level = f"{self.colors[level_name]}{level_name}{self.reset_color}"
        else:
            colored_level = level_name

        # Format the message
        message = record.getMessage()

        # Combine all parts
        formatted_message = f"{timestamp} | {colored_level:8s} | {agent_info}{record.name} | {message}"

        # Add exception info if present
        if record.exc_info:
            formatted_message += "\n" + self.formatException(record.exc_info)

        return formatted_message


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    console_output: bool = True,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Setup centralized logging for the PyUCX-AI framework.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        console_output: Whether to output to console
        max_file_size: Maximum size of log file before rotation
        backup_count: Number of backup log files to keep

    Returns:
        Configured logger instance
    """

    # Convert string level to logging constant
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create custom formatter
    formatter = PyUCXFormatter()

    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # File handler with rotation
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(level)

        # Use a simpler formatter for file output (no colors)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Set up specific loggers for framework components
    setup_component_loggers(level)

    # Log the setup completion
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized - Level: {log_level}, File: {log_file or 'None'}")

    return root_logger


def setup_component_loggers(level: int):
    """Setup loggers for specific framework components."""

    # Component-specific loggers
    components = [
        'pyucx_ai.core',
        'pyucx_ai.agents',
        'pyucx_ai.utils',
        'pyucx_ai.main'
    ]

    for component in components:
        logger = logging.getLogger(component)
        logger.setLevel(level)
        logger.propagate = True  # Inherit from root logger


def get_agent_logger(agent_name: str) -> logging.Logger:
    """
    Get a logger for a specific agent with enhanced context.

    Args:
        agent_name: Name of the agent (e.g., 'analyzer', 'planner')

    Returns:
        Logger instance with agent context
    """

    logger_name = f"pyucx_ai.agents.{agent_name}"
    logger = logging.getLogger(logger_name)

    # Add agent name to all log records
    old_factory = logging.getLogRecordFactory()

    def agent_record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.agent_name = agent_name.upper()
        return record

    logging.setLogRecordFactory(agent_record_factory)

    return logger


class LogContext:
    """Context manager for enhanced logging with additional context."""

    def __init__(self, logger: logging.Logger, context: Dict[str, Any]):
        """
        Initialize log context.

        Args:
            logger: Logger instance to enhance
            context: Additional context to add to log records
        """
        self.logger = logger
        self.context = context
        self.old_factory = None

    def __enter__(self):
        """Enter the context and modify log record factory."""
        self.old_factory = logging.getLogRecordFactory()

        def context_record_factory(*args, **kwargs):
            record = self.old_factory(*args, **kwargs)
            for key, value in self.context.items():
                setattr(record, key, value)
            return record

        logging.setLogRecordFactory(context_record_factory)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context and restore original log record factory."""
        if self.old_factory:
            logging.setLogRecordFactory(self.old_factory)


def log_function_call(logger: logging.Logger):
    """Decorator to log function calls with parameters and results."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            # Log function entry
            func_name = func.__name__
            logger.debug(f"Entering {func_name} with args={args}, kwargs={kwargs}")

            try:
                # Execute function
                result = func(*args, **kwargs)

                # Log successful completion
                logger.debug(f"Completed {func_name} successfully")
                return result

            except Exception as e:
                # Log exception
                logger.error(f"Error in {func_name}: {str(e)}", exc_info=True)
                raise

        return wrapper
    return decorator


def log_execution_time(logger: logging.Logger):
    """Decorator to log function execution time."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            import time

            start_time = time.time()
            func_name = func.__name__

            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(f"{func_name} completed in {execution_time:.2f} seconds")
                return result

            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"{func_name} failed after {execution_time:.2f} seconds: {str(e)}")
                raise

        return wrapper
    return decorator


def configure_third_party_loggers():
    """Configure logging levels for third-party libraries."""

    # Reduce verbosity of common third-party libraries
    third_party_configs = {
        'urllib3': logging.WARNING,
        'requests': logging.WARNING,
        'openai': logging.WARNING,
        'httpx': logging.WARNING,
        'httpcore': logging.WARNING,
        'langchain': logging.INFO,
        'langgraph': logging.INFO
    }

    for library, level in third_party_configs.items():
        logging.getLogger(library).setLevel(level)


def create_progress_logger(name: str, total_items: Optional[int] = None) -> logging.Logger:
    """
    Create a logger optimized for progress reporting.

    Args:
        name: Name of the progress logger
        total_items: Total number of items to process (for percentage calculation)

    Returns:
        Logger configured for progress reporting
    """

    logger = logging.getLogger(f"pyucx_ai.progress.{name}")

    class ProgressAdapter(logging.LoggerAdapter):
        """Adapter to add progress context to log messages."""

        def __init__(self, logger, total_items):
            super().__init__(logger, {})
            self.total_items = total_items
            self.current_item = 0

        def progress(self, message: str, increment: bool = True):
            """Log a progress message."""
            if increment:
                self.current_item += 1

            if self.total_items:
                percentage = (self.current_item / self.total_items) * 100
                progress_msg = f"[{percentage:5.1f}%] ({self.current_item}/{self.total_items}) {message}"
            else:
                progress_msg = f"({self.current_item}) {message}"

            self.info(progress_msg)

        def complete(self, message: str = "Processing completed"):
            """Log completion message."""
            if self.total_items:
                self.info(f"[100.0%] ({self.total_items}/{self.total_items}) {message}")
            else:
                self.info(f"({self.current_item}) {message}")

    return ProgressAdapter(logger, total_items)


# Convenience function for quick setup
def quick_setup(log_level: str = "INFO", log_file: Optional[str] = "pyucx_ai.log"):
    """
    Quick logging setup with sensible defaults.

    Args:
        log_level: Logging level
        log_file: Log file path (None to disable file logging)
    """

    setup_logging(
        log_level=log_level,
        log_file=log_file,
        console_output=True
    )

    # Configure third-party loggers
    configure_third_party_loggers()

    # Return the main framework logger
    return logging.getLogger("pyucx_ai")
