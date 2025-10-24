"""
Structured logging utilities for ChillFlow using structlog.

Provides consistent, structured logging across all ChillFlow services with
JSON output for observability platforms like Grafana/Loki.
"""

import logging
from pathlib import Path
from typing import Optional

import structlog
from structlog.stdlib import LoggerFactory


def setup_console_logging(
    log_level: str = "INFO",
    format_string: Optional[str] = None,
    service_name: Optional[str] = None,
) -> None:
    """
    Setup structured console logging for ChillFlow services.

    Args:
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR)
        format_string: Custom format string. Default: colored structured output
        service_name: Service name for context (e.g., "trip-assembler")

    Example:
        from core.utils.logging import setup_console_logging

        # Basic setup
        setup_console_logging()

        # With debug level
        setup_console_logging(log_level="DEBUG")

        # With service context
        setup_console_logging(service_name="trip-assembler")
    """
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="ISO"),
            (
                structlog.dev.ConsoleRenderer(colors=True)
                if format_string is None
                else structlog.processors.KeyValueRenderer()
            ),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper())
        ),
        logger_factory=LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Get logger
    logger = structlog.get_logger()

    if service_name:
        logger = logger.bind(service=service_name)

    # Log initial setup message
    logger.info("ChillFlow logging initialized", level=log_level)


def setup_json_file_logging(
    service_name: str,
    log_file: Optional[Path] = None,
    log_level: str = "DEBUG",
) -> None:
    """
    Add JSON file handler for Grafana/Loki collection.

    Args:
        service_name: Name of the service (e.g., "trip-assembler")
        log_file: Path to JSON log file (default: /tmp/chillflow/{service_name}.jsonl)
        log_level: Minimum log level for file (default: DEBUG to capture everything)

    Example:
        from core.utils.logging import setup_json_file_logging

        setup_json_file_logging("trip-assembler")

        # Use structured logging
        logger = structlog.get_logger()
        logger.info("Trip processed", trip_key="abc123", fare=18.50)
    """
    if log_file is None:
        log_file = Path(f"/tmp/chillflow/{service_name}.jsonl")

    # Create log directory
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Configure JSON file logging
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper())
        ),
        logger_factory=LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Get logger with service context
    logger = structlog.get_logger().bind(service=service_name)

    # Log setup message
    logger.info("JSON file logging initialized", log_file=str(log_file))


def get_logger(service_name: Optional[str] = None) -> structlog.BoundLogger:
    """
    Get a structured logger instance.

    Args:
        service_name: Optional service name for context

    Returns:
        Configured structlog logger

    Example:
        from core.utils.logging import get_logger

        logger = get_logger("trip-assembler")
        logger.info("Processing trip", trip_id="abc123", status="started")
    """
    logger = structlog.get_logger()

    if service_name:
        logger = logger.bind(service=service_name)

    return logger


def setup_development_logging(service_name: str = "chillflow") -> None:
    """
    Setup development-friendly logging with both console and file output.

    Args:
        service_name: Service name for context

    Example:
        from core.utils.logging import setup_development_logging

        setup_development_logging("trip-assembler")
    """
    # Setup console logging
    setup_console_logging(log_level="DEBUG", service_name=service_name)

    # Setup JSON file logging
    setup_json_file_logging(service_name, log_level="DEBUG")

    # Get logger and log setup
    logger = get_logger(service_name)
    logger.info(
        "Development logging setup complete",
        console=True,
        json_file=True,
        service=service_name,
    )
