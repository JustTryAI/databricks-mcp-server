"""
Logging utilities for the Databricks MCP server.
"""

import logging
import os
from typing import Optional

# Default log level from environment variable or INFO
DEFAULT_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Get a logger with the specified name and level.
    
    Args:
        name: The name of the logger
        level: Optional log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        A configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Set log level
    log_level = getattr(logging, level or DEFAULT_LOG_LEVEL)
    logger.setLevel(log_level)
    
    # Ensure there's at least one handler if none exists
    if not logger.handlers and not logger.parent.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger 