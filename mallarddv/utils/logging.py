"""
Logging utilities for MallardDataVault.
"""
import logging
from typing import Optional


def configure_logging(level: int = logging.INFO, log_file: Optional[str] = None):
    """
    Configure logging for MallardDataVault.
    
    Args:
        level: Logging level (default: logging.INFO)
        log_file: Optional file path to write logs to
    """
    logger = logging.getLogger("mallarddv")
    logger.setLevel(level)
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    
    # Add console handler to logger
    logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger