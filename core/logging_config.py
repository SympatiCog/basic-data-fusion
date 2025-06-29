"""
Logging configuration for Basic Data Fusion.

This module provides centralized logging configuration with proper
formatting, log levels, and file output options.
"""

import logging
import os
from pathlib import Path
from typing import Optional


def setup_logging(
    level: str = 'INFO',
    log_file: Optional[str] = None,
    log_dir: Optional[str] = None,
    format_string: Optional[str] = None
) -> None:
    """
    Set up logging configuration for the application.
    
    Args:
        level: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
        log_file: Name of log file (optional)
        log_dir: Directory for log files (defaults to 'logs')
        format_string: Custom format string (optional)
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Default format string
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create formatter
    formatter = logging.Formatter(format_string)
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        if log_dir is None:
            log_dir = 'logs'
        
        # Create log directory if it doesn't exist
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        # Full path to log file
        log_path = os.path.join(log_dir, log_file)
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
        logging.info(f"Logging to file: {log_path}")
    
    logging.info(f"Logging configured with level: {level}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def set_log_level(level: str) -> None:
    """
    Set the log level for all loggers.
    
    Args:
        level: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger().setLevel(numeric_level)
    
    # Update all handlers
    for handler in logging.getLogger().handlers:
        handler.setLevel(numeric_level)
    
    logging.info(f"Log level set to: {level}")


def add_file_handler(
    log_file: str,
    log_dir: str = 'logs',
    level: str = 'INFO',
    format_string: Optional[str] = None
) -> None:
    """
    Add a file handler to the root logger.
    
    Args:
        log_file: Name of log file
        log_dir: Directory for log files
        level: Logging level for this handler
        format_string: Custom format string (optional)
    """
    # Create log directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Full path to log file
    log_path = os.path.join(log_dir, log_file)
    
    # Create handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Set formatter
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(format_string)
    file_handler.setFormatter(formatter)
    
    # Add to root logger
    logging.getLogger().addHandler(file_handler)
    
    logging.info(f"Added file handler: {log_path}")


# Initialize basic logging on module import
setup_logging()