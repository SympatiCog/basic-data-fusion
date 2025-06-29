"""
Core infrastructure module for Basic Data Fusion.

This module provides the foundational components including configuration management,
database connections, logging setup, and custom exceptions.
"""

from .config import DataConfig, UIConfig, StateConfig, Config
from .database import DatabaseManager
from .exceptions import DataFusionError, ConfigurationError, DatabaseError
from .logging_config import setup_logging

__all__ = [
    # Configuration
    'DataConfig',
    'UIConfig', 
    'StateConfig',
    'Config',
    
    # Database
    'DatabaseManager',
    
    # Exceptions
    'DataFusionError',
    'ConfigurationError',
    'DatabaseError',
    
    # Logging
    'setup_logging',
]

# Version info
__version__ = "1.0.0" 