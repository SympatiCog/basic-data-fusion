"""
Custom exceptions for Basic Data Fusion.

This module defines application-specific exceptions that provide
clear error messages and context for different types of failures.
"""

from typing import Optional, Any


class DataFusionError(Exception):
    """Base exception for all Data Fusion application errors."""
    
    def __init__(self, message: str, context: Optional[dict] = None):
        super().__init__(message)
        self.message = message
        self.context = context or {}
    
    def __str__(self) -> str:
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{self.message} (Context: {context_str})"
        return self.message


class ConfigurationError(DataFusionError):
    """Raised when there are issues with configuration loading or validation."""
    
    def __init__(self, message: str, config_file: Optional[str] = None, field: Optional[str] = None):
        context = {}
        if config_file:
            context['config_file'] = config_file
        if field:
            context['field'] = field
        super().__init__(message, context)


class DatabaseError(DataFusionError):
    """Raised when there are database connection or query issues."""
    
    def __init__(self, message: str, query: Optional[str] = None, params: Optional[list] = None):
        context = {}
        if query:
            context['query'] = query
        if params:
            context['params'] = str(params)
        super().__init__(message, context)


class FileProcessingError(DataFusionError):
    """Raised when there are issues processing files."""
    
    def __init__(self, message: str, file_path: Optional[str] = None, operation: Optional[str] = None):
        context = {}
        if file_path:
            context['file_path'] = file_path
        if operation:
            context['operation'] = operation
        super().__init__(message, context)


class ValidationError(DataFusionError):
    """Raised when data validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Optional[Any] = None):
        context = {}
        if field:
            context['field'] = field
        if value is not None:
            context['value'] = str(value)
        super().__init__(message, context)


class SecurityError(DataFusionError):
    """Raised when security validation fails."""
    
    def __init__(self, message: str, security_check: Optional[str] = None, input_value: Optional[str] = None):
        context = {}
        if security_check:
            context['security_check'] = security_check
        if input_value:
            context['input_value'] = input_value
        super().__init__(message, context)


class QueryGenerationError(DataFusionError):
    """Raised when SQL query generation fails."""
    
    def __init__(self, message: str, query_type: Optional[str] = None, tables: Optional[list] = None):
        context = {}
        if query_type:
            context['query_type'] = query_type
        if tables:
            context['tables'] = tables
        super().__init__(message, context) 