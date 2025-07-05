"""
Enhanced security utilities for SQL injection prevention.
"""
import re
from typing import List, Set, Optional


def sanitize_sql_identifier(identifier: str) -> str:
    """
    Sanitize SQL identifiers (table names, column names) to prevent injection.
    
    Args:
        identifier: Raw identifier that may contain malicious content
        
    Returns:
        Sanitized identifier safe for SQL use
    """
    if not identifier or not isinstance(identifier, str):
        return "safe_identifier"
    
    # Remove null bytes and control characters
    identifier = re.sub(r'[\x00-\x1f\x7f]', '', identifier)
    
    # Remove SQL injection characters
    identifier = re.sub(r'[\'"`\;\\\-\*/\(\)\[\]\{\}]', '', identifier)
    
    # Remove SQL comments
    identifier = re.sub(r'--.*$', '', identifier)
    identifier = re.sub(r'/\*.*?\*/', '', identifier)
    
    # Keep only alphanumeric and underscore
    identifier = re.sub(r'[^a-zA-Z0-9_]', '_', identifier)
    
    # Consolidate underscores
    identifier = re.sub(r'_+', '_', identifier)
    
    # Remove leading/trailing underscores
    identifier = identifier.strip('_')
    
    # Check for and neutralize SQL keywords
    sql_keywords = {
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 
        'UNION', 'WHERE', 'FROM', 'JOIN', 'HAVING', 'GROUP', 'ORDER', 'BY',
        'EXEC', 'EXECUTE', 'SCRIPT', 'TRUNCATE', 'MERGE', 'GRANT', 'REVOKE',
        'TABLE', 'DATABASE', 'INDEX', 'VIEW', 'PROCEDURE', 'FUNCTION'
    }
    
    # Only sanitize if the entire identifier is a SQL keyword
    # Don't modify legitimate column names that contain SQL keywords as parts
    if identifier.upper() in sql_keywords:
        identifier = f"safe_{identifier}"
    
    # Remove consecutive underscores again
    identifier = re.sub(r'_+', '_', identifier)
    identifier = identifier.strip('_')
    
    # Ensure not empty and doesn't start with number
    if not identifier or identifier[0].isdigit():
        identifier = f"safe_{identifier}" if identifier else "safe_identifier"
    
    # Limit length
    if len(identifier) > 64:  # Reasonable limit for SQL identifiers
        identifier = identifier[:64]
    
    return identifier


def validate_table_name(table_name: str, allowed_tables: Set[str]) -> Optional[str]:
    """
    Validate table name against whitelist.
    
    Args:
        table_name: Table name to validate
        allowed_tables: Set of allowed table names
        
    Returns:
        Original table name if valid, None if invalid
        Note: This returns the original name for file path construction
    """
    if not table_name or not isinstance(table_name, str):
        return None
    
    # Check against whitelist of allowed tables using original name
    if table_name in allowed_tables:
        return table_name
    
    # Also check if sanitized version matches any allowed table
    sanitized = sanitize_sql_identifier(table_name)
    if sanitized in allowed_tables:
        return sanitized
    
    return None


def validate_column_name(column_name: str, allowed_columns: Set[str]) -> Optional[str]:
    """
    Validate and sanitize column name against whitelist.
    
    Args:
        column_name: Column name to validate  
        allowed_columns: Set of allowed column names
        
    Returns:
        Sanitized column name if valid, None if invalid
    """
    if not column_name or not isinstance(column_name, str):
        return None
    
    # First sanitize the column name
    sanitized = sanitize_sql_identifier(column_name)
    
    # Check against whitelist of allowed columns
    if sanitized in allowed_columns or column_name in allowed_columns:
        return sanitized
    
    return None


def build_safe_table_alias(table_name: str, demographics_table_name: str) -> str:
    """
    Build a safe table alias, preventing injection through alias names.
    
    Args:
        table_name: Original table name
        demographics_table_name: Name of demographics table
        
    Returns:
        Safe table alias
    """
    if table_name == demographics_table_name:
        return "demo"
    
    # Sanitize and create safe alias
    safe_alias = sanitize_sql_identifier(table_name)
    
    # Ensure it's not a SQL keyword
    sql_keywords = {
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'UNION', 'INSERT', 'UPDATE', 
        'DELETE', 'DROP', 'CREATE', 'ALTER', 'TABLE', 'INDEX', 'DATABASE'
    }
    
    if safe_alias.upper() in sql_keywords:
        safe_alias = f"tbl_{safe_alias}"
    
    return safe_alias


def validate_numeric_value(value) -> bool:
    """
    Validate that a value is a safe numeric value.
    
    Args:
        value: Value to validate
        
    Returns:
        True if safe numeric value, False otherwise
    """
    try:
        float_val = float(value)
        # Check for reasonable bounds to prevent overflow attacks
        if -1e15 <= float_val <= 1e15:
            return True
    except (ValueError, TypeError, OverflowError):
        pass
    
    return False


def validate_string_value(value: str, max_length: int = 1000) -> bool:
    """
    Validate that a string value is safe for use in SQL parameters.
    
    Args:
        value: String value to validate
        max_length: Maximum allowed length
        
    Returns:
        True if safe, False otherwise
    """
    if not isinstance(value, str):
        return False
    
    # Check length
    if len(value) > max_length:
        return False
    
    # Check for null bytes
    if '\x00' in value:
        return False
    
    return True


def escape_file_path(file_path: str) -> str:
    """
    Safely escape file path for use in SQL queries.
    
    Args:
        file_path: File path to escape
        
    Returns:
        Safely escaped file path
    """
    if not file_path or not isinstance(file_path, str):
        return ""
    
    # Replace backslashes with forward slashes for consistency
    file_path = file_path.replace('\\', '/')
    
    # Remove any null bytes
    file_path = file_path.replace('\x00', '')
    
    # Single quote escaping for SQL
    file_path = file_path.replace("'", "''")
    
    return file_path