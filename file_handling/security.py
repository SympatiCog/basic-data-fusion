"""
File security functions for Basic Data Fusion.

This module provides security functions for safe file handling,
including filename sanitization and path traversal prevention.
"""

import os
import re
from typing import Dict, List, Tuple

from core.exceptions import SecurityError

# Exception alias for this module
PathTraversalError = SecurityError


def secure_filename(filename: str) -> str:
    """
    Enhanced secure filename function that prevents path traversal and injection attacks.
    
    Args:
        filename: Original filename to sanitize
        
    Returns:
        Sanitized filename safe for filesystem operations
        
    Raises:
        SecurityError: If filename cannot be made secure
    """
    try:
        # Get basename only, preventing path traversal
        filename = os.path.basename(filename)

        # Remove null bytes and control characters
        filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)

        # Replace whitespace with underscores
        filename = re.sub(r'\s+', '_', filename)

        # Remove path traversal patterns completely
        filename = re.sub(r'\.\.*', '', filename)  # Remove any sequence of dots

        # Remove all non-alphanumeric except safe characters
        filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)

        # Consolidate underscores
        filename = re.sub(r'_+', '_', filename)

        # Strip leading/trailing underscores and dots
        filename = filename.strip('_.')

        # Ensure not empty
        if not filename:
            filename = "safe_file"

        # Ensure reasonable length
        if len(filename) > 255:
            name, ext = os.path.splitext(filename)
            if ext:
                filename = name[:250] + ext
            else:
                filename = filename[:255]

        return filename
    
    except Exception as e:
        raise SecurityError(f"Failed to secure filename: {e}", input_value=filename)


def sanitize_column_names(columns: List[str]) -> Tuple[List[str], Dict[str, str]]:
    """
    Enhanced sanitization of column names to prevent SQL injection and ensure safety.
    
    Args:
        columns: List of original column names
        
    Returns:
        Tuple of (sanitized_column_names, mapping_dict)
        mapping_dict maps original names to sanitized names
        
    Raises:
        SecurityError: If column sanitization fails
    """
    try:
        sanitized_columns = []
        column_mapping = {}

        # SQL keywords that should be prefixed to make them safe
        sql_keywords = {
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'UNION', 'WHERE', 'FROM', 'JOIN', 'HAVING', 'GROUP', 'ORDER', 'BY',
            'EXEC', 'EXECUTE', 'SCRIPT', 'TRUNCATE', 'MERGE', 'GRANT', 'REVOKE'
        }

        for original_col in columns:
            # Start with string conversion of the original column
            sanitized = str(original_col)

            # Remove null bytes, control characters, and dangerous SQL characters
            sanitized = re.sub(r'[\x00-\x1f\x7f\'"`;\\]', '', sanitized)

            # Remove SQL comment patterns
            sanitized = re.sub(r'--.*$', '', sanitized)  # Remove -- comments
            sanitized = re.sub(r'/\*.*?\*/', '', sanitized)  # Remove /* */ comments

            # Replace whitespace and problematic characters with underscores
            sanitized = re.sub(r'[\s\-\(\)\[\]\{\}\@\#\$\%\^\&\*\+\=\|\?\<\>\,\.\:\/\\]+', '_', sanitized)

            # Remove any remaining non-alphanumeric characters except underscores
            sanitized = re.sub(r'[^a-zA-Z0-9_]', '', sanitized)

            # Check for and modify SQL keywords (preserve original case)
            words = sanitized.split('_')
            safe_words = []
            for word in words:
                if word.upper() in sql_keywords:
                    # Prefix SQL keywords to make them safe, preserving original case
                    safe_words.append(f"FIELD_{word}")
                else:
                    safe_words.append(word)
            sanitized = '_'.join(safe_words)

            # Consolidate multiple consecutive underscores
            sanitized = re.sub(r'_+', '_', sanitized)

            # Remove leading/trailing underscores
            sanitized = sanitized.strip('_')

            # Ensure column name is not empty
            if not sanitized:
                sanitized = f"col_{len(sanitized_columns)}"

            # Ensure column name doesn't start with a number
            if sanitized and sanitized[0].isdigit():
                sanitized = f"col_{sanitized}"

            # Ensure uniqueness
            original_sanitized = sanitized
            counter = 1
            while sanitized in sanitized_columns:
                sanitized = f"{original_sanitized}_{counter}"
                counter += 1

            sanitized_columns.append(sanitized)
            column_mapping[original_col] = sanitized

        return sanitized_columns, column_mapping
    
    except Exception as e:
        raise SecurityError(f"Failed to sanitize column names: {e}")


def validate_file_path(file_path: str, allowed_directory: str) -> str:
    """
    Validate that a file path is within the allowed directory and safe.
    
    Args:
        file_path: Path to validate
        allowed_directory: Directory that must contain the file
        
    Returns:
        Normalized, validated file path
        
    Raises:
        PathTraversalError: If path traversal is detected
        SecurityError: If path is invalid
    """
    try:
        # Normalize paths
        file_path = os.path.normpath(file_path)
        allowed_directory = os.path.normpath(allowed_directory)
        
        # Check for path traversal patterns
        if '..' in file_path:
            raise PathTraversalError("Path traversal detected in file path")
        
        # Get absolute paths
        abs_file_path = os.path.abspath(file_path)
        abs_allowed_dir = os.path.abspath(allowed_directory)
        
        # Check if file path is within allowed directory
        if not abs_file_path.startswith(abs_allowed_dir):
            raise PathTraversalError(
                f"File path outside allowed directory: {file_path}",
                details={'file_path': file_path, 'allowed_directory': allowed_directory}
            )
        
        return abs_file_path
    
    except PathTraversalError:
        raise
    except Exception as e:
        raise SecurityError(f"Invalid file path: {e}", input_value=file_path)


def check_file_extension(filename: str, allowed_extensions: List[str]) -> bool:
    """
    Check if file has an allowed extension.
    
    Args:
        filename: Name of the file to check
        allowed_extensions: List of allowed file extensions (with or without dots)
        
    Returns:
        True if file extension is allowed
    """
    try:
        # Get file extension
        _, ext = os.path.splitext(filename.lower())
        
        # Normalize allowed extensions
        normalized_extensions = []
        for allowed_ext in allowed_extensions:
            if not allowed_ext.startswith('.'):
                allowed_ext = '.' + allowed_ext
            normalized_extensions.append(allowed_ext.lower())
        
        return ext in normalized_extensions
    
    except Exception:
        return False


def validate_file_size(file_content: bytes, max_size_mb: int = 50) -> bool:
    """
    Validate file size against maximum allowed size.
    
    Args:
        file_content: File content as bytes
        max_size_mb: Maximum allowed size in megabytes
        
    Returns:
        True if file size is within limit
    """
    try:
        max_size_bytes = max_size_mb * 1024 * 1024
        return len(file_content) <= max_size_bytes
    except Exception:
        return False


def detect_malicious_content(file_content: bytes, filename: str) -> List[str]:
    """
    Detect potentially malicious content in uploaded files.
    
    Args:
        file_content: File content as bytes
        filename: Name of the file
        
    Returns:
        List of security warnings/issues found
    """
    warnings = []
    
    try:
        # Convert to string for pattern matching (handle encoding errors gracefully)
        try:
            content_str = file_content.decode('utf-8', errors='ignore').lower()
        except Exception:
            content_str = str(file_content).lower()
        
        # Check for suspicious patterns
        suspicious_patterns = [
            (r'<script[^>]*>', 'JavaScript code detected'),
            (r'javascript:', 'JavaScript URL detected'),
            (r'vbscript:', 'VBScript URL detected'),
            (r'data:text/html', 'HTML data URL detected'),
            (r'<?php', 'PHP code detected'),
            (r'<%.*%>', 'Server-side code detected'),
            (r'union.*select', 'SQL injection pattern detected'),
            (r'drop.*table', 'SQL drop statement detected'),
            (r'exec.*\(', 'Executable code pattern detected'),
        ]
        
        for pattern, warning in suspicious_patterns:
            if re.search(pattern, content_str):
                warnings.append(f"{warning} in {filename}")
        
        # Check for excessive null bytes (could indicate binary exploitation)
        null_byte_count = file_content.count(b'\x00')
        if null_byte_count > 100:  # Arbitrary threshold
            warnings.append(f"Excessive null bytes ({null_byte_count}) in {filename}")
        
        # Check for very long lines (could indicate obfuscation)
        lines = content_str.split('\n')
        for i, line in enumerate(lines[:100]):  # Check first 100 lines
            if len(line) > 10000:  # Arbitrary threshold
                warnings.append(f"Extremely long line ({len(line)} chars) on line {i+1} in {filename}")
                break
    
    except Exception as e:
        warnings.append(f"Error scanning {filename} for malicious content: {e}")
    
    return warnings


def generate_safe_filename(original_filename: str, existing_files: List[str]) -> str:
    """
    Generate a safe, unique filename that doesn't conflict with existing files.
    
    Args:
        original_filename: Original filename to base the safe name on
        existing_files: List of existing filenames to avoid conflicts
        
    Returns:
        Safe, unique filename
    """
    try:
        # First, make the filename secure
        safe_name = secure_filename(original_filename)
        
        # If it doesn't conflict, return it
        if safe_name not in existing_files:
            return safe_name
        
        # Generate unique name by adding counter
        base_name, ext = os.path.splitext(safe_name)
        counter = 1
        
        while True:
            candidate = f"{base_name}_{counter}{ext}"
            if candidate not in existing_files:
                return candidate
            counter += 1
            
            # Prevent infinite loops
            if counter > 9999:
                import time
                timestamp = str(int(time.time()))
                return f"{base_name}_{timestamp}{ext}"
    
    except Exception as e:
        # Fallback to timestamp-based name
        import time
        timestamp = str(int(time.time()))
        return f"safe_file_{timestamp}.csv"