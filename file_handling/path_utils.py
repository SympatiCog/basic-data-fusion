"""
Path utilities for Basic Data Fusion.

This module provides utilities for safe path manipulation,
path shortening for display, and directory operations.
"""

import os
import time
from pathlib import Path
from typing import List, Optional

from core.exceptions import SecurityError, FileProcessingError

# Exception aliases for this module
PathTraversalError = SecurityError
FileHandlingError = FileProcessingError


def shorten_path(path_str: str, max_length: int = 60) -> str:
    """
    Shorten a file system path for display purposes.
    
    Strategies:
    1. Replace home directory with ~
    2. If still too long, use middle truncation with ...
    
    Args:
        path_str: The path string to shorten
        max_length: Maximum length for the shortened path
        
    Returns:
        Shortened path string suitable for display
    """
    if not path_str:
        return path_str
        
    try:
        # Convert to Path object for easier manipulation
        path = Path(path_str).expanduser().resolve()
        
        # Replace home directory with ~
        try:
            home = Path.home()
            if path.is_relative_to(home):
                relative_path = path.relative_to(home)
                shortened = "~/" + str(relative_path)
            else:
                shortened = str(path)
        except (ValueError, OSError):
            # Fallback if relative_to fails or home directory issues
            shortened = str(path)
        
        # If still too long, apply middle truncation
        if len(shortened) > max_length:
            # Keep first and last parts, truncate middle
            if "/" in shortened:
                parts = shortened.split("/")
                if len(parts) > 3:
                    # Keep first directory, last directory, and filename
                    first_part = parts[0]
                    last_parts = parts[-2:]  # Last directory and filename
                    truncated = f"{first_part}/.../{'/'.join(last_parts)}"
                    
                    # If still too long, just show last parts
                    if len(truncated) > max_length:
                        truncated = f".../{'/'.join(last_parts)}"
                    
                    shortened = truncated
                else:
                    # If only a few parts, just truncate the middle
                    if len(shortened) > max_length:
                        keep_length = (max_length - 3) // 2
                        shortened = shortened[:keep_length] + "..." + shortened[-keep_length:]
            else:
                # Single name without slashes, just truncate
                if len(shortened) > max_length:
                    keep_length = (max_length - 3) // 2
                    shortened = shortened[:keep_length] + "..." + shortened[-keep_length:]
        
        return shortened
    
    except Exception:
        # Fallback to simple truncation if anything goes wrong
        if len(path_str) > max_length:
            keep_length = (max_length - 3) // 2
            return path_str[:keep_length] + "..." + path_str[-keep_length:]
        return path_str


def get_directory_mtime(directory: str) -> float:
    """
    Get the latest modification time in a directory and its CSV files.
    
    Args:
        directory: Directory path to check
        
    Returns:
        Latest modification time as timestamp
    """
    try:
        if not os.path.exists(directory):
            return 0.0

        # Get directory modification time
        latest_mtime = os.path.getmtime(directory)
        
        # Get modification times of all CSV files
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    file_mtime = os.path.getmtime(file_path)
                    latest_mtime = max(latest_mtime, file_mtime)
        
        return latest_mtime
    
    except Exception:
        return time.time()  # Return current time if error


def ensure_safe_path(file_path: str, base_directory: str) -> str:
    """
    Ensure a file path is safe and within the specified base directory.
    
    Args:
        file_path: Path to validate and normalize
        base_directory: Base directory that must contain the path
        
    Returns:
        Normalized, safe file path
        
    Raises:
        PathTraversalError: If path traversal is detected
        FileHandlingError: If path normalization fails
    """
    try:
        # Normalize both paths
        file_path = os.path.normpath(file_path)
        base_directory = os.path.normpath(base_directory)
        
        # Check for obvious path traversal patterns
        if '...' in file_path or '..' in file_path:
            raise PathTraversalError(f"Path traversal detected: {file_path}")
        
        # Convert to absolute paths for comparison
        abs_file_path = os.path.abspath(file_path)
        abs_base_dir = os.path.abspath(base_directory)
        
        # Ensure the file path is within the base directory
        if not abs_file_path.startswith(abs_base_dir):
            raise PathTraversalError(
                f"Path outside base directory: {file_path}",
                details={'file_path': file_path, 'base_directory': base_directory}
            )
        
        return abs_file_path
    
    except PathTraversalError:
        raise
    except Exception as e:
        raise FileHandlingError(f"Failed to validate path: {e}", file_path=file_path)


def create_safe_directory(directory_path: str, base_directory: Optional[str] = None) -> str:
    """
    Create a directory safely, ensuring it's within the base directory if specified.
    
    Args:
        directory_path: Path to the directory to create
        base_directory: Optional base directory for validation
        
    Returns:
        Absolute path to the created directory
        
    Raises:
        PathTraversalError: If path is outside base directory
        FileHandlingError: If directory creation fails
    """
    try:
        # Validate path if base directory is specified
        if base_directory:
            directory_path = ensure_safe_path(directory_path, base_directory)
        
        # Create directory
        Path(directory_path).mkdir(parents=True, exist_ok=True)
        
        return os.path.abspath(directory_path)
    
    except PathTraversalError:
        raise
    except Exception as e:
        raise FileHandlingError(
            f"Failed to create directory: {e}",
            file_path=directory_path,
            operation="create_directory"
        )


def list_csv_files(directory: str, recursive: bool = False) -> List[str]:
    """
    List all CSV files in a directory.
    
    Args:
        directory: Directory to search
        recursive: Whether to search subdirectories
        
    Returns:
        List of CSV file paths
        
    Raises:
        FileHandlingError: If directory access fails
    """
    try:
        csv_files = []
        
        if recursive:
            # Use recursive search
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if file.lower().endswith('.csv'):
                        csv_files.append(os.path.join(root, file))
        else:
            # Non-recursive search
            if os.path.exists(directory):
                for item in os.listdir(directory):
                    item_path = os.path.join(directory, item)
                    if os.path.isfile(item_path) and item.lower().endswith('.csv'):
                        csv_files.append(item_path)
        
        return sorted(csv_files)
    
    except Exception as e:
        raise FileHandlingError(
            f"Failed to list CSV files in directory: {e}",
            file_path=directory,
            operation="list_files"
        )


def get_relative_path(file_path: str, base_directory: str) -> str:
    """
    Get the relative path of a file from a base directory.
    
    Args:
        file_path: Absolute path to the file
        base_directory: Base directory to calculate relative path from
        
    Returns:
        Relative path string
        
    Raises:
        FileHandlingError: If path calculation fails
    """
    try:
        file_path = os.path.abspath(file_path)
        base_directory = os.path.abspath(base_directory)
        
        return os.path.relpath(file_path, base_directory)
    
    except Exception as e:
        raise FileHandlingError(
            f"Failed to calculate relative path: {e}",
            details={'file_path': file_path, 'base_directory': base_directory}
        )


def is_safe_filename(filename: str) -> bool:
    """
    Check if a filename is safe (no path traversal, special characters, etc.).
    
    Args:
        filename: Filename to check
        
    Returns:
        True if filename is safe
    """
    try:
        # Check for path separators
        if os.path.sep in filename or '/' in filename or '\\' in filename:
            return False
        
        # Check for path traversal
        if '..' in filename or filename.startswith('.'):
            return False
        
        # Check for reserved names (Windows)
        reserved_names = {
            'CON', 'PRN', 'AUX', 'NUL',
            'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
            'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
        }
        
        base_name = os.path.splitext(filename)[0].upper()
        if base_name in reserved_names:
            return False
        
        # Check for control characters
        for char in filename:
            if ord(char) < 32 or ord(char) == 127:
                return False
        
        # Check for dangerous characters
        dangerous_chars = '<>:"|?*'
        if any(char in filename for char in dangerous_chars):
            return False
        
        return True
    
    except Exception:
        return False


def normalize_path_separators(path: str) -> str:
    """
    Normalize path separators to forward slashes for cross-platform compatibility.
    
    Args:
        path: Path string to normalize
        
    Returns:
        Path with normalized separators
    """
    return path.replace('\\', '/')


def get_file_size_human_readable(file_path: str) -> str:
    """
    Get file size in human-readable format.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Human-readable file size string
    """
    try:
        if not os.path.exists(file_path):
            return "File not found"
        
        size_bytes = os.path.getsize(file_path)
        
        # Convert to appropriate unit
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        
        return f"{size_bytes:.1f} PB"
    
    except Exception:
        return "Unknown size"


def cleanup_empty_directories(base_directory: str) -> List[str]:
    """
    Remove empty directories within the base directory.
    
    Args:
        base_directory: Base directory to clean up
        
    Returns:
        List of removed directory paths
    """
    removed_dirs = []
    
    try:
        # Walk through directories from bottom up
        for root, dirs, files in os.walk(base_directory, topdown=False):
            # Skip the base directory itself
            if root == base_directory:
                continue
            
            try:
                # Try to remove if empty
                if not dirs and not files:
                    os.rmdir(root)
                    removed_dirs.append(root)
            except OSError:
                # Directory not empty or permission denied
                pass
    
    except Exception:
        pass  # Fail silently for cleanup operations
    
    return removed_dirs