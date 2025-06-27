"""
Backward compatibility adapter for Basic Data Fusion.

This module serves as a compatibility layer for the refactored modular architecture.
All functions are now imported from their respective specialized modules.

The original monolithic utils.py has been refactored into:
- core/: Configuration, database, exceptions
- data_handling/: Merge strategies, metadata, validation  
- query/: SQL query generation (secure and legacy)
- file_handling/: File operations and security
- analysis/: Data analysis and statistics

This file maintains backward compatibility by re-exporting the public API.
"""

import logging
from threading import Lock

# Core infrastructure imports
from core.config import Config
from core.database import DatabaseManager, get_database_manager
from core.exceptions import (
    DataFusionError,
    ConfigurationError,
    DatabaseError,
    FileProcessingError,
    ValidationError,
    SecurityError,
    QueryGenerationError
)

# Backward compatibility aliases
DataProcessingError = ValidationError
FileOperationError = FileProcessingError
QueryError = QueryGenerationError
SQLInjectionError = SecurityError  
PathTraversalError = SecurityError

# Compatibility function for get_db_connection
def get_db_connection():
    """
    Get a database connection (backward compatibility function).
    
    Returns:
        DuckDB connection object
    """
    db_manager = get_database_manager()
    return db_manager.get_connection()

# Data handling imports
from data_handling.merge_strategy import MergeKeys, FlexibleMergeStrategy, create_merge_strategy
from data_handling.metadata import (
    get_table_info,
    clear_table_info_cache,
    get_cache_stats,
    scan_csv_files,
    validate_csv_structure,
    extract_column_metadata,
    calculate_numeric_ranges
)

# Query generation imports
from query.query_factory import (
    generate_base_query,
    generate_query_suite,
    get_query_factory,
    QueryFactory,
    QueryMode
)
from query.query_builder import (
    generate_base_query_logic,
    generate_data_query,
    generate_count_query
)
from query.query_secure import (
    generate_base_query_logic_secure,
    generate_data_query_secure, 
    generate_count_query_secure,
    generate_secure_query_suite,
    validate_query_parameters
)

# File handling imports
from file_handling.security import (
    secure_filename,
    detect_malicious_content,
    validate_file_path,
    check_file_extension,
    validate_file_size,
    sanitize_column_names,
    generate_safe_filename
)
from file_handling.csv_utils import (
    validate_csv_file,
    process_csv_file,
    scan_csv_files as scan_csv_files_fh,
    get_csv_info,
    validate_csv_structure
)
from file_handling.upload import (
    save_uploaded_files_to_data_dir as _save_uploaded_files_to_data_dir,
    check_for_duplicate_files,
    FileActionChoice,
    DuplicateFileInfo,
    UploadResult
)

# Backward compatibility wrapper for save_uploaded_files_to_data_dir
def save_uploaded_files_to_data_dir(file_contents, filenames, data_dir, duplicate_actions=None, sanitize_columns=True):
    """
    Backward compatibility wrapper that returns tuple instead of UploadResult.
    
    Returns:
        tuple: (success_messages, error_messages) for backward compatibility
    """
    result = _save_uploaded_files_to_data_dir(file_contents, filenames, data_dir, duplicate_actions, sanitize_columns)
    return result.success_messages, result.error_messages
from file_handling.path_utils import (
    ensure_safe_path,
    create_safe_directory,
    list_csv_files,
    get_relative_path,
    is_safe_filename,
    normalize_path_separators,
    get_file_size_human_readable,
    cleanup_empty_directories
)

# Simple implementations for backward compatibility
def get_file_hash(file_path: str) -> str:
    """Calculate MD5 hash of a file for backward compatibility."""
    import hashlib
    with open(file_path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

def ensure_directory_exists(directory_path: str) -> bool:
    """Ensure directory exists for backward compatibility."""
    import os
    os.makedirs(directory_path, exist_ok=True)
    return os.path.exists(directory_path)

# Backward compatibility aliases
sanitize_file_path = validate_file_path
check_path_traversal = validate_file_path

# Analysis imports
from analysis.export import (
    enwiden_longitudinal_data,
    generate_export_filename,
    validate_export_data,
    prepare_export_data,
    estimate_export_size
)
from analysis.demographics import (
    generate_final_data_summary,
    has_multisite_data as has_multisite_data_demo,
    get_demographic_summary,
    validate_demographic_filters
)
from analysis.filtering import (
    generate_filtering_report,
    validate_behavioral_filters
)
from analysis.statistics import (
    get_unique_column_values,
    calculate_column_statistics,
    calculate_correlation_matrix,
    identify_data_quality_issues,
    generate_data_profile
)

# Simple compatibility functions for missing exports
def export_query_parameters_to_toml(params: dict, file_path: str) -> bool:
    """Export query parameters to TOML file (backward compatibility)."""
    import toml
    try:
        with open(file_path, 'w') as f:
            toml.dump(params, f)
        return True
    except Exception:
        return False

def import_query_parameters_from_toml(file_path: str) -> dict:
    """Import query parameters from TOML file (backward compatibility)."""
    import toml
    try:
        with open(file_path, 'r') as f:
            return toml.load(f)
    except Exception:
        return {}

def validate_imported_query_parameters(params: dict) -> bool:
    """Validate imported query parameters (backward compatibility)."""
    required_keys = ['demographic_filters', 'behavioral_filters', 'tables_to_join']
    return all(key in params for key in required_keys)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Backward compatibility globals and utilities
_file_access_lock = Lock()  # For backward compatibility with temp fix

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
    from pathlib import Path
    
    if not path_str:
        return path_str
        
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
            if len(parts) > 2:
                # Calculate space for first, last, and ellipsis
                ellipsis = "..."
                first_part = parts[0]
                last_part = parts[-1]
                
                # Reserve space for first part, ellipsis, and last part
                reserved = len(first_part) + len(ellipsis) + len(last_part) + 2  # +2 for slashes
                
                if reserved <= max_length:
                    # Add middle parts until we exceed the limit
                    middle_parts = []
                    remaining_space = max_length - reserved
                    
                    # Try to include some middle parts
                    for part in parts[1:-1]:
                        if len("/".join(middle_parts)) + len(part) + 1 <= remaining_space:
                            middle_parts.append(part)
                        else:
                            break
                    
                    if middle_parts:
                        shortened = f"{first_part}/{'/'.join(middle_parts)}/{ellipsis}/{last_part}"
                    else:
                        shortened = f"{first_part}/{ellipsis}/{last_part}"
                else:
                    # Even first and last are too long, just truncate
                    shortened = shortened[:max_length-3] + "..."
        else:
            # No slashes, just truncate
            shortened = shortened[:max_length-3] + "..."
    
    return shortened

def is_numeric_column(data_dir: str, table_name: str, column_name: str, demo_table_name: str, demographics_file_name: str) -> bool:
    """
    Check if a column contains numeric data.
    
    Args:
        data_dir: Directory containing data files
        table_name: Name of the table
        column_name: Name of the column
        demo_table_name: Name of demographics table
        demographics_file_name: Name of demographics file
        
    Returns:
        True if column is numeric, False otherwise
    """
    import os
    import pandas as pd
    
    try:
        # Determine file path
        if table_name == demo_table_name:
            file_path = os.path.join(data_dir, demographics_file_name)
        else:
            file_path = os.path.join(data_dir, f"{table_name}.csv")
        
        if not os.path.exists(file_path):
            return False
        
        # Read a small sample to check data type
        with _file_access_lock:
            try:
                df_sample = pd.read_csv(file_path, usecols=[column_name], nrows=100, low_memory=False)
                if column_name not in df_sample.columns:
                    return False
                
                # Check if column can be converted to numeric
                series = df_sample[column_name].dropna()
                if len(series) == 0:
                    return False
                
                # Try to convert to numeric
                pd.to_numeric(series, errors='raise')
                return True
                
            except (ValueError, TypeError, pd.errors.ParserError):
                return False
            except Exception:
                return False
    
    except Exception:
        return False

def has_multisite_data(data_dir: str, demographics_file_name: str) -> bool:
    """
    Check if the dataset has multisite/substudy structure.
    
    Args:
        data_dir: Directory containing data files
        demographics_file_name: Name of demographics file
        
    Returns:
        True if multisite data is detected, False otherwise
    """
    import os
    import pandas as pd
    
    try:
        file_path = os.path.join(data_dir, demographics_file_name)
        if not os.path.exists(file_path):
            return False
        
        with _file_access_lock:
            try:
                # Read just the column names first
                df_sample = pd.read_csv(file_path, nrows=0)
                columns = df_sample.columns.str.lower()
                
                # Look for common multisite indicators
                multisite_indicators = ['site', 'substudy', 'center', 'location', 'cohort']
                return any(indicator in col for col in columns for indicator in multisite_indicators)
                
            except Exception:
                return False
    
    except Exception:
        return False

# Legacy function mappings for backward compatibility
def reset_db_connection():
    """Reset the database connection (useful for testing or error recovery)."""
    from core.database import reset_database_manager
    reset_database_manager()

# Export all public functions for backward compatibility
__all__ = [
    # Core classes and functions
    'Config',
    'get_db_connection',
    'reset_db_connection',
    
    # Exceptions
    'DataFusionError',
    'ConfigurationError',
    'DatabaseError',
    'FileProcessingError',
    'ValidationError',
    'SecurityError',
    'QueryGenerationError',
    # Backward compatibility aliases
    'DataProcessingError',
    'FileOperationError',
    'QueryError',
    'SQLInjectionError',
    'PathTraversalError',
    
    # Data handling
    'MergeKeys',
    'FlexibleMergeStrategy',
    'create_merge_strategy',
    'get_table_info',
    'clear_table_info_cache',
    'get_cache_stats',
    'scan_csv_files',
    'validate_csv_structure',
    'extract_column_metadata',
    'calculate_numeric_ranges',
    
    # Query generation
    'generate_base_query_logic',
    'generate_data_query',
    'generate_count_query',
    'generate_base_query_logic_secure',
    'generate_data_query_secure',
    'generate_count_query_secure',
    'generate_secure_query_suite',
    'validate_query_parameters',
    'generate_base_query',
    'generate_query_suite',
    'get_query_factory',
    'QueryFactory',
    'QueryMode',
    
    # File handling
    'secure_filename',
    'detect_malicious_content', 
    'validate_file_path',
    'check_file_extension',
    'validate_file_size',
    'sanitize_column_names',
    'generate_safe_filename',
    'validate_csv_file',
    'process_csv_file',
    'scan_csv_files_fh',
    'get_csv_info',
    'validate_csv_structure',
    'save_uploaded_files_to_data_dir',
    'check_for_duplicate_files',
    'FileActionChoice',
    'DuplicateFileInfo', 
    'UploadResult',
    'get_file_hash',
    'ensure_directory_exists',
    # Backward compatibility aliases
    'sanitize_file_path',
    'check_path_traversal',
    
    # Analysis and export
    'enwiden_longitudinal_data',
    'export_query_parameters_to_toml',
    'import_query_parameters_from_toml',
    'validate_imported_query_parameters',
    'generate_export_filename',
    'generate_filtering_report',
    'generate_final_data_summary',
    'get_unique_column_values',
    'calculate_column_statistics',
    'calculate_correlation_matrix',
    'identify_data_quality_issues',
    'generate_data_profile',
    
    # Utility functions
    'shorten_path',
    'is_numeric_column',
    'has_multisite_data',
    
    # Backward compatibility
    '_file_access_lock',
]

# Log the successful refactoring
logging.info("Successfully loaded refactored utils.py with modular architecture")