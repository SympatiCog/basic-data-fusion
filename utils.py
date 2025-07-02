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
from core.database import get_database_manager
from core.exceptions import (
    ConfigurationError,
    DatabaseError,
    DataFusionError,
    FileProcessingError,
    QueryGenerationError,
    SecurityError,
    ValidationError,
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

def get_db_connection_context():
    """
    Get a database connection context manager (for safer connection handling).

    Returns:
        Context manager that yields DuckDB connection object
    """
    db_manager = get_database_manager()
    return db_manager.get_connection_context()

# Data handling imports
from data_handling.merge_strategy import FlexibleMergeStrategy, MergeKeys, create_merge_strategy
from data_handling.metadata import (
    calculate_numeric_ranges,
    clear_table_info_cache,
    extract_column_metadata,
    get_cache_stats,
    get_table_info,
    get_table_info_cached,
    get_unique_session_values,
    scan_csv_files,
    validate_csv_structure,
)
# Backward compatibility aliases
calculate_numeric_ranges_fast = calculate_numeric_ranges
_get_table_info_cached = get_table_info_cached

# Backward compatibility wrapper for extract_column_metadata
def extract_column_metadata_fast(file_path, table_name, is_demo_table, merge_keys, demo_table_name):
    """Backward compatibility wrapper that returns expected format for tests."""
    column_dtypes, column_tables, errors = extract_column_metadata(
        file_path, table_name, is_demo_table, merge_keys, demo_table_name
    )
    
    # Return format expected by tests:
    # 1. List of column names
    # 2. Dict with table-prefixed keys and dtypes
    # 3. List of errors
    columns = list(column_dtypes.keys())
    dtypes = {f"{table_name}.{col}": dtype for col, dtype in column_dtypes.items()}
    
    return columns, dtypes, errors
from file_handling.csv_utils import get_csv_info, process_csv_file, validate_csv_file
from file_handling.csv_utils import scan_csv_files as scan_csv_files_fh

# File handling imports
from file_handling.security import (
    check_file_extension,
    detect_malicious_content,
    generate_safe_filename,
    sanitize_column_names,
    secure_filename,
    validate_file_path,
    validate_file_size,
)
from file_handling.upload import DuplicateFileInfo, FileActionChoice, UploadResult, check_for_duplicate_files
from file_handling.upload import save_uploaded_files_to_data_dir as _save_uploaded_files_to_data_dir
from query.query_builder import generate_base_query_logic, generate_count_query, generate_data_query, get_table_alias

# Query generation imports
from query.query_factory import QueryFactory, QueryMode, generate_base_query, generate_query_suite, get_query_factory
from query.query_secure import (
    generate_base_query_logic_secure,
    generate_count_query_secure,
    generate_data_query_secure,
    generate_secure_query_suite,
    validate_query_parameters,
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
from analysis.demographics import generate_final_data_summary, has_multisite_data, get_study_site_values
from analysis.export import enwiden_longitudinal_data, generate_export_filename
from analysis.filtering import generate_filtering_report
from analysis.statistics import (
    calculate_column_statistics,
    calculate_correlation_matrix,
    generate_data_profile,
    get_unique_column_values,
    identify_data_quality_issues,
    is_numeric_column,
    is_numeric_dtype,
)

# File handling imports
from file_handling.path_utils import shorten_path

# Query export imports
from query.query_export import (
    export_query_parameters_to_toml,
    import_query_parameters_from_toml,
    validate_imported_query_parameters,
)



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Backward compatibility globals and utilities
_file_access_lock = Lock()  # For backward compatibility with temp fix


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
    'is_numeric_dtype',
    'has_multisite_data',
    'get_study_site_values',
    'get_unique_session_values',
    'get_table_alias',

    # Backward compatibility
    '_file_access_lock',
    'calculate_numeric_ranges_fast',
    'extract_column_metadata_fast',
    '_get_table_info_cached',
]

# Log the successful refactoring
logging.info("Successfully loaded refactored utils.py with modular architecture")

