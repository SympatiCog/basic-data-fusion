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
from typing import List

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

# Data handling imports
from data_handling.merge_strategy import FlexibleMergeStrategy, MergeKeys, create_merge_strategy
from data_handling.metadata import (
    calculate_numeric_ranges,
    clear_table_info_cache,
    extract_column_metadata,
    get_cache_stats,
    get_table_info,
    scan_csv_files,
    validate_csv_structure,
)
from file_handling.csv_utils import get_csv_info, process_csv_file, validate_csv_file, validate_csv_structure
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
from query.query_builder import generate_base_query_logic, generate_count_query, generate_data_query

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
from analysis.demographics import generate_final_data_summary
from analysis.export import enwiden_longitudinal_data, generate_export_filename
from analysis.filtering import generate_filtering_report
from analysis.statistics import (
    calculate_column_statistics,
    calculate_correlation_matrix,
    generate_data_profile,
    get_unique_column_values,
    identify_data_quality_issues,
)


# Simple compatibility functions for missing exports
def export_query_parameters_to_toml(
    age_range=None,
    substudies=None,
    sessions=None,
    phenotypic_filters=None,
    selected_tables=None,
    selected_columns=None,
    enwiden_longitudinal=False,
    consolidate_baseline=False,
    user_notes="",
    app_version="1.0.0"
) -> str:
    """Export query parameters to TOML format string."""
    from datetime import datetime

    import toml

    try:
        # Build query parameters dictionary
        query_params = {
            'metadata': {
                'export_timestamp': datetime.now().isoformat(),
                'app_version': app_version,
                'format_version': '1.0',
                'user_notes': user_notes
            },
            'cohort_filters': {},
            'phenotypic_filters': phenotypic_filters or [],
            'export_selection': {
                'selected_tables': selected_tables or [],
                'selected_columns': selected_columns or {},
                'enwiden_longitudinal': enwiden_longitudinal,
                'consolidate_baseline': consolidate_baseline
            }
        }

        # Add cohort filters
        if age_range and len(age_range) == 2:
            query_params['cohort_filters']['age_range'] = age_range

        if substudies:
            query_params['cohort_filters']['substudies'] = substudies

        if sessions:
            query_params['cohort_filters']['sessions'] = sessions

        # Convert to TOML format
        return toml.dumps(query_params)

    except Exception as e:
        logging.error(f"Error exporting query parameters to TOML: {e}")
        return ""

def import_query_parameters_from_toml(toml_string: str) -> tuple:
    """Import and validate query parameters from TOML format string.
    
    Returns:
        Tuple of (parsed_data, error_messages)
    """
    import toml

    errors = []
    parsed_data = {}

    try:
        # Parse TOML string
        data = toml.loads(toml_string)

        # Extract metadata
        metadata = data.get('metadata', {})
        parsed_data['metadata'] = {
            'export_timestamp': metadata.get('export_timestamp'),
            'app_version': metadata.get('app_version'),
            'format_version': metadata.get('format_version'),
            'user_notes': metadata.get('user_notes', '')
        }

        # Extract cohort filters
        cohort_filters = data.get('cohort_filters', {})
        parsed_data['cohort_filters'] = cohort_filters

        # Extract phenotypic filters
        phenotypic_filters = data.get('phenotypic_filters', [])
        parsed_data['phenotypic_filters'] = phenotypic_filters

        # Extract export selection
        export_selection = data.get('export_selection', {})
        parsed_data['export_selection'] = export_selection

        return parsed_data, errors

    except Exception as e:
        errors.append(f"Error parsing TOML file: {str(e)}")
        return {}, errors

def validate_imported_query_parameters(params: dict, available_tables=None, demographics_columns=None, behavioral_columns=None, config=None) -> tuple:
    """Validate imported query parameters against current dataset.
    
    Returns:
        Tuple of (validation_results_dict, validation_errors_list)
        where validation_results_dict contains 'valid_parameters' and 'invalid_parameters'
    """
    validation_errors = []
    valid_parameters = {}
    invalid_parameters = {}

    try:
        # Basic structure validation
        if not isinstance(params, dict):
            validation_errors.append("Parameters must be a dictionary")
            return {'valid_parameters': {}, 'invalid_parameters': params, 'is_valid': False}, validation_errors

        # Initialize parameter sections
        valid_parameters = {
            'cohort_filters': {},
            'phenotypic_filters': [],
            'export_selection': {}
        }
        invalid_parameters = {
            'cohort_filters': {},
            'phenotypic_filters': [],
            'export_selection': {}
        }

        # Validate cohort filters
        cohort_filters = params.get('cohort_filters', {})
        if cohort_filters:
            # Validate age range
            age_range = cohort_filters.get('age_range')
            if age_range:
                if not isinstance(age_range, list) or len(age_range) != 2:
                    validation_errors.append("Age range must be a list of two values")
                    invalid_parameters['cohort_filters']['age_range'] = age_range
                elif age_range[0] >= age_range[1]:
                    validation_errors.append("Age range minimum must be less than maximum")
                    invalid_parameters['cohort_filters']['age_range'] = age_range
                else:
                    valid_parameters['cohort_filters']['age_range'] = age_range

            # Validate substudies
            substudies = cohort_filters.get('substudies')
            if substudies:
                if not isinstance(substudies, list):
                    validation_errors.append("Substudies must be a list")
                    invalid_parameters['cohort_filters']['substudies'] = substudies
                else:
                    valid_parameters['cohort_filters']['substudies'] = substudies

            # Validate sessions
            sessions = cohort_filters.get('sessions')
            if sessions:
                if not isinstance(sessions, list):
                    validation_errors.append("Sessions must be a list")
                    invalid_parameters['cohort_filters']['sessions'] = sessions
                else:
                    valid_parameters['cohort_filters']['sessions'] = sessions

        # Validate phenotypic filters
        phenotypic_filters = params.get('phenotypic_filters', [])
        if phenotypic_filters:
            if not isinstance(phenotypic_filters, list):
                validation_errors.append("Phenotypic filters must be a list")
                invalid_parameters['phenotypic_filters'] = phenotypic_filters
            else:
                for i, pf in enumerate(phenotypic_filters):
                    if not isinstance(pf, dict):
                        validation_errors.append(f"Phenotypic filter {i+1} must be a dictionary")
                        invalid_parameters['phenotypic_filters'].append(pf)
                        continue

                    # Check if table exists in available tables
                    table_name = pf.get('table')
                    if table_name:
                        # Get demographics table name for special handling
                        demographics_table_name = 'demographics'  # Default
                        if config and hasattr(config, 'data') and hasattr(config.data, 'demographics_file'):
                            demographics_table_name = config.data.demographics_file.replace('.csv', '')
                        
                        # Demographics table is always valid (handled separately from behavioral tables)
                        is_demographics = table_name.lower() == demographics_table_name.lower()
                        is_behavioral_table_valid = (not available_tables) or (table_name in available_tables)
                        
                        if is_demographics or is_behavioral_table_valid:
                            valid_parameters['phenotypic_filters'].append(pf)
                        else:
                            validation_errors.append(f"Table '{table_name}' in filter {i+1} is not available in current dataset")
                            invalid_parameters['phenotypic_filters'].append(pf)
                    else:
                        # Missing table name
                        validation_errors.append(f"Filter {i+1} is missing table name")
                        invalid_parameters['phenotypic_filters'].append(pf)

        # Validate export selection
        export_selection = params.get('export_selection', {})
        if export_selection:
            selected_tables = export_selection.get('selected_tables', [])
            valid_tables = []
            invalid_tables = []

            # Get demographics table name for special handling  
            demographics_table_name = 'demographics'  # Default
            if config and hasattr(config, 'data') and hasattr(config.data, 'demographics_file'):
                demographics_table_name = config.data.demographics_file.replace('.csv', '')
            
            for table in selected_tables:
                # Demographics table is always valid (handled separately from behavioral tables)
                is_demographics = table.lower() == demographics_table_name.lower()
                is_behavioral_table_valid = (not available_tables) or (table in available_tables)
                
                if is_demographics or is_behavioral_table_valid:
                    valid_tables.append(table)
                else:
                    validation_errors.append(f"Selected table '{table}' is not available in current dataset")
                    invalid_tables.append(table)

            if valid_tables:
                valid_parameters['export_selection']['selected_tables'] = valid_tables
            if invalid_tables:
                invalid_parameters['export_selection']['selected_tables'] = invalid_tables

            # Copy other export selection parameters
            for key in ['selected_columns', 'enwiden_longitudinal', 'consolidate_baseline']:
                if key in export_selection:
                    valid_parameters['export_selection'][key] = export_selection[key]

        # Build validation results dictionary
        validation_results = {
            'valid_parameters': valid_parameters,
            'invalid_parameters': invalid_parameters,
            'is_valid': len(validation_errors) == 0
        }

        return validation_results, validation_errors

    except Exception as e:
        validation_errors.append(f"Error validating parameters: {str(e)}")
        return {'valid_parameters': {}, 'invalid_parameters': params, 'is_valid': False}, validation_errors

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

def is_numeric_dtype(dtype_str: str) -> bool:
    """
    Check if a dtype string represents a numeric type.
    
    Args:
        dtype_str: String representation of the data type (e.g., 'float64', 'int32', 'object')
        
    Returns:
        True if the dtype represents a numeric type, False otherwise
    """
    if not dtype_str:
        return False

    dtype_lower = str(dtype_str).lower()
    return ('int' in dtype_lower or 'float' in dtype_lower) and 'object' not in dtype_lower


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


def get_study_site_values(config) -> List[str]:
    """
    Get the actual study site values from the demographics data.
    Handles multiple formats:
    - Space-separated: "Discovery Longitudinal_Adult"
    - Comma-separated in braces: "{Discovery, Longitudinal_Adult}"
    - Single values: "Discovery" or "{Discovery}"
    
    Args:
        config: Configuration object
        
    Returns:
        List of unique study site values found in the data
    """
    try:
        import os
        import re

        import pandas as pd

        if not config.STUDY_SITE_COLUMN:
            return []

        demographics_path = os.path.join(config.data.data_dir, config.data.demographics_file)
        if not os.path.exists(demographics_path):
            return []

        # Read the demographics file and extract unique study site values
        df = pd.read_csv(demographics_path)
        if config.STUDY_SITE_COLUMN not in df.columns:
            return []

        all_sites = set()
        unique_site_entries = df[config.STUDY_SITE_COLUMN].dropna().unique()

        for entry in unique_site_entries:
            entry_str = str(entry).strip()
            if not entry_str:
                continue

            # Remove outer curly braces and quotes
            cleaned = re.sub(r'^["\'{]*\{?|[}\'"]*$', '', entry_str)
            cleaned = re.sub(r'^\{|[}]*$', '', cleaned)
            cleaned = cleaned.strip()

            # Check if this is comma-separated or space-separated
            if ',' in cleaned:
                # Comma-separated format: "Discovery, Longitudinal_Adult"
                parts = [part.strip().strip('"\'') for part in cleaned.split(',')]
                all_sites.update(part for part in parts if part)
            elif ' ' in cleaned and not cleaned.endswith(' '):
                # Space-separated format: "Discovery Longitudinal_Adult"
                # But not just trailing spaces
                parts = [part.strip() for part in cleaned.split()]
                all_sites.update(part for part in parts if part)
            else:
                # Single value: "Discovery" or just trailing spaces
                clean_site = cleaned.strip().strip('"\'')
                if clean_site:
                    all_sites.add(clean_site)

        return sorted(all_sites)

    except Exception as e:
        logging.warning(f"Could not extract study site values: {e}")
        return []

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

    # Backward compatibility
    '_file_access_lock',
]

# Log the successful refactoring
logging.info("Successfully loaded refactored utils.py with modular architecture")

