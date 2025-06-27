"""
Metadata extraction and table information for Basic Data Fusion.

This module provides functions for scanning CSV files, extracting metadata,
and managing table information with efficient caching.
"""

import hashlib
import logging
import os
import time
from functools import lru_cache
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.exceptions import DataProcessingError, FileHandlingError
from .merge_strategy import MergeKeys, FlexibleMergeStrategy


# Thread-safe cache for table information
_table_info_cache = {}
_table_info_cache_lock = Lock()
_file_access_lock = Lock()


def scan_csv_files(data_dir: str) -> Tuple[List[str], List[str]]:
    """
    Scan a directory for CSV files.
    
    Args:
        data_dir: Directory to scan for CSV files
        
    Returns:
        Tuple of (list of CSV filenames, list of error messages)
    """
    try:
        files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        return files, []
    except FileNotFoundError:
        return [], [f"Directory not found: {data_dir}"]
    except PermissionError:
        return [], [f"Permission denied: {data_dir}"]
    except OSError as e:
        return [], [f"Error reading directory {data_dir}: {e}"]


def validate_csv_structure(file_path: str, filename: str, merge_keys: MergeKeys) -> Tuple[bool, Optional[str]]:
    """
    Validate the basic structure of a CSV file.
    
    Args:
        file_path: Path to the CSV file
        filename: Name of the file for error messages
        merge_keys: Merge keys to validate against
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        with _file_access_lock:
            df_sample = pd.read_csv(file_path, nrows=10, low_memory=False)
        
        if df_sample.empty:
            return False, f"File {filename} is empty"
        
        # Check for required merge columns
        if merge_keys.is_longitudinal:
            if merge_keys.primary_id not in df_sample.columns:
                return False, f"Missing primary ID column '{merge_keys.primary_id}' in {filename}"
            if merge_keys.session_id and merge_keys.session_id not in df_sample.columns:
                return False, f"Missing session ID column '{merge_keys.session_id}' in {filename}"
        else:
            if merge_keys.primary_id not in df_sample.columns:
                return False, f"Missing primary ID column '{merge_keys.primary_id}' in {filename}"
        
        return True, None
    except Exception as e:
        return False, f"Error validating {filename}: {str(e)}"


def extract_column_metadata(file_path: str, table_name: str, is_demo_table: bool, 
                           merge_keys: MergeKeys, demo_table_name: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Extract column names and data types from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        table_name: Name of the table
        is_demo_table: Whether this is the demographics table
        merge_keys: Merge keys for the dataset
        demo_table_name: Name of the demographics table
        
    Returns:
        Tuple of (column_dtypes, column_tables)
    """
    try:
        with _file_access_lock:
            df_sample = pd.read_csv(file_path, nrows=100, low_memory=False)
        
        # Exclude ID columns from the results
        exclude_columns = {merge_keys.primary_id}
        if merge_keys.session_id:
            exclude_columns.add(merge_keys.session_id)
        if merge_keys.composite_id:
            exclude_columns.add(merge_keys.composite_id)
        
        column_dtypes = {}
        column_tables = {}
        
        for col in df_sample.columns:
            if col not in exclude_columns:
                dtype = str(df_sample[col].dtype)
                column_dtypes[col] = dtype
                column_tables[col] = table_name
        
        return column_dtypes, column_tables
    except Exception as e:
        logging.error(f"Error extracting metadata from {file_path}: {e}")
        return {}, {}


def calculate_numeric_ranges(file_path: str, table_name: str, is_demo_table: bool,
                           column_dtypes: Dict[str, str], merge_keys: MergeKeys,
                           demo_table_name: str) -> Dict[str, Tuple[float, float]]:
    """
    Calculate numeric ranges for columns in a CSV file.
    
    Args:
        file_path: Path to the CSV file
        table_name: Name of the table
        is_demo_table: Whether this is the demographics table
        column_dtypes: Dictionary of column data types
        merge_keys: Merge keys for the dataset
        demo_table_name: Name of the demographics table
        
    Returns:
        Dictionary mapping column names to (min, max) tuples
    """
    numeric_ranges = {}
    
    try:
        with _file_access_lock:
            # Use chunked reading for memory efficiency
            chunk_size = 10000
            
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
                for col, dtype in column_dtypes.items():
                    if col in chunk.columns and 'int' in dtype.lower() or 'float' in dtype.lower():
                        try:
                            # Convert to numeric, handling errors
                            numeric_values = pd.to_numeric(chunk[col], errors='coerce').dropna()
                            if not numeric_values.empty:
                                col_min = numeric_values.min()
                                col_max = numeric_values.max()
                                
                                if col in numeric_ranges:
                                    # Update existing range
                                    existing_min, existing_max = numeric_ranges[col]
                                    numeric_ranges[col] = (min(col_min, existing_min), max(col_max, existing_max))
                                else:
                                    # Initialize range
                                    numeric_ranges[col] = (col_min, col_max)
                        except Exception as e:
                            logging.warning(f"Error calculating range for column {col}: {e}")
                            continue
    except Exception as e:
        logging.error(f"Error calculating ranges for {file_path}: {e}")
    
    return numeric_ranges


def get_directory_mtime(directory: str) -> float:
    """
    Get the latest modification time of the directory and its CSV files.
    
    Args:
        directory: Directory path to check
        
    Returns:
        Latest modification time as timestamp
    """
    try:
        if not os.path.exists(directory):
            return 0
        
        # Get directory modification time
        dir_mtime = os.path.getmtime(directory)
        
        # Get modification times of all CSV files
        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        for csv_file in csv_files:
            file_path = os.path.join(directory, csv_file)
            file_mtime = os.path.getmtime(file_path)
            dir_mtime = max(dir_mtime, file_mtime)
        
        return dir_mtime
    except Exception:
        return 0


def get_config_hash(config_params: Dict[str, Any]) -> str:
    """
    Generate a hash for configuration parameters.
    
    Args:
        config_params: Dictionary of configuration parameters
        
    Returns:
        MD5 hash string
    """
    config_str = str(sorted(config_params.items()))
    return hashlib.md5(config_str.encode()).hexdigest()


def detect_merge_structure(demographics_path: str, primary_id: str, 
                          session_col: str, composite_id: str) -> MergeKeys:
    """
    Detect merge structure directly from file without Config instance.
    
    Args:
        demographics_path: Path to demographics file
        primary_id: Primary ID column name
        session_col: Session column name
        composite_id: Composite ID column name
        
    Returns:
        MergeKeys object with detected structure
    """
    try:
        if not os.path.exists(demographics_path):
            logging.warning(f"Demographics file not found: {demographics_path}. Using cross-sectional defaults.")
            return MergeKeys(primary_id=primary_id, is_longitudinal=False)
        
        with _file_access_lock:
            df_headers = pd.read_csv(demographics_path, nrows=0, low_memory=False)
        columns = df_headers.columns.tolist()
        
        has_primary_id = primary_id in columns
        has_session_id = session_col and session_col in columns
        
        if has_primary_id and has_session_id:
            return MergeKeys(
                primary_id=primary_id,
                session_id=session_col,
                composite_id=composite_id,
                is_longitudinal=True
            )
        elif has_primary_id:
            return MergeKeys(primary_id=primary_id, is_longitudinal=False)
        else:
            # Fallback to first column if no standard ID found
            fallback_id = columns[0] if columns else 'id'
            logging.warning(f"No standard ID column found. Using {fallback_id} as primary ID.")
            return MergeKeys(primary_id=fallback_id, is_longitudinal=False)
    except Exception as e:
        logging.error(f"Error detecting merge structure: {e}")
        return MergeKeys(primary_id=primary_id, is_longitudinal=False)


def get_table_info_cached(config_hash: str, dir_mtime: float, data_dir: str,
                         demographics_file: str, primary_id: str, session_col: str,
                         composite_id: str, age_col: str) -> Tuple[Any, ...]:
    """
    Get table information with caching.
    
    Args:
        config_hash: Hash of configuration parameters
        dir_mtime: Directory modification time
        data_dir: Data directory path
        demographics_file: Demographics file name
        primary_id: Primary ID column name
        session_col: Session column name
        composite_id: Composite ID column name
        age_col: Age column name
        
    Returns:
        Tuple containing table information
    """
    cache_key = (config_hash, dir_mtime)
    
    with _table_info_cache_lock:
        if cache_key in _table_info_cache:
            return _table_info_cache[cache_key]
        
        # FIFO cache management (max 4 entries)
        if len(_table_info_cache) >= 4:
            oldest_key = next(iter(_table_info_cache))
            del _table_info_cache[oldest_key]
        
        # Compute table info
        result = get_table_info_impl(data_dir, demographics_file, primary_id,
                                   session_col, composite_id, age_col)
        
        _table_info_cache[cache_key] = result
        return result


def get_table_info_impl(data_dir: str, demographics_file: str, primary_id: str,
                       session_col: str, composite_id: str, age_col: str) -> Tuple[Any, ...]:
    """
    Implementation of table information extraction.
    
    Args:
        data_dir: Data directory path
        demographics_file: Demographics file name
        primary_id: Primary ID column name
        session_col: Session column name
        composite_id: Composite ID column name
        age_col: Age column name
        
    Returns:
        Tuple containing:
        - available_tables: List of table names
        - column_dtypes: Dictionary mapping columns to data types
        - column_tables: Dictionary mapping columns to table names
        - numeric_ranges: Dictionary mapping columns to (min, max) ranges
        - merge_keys: MergeKeys object
        - errors: List of error messages
    """
    try:
        # Scan for CSV files
        csv_files, scan_errors = scan_csv_files(data_dir)
        errors = scan_errors.copy()
        
        if not csv_files:
            return [], {}, {}, {}, MergeKeys(primary_id=primary_id), errors
        
        # Detect merge structure
        demographics_path = os.path.join(data_dir, demographics_file)
        merge_keys = detect_merge_structure(demographics_path, primary_id, session_col, composite_id)
        
        # Initialize result containers
        available_tables = []
        column_dtypes = {}
        column_tables = {}
        numeric_ranges = {}
        demo_table_name = os.path.splitext(demographics_file)[0]
        
        # Process each CSV file
        for csv_file in csv_files:
            file_path = os.path.join(data_dir, csv_file)
            table_name = os.path.splitext(csv_file)[0]
            is_demo_table = (table_name == demo_table_name)
            
            # Validate CSV structure
            is_valid, error_msg = validate_csv_structure(file_path, csv_file, merge_keys)
            if not is_valid:
                errors.append(error_msg)
                continue
            
            # Extract metadata
            file_dtypes, file_tables = extract_column_metadata(
                file_path, table_name, is_demo_table, merge_keys, demo_table_name
            )
            
            # Calculate numeric ranges
            file_ranges = calculate_numeric_ranges(
                file_path, table_name, is_demo_table, file_dtypes, merge_keys, demo_table_name
            )
            
            # Accumulate results
            available_tables.append(table_name)
            column_dtypes.update(file_dtypes)
            column_tables.update(file_tables)
            numeric_ranges.update(file_ranges)
        
        return available_tables, column_dtypes, column_tables, numeric_ranges, merge_keys, errors
    
    except Exception as e:
        error_msg = f"Error processing table information: {e}"
        logging.error(error_msg)
        return [], {}, {}, {}, MergeKeys(primary_id=primary_id), [error_msg]


def get_table_info(config_params: Dict[str, Any]) -> Tuple[Any, ...]:
    """
    Public interface for getting table information with caching.
    
    Args:
        config_params: Dictionary containing configuration parameters
        
    Returns:
        Tuple containing table information
    """
    data_dir = config_params.get('data_dir', 'data')
    demographics_file = config_params.get('demographics_file', 'demographics.csv')
    primary_id = config_params.get('primary_id_column', 'ursi')
    session_col = config_params.get('session_column', 'session_num')
    composite_id = config_params.get('composite_id_column', 'customID')
    age_col = config_params.get('age_column', 'age')
    
    # Generate cache key components
    config_hash = get_config_hash(config_params)
    dir_mtime = get_directory_mtime(data_dir)
    
    return get_table_info_cached(
        config_hash, dir_mtime, data_dir, demographics_file,
        primary_id, session_col, composite_id, age_col
    )


def clear_table_info_cache() -> None:
    """Clear the table information cache."""
    with _table_info_cache_lock:
        _table_info_cache.clear()
        logging.info("Table information cache cleared")


def get_cache_stats() -> Dict[str, Any]:
    """Get statistics about the table information cache."""
    with _table_info_cache_lock:
        return {
            'cache_size': len(_table_info_cache),
            'cache_keys': list(_table_info_cache.keys()),
            'max_cache_size': 4
        }