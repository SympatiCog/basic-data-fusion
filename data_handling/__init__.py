"""
Data handling module for Basic Data Fusion.

This module provides data handling functionality including merge strategies,
metadata extraction, data validation, and processing capabilities.
"""

from .merge_strategy import MergeKeys, MergeStrategy, FlexibleMergeStrategy, create_merge_strategy
from .metadata import (
    scan_csv_files,
    validate_csv_structure,
    extract_column_metadata,
    calculate_numeric_ranges,
    get_table_info,
    clear_table_info_cache,
    get_cache_stats
)

__all__ = [
    # Merge strategy
    'MergeKeys',
    'MergeStrategy', 
    'FlexibleMergeStrategy',
    'create_merge_strategy',
    
    # Metadata
    'scan_csv_files',
    'validate_csv_structure',
    'extract_column_metadata',
    'calculate_numeric_ranges',
    'get_table_info',
    'clear_table_info_cache',
    'get_cache_stats',
]

# Version info
__version__ = "1.0.0"