"""
Analysis module for Basic Data Fusion.

This module provides comprehensive data analysis capabilities including
demographics analysis, filtering tracking, statistical calculations,
and data export functionality.
"""

# Demographics analysis
from .demographics import (
    calculate_demographics_breakdown,
    generate_final_data_summary,
    has_multisite_data,
    detect_rockland_format,
    get_demographic_summary,
    validate_demographic_filters
)

# Filtering analysis
from .filtering import (
    FilterStep,
    FilterTracker,
    generate_filtering_report,
    validate_behavioral_filters,
    analyze_filter_impact
)

# Statistical analysis
from .statistics import (
    get_unique_column_values,
    calculate_column_statistics,
    calculate_correlation_matrix,
    identify_data_quality_issues,
    generate_data_profile
)

# Data export and transformation
from .export import (
    enwiden_longitudinal_data,
    consolidate_baseline_columns,
    generate_export_filename,
    validate_export_data,
    prepare_export_data,
    estimate_export_size
)

__all__ = [
    # Demographics analysis
    'calculate_demographics_breakdown',
    'generate_final_data_summary',
    'has_multisite_data',
    'detect_rockland_format',
    'get_demographic_summary',
    'validate_demographic_filters',
    
    # Filtering analysis
    'FilterStep',
    'FilterTracker',
    'generate_filtering_report',
    'validate_behavioral_filters',
    'analyze_filter_impact',
    
    # Statistical analysis
    'get_unique_column_values',
    'calculate_column_statistics',
    'calculate_correlation_matrix',
    'identify_data_quality_issues',
    'generate_data_profile',
    
    # Data export and transformation
    'enwiden_longitudinal_data',
    'consolidate_baseline_columns',
    'generate_export_filename',
    'validate_export_data',
    'prepare_export_data',
    'estimate_export_size',
]

# Version info
__version__ = "1.0.0"