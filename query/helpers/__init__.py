"""
Helper functions for the Query module.

This package contains utility functions for UI building, data formatting,
and validation logic used throughout the query interface.
"""

from .ui_builders import (
    build_phenotypic_filter_card,
    build_demographic_filter_section,
    build_column_selection_ui,
    build_data_preview_table
)

from .data_formatters import (
    convert_phenotypic_to_behavioral_filters,
    format_participant_count,
    format_data_summary,
    generate_export_filename
)

from .validation import (
    validate_filter_parameters,
    validate_table_selection,
    validate_column_selection
)

__all__ = [
    # UI Builders
    'build_phenotypic_filter_card',
    'build_demographic_filter_section',
    'build_column_selection_ui', 
    'build_data_preview_table',
    
    # Data Formatters
    'convert_phenotypic_to_behavioral_filters',
    'format_participant_count',
    'format_data_summary',
    'generate_export_filename',
    
    # Validation
    'validate_filter_parameters',
    'validate_table_selection',
    'validate_column_selection'
]