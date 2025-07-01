"""
Query parameter export/import functions for Basic Data Fusion.

This module provides functions for exporting and importing query parameters
to/from TOML format for reproducible analyses.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import toml

from core.exceptions import ValidationError

# Exception alias for this module
DataProcessingError = ValidationError


def export_query_parameters_to_toml(
    age_range: Optional[List[float]] = None,
    substudies: Optional[List[str]] = None,
    sessions: Optional[List[str]] = None,
    phenotypic_filters: Optional[List[Dict[str, Any]]] = None,
    selected_tables: Optional[List[str]] = None,
    selected_columns: Optional[Dict[str, List[str]]] = None,
    enwiden_longitudinal: bool = False,
    consolidate_baseline: bool = False,
    user_notes: str = "",
    app_version: str = "1.0.0"
) -> str:
    """
    Export query parameters to TOML format string.
    
    Args:
        age_range: Age range filter [min, max]
        substudies: List of substudy values
        sessions: List of session values
        phenotypic_filters: List of phenotypic filter dictionaries
        selected_tables: List of selected table names
        selected_columns: Dictionary mapping table names to column lists
        enwiden_longitudinal: Whether to widen longitudinal data
        consolidate_baseline: Whether to consolidate baseline columns
        user_notes: User-provided notes for the export
        app_version: Application version string
        
    Returns:
        TOML-formatted string containing all query parameters
        
    Raises:
        DataProcessingError: If export fails
    """
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
        error_msg = f"Error exporting query parameters to TOML: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, operation="export_toml")


def import_query_parameters_from_toml(toml_string: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Import and validate query parameters from TOML format string.
    
    Args:
        toml_string: TOML-formatted string containing query parameters
        
    Returns:
        Tuple of (parsed_data_dict, error_messages_list)
        
    Raises:
        DataProcessingError: If parsing fails completely
    """
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

    except toml.TomlDecodeError as e:
        error_msg = f"Invalid TOML format: {e}"
        errors.append(error_msg)
        logging.error(error_msg)
        raise DataProcessingError(error_msg, operation="import_toml")
    except Exception as e:
        error_msg = f"Error parsing TOML file: {e}"
        errors.append(error_msg)
        logging.error(error_msg)
        raise DataProcessingError(error_msg, operation="import_toml")


def validate_imported_query_parameters(
    params: Dict[str, Any],
    available_tables: Optional[List[str]] = None,
    demographics_columns: Optional[List[str]] = None,
    behavioral_columns: Optional[List[str]] = None,
    config=None
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Validate imported query parameters against current dataset.
    
    Args:
        params: Parsed parameters dictionary from import
        available_tables: List of available table names in current dataset
        demographics_columns: List of available demographics columns
        behavioral_columns: List of available behavioral columns
        config: Configuration object for additional validation context
        
    Returns:
        Tuple of (validation_results_dict, validation_errors_list)
        where validation_results_dict contains 'valid_parameters', 'invalid_parameters', and 'is_valid'
        
    Raises:
        DataProcessingError: If validation process fails
    """
    validation_errors = []
    valid_parameters = {}
    invalid_parameters = {}

    try:
        # Basic structure validation
        if not isinstance(params, dict):
            error_msg = "Parameters must be a dictionary"
            validation_errors.append(error_msg)
            return {
                'valid_parameters': {},
                'invalid_parameters': params,
                'is_valid': False
            }, validation_errors

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
        error_msg = f"Error validating parameters: {e}"
        validation_errors.append(error_msg)
        logging.error(error_msg)
        raise DataProcessingError(error_msg, operation="validate_import")


def generate_query_export_filename(
    user_notes: str = "",
    timestamp: Optional[datetime] = None
) -> str:
    """
    Generate a standardized filename for query parameter exports.
    
    Args:
        user_notes: User-provided notes to include in filename
        timestamp: Optional timestamp (defaults to current time)
        
    Returns:
        Standardized filename string
    """
    if timestamp is None:
        timestamp = datetime.now()
    
    # Format timestamp
    time_str = timestamp.strftime("%Y%m%d_%H%M%S")
    
    # Clean user notes for filename
    if user_notes:
        # Remove invalid filename characters and limit length
        import re
        clean_notes = re.sub(r'[<>:"/\\|?*]', '_', user_notes)
        clean_notes = clean_notes.strip()[:30]  # Limit length
        if clean_notes:
            return f"query_params_{time_str}_{clean_notes}.toml"
    
    return f"query_params_{time_str}.toml"


def get_export_summary(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a human-readable summary of exported query parameters.
    
    Args:
        params: Query parameters dictionary
        
    Returns:
        Dictionary with summary information
    """
    try:
        summary = {
            'metadata': params.get('metadata', {}),
            'filters_applied': [],
            'tables_selected': [],
            'export_options': {}
        }
        
        # Summarize cohort filters
        cohort_filters = params.get('cohort_filters', {})
        if cohort_filters:
            age_range = cohort_filters.get('age_range')
            if age_range:
                summary['filters_applied'].append(f"Age: {age_range[0]}-{age_range[1]}")
            
            substudies = cohort_filters.get('substudies')
            if substudies:
                summary['filters_applied'].append(f"Substudies: {', '.join(substudies)}")
            
            sessions = cohort_filters.get('sessions')
            if sessions:
                summary['filters_applied'].append(f"Sessions: {', '.join(sessions)}")
        
        # Summarize phenotypic filters
        phenotypic_filters = params.get('phenotypic_filters', [])
        if phenotypic_filters:
            for pf in phenotypic_filters:
                table = pf.get('table', 'Unknown')
                column = pf.get('column', 'Unknown')
                filter_type = pf.get('filter_type', 'Unknown')
                summary['filters_applied'].append(f"{table}.{column} ({filter_type})")
        
        # Summarize export selection
        export_selection = params.get('export_selection', {})
        if export_selection:
            selected_tables = export_selection.get('selected_tables', [])
            summary['tables_selected'] = selected_tables
            
            summary['export_options'] = {
                'enwiden_longitudinal': export_selection.get('enwiden_longitudinal', False),
                'consolidate_baseline': export_selection.get('consolidate_baseline', False)
            }
        
        return summary
    
    except Exception as e:
        logging.warning(f"Error generating export summary: {e}")
        return {'error': f"Could not generate summary: {e}"}