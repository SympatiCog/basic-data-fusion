"""
Query parameter export/import functionality for Basic Data Fusion.

This module provides functions for saving and loading query parameters
to/from TOML format for sharing and persistence.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import toml

from core.exceptions import ValidationError


def export_query_parameters_to_toml(
    age_range: Optional[List[int]] = None,
    substudies: Optional[List[str]] = None,
    sessions: Optional[List[str]] = None,
    phenotypic_filters: Optional[List[Dict[str, Any]]] = None,
    selected_tables: Optional[List[str]] = None,
    selected_columns: Optional[Dict[str, List[str]]] = None,
    enwiden_longitudinal: bool = False,
    user_notes: str = "",
    app_version: str = "1.0.0"
) -> str:
    """
    Export query parameters to TOML format string.
    
    Args:
        age_range: Age filter range [min, max]
        substudies: List of selected substudies
        sessions: List of selected sessions
        phenotypic_filters: List of phenotypic filter definitions
        selected_tables: List of selected table names
        selected_columns: Dictionary mapping table names to column lists
        enwiden_longitudinal: Whether to enwiden longitudinal data
        user_notes: User-provided notes
        app_version: Application version
        
    Returns:
        TOML format string
        
    Raises:
        ValidationError: If parameter validation fails
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
            'filters': {
                'demographic': {},
                'phenotypic': phenotypic_filters or []
            },
            'selection': {
                'tables': selected_tables or [],
                'columns': selected_columns or {}
            },
            'options': {
                'enwiden_longitudinal': enwiden_longitudinal
            }
        }
        
        # Add demographic filters
        if age_range and len(age_range) == 2:
            query_params['filters']['demographic']['age_range'] = {
                'min': age_range[0],
                'max': age_range[1]
            }
        
        if substudies:
            query_params['filters']['demographic']['substudies'] = substudies
        
        if sessions:
            query_params['filters']['demographic']['sessions'] = sessions
        
        # Convert to TOML format
        return toml.dumps(query_params)
    
    except Exception as e:
        error_msg = f"Error exporting query parameters to TOML: {e}"
        logging.error(error_msg)
        raise ValidationError(error_msg, field="query_parameters")


def import_query_parameters_from_toml(toml_string: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Import and validate query parameters from TOML format string.
    
    Args:
        toml_string: TOML format string containing query parameters
        
    Returns:
        Tuple of (parsed_data, error_messages)
        
    Raises:
        ValidationError: If TOML parsing fails
    """
    errors = []
    parsed_data = {}
    
    try:
        # Parse TOML string
        data = toml.loads(toml_string)
        
        # Validate required sections
        required_sections = ['metadata', 'filters', 'selection', 'options']
        for section in required_sections:
            if section not in data:
                errors.append(f"Missing required section: {section}")
        
        if errors:
            return parsed_data, errors
        
        # Extract metadata
        metadata = data.get('metadata', {})
        parsed_data['metadata'] = {
            'export_timestamp': metadata.get('export_timestamp'),
            'app_version': metadata.get('app_version'),
            'format_version': metadata.get('format_version'),
            'user_notes': metadata.get('user_notes', '')
        }
        
        # Validate format version
        format_version = metadata.get('format_version')
        if format_version != '1.0':
            errors.append(f"Unsupported format version: {format_version}")
        
        # Extract demographic filters
        demographic_filters = data.get('filters', {}).get('demographic', {})
        
        # Age range
        age_range_data = demographic_filters.get('age_range')
        if age_range_data:
            try:
                age_min = age_range_data.get('min')
                age_max = age_range_data.get('max')
                if age_min is not None and age_max is not None:
                    if age_min >= age_max:
                        errors.append("Age range minimum must be less than maximum")
                    else:
                        parsed_data['age_range'] = [int(age_min), int(age_max)]
            except (ValueError, TypeError):
                errors.append("Invalid age range values")
        
        # Substudies
        substudies = demographic_filters.get('substudies')
        if substudies:
            if isinstance(substudies, list):
                parsed_data['substudies'] = [str(s) for s in substudies]
            else:
                errors.append("Substudies must be a list")
        
        # Sessions
        sessions = demographic_filters.get('sessions')
        if sessions:
            if isinstance(sessions, list):
                parsed_data['sessions'] = [str(s) for s in sessions]
            else:
                errors.append("Sessions must be a list")
        
        # Phenotypic filters
        phenotypic_filters = data.get('filters', {}).get('phenotypic', [])
        if phenotypic_filters:
            if isinstance(phenotypic_filters, list):
                validated_filters = []
                for i, filter_def in enumerate(phenotypic_filters):
                    if isinstance(filter_def, dict):
                        # Validate required fields
                        required_fields = ['table', 'column', 'type', 'value']
                        filter_errors = []
                        for field in required_fields:
                            if field not in filter_def:
                                filter_errors.append(f"Missing field '{field}' in filter {i}")
                        
                        if not filter_errors:
                            validated_filters.append(filter_def)
                        else:
                            errors.extend(filter_errors)
                    else:
                        errors.append(f"Phenotypic filter {i} must be a dictionary")
                
                parsed_data['phenotypic_filters'] = validated_filters
            else:
                errors.append("Phenotypic filters must be a list")
        
        # Table and column selection
        selection = data.get('selection', {})
        
        # Selected tables
        selected_tables = selection.get('tables')
        if selected_tables:
            if isinstance(selected_tables, list):
                parsed_data['selected_tables'] = [str(t) for t in selected_tables]
            else:
                errors.append("Selected tables must be a list")
        
        # Selected columns
        selected_columns = selection.get('columns')
        if selected_columns:
            if isinstance(selected_columns, dict):
                validated_columns = {}
                for table, columns in selected_columns.items():
                    if isinstance(columns, list):
                        validated_columns[str(table)] = [str(c) for c in columns]
                    else:
                        errors.append(f"Columns for table '{table}' must be a list")
                parsed_data['selected_columns'] = validated_columns
            else:
                errors.append("Selected columns must be a dictionary")
        
        # Options
        options = data.get('options', {})
        parsed_data['enwiden_longitudinal'] = bool(options.get('enwiden_longitudinal', False))
        
        return parsed_data, errors
    
    except toml.TomlDecodeError as e:
        error_msg = f"Invalid TOML format: {e}"
        logging.error(error_msg)
        raise ValidationError(error_msg, field="toml_string")
    except Exception as e:
        error_msg = f"Error importing query parameters from TOML: {e}"
        logging.error(error_msg)
        raise ValidationError(error_msg, field="query_parameters")


def validate_query_parameters(
    age_range: Optional[List[int]] = None,
    substudies: Optional[List[str]] = None,
    sessions: Optional[List[str]] = None,
    phenotypic_filters: Optional[List[Dict[str, Any]]] = None,
    selected_tables: Optional[List[str]] = None,
    selected_columns: Optional[Dict[str, List[str]]] = None
) -> List[str]:
    """
    Validate query parameters and return any errors.
    
    Args:
        age_range: Age filter range [min, max]
        substudies: List of selected substudies
        sessions: List of selected sessions
        phenotypic_filters: List of phenotypic filter definitions
        selected_tables: List of selected table names
        selected_columns: Dictionary mapping table names to column lists
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    try:
        # Validate age range
        if age_range:
            if not isinstance(age_range, (list, tuple)) or len(age_range) != 2:
                errors.append("Age range must be a list/tuple of exactly 2 values")
            else:
                try:
                    age_min, age_max = int(age_range[0]), int(age_range[1])
                    if age_min >= age_max:
                        errors.append("Age range minimum must be less than maximum")
                    if age_min < 0 or age_max > 150:
                        errors.append("Age range values must be between 0 and 150")
                except (ValueError, TypeError):
                    errors.append("Age range values must be numeric")
        
        # Validate substudies
        if substudies and not isinstance(substudies, list):
            errors.append("Substudies must be a list")
        
        # Validate sessions
        if sessions and not isinstance(sessions, list):
            errors.append("Sessions must be a list")
        
        # Validate phenotypic filters
        if phenotypic_filters:
            if not isinstance(phenotypic_filters, list):
                errors.append("Phenotypic filters must be a list")
            else:
                for i, filter_def in enumerate(phenotypic_filters):
                    if not isinstance(filter_def, dict):
                        errors.append(f"Phenotypic filter {i} must be a dictionary")
                        continue
                    
                    required_fields = ['table', 'column', 'type', 'value']
                    for field in required_fields:
                        if field not in filter_def:
                            errors.append(f"Missing field '{field}' in phenotypic filter {i}")
                    
                    # Validate filter type
                    filter_type = filter_def.get('type')
                    if filter_type not in ['range', 'categorical']:
                        errors.append(f"Invalid filter type '{filter_type}' in filter {i}")
        
        # Validate selected tables
        if selected_tables and not isinstance(selected_tables, list):
            errors.append("Selected tables must be a list")
        
        # Validate selected columns
        if selected_columns:
            if not isinstance(selected_columns, dict):
                errors.append("Selected columns must be a dictionary")
            else:
                for table, columns in selected_columns.items():
                    if not isinstance(columns, list):
                        errors.append(f"Columns for table '{table}' must be a list")
        
        return errors
    
    except Exception as e:
        errors.append(f"Error validating query parameters: {e}")
        return errors


def create_query_parameters_template() -> str:
    """
    Create a template TOML string for query parameters.
    
    Returns:
        Template TOML string with example values
    """
    template_data = {
        'metadata': {
            'export_timestamp': datetime.now().isoformat(),
            'app_version': '1.0.0',
            'format_version': '1.0',
            'user_notes': 'Example query parameters'
        },
        'filters': {
            'demographic': {
                'age_range': {
                    'min': 18,
                    'max': 65
                },
                'substudies': ['Discovery', 'Longitudinal_Adult'],
                'sessions': ['1', '2']
            },
            'phenotypic': [
                {
                    'table': 'example_table',
                    'column': 'example_column',
                    'type': 'range',
                    'value': [0, 100]
                }
            ]
        },
        'selection': {
            'tables': ['demographics', 'example_table'],
            'columns': {
                'demographics': ['age', 'sex'],
                'example_table': ['example_column']
            }
        },
        'options': {
            'enwiden_longitudinal': False
        }
    }
    
    return toml.dumps(template_data)