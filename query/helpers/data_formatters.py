"""
Data formatting helper functions for the Query interface.

This module contains functions that format and transform data
for display and processing.
"""

import logging


def convert_phenotypic_to_behavioral_filters(phenotypic_filters_state):
    """Convert phenotypic filters to behavioral filters format for query generation."""
    if not phenotypic_filters_state or not phenotypic_filters_state.get('filters'):
        return []

    behavioral_filters = []
    for filter_data in phenotypic_filters_state['filters']:
        if not filter_data.get('enabled'):
            continue

        if filter_data.get('table') and filter_data.get('column') and filter_data.get('filter_type'):
            behavioral_filter = {
                'table': filter_data['table'],
                'column': filter_data['column'],
                'filter_type': filter_data['filter_type']
            }

            if filter_data['filter_type'] == 'numeric':
                min_val, max_val = filter_data.get('min_val'), filter_data.get('max_val')
                if min_val is not None and max_val is not None:
                    behavioral_filter['value'] = [min_val, max_val]
            elif filter_data['filter_type'] == 'categorical':
                selected_values = filter_data.get('selected_values', [])
                if selected_values:
                    # Handle boolean columns differently - check if this is likely a boolean column
                    if all(val in ['Yes', 'No', True, False, 1, 0] for val in selected_values):
                        # For boolean columns, use explicit boolean values
                        behavioral_filter['value'] = [
                            True if v in ['Yes', True, 1] else False if v in ['No', False, 0] else v 
                            for v in selected_values
                        ]
                        behavioral_filter['is_boolean'] = True  # Flag for query generation
                    else:
                        behavioral_filter['value'] = selected_values

            if 'value' in behavioral_filter:
                behavioral_filters.append(behavioral_filter)
    return behavioral_filters


def format_participant_count(count):
    """Format participant count for display."""
    # To be implemented in Phase 3
    if count == 0:
        return "0 participants"
    else:
        return f"{count:,} participants"


def format_data_summary(data_info):
    """Format data summary information for display."""
    # To be implemented in Phase 3
    pass


def generate_export_filename(query_params):
    """Generate appropriate filename for data export."""
    # To be implemented in Phase 3
    pass