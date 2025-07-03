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
    enabled_count = sum(1 for f in phenotypic_filters_state['filters'] if f.get('enabled'))
    if enabled_count > 0:
        logging.debug(f"Converting {len(phenotypic_filters_state['filters'])} phenotypic filters ({enabled_count} enabled)")

    for filter_data in phenotypic_filters_state['filters']:
        if not filter_data.get('enabled'):
            logging.debug(f"Skipping disabled filter: {filter_data.get('table', 'unknown')}.{filter_data.get('column', 'unknown')}")
            continue

        if filter_data.get('table') and filter_data.get('column') and filter_data.get('filter_type'):
            # Map to the format expected by secure query functions
            behavioral_filter = {
                'table': filter_data['table'],
                'column': filter_data['column'],
                'type': filter_data['filter_type']  # Use 'type' not 'filter_type'
            }

            if filter_data['filter_type'] == 'numeric':
                # For numeric filters, use 'value' as [min, max] tuple
                min_val = filter_data.get('min_val')
                max_val = filter_data.get('max_val')
                if min_val is not None and max_val is not None:
                    behavioral_filter['value'] = [min_val, max_val]
                    behavioral_filter['type'] = 'range'  # Secure query expects 'range' not 'numeric'
                    logging.debug(f"Added numeric filter: {filter_data['table']}.{filter_data['column']} BETWEEN {min_val} AND {max_val}")
            elif filter_data['filter_type'] == 'categorical':
                # For categorical filters, use 'value' directly
                selected_values = filter_data.get('selected_values', [])
                if selected_values:
                    behavioral_filter['value'] = selected_values
                    logging.debug(f"Added categorical filter: {filter_data['table']}.{filter_data['column']} IN {selected_values}")

            # Only add the filter if it has a valid value
            if 'value' in behavioral_filter:
                behavioral_filters.append(behavioral_filter)
        else:
            logging.warning(f"Incomplete filter data: table={filter_data.get('table')}, column={filter_data.get('column')}, type={filter_data.get('filter_type')}")

    if enabled_count > 0:
        logging.debug(f"Converted to {len(behavioral_filters)} behavioral filters")
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