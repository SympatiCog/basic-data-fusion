"""
Data formatting helper functions for the Query interface.

This module contains functions that format and transform data
for display and processing.
"""

import logging

# Import from the original location to avoid duplication
from pages.query import convert_phenotypic_to_behavioral_filters


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