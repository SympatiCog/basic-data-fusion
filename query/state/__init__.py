"""
State management package for the Query interface.

This package provides unified state management for the query page,
consolidating multiple individual stores into a single, typed state system.
"""

from .models import QueryPageState, DemographicFilters, PhenotypicFilter, ExportOptions
from .helpers import (
    get_query_state,
    update_query_state,
    get_demographic_filters,
    update_demographic_filters,
    get_phenotypic_filters,
    update_phenotypic_filters,
    get_export_options,
    update_export_options,
    migrate_from_individual_stores
)

__all__ = [
    'QueryPageState',
    'DemographicFilters', 
    'PhenotypicFilter',
    'ExportOptions',
    'get_query_state',
    'update_query_state',
    'get_demographic_filters',
    'update_demographic_filters',
    'get_phenotypic_filters',
    'update_phenotypic_filters',
    'get_export_options',
    'update_export_options',
    'migrate_from_individual_stores'
]