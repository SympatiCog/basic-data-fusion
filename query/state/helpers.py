"""
Helper utilities for consolidated Query page state management.

This module provides convenience functions for interacting with the unified state store,
abstracting the complexity of the consolidated state structure from individual callbacks.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from functools import wraps

from .models import (
    QueryPageState,
    DemographicFilters,
    PhenotypicFiltersState,
    ExportOptions,
    UIState,
    DataState,
    ImportExportState,
    STORE_MIGRATION_MAP
)


# Core state management functions

def get_query_state(store_data: Optional[Dict[str, Any]]) -> QueryPageState:
    """
    Get QueryPageState instance from store data.
    
    Args:
        store_data: Raw data from the consolidated state store
        
    Returns:
        QueryPageState instance
    """
    if not store_data:
        return QueryPageState()
    
    return QueryPageState.from_dict(store_data)


def update_query_state(current_state: Optional[Dict[str, Any]], 
                      updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update query state with new values.
    
    Args:
        current_state: Current state data
        updates: Dictionary of updates to apply
        
    Returns:
        Updated state dictionary
    """
    state = get_query_state(current_state)
    
    # Apply updates using dot notation paths
    for path, value in updates.items():
        _set_nested_value(state, path, value)
    
    # Update timestamp
    state.last_updated = datetime.now().isoformat()
    
    return state.to_dict()


def _set_nested_value(obj: Any, path: str, value: Any) -> None:
    """Set value at nested path (e.g., 'data.available_tables')."""
    parts = path.split('.')
    current = obj
    
    for part in parts[:-1]:
        if hasattr(current, part):
            current = getattr(current, part)
        elif isinstance(current, dict):
            current = current[part]
        else:
            raise AttributeError(f"Cannot access {part} in {type(current)}")
    
    final_part = parts[-1]
    if hasattr(current, final_part):
        setattr(current, final_part, value)
    elif isinstance(current, dict):
        current[final_part] = value
    else:
        raise AttributeError(f"Cannot set {final_part} in {type(current)}")


def _get_nested_value(obj: Any, path: str) -> Any:
    """Get value at nested path (e.g., 'data.available_tables')."""
    parts = path.split('.')
    current = obj
    
    for part in parts:
        if hasattr(current, part):
            current = getattr(current, part)
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    
    return current


# Demographic filters helpers

def get_demographic_filters(store_data: Optional[Dict[str, Any]]) -> DemographicFilters:
    """Get demographic filters from state."""
    state = get_query_state(store_data)
    return state.demographic_filters


def update_demographic_filters(current_state: Optional[Dict[str, Any]], 
                             updates: DemographicFilters) -> Dict[str, Any]:
    """Update demographic filters in state."""
    return update_query_state(current_state, {'demographic_filters': updates})


def update_age_range(current_state: Optional[Dict[str, Any]], 
                    age_range: List[Union[int, float]]) -> Dict[str, Any]:
    """Update age range in demographic filters."""
    return update_query_state(current_state, {'demographic_filters.age_range': age_range})


def update_study_sites(current_state: Optional[Dict[str, Any]], 
                      study_sites: List[str]) -> Dict[str, Any]:
    """Update study sites in demographic filters."""
    return update_query_state(current_state, {'demographic_filters.study_sites': study_sites})


def update_sessions(current_state: Optional[Dict[str, Any]], 
                   sessions: List[str]) -> Dict[str, Any]:
    """Update sessions in demographic filters."""
    return update_query_state(current_state, {'demographic_filters.sessions': sessions})


# Phenotypic filters helpers

def get_phenotypic_filters(store_data: Optional[Dict[str, Any]]) -> PhenotypicFiltersState:
    """Get phenotypic filters from state."""
    state = get_query_state(store_data)
    return state.phenotypic_filters


def update_phenotypic_filters(current_state: Optional[Dict[str, Any]], 
                            filters_state: PhenotypicFiltersState) -> Dict[str, Any]:
    """Update phenotypic filters in state."""
    return update_query_state(current_state, {'phenotypic_filters': filters_state})


def add_phenotypic_filter(current_state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Add a new phenotypic filter."""
    state = get_query_state(current_state)
    new_filter = {
        'id': state.phenotypic_filters['next_id'],
        'table': None,
        'column': None,
        'filter_type': None,
        'enabled': False,
        'range_values': None,
        'categorical_values': None
    }
    
    updated_filters = state.phenotypic_filters.copy()
    updated_filters['filters'].append(new_filter)
    updated_filters['next_id'] += 1
    
    return update_query_state(current_state, {'phenotypic_filters': updated_filters})


def clear_phenotypic_filters(current_state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Clear all phenotypic filters."""
    state = get_query_state(current_state)
    updated_filters = {
        'filters': [],
        'next_id': state.phenotypic_filters['next_id']
    }
    return update_query_state(current_state, {'phenotypic_filters': updated_filters})


# Data state helpers

def get_data_state(store_data: Optional[Dict[str, Any]]) -> DataState:
    """Get data state from store."""
    state = get_query_state(store_data)
    return state.data


def update_available_tables(current_state: Optional[Dict[str, Any]], 
                          tables: List[str]) -> Dict[str, Any]:
    """Update available tables in data state."""
    return update_query_state(current_state, {'data.available_tables': tables})


def update_column_info(current_state: Optional[Dict[str, Any]], 
                      column_dtypes: Dict[str, str],
                      column_ranges: Dict[str, List[Union[int, float]]]) -> Dict[str, Any]:
    """Update column information in data state."""
    return update_query_state(current_state, {
        'data.column_dtypes': column_dtypes,
        'data.column_ranges': column_ranges
    })


# Export options helpers

def get_export_options(store_data: Optional[Dict[str, Any]]) -> ExportOptions:
    """Get export options from state."""
    state = get_query_state(store_data)
    return state.export_options


def update_export_options(current_state: Optional[Dict[str, Any]], 
                         updates: ExportOptions) -> Dict[str, Any]:
    """Update export options in state."""
    return update_query_state(current_state, {'export_options': updates})


def update_selected_tables(current_state: Optional[Dict[str, Any]], 
                          tables: List[str]) -> Dict[str, Any]:
    """Update selected tables for export."""
    return update_query_state(current_state, {'export_options.selected_tables': tables})


def update_selected_columns(current_state: Optional[Dict[str, Any]], 
                          columns: Dict[str, List[str]]) -> Dict[str, Any]:
    """Update selected columns for export."""
    return update_query_state(current_state, {'export_options.selected_columns': columns})


# UI state helpers

def get_ui_state(store_data: Optional[Dict[str, Any]]) -> UIState:
    """Get UI state from store."""
    state = get_query_state(store_data)
    return state.ui_state


def update_modal_state(current_state: Optional[Dict[str, Any]], 
                      export_modal: Optional[bool] = None,
                      import_modal: Optional[bool] = None) -> Dict[str, Any]:
    """Update modal visibility states."""
    updates = {}
    if export_modal is not None:
        updates['ui_state.export_modal_open'] = export_modal
    if import_modal is not None:
        updates['ui_state.import_modal_open'] = import_modal
    
    return update_query_state(current_state, updates)


def update_participant_count(current_state: Optional[Dict[str, Any]], 
                           count: int) -> Dict[str, Any]:
    """Update live participant count."""
    return update_query_state(current_state, {'ui_state.live_participant_count': count})


# Backward compatibility helpers

def migrate_from_individual_stores(**store_values) -> Dict[str, Any]:
    """
    Migrate data from individual stores to consolidated state.
    
    Args:
        **store_values: Keyword arguments where keys are old store IDs
                       and values are the store data
                       
    Returns:
        Consolidated state dictionary
    """
    state = QueryPageState()
    
    for store_id, value in store_values.items():
        if store_id in STORE_MIGRATION_MAP and value is not None:
            path = STORE_MIGRATION_MAP[store_id]
            try:
                _set_nested_value(state, path, value)
            except AttributeError:
                # Log but continue - some migrations might not be perfect
                print(f"Warning: Could not migrate {store_id} to {path}")
    
    state.last_updated = datetime.now().isoformat()
    return state.to_dict()


def extract_to_individual_store(store_data: Optional[Dict[str, Any]], 
                              store_id: str) -> Any:
    """
    Extract individual store value from consolidated state.
    
    Args:
        store_data: Consolidated state data
        store_id: ID of the individual store to extract
        
    Returns:
        Value for the individual store
    """
    if not store_data or store_id not in STORE_MIGRATION_MAP:
        return None
    
    state = get_query_state(store_data)
    path = STORE_MIGRATION_MAP[store_id]
    
    try:
        return _get_nested_value(state, path)
    except (AttributeError, KeyError):
        return None


# Decorator for callback compatibility

def with_consolidated_state(extract_stores: List[str]):
    """
    Decorator to make existing callbacks work with consolidated state.
    
    Args:
        extract_stores: List of store IDs to extract from consolidated state
    """
    def decorator(callback_func):
        @wraps(callback_func)
        def wrapper(*args, **kwargs):
            # Last argument should be the consolidated state
            if args:
                consolidated_state = args[-1]
                extracted_values = []
                
                for store_id in extract_stores:
                    value = extract_to_individual_store(consolidated_state, store_id)
                    extracted_values.append(value)
                
                # Replace the consolidated state with extracted individual store values
                new_args = list(args[:-1]) + extracted_values
                return callback_func(*new_args, **kwargs)
            
            return callback_func(*args, **kwargs)
        
        return wrapper
    return decorator