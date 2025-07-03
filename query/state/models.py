"""
State models for the consolidated Query page state management.

This module defines typed data structures for the unified state store,
replacing the 19+ individual dcc.Store components with a single consolidated state.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union
from typing_extensions import TypedDict


# Individual state component types
class PhenotypicFilter(TypedDict):
    """Single phenotypic filter configuration."""
    id: int
    table: Optional[str]
    column: Optional[str]
    filter_type: Optional[str]  # 'range' or 'categorical'
    enabled: bool
    range_values: Optional[List[Union[int, float]]]
    categorical_values: Optional[List[str]]


class PhenotypicFiltersState(TypedDict):
    """Phenotypic filters state structure."""
    filters: List[PhenotypicFilter]
    next_id: int


class DemographicFilters(TypedDict):
    """Demographic filter settings."""
    age_range: Optional[List[Union[int, float]]]
    study_sites: Optional[List[str]]
    sessions: Optional[List[str]]


class ExportOptions(TypedDict):
    """Data export configuration."""
    selected_tables: List[str]
    selected_columns: Dict[str, List[str]]
    enwiden_data: bool
    consolidate_baseline: bool
    export_format: str  # 'csv', 'excel', etc.


class UIState(TypedDict):
    """UI component state tracking."""
    export_modal_open: bool
    import_modal_open: bool
    current_query_metadata: Optional[Dict[str, Any]]
    live_participant_count: Optional[int]


class DataState(TypedDict):
    """Core data information state."""
    available_tables: List[str]
    demographics_columns: List[str]
    behavioral_columns: Dict[str, List[str]]
    column_dtypes: Dict[str, str]
    column_ranges: Dict[str, List[Union[int, float]]]
    merge_keys: Optional[Dict[str, Any]]
    session_values: List[str]
    study_site_values: List[str]


class ImportExportState(TypedDict):
    """Import/export operation state."""
    import_validation_results: Optional[Dict[str, Any]]
    imported_file_content: Optional[str]
    export_query_trigger: Optional[int]
    import_query_trigger: Optional[int]


# Main consolidated state structure
@dataclass
class QueryPageState:
    """
    Consolidated state for the Query page.
    
    This replaces 19+ individual dcc.Store components with a single,
    typed state structure for better maintainability and consistency.
    """
    
    # Core data information
    data: DataState = field(default_factory=lambda: DataState(
        available_tables=[],
        demographics_columns=[],
        behavioral_columns={},
        column_dtypes={},
        column_ranges={},
        merge_keys=None,
        session_values=[],
        study_site_values=[]
    ))
    
    # Filter configurations
    demographic_filters: DemographicFilters = field(default_factory=lambda: DemographicFilters(
        age_range=None,
        study_sites=None,
        sessions=None
    ))
    
    phenotypic_filters: PhenotypicFiltersState = field(default_factory=lambda: PhenotypicFiltersState(
        filters=[],
        next_id=1
    ))
    
    # Export settings
    export_options: ExportOptions = field(default_factory=lambda: ExportOptions(
        selected_tables=[],
        selected_columns={},
        enwiden_data=False,
        consolidate_baseline=True,
        export_format='csv'
    ))
    
    # UI state
    ui_state: UIState = field(default_factory=lambda: UIState(
        export_modal_open=False,
        import_modal_open=False,
        current_query_metadata=None,
        live_participant_count=None
    ))
    
    # Import/export operations
    import_export: ImportExportState = field(default_factory=lambda: ImportExportState(
        import_validation_results=None,
        imported_file_content=None,
        export_query_trigger=None,
        import_query_trigger=None
    ))
    
    # User session tracking
    user_session_id: Optional[str] = None
    
    # Timestamp for state management
    last_updated: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary for storage in dcc.Store."""
        return {
            'data': self.data,
            'demographic_filters': self.demographic_filters,
            'phenotypic_filters': self.phenotypic_filters,
            'export_options': self.export_options,
            'ui_state': self.ui_state,
            'import_export': self.import_export,
            'user_session_id': self.user_session_id,
            'last_updated': self.last_updated
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryPageState':
        """Create state instance from dictionary."""
        if not data:
            return cls()
        
        return cls(
            data=data.get('data', cls().data),
            demographic_filters=data.get('demographic_filters', cls().demographic_filters),
            phenotypic_filters=data.get('phenotypic_filters', cls().phenotypic_filters),
            export_options=data.get('export_options', cls().export_options),
            ui_state=data.get('ui_state', cls().ui_state),
            import_export=data.get('import_export', cls().import_export),
            user_session_id=data.get('user_session_id'),
            last_updated=data.get('last_updated')
        )


# State migration utilities for backward compatibility

# Mapping from old store IDs to new state paths
STORE_MIGRATION_MAP = {
    'available-tables-store': 'data.available_tables',
    'demographics-columns-store': 'data.demographics_columns',
    'behavioral-columns-store': 'data.behavioral_columns',
    'column-dtypes-store': 'data.column_dtypes',
    'column-ranges-store': 'data.column_ranges',
    'merge-keys-store': 'data.merge_keys',
    'session-values-store': 'data.session_values',
    'study-site-store': 'demographic_filters.study_sites',
    'session-selection-store': 'demographic_filters.sessions',
    'phenotypic-filters-store': 'phenotypic_filters',
    'selected-columns-per-table-store': 'export_options.selected_columns',
    'age-slider-state-store': 'demographic_filters.age_range',
    'table-multiselect-state-store': 'export_options.selected_tables',
    'enwiden-data-checkbox-state-store': 'export_options.enwiden_data',
    'import-validation-results-store': 'import_export.import_validation_results',
    'imported-file-content-store': 'import_export.imported_file_content',
    'query-export-modal-state': 'ui_state.export_modal_open',
    'query-import-modal-state': 'ui_state.import_modal_open',
    'current-query-metadata-store': 'ui_state.current_query_metadata'
}