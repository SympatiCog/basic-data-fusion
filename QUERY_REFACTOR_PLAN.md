# Detailed Query Module Refactoring Plan

## **Current State Analysis**
- **File**: `pages/query.py` (2,429 lines)
- **Callbacks**: 35 Dash callbacks 
- **Functions**: ~20 helper functions mixed with callbacks
- **Architecture**: Monolithic UI + business logic + state management

## **Phase 1: Foundation & Structure (Priority: High)**

### 1.1 Create Query Package Structure
```
query/
├── __init__.py          # ✅ Exists - well-structured API
├── ui/                  # 🆕 New UI components
│   ├── __init__.py
│   ├── layout.py        # Main page layout
│   ├── components.py    # Reusable UI components
│   └── styles.py        # Styling constants
├── callbacks/           # 🆕 New callback modules  
│   ├── __init__.py
│   ├── data_loading.py  # Data status, table info
│   ├── filters.py       # Demographic & phenotypic filters
│   ├── export.py        # Data generation & export
│   └── state.py         # State management callbacks
├── helpers/             # 🆕 New helper functions
│   ├── __init__.py
│   ├── ui_builders.py   # UI generation helpers
│   ├── data_formatters.py # Data display formatting
│   └── validation.py    # UI validation logic
└── [existing files]     # Keep current modules
```

### 1.2 Extract Layout Components
**Target Files:**
- `query/ui/layout.py` - Main page layout definition
- `query/ui/components.py` - Reusable components (cards, filters, buttons)
- `query/ui/styles.py` - Centralized styling constants

**Components to Extract from `query.py:36-300`:**
- Demographics filter card
- Phenotypic filter card  
- Data export selection area
- Query results container
- Modals (export parameters, import parameters)

## **Phase 2: Callback Decomposition (Priority: High)**

### 2.1 Data Loading Callbacks → `query/callbacks/data_loading.py`
**Functions to Move:**
- `load_initial_data_info()` - Line 970
- `update_data_status_section()` - Line 296
- `update_table_multiselect_options()` - Line 1012
- `update_column_selection_area()` - Line 1072

### 2.2 Filter Management → `query/callbacks/filters.py`
**Functions to Move:**
- `update_age_slider()` - Line 336
- `update_dynamic_demographic_filters()` - Line 382
- `manage_phenotypic_filters()` - Line 467 (Complex - 138 lines)
- `render_phenotypic_filters()` - Line 605 (Complex - 179 lines)
- `update_phenotypic_session_notice()` - Line 784
- `update_live_participant_count()` - Line 867 (Complex - 103 lines)

### 2.3 Data Export → `query/callbacks/export.py`
**Functions to Move:**
- `handle_generate_data()` - Line 1195 (Complex - largest callback)
- Export parameter modal callbacks
- Download functionality callbacks

### 2.4 State Management → `query/callbacks/state.py`
**Functions to Move:**
- All `restore_*_value()` functions (Lines 1024-1072)
- `update_*_store()` functions (Lines 442-466)
- `update_selected_columns_store()` - Line 1130

## **Phase 3: Helper Function Extraction (Priority: Medium)**

### 3.1 UI Builders → `query/helpers/ui_builders.py`
**Functions to Extract:**
- Phenotypic filter card generation logic
- Dynamic demographic filter builders
- Column selection UI generation
- Data preview table formatting

### 3.2 Data Formatters → `query/helpers/data_formatters.py`
**Functions to Extract:**
- `convert_phenotypic_to_behavioral_filters()` - Line 800
- Data summary formatting functions
- Participant count formatting
- Export filename generation

### 3.3 Validation Logic → `query/helpers/validation.py`
**Functions to Extract:**
- Filter validation logic
- Table/column selection validation
- Parameter validation helpers

## **Phase 4: State Consolidation (Priority: Medium)**

### 4.1 Replace Multiple Stores with Single State Store
**Current State Stores (19 individual stores):**
```python
# Replace these fragmented stores:
age-slider-state-store, table-multiselect-state-store,
selected-columns-state-store, phenotypic-filters-state-store,
study-site-dropdown-state-store, session-dropdown-state-store,
# ... 13 more stores
```

**With Single Consolidated Store:**
```python
dcc.Store(id='query-page-state', data={
    'demographic_filters': {...},
    'phenotypic_filters': [...],
    'selected_tables': [...],
    'selected_columns': {...},
    'export_options': {...}
})
```

### 4.2 Create State Management Classes
```python
# query/helpers/state_models.py
@dataclass
class QueryPageState:
    demographic_filters: Dict
    phenotypic_filters: List[Dict]
    selected_tables: List[str]
    selected_columns: Dict[str, List[str]]
    export_options: Dict
```

## **Phase 5: Integration & Testing (Priority: High)**

### 5.1 Update Main Application
**File**: `app.py`
```python
# Replace current import
from pages import query

# With modular imports  
from query.ui.layout import layout as query_layout
from query.callbacks import register_all_callbacks

# Register page with new layout
dash.register_page(
    __name__, 
    path='/', 
    title='Query Data',
    layout=query_layout
)

# Register all callbacks
register_all_callbacks(app)
```

### 5.2 Callback Registration System
**File**: `query/callbacks/__init__.py`
```python
def register_all_callbacks(app):
    """Register all query page callbacks with the Dash app."""
    from . import data_loading, filters, export, state
    
    data_loading.register_callbacks(app)
    filters.register_callbacks(app)
    export.register_callbacks(app)
    state.register_callbacks(app)
```

### 5.3 Comprehensive Testing Strategy
- **Unit Tests**: Test extracted helper functions independently
- **Integration Tests**: Test callback chains and state transitions
- **UI Tests**: Verify layout rendering and component interactions
- **Regression Tests**: Ensure existing functionality preserved

## **Phase 6: Cleanup & Optimization (Priority: Low)**

### 6.1 Legacy File Management
- **Deprecate**: `pages/query.py` (create minimal redirect)
- **Archive**: Move to `legacy/query_old.py` for reference
- **Clean**: Remove unused imports and dependencies

### 6.2 Performance Optimizations
- **Callback Optimization**: Reduce unnecessary callback triggers
- **State Caching**: Implement intelligent state caching
- **Lazy Loading**: Load heavy components on demand

## **Implementation Timeline**

**Week 1-2: Foundation**
- Phase 1: Create package structure, extract layout
- Phase 2.1: Extract data loading callbacks

**Week 3-4: Core Functionality** 
- Phase 2.2: Extract filter management (most complex)
- Phase 2.3: Extract export functionality

**Week 5: Integration**
- Phase 2.4: Extract state management
- Phase 5.1-5.2: Update app integration

**Week 6: Polish**
- Phase 4: State consolidation (optional optimization)
- Phase 5.3: Comprehensive testing
- Phase 6: Cleanup and optimization

## **Risk Mitigation**

1. **Callback Dependencies**: Map all Input/Output relationships before moving
2. **State Synchronization**: Ensure state stores remain consistent during refactoring  
3. **Import Cycles**: Carefully manage imports between new modules
4. **Regression Testing**: Maintain comprehensive test coverage throughout
5. **Incremental Deployment**: Refactor in small, testable chunks

This plan transforms the monolithic 2,429-line file into a maintainable, modular architecture while preserving all existing functionality and leveraging the already-excellent query generation infrastructure.

## **Key Benefits**

1. **Maintainability**: Smaller, focused modules are easier to understand and modify
2. **Testability**: Extracted helper functions can be unit tested independently
3. **Reusability**: UI components and helpers can be reused across the application
4. **Collaboration**: Multiple developers can work on different modules simultaneously
5. **Performance**: Reduced callback complexity and optimized state management
6. **Scalability**: New features can be added without modifying the core architecture

## **Success Metrics**

- **Code Reduction**: Reduce main query file from 2,429 to <200 lines
- **Modularity**: 35 callbacks distributed across 4 focused modules
- **Test Coverage**: Achieve >90% test coverage for extracted functions
- **Performance**: Maintain or improve page load and interaction response times
- **Documentation**: Clear API documentation for all new modules