# StateManager Integration Plan

Based on comprehensive analysis of the current dcc.Store usage patterns, this document provides a detailed plan to create and integrate a StateManager abstraction that enables scalable state management while preserving existing functionality.

## Current State Analysis Summary

### Most Critical Stores (by usage frequency and impact):

1. **`merge-keys-store`** (11 references) - **CRITICAL**
   - Contains merge strategy information for cross-sectional vs longitudinal data
   - Used by nearly every callback in query.py
   - Dependencies: Available to all major data operations callbacks

2. **`merged-dataframe-store`** (5+ references) - **CRITICAL for cross-page data flow**
   - Primary data sharing mechanism between Query → Plotting → Profiling pages
   - Contains full processed dataset with metadata
   - Generated in query.py, consumed by plotting.py and profiling.py

3. **`available-tables-store`** (9 references) - **CRITICAL**
   - Lists behavioral tables available for querying
   - Core dependency for table selection and data initialization
   - Updated by initial data loading callback

4. **`phenotypic-filters-store`** (7 references) - **HIGH IMPACT**
   - Complex nested data structure storing filter configurations
   - Manages dynamic filter UI state with sophisticated management logic
   - Impacts live participant count and data generation

### Major Callback Pattern Categories:

#### 1. **Initial Data Loading Pattern** (Most Complex)
```python
@callback(
    [Output('available-tables-store', 'data'),
     Output('demographics-columns-store', 'data'),
     Output('behavioral-columns-store', 'data'),
     Output('column-dtypes-store', 'data'),
     Output('column-ranges-store', 'data'),
     Output('merge-keys-store', 'data'),
     Output('session-values-store', 'data'),
     Output('all-messages-store', 'data'),
     Output('merge-strategy-info', 'children')],
    [Input('query-data-status-section', 'id')]
)
```
- **9 simultaneous outputs** - Would need careful modification
- Populates core data stores on page load

#### 2. **Live Participant Count Pattern** (High Frequency Updates)
```python
@callback(
    Output('live-participant-count', 'children'),
    [Input('age-slider', 'value'),
     Input('rockland-substudy-store', 'data'),
     Input('session-selection-store', 'data'),
     Input('phenotypic-filters-store', 'data'),
     Input('merge-keys-store', 'data'),
     Input('available-tables-store', 'data')]
)
```
- **6 inputs** trigger real-time count updates
- Performance-critical callback that runs frequently

#### 3. **Data Generation Pattern** (Most Complex Logic)
```python
@callback(
    [Output('data-preview-area', 'children'),
     Output('merged-dataframe-store', 'data'),
     Output('data-processing-loading-output', 'children')],
    Input('generate-data-button', 'n_clicks'),
    [State('age-slider', 'value'),
     State('rockland-substudy-store', 'data'),
     State('session-selection-store', 'data'),
     State('phenotypic-filters-store', 'data'),
     State('selected-columns-per-table-store', 'data'),
     State('enwiden-data-checkbox', 'value'),
     State('merge-keys-store', 'data'),
     State('available-tables-store', 'data'),
     State('table-multiselect', 'value')]
)
```
- **9 State dependencies** - Complex state management
- Generates the main data output shared across pages

## Phase 1: Architecture Design & Foundation

### 1.1 Create StateManager Architecture

**Core Components:**
```python
# state_manager.py
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict
import json
import redis
from config_manager import get_config

class StateBackend(ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[Any]: pass
    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None): pass
    @abstractmethod
    def delete(self, key: str): pass
    @abstractmethod
    def exists(self, key: str) -> bool: pass

class ClientStateBackend(StateBackend):
    """Preserves existing dcc.Store behavior"""
    def get(self, key: str) -> str:
        return "CLIENT_MANAGED"  # Signal to use dcc.Store
    
class RedisStateBackend(StateBackend):
    """Server-side Redis storage"""
    
class DatabaseStateBackend(StateBackend):
    """Database-backed state storage"""

class StateManager:
    def __init__(self, backend_type: str = 'client'):
        self.backend = self._create_backend(backend_type)
        self.user_context = None
    
    def get_store_data(self, store_id: str) -> Any:
        key = self._build_key(store_id)
        return self.backend.get(key)
    
    def set_store_data(self, store_id: str, data: Any) -> Any:
        key = self._build_key(store_id)
        self.backend.set(key, data)
        return data if self.backend.__class__.__name__ == 'ClientStateBackend' else dash.no_update
```

### 1.2 Store Classification & Migration Priority

**Critical Stores (Phase 1):**
- `merge-keys-store` (11 refs) - Core data structure detection
- `merged-dataframe-store` (5+ refs) - Cross-page data sharing
- `available-tables-store` (9 refs) - Table metadata

**High-Impact Stores (Phase 2):**
- `phenotypic-filters-store` (7 refs) - Complex filter management
- `demographics-columns-store` - Core data schema
- `behavioral-columns-store` - Table structure info

**UI Preference Stores (Phase 3):**
- `plot-config-state-store` - Visualization preferences
- `age-slider-state-store` - Filter persistence
- `table-multiselect-state-store` - Table selection

## Phase 2: Implementation Strategy

### 2.1 Create State Manager Infrastructure

**Files to Create:**
1. `state_manager.py` - Core StateManager classes
2. `state_backends.py` - Backend implementations
3. `state_utils.py` - Helper functions and decorators
4. `tests/test_state_manager.py` - Comprehensive tests

### 2.2 Callback Modification Strategy

**Pattern 1: Simple Store Access**
```python
# Before
@callback(
    Output('component', 'property'),
    Input('store-id', 'data')
)
def callback(store_data):
    return process(store_data)

# After
@callback(
    Output('component', 'property'),
    Input('store-id', 'data'),
    State('user-session-id', 'data')  # New: user context
)
def callback(store_data, session_id):
    state_manager.set_user_context(session_id)
    # store_data may be None if using server backend
    data = state_manager.get_store_data('store-id') or store_data
    return process(data)
```

**Pattern 2: Multi-Output Callbacks (Most Complex)**
```python
# Before: 9-output callback in query.py
@callback(
    [Output('available-tables-store', 'data'),
     Output('demographics-columns-store', 'data'),
     # ... 7 more outputs
    ],
    [Input('query-data-status-section', 'id')]
)

# After: StateManager coordination
@callback(
    [Output('available-tables-store', 'data'),
     Output('demographics-columns-store', 'data'),
     # ... keep same outputs for compatibility
    ],
    [Input('query-data-status-section', 'id')],
    State('user-session-id', 'data')
)
def initialize_data_stores(_, session_id):
    state_manager.set_user_context(session_id)
    
    # Process data (same logic)
    results = get_table_info(config)
    
    # Store server-side AND return for client
    state_manager.set_store_data('available-tables-store', results[0])
    state_manager.set_store_data('demographics-columns-store', results[1])
    # ... etc
    
    # Return data for existing dcc.Store compatibility
    return results
```

## Phase 3: Migration Phases

### Phase 3.1: Foundation Setup (Week 1-2)
1. **Create StateManager infrastructure**
2. **Add user session management**
3. **Implement client backend (no-op)**
4. **Add comprehensive tests**

### Phase 3.2: Critical Store Migration (Week 3-4)
1. **migrate-keys-store**: Most referenced, fundamental to data operations
2. **available-tables-store**: Core table metadata
3. **Test cross-page data flow integrity**

### Phase 3.3: Complex Filter Migration (Week 5-6)
1. **phenotypic-filters-store**: Complex nested structure
2. **merged-dataframe-store**: Cross-page data sharing
3. **Validate live participant count performance**

### Phase 3.4: UI Preference Migration (Week 7-8)
1. **Plot configuration stores**
2. **Filter state persistence stores**
3. **Performance optimization**

## Phase 4: Integration Points & Testing

### 4.1 Critical Integration Points

**App.py Modifications:**
```python
# Add StateManager initialization
from state_manager import StateManager, get_state_manager

# Add user session management
app.layout = dbc.Container([
    dcc.Store(id='user-session-id', storage_type='session'),  # New
    # ... existing stores remain for backward compatibility
])

# Add session initialization callback
@app.callback(
    Output('user-session-id', 'data'),
    Input('global-location', 'pathname'),
    prevent_initial_call=False
)
def initialize_user_session(_):
    return str(uuid.uuid4())  # Generate unique session ID
```

**Config Integration:**
```python
# config_manager.py additions
def get_state_manager() -> StateManager:
    config = get_config()
    backend_type = getattr(config, 'STATE_BACKEND', 'client')
    return StateManager(backend_type)
```

### 4.2 Testing Strategy

**Test Categories:**
1. **Unit Tests**: StateManager, backends, helper functions
2. **Integration Tests**: Callback modifications work correctly
3. **Performance Tests**: Server-side state doesn't degrade performance
4. **Cross-Page Tests**: Data flow between pages preserved
5. **Backward Compatibility**: Existing functionality unchanged

**Critical Test Scenarios:**
- Multi-output callback with 9 simultaneous updates
- Live participant count performance with frequent updates
- Cross-page data sharing (Query → Plotting → Profiling)
- Complex filter state management

### 4.3 Deployment Strategy

**Feature Flag Approach:**
```python
# Enable gradual rollout
class Config:
    STATE_BACKEND: str = 'client'  # 'client', 'redis', 'database'
    ENABLE_SERVER_STATE: bool = False
    
# Allows testing server state without breaking production
```

**Rollback Plan:**
- Keep all existing dcc.Store components
- StateManager returns appropriate data for client or server mode
- Can switch backends via configuration without code changes

## Phase 5: Future Enhancements

### 5.1 Multi-User Support Ready
- User authentication integration points identified
- State isolation by user ID prepared
- Role-based access control hooks available

### 5.2 Performance Optimizations
- Caching layer for expensive state operations
- State compression for large datasets
- Background state cleanup and maintenance

### 5.3 Enterprise Features
- State backup and recovery
- Audit logging for state changes
- State synchronization across instances

## Cross-Page Data Flow Dependencies

1. **Query Page → Plotting Page:**
   - `merged-dataframe-store` → plotting data
   - `plot-config-store` manages visualization state

2. **Query Page → Profiling Page:**
   - `merged-dataframe-store` → profiling analysis
   - `profiling-options-state-store` for persistence

3. **Settings Page → All Pages:**
   - `app-config-store` affects configuration globally

## Implementation Timeline

**Week 1-2**: Foundation (StateManager, backends, tests)
**Week 3-4**: Critical stores (merge-keys, available-tables)
**Week 5-6**: Complex stores (filters, dataframes)
**Week 7-8**: UI stores and optimization
**Week 9**: Integration testing and performance validation
**Week 10**: Documentation and deployment preparation

## Critical Dependencies for Modifications

1. **High-Risk Changes:** 
   - `merge-keys-store`, `phenotypic-filters-store`, `merged-dataframe-store`
   - Multi-output callbacks with 6+ outputs/inputs

2. **Cross-Page Impact:**
   - Any changes to `merged-dataframe-store` structure affects 3 pages
   - Configuration changes in `app-config-store` have global effects

3. **Performance-Sensitive:**
   - Live participant count callback runs on every filter change
   - Plot generation callbacks with complex data transformations

This plan provides a gradual, low-risk migration path that maintains existing functionality while enabling future scaling requirements. The sophisticated state management system has complex interdependencies that require careful coordination to maintain data flow integrity across the multi-page application.