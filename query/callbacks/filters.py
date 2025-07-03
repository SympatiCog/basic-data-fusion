"""
Filter management callbacks for the Query interface.

This module contains callbacks responsible for:
- Age slider updates
- Dynamic demographic filters
- Phenotypic filter management (Re-refactored for stability)
- Live participant count updates
"""

import logging
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html, no_update

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

from analysis.demographics import has_multisite_data
from config_manager import get_config
from utils import (
    MergeKeys,
    get_study_site_values,
    get_unique_column_values,
    is_numeric_dtype,
    get_db_connection,
)

# --- AGE AND DYNAMIC DEMOGRAPHIC FILTERS ---

def update_age_slider(demo_cols, col_ranges, stored_age_value):
    config = get_config()
    if not demo_cols or config.AGE_COLUMN not in demo_cols or not col_ranges:
        return 0, 100, [0, 100], {}, True, f"Age filter disabled: '{config.AGE_COLUMN}' not found."

    age_col_key = config.AGE_COLUMN
    if age_col_key in col_ranges:
        min_age, max_age = map(int, col_ranges[age_col_key])
        default_min, default_max = config.DEFAULT_AGE_SELECTION
        value = [max(min_age, default_min), min(max_age, default_max)]
        if stored_age_value and len(stored_age_value) == 2:
            stored_min, stored_max = stored_age_value
            if min_age <= stored_min <= max_age and min_age <= stored_max <= max_age:
                value = stored_age_value
        marks = {i: str(i) for i in range(min_age, max_age + 1, 10)}
        marks.update({min_age: str(min_age), max_age: str(max_age)})
        return min_age, max_age, value, marks, False, f"Age range: {min_age}-{max_age}"
    
    return 0, 100, config.DEFAULT_AGE_SELECTION, {}, True, f"Age range for '{config.AGE_COLUMN}' not found."

def update_dynamic_demographic_filters(demo_cols, session_values, merge_keys_dict,
                                     input_rockland_values, input_session_values):
    config = get_config()
    if not demo_cols:
        return html.P("Demographic information not yet available.", style={'fontStyle': 'italic'})
    children = []
    if has_multisite_data(demo_cols, config.STUDY_SITE_COLUMN):
        children.append(html.H5("Study Site Selection", style={'marginTop': '15px'}))
        study_site_values = get_study_site_values(config) or config.ROCKLAND_BASE_STUDIES
        selected_sites = input_rockland_values if input_rockland_values is not None else study_site_values
        children.append(dcc.Dropdown(id='study-site-dropdown', options=[{'label': s, 'value': s} for s in study_site_values], value=selected_sites, multi=True, placeholder="Select Study Sites..."))
    if merge_keys_dict:
        mk = MergeKeys.from_dict(merge_keys_dict)
        if mk.is_longitudinal and mk.session_id and session_values:
            children.append(html.H5("Session/Visit Selection", style={'marginTop': '15px'}))
            session_value = input_session_values if input_session_values is not None else session_values
            children.append(dcc.Dropdown(id='session-dropdown', options=[{'label': s, 'value': s} for s in session_values], value=session_value, multi=True, placeholder=f"Select {mk.session_id} values..."))
    return children if children else html.P("No dataset-specific filters applicable.", style={'fontStyle': 'italic'})

# --- STABLE PHENOTYPIC FILTER CALLBACKS (MULTI-CALLBACK APPROACH - CORRECTED) ---

def _unwrap_nested_arrays(value):
    """Recursively unwrap deeply nested arrays to get the actual value."""
    if not isinstance(value, list):
        return value
    
    # Handle empty lists
    if not value:
        return None
    
    # If it's a list with exactly 2 elements and both are numbers, it might be a valid range slider value
    if len(value) == 2 and all(isinstance(x, (int, float)) for x in value):
        return value
    
    # If it's a list with multiple non-list elements, it might be a valid multi-select value
    if len(value) > 1 and all(not isinstance(x, list) for x in value):
        return value
    
    # If it's a list with one element, recursively unwrap it
    if len(value) == 1:
        return _unwrap_nested_arrays(value[0])
    
    # If it's a list with multiple elements, find the first non-None value
    for item in value:
        unwrapped = _unwrap_nested_arrays(item)
        if unwrapped is not None:
            return unwrapped
    
    return None

def _get_triggered_info():
    """Helper to safely get the component that triggered the callback and its value."""
    ctx = dash.callback_context
    if not ctx.triggered or not ctx.triggered_id:
        return None, None, None
    
    # The triggered_id contains the actual component that triggered
    triggered_id = ctx.triggered_id
    filter_id = triggered_id.get('index') if isinstance(triggered_id, dict) else None
    
    # Get all values from the callback context
    all_values = ctx.triggered[0]['value']
    
    # For dash.ALL patterns, we need to extract the specific value for the triggered component
    if isinstance(all_values, list) and filter_id is not None:
        # Try to find the index of the triggered component
        inputs = ctx.inputs_list[0] if hasattr(ctx, 'inputs_list') and ctx.inputs_list else []
        triggered_value = None
        
        for i, input_spec in enumerate(inputs):
            if input_spec.get('id', {}).get('index') == filter_id:
                if i < len(all_values):
                    triggered_value = all_values[i]
                break
        
        # If we couldn't find the specific value, try to extract any valid value
        if triggered_value is None:
            for val in all_values:
                unwrapped = _unwrap_nested_arrays(val)
                if unwrapped is not None:
                    triggered_value = unwrapped
                    break
    else:
        triggered_value = all_values
    
    # Always unwrap nested arrays to prevent cascading array nesting
    triggered_value = _unwrap_nested_arrays(triggered_value)
    
    return triggered_id, triggered_value, filter_id

def _update_filter_property(state, filter_id, props_to_update):
    """Safely updates properties for a specific filter in the state."""
    if not state or 'filters' not in state:
        return dash.no_update
    
    import copy
    new_filters = []
    for f in state['filters']:
        if f['id'] == filter_id:
            # Use deep copy to avoid any shared references
            updated_filter = copy.deepcopy(f)
            updated_filter.update(props_to_update)
            new_filters.append(updated_filter)
        else:
            # Also deep copy other filters to ensure no shared state
            new_filters.append(copy.deepcopy(f))
    return {'filters': new_filters, 'next_id': state['next_id']}

def add_phenotypic_filter(n_clicks, state):
    if not n_clicks: return dash.no_update
    import copy
    state = state or {'filters': [], 'next_id': 1}
    new_filter = {'id': state['next_id'], 'table': None, 'column': None, 'filter_type': None, 'enabled': False}
    # Create deep copies to avoid shared state
    new_filters = copy.deepcopy(state['filters']) + [new_filter]
    return {'filters': new_filters, 'next_id': state['next_id'] + 1}

def clear_phenotypic_filters(n_clicks, state):
    if not n_clicks: return dash.no_update
    return {'filters': [], 'next_id': state.get('next_id', 1)}

def remove_phenotypic_filter(n_clicks_all, state):
    if not any(n_clicks_all): return dash.no_update
    import copy
    _, _, filter_id = _get_triggered_info()
    if filter_id is None: return dash.no_update
    # Create deep copies of filters that aren't being removed
    new_filters = [copy.deepcopy(f) for f in state['filters'] if f['id'] != filter_id]
    return {'filters': new_filters, 'next_id': state['next_id']}

def update_filter_table(values, state):
    _, value, filter_id = _get_triggered_info()
    if filter_id is None: return dash.no_update
    
    # Additional defensive unwrapping (should be handled by _get_triggered_info, but adding safety)
    value = _unwrap_nested_arrays(value)
    
    logging.debug(f"update_filter_table: filter_id={filter_id}, value={value}, type={type(value)}")
    
    return _update_filter_property(state, filter_id, {'table': value, 'column': None, 'filter_type': None, 'enabled': False})

def update_filter_column(values, state):
    _, value, filter_id = _get_triggered_info()
    if filter_id is None: return dash.no_update
    
    # Additional defensive unwrapping (should be handled by _get_triggered_info, but adding safety)
    value = _unwrap_nested_arrays(value)
    
    logging.debug(f"update_filter_column: filter_id={filter_id}, value={value}, type={type(value)}")
    
    # When a column is selected, try to auto-enable if it's numeric
    if value and state and 'filters' in state:
        # Find the current filter
        current_filter = None
        for f in state['filters']:
            if f['id'] == filter_id:
                current_filter = f
                break
        
        if current_filter and current_filter.get('table'):
            try:
                # Try to determine if this is a numeric column and auto-enable
                from config_manager import get_config
                from utils import is_numeric_dtype
                
                config = get_config()
                # Try to get column type from stored data
                # This is a simplified check - the full auto-enablement happens in render
                
                # For now, just reset the filter properties
                return _update_filter_property(state, filter_id, {'column': value, 'filter_type': None, 'enabled': False})
            except Exception as e:
                logging.debug(f"Could not auto-check column type: {e}")
    
    # Reset filter properties when column changes
    return _update_filter_property(state, filter_id, {'column': value, 'filter_type': None, 'enabled': False})

def update_numeric_filter(values, state):
    _, value, filter_id = _get_triggered_info()
    logging.info(f"update_numeric_filter called: filter_id={filter_id}, value={value}, type={type(value)}")
    
    if filter_id is None or not value: 
        logging.info(f"update_numeric_filter: early return - filter_id={filter_id}, value={value}")
        return dash.no_update
    
    # Handle case where unwrapping resulted in a single value instead of a list
    if not isinstance(value, list):
        logging.info(f"update_numeric_filter: received single value {value}, skipping update")
        return dash.no_update
    
    # Ensure we have exactly 2 values for a range
    if len(value) != 2:
        logging.info(f"update_numeric_filter: received {len(value)} values, expected 2")
        return dash.no_update
    
    logging.info(f"update_numeric_filter: updating filter {filter_id} with range {value}")
    result = _update_filter_property(state, filter_id, {'min_val': value[0], 'max_val': value[1], 'filter_type': 'numeric', 'enabled': True})
    logging.info(f"update_numeric_filter: result = {result}")
    return result

def update_categorical_filter(values, state):
    _, value, filter_id = _get_triggered_info()
    logging.info(f"update_categorical_filter called: filter_id={filter_id}, value={value}, type={type(value)}")
    
    if filter_id is None: 
        logging.info(f"update_categorical_filter: early return - filter_id is None")
        return dash.no_update
    
    # Additional defensive unwrapping for categorical values
    value = _unwrap_nested_arrays(value)
    
    # Ensure value is a list for categorical filters
    if value is not None and not isinstance(value, list):
        value = [value]
    
    logging.info(f"update_categorical_filter: updating filter {filter_id} with values {value}")
    result = _update_filter_property(state, filter_id, {'selected_values': value, 'filter_type': 'categorical', 'enabled': bool(value)})
    logging.info(f"update_categorical_filter: result = {result}")
    return result

def auto_enable_filter_on_column_selection(filters_state, column_dtypes, column_ranges):
    """Auto-enable filters when table and column are selected, especially for numeric columns."""
    if not filters_state or not filters_state.get('filters'):
        return dash.no_update
    
    from config_manager import get_config
    from utils import is_numeric_dtype
    
    config = get_config()
    updated_filters = []
    state_changed = False
    
    for f in filters_state['filters']:
        updated_filter = f.copy()
        table = f.get('table')
        column = f.get('column')
        
        # Auto-enable numeric filters that have table and column but aren't set up yet
        if (table and column and 
            f.get('filter_type') is None and 
            column_dtypes and column in column_dtypes and 
            is_numeric_dtype(column_dtypes[column])):
            
            if column in (column_ranges or {}):
                min_v, max_v = column_ranges[column]
                updated_filter.update({
                    'filter_type': 'numeric',
                    'min_val': int(min_v),
                    'max_val': int(max_v),
                    'enabled': True
                })
                state_changed = True
                logging.debug(f"Auto-enabled numeric filter {f['id']} for column {column}")
        
        # Auto-setup categorical filters that have table and column but aren't set up yet
        elif (table and column and 
              f.get('filter_type') is None and 
              column_dtypes and column in column_dtypes and 
              not is_numeric_dtype(column_dtypes[column])):
            
            updated_filter.update({
                'filter_type': 'categorical',
                'selected_values': f.get('selected_values', []),
                'enabled': bool(f.get('selected_values', []))
            })
            state_changed = True
            logging.debug(f"Auto-setup categorical filter {f['id']} for column {column}")
        
        updated_filters.append(updated_filter)
    
    if state_changed:
        return {'filters': updated_filters, 'next_id': filters_state['next_id']}
    
    return dash.no_update

def unified_filter_update(table_values, column_values, range_values, categorical_values, state):
    """Unified callback to handle all filter value updates and avoid race conditions."""
    import copy
    import dash
    
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update
    
    # Only process the first trigger to avoid duplicate updates
    trigger = ctx.triggered[0]
    trigger_id = trigger['prop_id']
    triggered_value = trigger['value']
    
    logging.info(f"unified_filter_update: processing trigger {trigger_id} = {triggered_value}")
    
    # Skip updates for None values during initialization
    if triggered_value is None:
        logging.info(f"unified_filter_update: skipping None value for {trigger_id}")
        return dash.no_update
    
    # Extract filter index from the trigger ID
    import json
    if '.value' in trigger_id:
        id_part = trigger_id.split('.value')[0]
        try:
            filter_spec = json.loads(id_part)
            filter_id = filter_spec.get('index')
            filter_type = filter_spec.get('type')
        except:
            logging.error(f"Could not parse filter ID from {trigger_id}")
            return dash.no_update
    else:
        return dash.no_update
    
    if not state or 'filters' not in state or filter_id is None:
        return dash.no_update
    
    # Create deep copy of state only once per update
    new_state = copy.deepcopy(state)
    
    # Find the filter to update
    target_filter = None
    for f in new_state['filters']:
        if f['id'] == filter_id:
            target_filter = f
            break
    
    if not target_filter:
        logging.warning(f"Could not find filter with ID {filter_id}")
        return dash.no_update
    
    # Unwrap the triggered value
    unwrapped_value = _unwrap_nested_arrays(triggered_value)
    
    # Update based on filter type - only update the specific property that changed
    if filter_type == 'phenotypic-table':
        target_filter['table'] = unwrapped_value
        target_filter['column'] = None  # Reset column when table changes
        target_filter['filter_type'] = None
        target_filter['enabled'] = False
        logging.info(f"Updated table for filter {filter_id}: {unwrapped_value}")
        
    elif filter_type == 'phenotypic-column':
        target_filter['column'] = unwrapped_value
        target_filter['filter_type'] = None  # Reset type when column changes
        target_filter['enabled'] = False
        logging.info(f"Updated column for filter {filter_id}: {unwrapped_value}")
        
    elif filter_type == 'phenotypic-range':
        if isinstance(unwrapped_value, list) and len(unwrapped_value) == 2:
            target_filter['min_val'] = unwrapped_value[0]
            target_filter['max_val'] = unwrapped_value[1]
            target_filter['filter_type'] = 'numeric'
            target_filter['enabled'] = True
            logging.info(f"Updated range for filter {filter_id}: {unwrapped_value}")
        else:
            logging.warning(f"Invalid range value for filter {filter_id}: {unwrapped_value}")
            return dash.no_update
            
    elif filter_type == 'phenotypic-categorical':
        # Ensure categorical values are in list format
        if unwrapped_value is not None and not isinstance(unwrapped_value, list):
            unwrapped_value = [unwrapped_value]
        
        target_filter['selected_values'] = unwrapped_value
        target_filter['filter_type'] = 'categorical'
        target_filter['enabled'] = bool(unwrapped_value)
        logging.info(f"Updated categorical values for filter {filter_id}: {unwrapped_value}")
    
    logging.info(f"unified_filter_update: returning updated state")
    return new_state

# --- RENDER, COUNT, AND NOTICE CALLBACKS ---

def render_phenotypic_filters(filters_state, available_tables, behavioral_columns, demo_columns, column_dtypes, column_ranges, merge_keys_dict):
    config = get_config()
    if not filters_state or not filters_state.get('filters'):
        return html.Div("No phenotypic filters added yet.", className="text-muted font-italic")
    
    data = {'tables': available_tables or [], 'behavioral_cols': behavioral_columns or {}, 'demo_cols': demo_columns or [], 'dtypes': column_dtypes or {}, 'ranges': column_ranges or {}}
    merge_keys = MergeKeys.from_dict(merge_keys_dict) if merge_keys_dict else MergeKeys(primary_id="unknown")
    demographics_table_name = config.get_demographics_table_name()
    table_options = [{'label': demographics_table_name, 'value': demographics_table_name}] + [{'label': table, 'value': table} for table in data['tables']]
    
    filter_cards = []
    for f in filters_state['filters']:
        filter_id, table, column = f['id'], f.get('table'), f.get('column')
        
        # Fix invalid column values automatically
        if not isinstance(column, (str, type(None))):
            logging.warning(f"Filter {filter_id} has an invalid column value: {column}. Fixing automatically.")
            column = _unwrap_nested_arrays(column)
            # Update the filter state to fix the issue
            f['column'] = column
        
        # Also fix invalid table values
        if not isinstance(table, (str, type(None))):
            logging.warning(f"Filter {filter_id} has an invalid table value: {table}. Fixing automatically.")
            table = _unwrap_nested_arrays(table)
            f['table'] = table
        
        column_options = []
        if table:
            cols = data['demo_cols'] if table == demographics_table_name else data['behavioral_cols'].get(table, [])
            id_cols = {merge_keys.primary_id, merge_keys.session_id, merge_keys.composite_id}
            column_options = [{'label': c, 'value': c} for c in cols if c not in id_cols]

        filter_component = html.Div("Select table and column", className="text-muted small")
        if table and column:
            dtype = data['dtypes'].get(column)
            if dtype and is_numeric_dtype(dtype):
                if column in data['ranges']:
                    min_v, max_v = map(int, data['ranges'][column])
                    val = [f.get('min_val', min_v), f.get('max_val', max_v)]
                    filter_component = dcc.RangeSlider(id={'type': 'phenotypic-range', 'index': filter_id}, min=min_v, max=max_v, value=val, tooltip={"placement": "bottom", "always_visible": True}, allowCross=False, step=1, marks={min_v: str(min_v), max_v: str(max_v)})
                    
                    # Note: Auto-enablement will be handled by a separate callback to avoid state mutation in render
                else: 
                    filter_component = html.Div("No range data", className="text-warning")
            else:
                unique_vals, err = get_unique_column_values(config.DATA_DIR, table, column, demographics_table_name, config.DEMOGRAPHICS_FILE)
                if err: 
                    filter_component = html.Div(f"Error: {err}", className="text-danger")
                elif not unique_vals: 
                    filter_component = html.Div("No unique values", className="text-warning")
                else: 
                    filter_component = dcc.Dropdown(id={'type': 'phenotypic-categorical', 'index': filter_id}, options=[{'label': str(v), 'value': v} for v in unique_vals], value=f.get('selected_values', []), multi=True, placeholder="Select value(s)...")
        
        filter_cards.append(dbc.Card(dbc.CardBody(dbc.Row([
            dbc.Col(dcc.Dropdown(id={'type': 'phenotypic-table', 'index': filter_id}, options=table_options, value=table, placeholder="Select Table"), width=3),
            dbc.Col(dcc.Dropdown(id={'type': 'phenotypic-column', 'index': filter_id}, options=column_options, value=column, placeholder="Select Column", disabled=not table), width=3),
            dbc.Col(filter_component, width=5),
            dbc.Col(html.Div([dbc.Badge("Active" if f.get('enabled') else "Inactive", color="success" if f.get('enabled') else "secondary", className="mb-1"), html.Br(), dbc.Button("X", id={'type': 'phenotypic-remove', 'index': filter_id}, color="danger", size="sm", className="p-1")]), width=1, className="text-center")
        ])), className="mb-2"))
    return filter_cards

def update_live_participant_count(age_range, study_site_values, session_values, phenotypic_filters_state, merge_keys_dict, available_tables, user_session_id):
    if not merge_keys_dict: return dbc.Alert("Merge strategy not determined.", color="warning")
    config = get_config()
    merge_keys = MergeKeys.from_dict(merge_keys_dict)
    from query.helpers.data_formatters import convert_phenotypic_to_behavioral_filters
    
    logging.info(f"update_live_participant_count: phenotypic_filters_state = {phenotypic_filters_state}")
    behavioral_filters = convert_phenotypic_to_behavioral_filters(phenotypic_filters_state)
    logging.info(f"update_live_participant_count: behavioral_filters = {behavioral_filters}")
    demographic_filters = {'age_range': age_range}
    if study_site_values: demographic_filters['substudies'] = study_site_values
    if session_values: demographic_filters['sessions'] = session_values
    tables_for_query = {config.get_demographics_table_name(), *(p['table'] for p in behavioral_filters if p.get('table'))}
    if merge_keys.is_longitudinal and demographic_filters.get('sessions') and len(tables_for_query) == 1 and available_tables and available_tables[0] not in tables_for_query:
        tables_for_query.add(available_tables[0])
    try:
        from query.query_factory import QueryMode, get_query_factory
        query_factory = get_query_factory(mode=QueryMode.SECURE)
        base_query, params = query_factory.get_base_query_logic(config, merge_keys, demographic_filters, behavioral_filters, list(tables_for_query))
        count_query, count_params = query_factory.get_count_query(base_query, params, merge_keys)
        if not count_query: return dbc.Alert("No query generated.", color="info")
        con = get_db_connection()
        result = con.execute(count_query, count_params).fetchone()
        count = result[0] if result else 0
        return dbc.Alert(f"Matching Rows: {count}", color="info")
    except Exception as e:
        logging.error(f"Error during live count query: {e}", exc_info=True)
        return dbc.Alert(f"Error calculating count: {e}", color="danger")

def update_phenotypic_session_notice(filters_state):
    if filters_state and filters_state.get('filters'):
        return dbc.Alert("Select any Table and Column to add a filter. Use 'Clear All' to remove all filters.", color="info", className="py-2 mb-2", dismissable=True)
    return None

# --- CALLBACK REGISTRATION ---

def register_callbacks(app):
    # Age and dynamic filters
    app.callback(
        [Output('age-slider', 'min'), Output('age-slider', 'max'), Output('age-slider', 'value'), Output('age-slider', 'marks'), Output('age-slider', 'disabled'), Output('age-slider-info', 'children')],
        [Input('demographics-columns-store', 'data'), Input('column-ranges-store', 'data')],
        State('age-slider-state-store', 'data')
    )(update_age_slider)
    app.callback(
        Output('dynamic-demo-filters-placeholder', 'children'),
        [Input('demographics-columns-store', 'data'), Input('session-values-store', 'data'), Input('merge-keys-store', 'data'), Input('study-site-store', 'data'), Input('session-selection-store', 'data')]
    )(update_dynamic_demographic_filters)

    # Stable multi-callback implementation for phenotypic filters
    app.callback(Output('phenotypic-filters-store', 'data', allow_duplicate=True), Input('phenotypic-add-button', 'n_clicks'), State('phenotypic-filters-store', 'data'), prevent_initial_call=True)(add_phenotypic_filter)
    app.callback(Output('phenotypic-filters-store', 'data', allow_duplicate=True), Input('phenotypic-clear-button', 'n_clicks'), State('phenotypic-filters-store', 'data'), prevent_initial_call=True)(clear_phenotypic_filters)
    app.callback(Output('phenotypic-filters-store', 'data', allow_duplicate=True), Input({'type': 'phenotypic-remove', 'index': dash.ALL}, 'n_clicks'), State('phenotypic-filters-store', 'data'), prevent_initial_call=True)(remove_phenotypic_filter)
    
    # Single unified callback for all filter value updates to avoid race conditions
    app.callback(
        Output('phenotypic-filters-store', 'data', allow_duplicate=True),
        [
            Input({'type': 'phenotypic-table', 'index': dash.ALL}, 'value'),
            Input({'type': 'phenotypic-column', 'index': dash.ALL}, 'value'), 
            Input({'type': 'phenotypic-range', 'index': dash.ALL}, 'value'),
            Input({'type': 'phenotypic-categorical', 'index': dash.ALL}, 'value')
        ],
        State('phenotypic-filters-store', 'data'),
        prevent_initial_call=True
    )(unified_filter_update)
    
    # Auto-enablement callback - DISABLED due to heap corruption issues
    # app.callback(
    #     Output('phenotypic-filters-store', 'data', allow_duplicate=True),
    #     [Input('phenotypic-filters-store', 'data'), Input('column-dtypes-store', 'data'), Input('column-ranges-store', 'data')],
    #     prevent_initial_call=True
    # )(auto_enable_filter_on_column_selection)

    # UI Rendering and Live Updates
    app.callback(
        Output('phenotypic-filters-list', 'children'),
        [Input('phenotypic-filters-store', 'data'), Input('available-tables-store', 'data'), Input('behavioral-columns-store', 'data'), Input('demographics-columns-store', 'data'), Input('column-dtypes-store', 'data'), Input('column-ranges-store', 'data'), Input('merge-keys-store', 'data')]
    )(render_phenotypic_filters)
    app.callback(
        Output('live-participant-count', 'children'),
        [Input('age-slider', 'value'), Input('study-site-store', 'data'), Input('session-selection-store', 'data'), Input('phenotypic-filters-store', 'data'), Input('merge-keys-store', 'data'), Input('available-tables-store', 'data')],
        State('user-session-id', 'data'), prevent_initial_call=True
    )(update_live_participant_count)
    app.callback(
        Output('phenotypic-session-notice', 'children'),
        Input('phenotypic-filters-store', 'data')
    )(update_phenotypic_session_notice)