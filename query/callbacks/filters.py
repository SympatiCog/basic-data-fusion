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
import copy

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

# --- STABLE MONOLITHIC PHENOTYPIC FILTER CALLBACK ---

def validate_filter_state(state):
    """
    Validate and sanitize filter state to prevent corruption.
    
    Returns a clean, validated state dictionary.
    """
    if not isinstance(state, dict):
        return {'filters': [], 'next_id': 1}
    
    # Ensure required keys exist
    if 'filters' not in state or 'next_id' not in state:
        return {'filters': [], 'next_id': 1}
    
    # Validate filters structure
    if not isinstance(state['filters'], list):
        return {'filters': [], 'next_id': state.get('next_id', 1)}
    
    # Sanitize each filter
    clean_filters = []
    for f in state['filters']:
        if isinstance(f, dict) and 'id' in f:
            # Ensure filter has required structure
            clean_filter = {
                'id': f['id'],
                'table': f.get('table'),
                'column': f.get('column'),
                'filter_type': f.get('filter_type'),
                'enabled': f.get('enabled', False)
            }
            # Preserve additional filter data
            if 'selected_values' in f:
                clean_filter['selected_values'] = f['selected_values']
            if 'range_values' in f:
                clean_filter['range_values'] = f['range_values']
            # Preserve range filter values (stored as min_val/max_val)
            if 'min_val' in f:
                clean_filter['min_val'] = f['min_val']
            if 'max_val' in f:
                clean_filter['max_val'] = f['max_val']
            clean_filters.append(clean_filter)
    
    return {
        'filters': clean_filters,
        'next_id': state.get('next_id', 1)
    }

def manage_phenotypic_filters(
    add_clicks, clear_clicks, remove_clicks,
    table_values, column_values, range_values, categorical_values,
    current_state, last_add_clicks
):
    """
    Single, robust callback to manage all phenotypic filter state changes.
    This version is designed to be state-aware to prevent unintended changes on page load.
    """
    ctx = dash.callback_context
    if not ctx.triggered_id:
        return dash.no_update, dash.no_update

    # Sanitize click inputs to handle None values
    add_clicks = add_clicks or 0
    last_add_clicks = last_add_clicks or 0

    state = validate_filter_state(current_state)
    new_state = copy.deepcopy(state)
    
    triggered_id = ctx.triggered_id
    
    # --- Handle Add Button ---
    if triggered_id == 'phenotypic-add-button':
        if add_clicks > last_add_clicks:
            new_filter = {'id': new_state['next_id'], 'table': None, 'column': None, 'filter_type': None, 'enabled': False}
            new_state['filters'].append(new_filter)
            new_state['next_id'] += 1
            return new_state, add_clicks
        else:
            # No new click, so no change to filter state. Still need to pass back click count.
            return dash.no_update, add_clicks

    # --- Handle Clear Button ---
    if triggered_id == 'phenotypic-clear-button':
        if not new_state['filters']:  # No filters to clear
            return dash.no_update, last_add_clicks
        
        # Return a cleared state and the last click count
        return {'filters': [], 'next_id': new_state['next_id']}, last_add_clicks

    # --- Handle Pattern-Matching Inputs ---
    if isinstance(triggered_id, dict):
        filter_id = triggered_id.get('index')
        component_type = triggered_id.get('type')
        
        target_filter = next((f for f in new_state['filters'] if f['id'] == filter_id), None)
        if not target_filter:
            return dash.no_update, last_add_clicks

        triggered_value = ctx.triggered[0]['value']

        # On page load, a trigger might have a None value. This is not a user action.
        # The remove button is an exception, as its n_clicks starts at None.
        if triggered_value is None and component_type != 'phenotypic-remove':
             return dash.no_update, last_add_clicks

        state_changed = False

        if component_type == 'phenotypic-remove':
            new_state['filters'] = [f for f in new_state['filters'] if f['id'] != filter_id]
            state_changed = True

        elif component_type == 'phenotypic-table':
            if triggered_value != target_filter.get('table'):
                target_filter.update({'table': triggered_value, 'column': None, 'filter_type': None, 'enabled': False})
                state_changed = True
            
        elif component_type == 'phenotypic-column':
            if triggered_value != target_filter.get('column'):
                target_filter.update({'column': triggered_value, 'filter_type': None, 'enabled': False})
                state_changed = True
            
        elif component_type == 'phenotypic-range':
            if isinstance(triggered_value, list) and len(triggered_value) == 2:
                current_vals = (target_filter.get('min_val'), target_filter.get('max_val'))
                if tuple(triggered_value) != current_vals:
                    target_filter.update({'min_val': triggered_value[0], 'max_val': triggered_value[1], 'filter_type': 'range', 'enabled': True})
                    state_changed = True

        elif component_type == 'phenotypic-categorical':
            clean_value = triggered_value if isinstance(triggered_value, list) else ([triggered_value] if triggered_value is not None else [])
            current_values = target_filter.get('selected_values', [])
            # Compare sorted lists to handle order differences
            if sorted(clean_value) != sorted(current_values):
                target_filter.update({'selected_values': clean_value, 'filter_type': 'categorical', 'enabled': bool(clean_value)})
                state_changed = True
        
        if state_changed:
            return new_state, last_add_clicks
    
    # If no changes were made, don't update the state
    return dash.no_update, last_add_clicks

# --- RENDER, COUNT, AND NOTICE CALLBACKS ---

def render_phenotypic_filters(filters_state, available_tables, behavioral_columns, demo_columns, column_dtypes, column_ranges, merge_keys_dict, trigger):
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
        
        # Fix corrupted column values (arrays instead of strings)
        if not isinstance(column, (str, type(None))):
            logging.warning(f"Filter {filter_id} has invalid column value: {column}. Attempting to fix.")
            if isinstance(column, list):
                # Extract the last non-None value from the array
                column = next((v for v in reversed(column) if v is not None), None)
                f['column'] = column  # Fix the corruption in place
                logging.info(f"Fixed filter {filter_id} column value to: {column}")
            else:
                column = None
                f['column'] = None
        
        # Also fix corrupted table values
        if not isinstance(table, (str, type(None))):
            if isinstance(table, list):
                table = next((v for v in reversed(table) if v is not None), None)
                f['table'] = table
                logging.info(f"Fixed filter {filter_id} table value to: {table}")
            else:
                table = None
                f['table'] = None
        
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
                else: filter_component = html.Div("No range data", className="text-warning")
            else:
                unique_vals, err = get_unique_column_values(config.DATA_DIR, table, column, demographics_table_name, config.DEMOGRAPHICS_FILE)
                if err: filter_component = html.Div(f"Error: {err}", className="text-danger")
                elif not unique_vals: filter_component = html.Div("No unique values", className="text-warning")
                else: filter_component = dcc.Dropdown(id={'type': 'phenotypic-categorical', 'index': filter_id}, options=[{'label': str(v), 'value': v} for v in unique_vals], value=f.get('selected_values', []), multi=True, placeholder="Select value(s)...")
        
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
    behavioral_filters = convert_phenotypic_to_behavioral_filters(phenotypic_filters_state)
    demographic_filters = {'age_range': age_range}
    if study_site_values: demographic_filters['substudies'] = study_site_values
    if session_values: demographic_filters['sessions'] = session_values
    tables_for_query = {config.get_demographics_table_name(), *(p['table'] for p in behavioral_filters if p.get('table'))}
    if merge_keys.is_longitudinal and demographic_filters.get('sessions') and len(tables_for_query) == 1 and available_tables and available_tables[0] not in tables_for_query:
        tables_for_query.add(available_tables[0])
    try:
        from query.query_factory import QueryMode, get_query_factory
        query_factory = get_query_factory(mode=QueryMode.SECURE)
        
        # Validate behavioral filters before query generation
        validated_filters = []
        for bf in behavioral_filters:
            if bf.get('table') and bf.get('column') and bf.get('filter_type') and 'value' in bf:
                # Ensure values are not corrupted
                if bf['filter_type'] == 'categorical':
                    values = bf['value']
                    if isinstance(values, list) and all(isinstance(v, (str, int, float, bool)) for v in values):
                        validated_filters.append(bf)
                    else:
                        logging.warning(f"Skipping corrupted categorical filter: {bf}")
                elif bf['filter_type'] in ['numeric', 'range']:
                    values = bf['value']
                    if isinstance(values, list) and len(values) == 2 and all(isinstance(v, (int, float)) for v in values):
                        validated_filters.append(bf)
                    else:
                        logging.warning(f"Skipping corrupted numeric/range filter: {bf}")
        
        base_query, params = query_factory.get_base_query_logic(config, merge_keys, demographic_filters, validated_filters, list(tables_for_query))
        count_query, count_params = query_factory.get_count_query(base_query, params, merge_keys)
        if not count_query: return dbc.Alert("No query generated.", color="info")
        
        con = get_db_connection()
        result = con.execute(count_query, count_params).fetchone()
        count = result[0] if result else 0
        return dbc.Alert(f"Matching Rows: {count}", color="info")
        
    except Exception as e:
        # Enhanced error handling to prevent memory corruption
        error_msg = str(e)
        logging.error(f"Error during live count query: {error_msg}", exc_info=True)
        
        # Handle specific DuckDB errors gracefully
        if "Conversion Error" in error_msg and "BOOLEAN" in error_msg:
            return dbc.Alert("Filter error: Boolean column handling issue. Please try different filter values.", color="warning")
        elif "cast" in error_msg.lower():
            return dbc.Alert("Filter error: Data type mismatch. Please check your filter selections.", color="warning")
        else:
            return dbc.Alert(f"Query error: {error_msg}", color="danger")

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

    # Stable monolithic callback for all phenotypic filter state changes
    app.callback(
        [Output('phenotypic-filters-store', 'data'),
         Output('phenotypic-add-button-clicks-store', 'data')],
        [Input('phenotypic-add-button', 'n_clicks'),
         Input('phenotypic-clear-button', 'n_clicks'),
         Input({'type': 'phenotypic-remove', 'index': dash.ALL}, 'n_clicks'),
         Input({'type': 'phenotypic-table', 'index': dash.ALL}, 'value'),
         Input({'type': 'phenotypic-column', 'index': dash.ALL}, 'value'),
         Input({'type': 'phenotypic-range', 'index': dash.ALL}, 'value'),
         Input({'type': 'phenotypic-categorical', 'index': dash.ALL}, 'value')],
        [State('phenotypic-filters-store', 'data'),
         State('phenotypic-add-button-clicks-store', 'data')],
        prevent_initial_call=True
    )(manage_phenotypic_filters)

    # UI Rendering and Live Updates
    app.callback(
        Output('phenotypic-filters-list', 'children'),
        [Input('phenotypic-filters-store', 'data'), 
         Input('available-tables-store', 'data'), 
         Input('behavioral-columns-store', 'data'), 
         Input('demographics-columns-store', 'data'), 
         Input('column-dtypes-store', 'data'), 
         Input('column-ranges-store', 'data'), 
         Input('merge-keys-store', 'data'),
         Input('phenotypic-filter-render-trigger-store', 'data')]
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