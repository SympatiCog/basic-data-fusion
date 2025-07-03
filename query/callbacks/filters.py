"""
Filter management callbacks for the Query interface.

This module contains callbacks responsible for:
- Age slider updates
- Dynamic demographic filters
- Phenotypic filter management
- Live participant count updates
"""

import logging
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html, no_update, MATCH

from analysis.demographics import has_multisite_data
from config_manager import get_config
from utils import (
    MergeKeys,
    get_study_site_values,
    get_unique_column_values,
    is_numeric_dtype,
    get_db_connection,
)


def update_age_slider(demo_cols, col_ranges, stored_age_value):
    """Update age slider properties based on demographic data."""
    config = get_config()  # Get fresh config
    if not demo_cols or config.AGE_COLUMN not in demo_cols or not col_ranges:
        return 0, 100, [0, 100], {}, True, f"Age filter disabled: '{config.AGE_COLUMN}' column not found in demographics or ranges not available."

    # Look up age column range using simple column name
    age_col_key = config.AGE_COLUMN

    if age_col_key in col_ranges:
        min_age, max_age = col_ranges[age_col_key]
        min_age = int(min_age)
        max_age = int(max_age)

        default_min, default_max = config.DEFAULT_AGE_SELECTION

        # Use stored value if available and valid, otherwise use default
        if stored_age_value is not None and len(stored_age_value) == 2:
            stored_min, stored_max = stored_age_value
            if min_age <= stored_min <= max_age and min_age <= stored_max <= max_age:
                value = stored_age_value
            else:
                value = [max(min_age, default_min), min(max_age, default_max)]
        else:
            value = [max(min_age, default_min), min(max_age, default_max)]

        marks = {i: str(i) for i in range(min_age, max_age + 1, 10)}
        if min_age not in marks:
            marks[min_age] = str(min_age)
        if max_age not in marks:
            marks[max_age] = str(max_age)

        return min_age, max_age, value, marks, False, f"Age range: {min_age}-{max_age}"
    else:
        # Fallback if age column is in demo_cols but no range found (should ideally not happen if get_table_info is robust)
        return 0, 100, [config.DEFAULT_AGE_SELECTION[0], config.DEFAULT_AGE_SELECTION[1]], {}, True, f"Age filter disabled: Range for '{config.AGE_COLUMN}' column not found."


def update_dynamic_demographic_filters(demo_cols, session_values, merge_keys_dict,
                                     input_rockland_values, input_session_values):
    """Update dynamic demographic filters based on available data."""
    config = get_config()  # Get fresh config
    if not demo_cols:
        return html.P("Demographic information not yet available to populate dynamic filters.")

    children = []

    # Multisite/Multistudy Filters
    if has_multisite_data(demo_cols, config.STUDY_SITE_COLUMN):
        children.append(html.H5("Study Site Selection", style={'marginTop': '15px'}))

        # Get actual study site values from data
        study_site_values = get_study_site_values(config)
        if not study_site_values:
            # Fallback to Rockland defaults if no values found
            study_site_values = config.ROCKLAND_BASE_STUDIES

        # Use input values if available, otherwise use default (all sites selected)
        selected_sites = input_rockland_values if input_rockland_values else study_site_values

        children.append(
            dcc.Dropdown(
                id='study-site-dropdown',
                options=[{'label': s, 'value': s} for s in study_site_values],
                value=selected_sites,
                multi=True,
                placeholder="Select Study Sites..."
            )
        )

    # Session Filters
    if merge_keys_dict:
        mk = MergeKeys.from_dict(merge_keys_dict)
        if mk.is_longitudinal and mk.session_id and session_values:
            children.append(html.H5("Session/Visit Selection", style={'marginTop': '15px'}))
            # Use input values if available, otherwise default to all available sessions
            session_value = input_session_values if input_session_values else session_values
            children.append(
                dcc.Dropdown(
                    id='session-dropdown',
                    options=[{'label': s, 'value': s} for s in session_values],
                    value=session_value,
                    multi=True,
                    placeholder=f"Select {mk.session_id} values..."
                )
            )

    if not children:
        return html.P("No dataset-specific demographic filters applicable.", style={'fontStyle': 'italic'})

    return children


# DISABLED: Live Participant Count Callback 
# This callback has been disabled to prevent conflicts with the original callback in pages/query.py
# The original callback will be restored until the complete migration is finished
# 
# TODO: Re-enable this callback once manage_phenotypic_filters() has been fully migrated
# 
# @callback(
#     Output('live-participant-count', 'children'),
#     [Input('age-slider', 'value'),
#      Input('study-site-store', 'data'),
#      Input('session-selection-store', 'data'),
#      Input('phenotypic-filters-store', 'data'),
#      Input('merge-keys-store', 'data'),
#      Input('available-tables-store', 'data')],
#     prevent_initial_call=False
# )
# def update_live_participant_count_DISABLED(...):
#     # Function disabled - see pages/query.py for active callback
#     pass


# MAJOR PHENOTYPIC FILTER CALLBACKS
# Extracted from pages/query.py during Phase 2.2 refactoring

def manage_phenotypic_filters(
    add_clicks, clear_clicks, remove_clicks,
    table_values, column_values, range_values, categorical_values,
    current_state
):
    """Single callback to manage all phenotypic filter state changes."""
    import dash
    ctx = dash.callback_context

    if not ctx.triggered:
        # Handle initial call - return empty state if no current state
        if not current_state:
            return {'filters': [], 'next_id': 1}
        return dash.no_update

    # Ensure we have a valid state structure
    if not current_state or not isinstance(current_state, dict):
        current_state = {'filters': [], 'next_id': 1}

    if 'filters' not in current_state:
        current_state['filters'] = []
    if 'next_id' not in current_state:
        current_state['next_id'] = 1

    # Additional validation - ensure filters is a list and next_id is a number
    if not isinstance(current_state.get('filters'), list):
        current_state['filters'] = []
    if not isinstance(current_state.get('next_id'), int) or current_state.get('next_id') < 1:
        current_state['next_id'] = 1

    triggered_component_id = ctx.triggered_id

    # Handle add filter - with additional safeguards
    if triggered_component_id == 'phenotypic-add-button':
        # Additional safety check - ensure the button was actually clicked (not just initialized)
        if add_clicks is None or add_clicks == 0:
            return dash.no_update

        new_filter = {
            'id': current_state['next_id'],
            'table': None,
            'column': None,
            'filter_type': None,
            'enabled': False,
            'min_val': None,
            'max_val': None,
            'range_min': None,
            'range_max': None,
            'selected_values': [],
            'available_values': [],
            'expanded': True
        }
        new_state = current_state.copy()
        new_state['filters'] = current_state['filters'] + [new_filter]
        new_state['next_id'] = current_state['next_id'] + 1
        return new_state

    # Handle clear all filters
    if triggered_component_id == 'phenotypic-clear-button':
        # Additional safety check - ensure the button was actually clicked
        if clear_clicks is None or clear_clicks == 0:
            return dash.no_update

        return {'filters': [], 'next_id': 1}

    # Handle remove filter
    if isinstance(triggered_component_id, dict) and triggered_component_id.get('type') == 'phenotypic-remove':
        filter_id = triggered_component_id['index']
        new_filters = [f for f in current_state['filters'] if f['id'] != filter_id]
        new_state = current_state.copy()
        new_state['filters'] = new_filters
        return new_state

    # Handle table/column/value changes
    if isinstance(triggered_component_id, dict) and triggered_component_id['type'].startswith('phenotypic-'):
        filter_id = triggered_component_id['index']
        component_type = triggered_component_id['type']
        triggered_value = ctx.triggered[0]['value']

        # Find the filter to update
        new_filters = []
        for filter_data in current_state['filters']:
            if filter_data['id'] == filter_id:
                updated_filter = filter_data.copy()

                if component_type == 'phenotypic-table':
                    updated_filter['table'] = triggered_value
                    # Reset dependent fields when table changes
                    updated_filter['column'] = None
                    updated_filter['filter_type'] = None
                    updated_filter['enabled'] = False
                    updated_filter['min_val'] = None
                    updated_filter['max_val'] = None
                    updated_filter['selected_values'] = []

                elif component_type == 'phenotypic-column':
                    updated_filter['column'] = triggered_value
                    # Reset filter values when column changes
                    updated_filter['filter_type'] = None
                    updated_filter['enabled'] = False
                    updated_filter['min_val'] = None
                    updated_filter['max_val'] = None
                    updated_filter['selected_values'] = []

                elif component_type == 'phenotypic-range':
                    if triggered_value and len(triggered_value) == 2:
                        updated_filter['min_val'] = triggered_value[0]
                        updated_filter['max_val'] = triggered_value[1]
                        updated_filter['filter_type'] = 'numeric'
                        updated_filter['enabled'] = True

                elif component_type == 'phenotypic-categorical':
                    updated_filter['selected_values'] = triggered_value or []
                    updated_filter['filter_type'] = 'categorical'
                    updated_filter['enabled'] = bool(triggered_value)

                new_filters.append(updated_filter)
            else:
                new_filters.append(filter_data)

        new_state = current_state.copy()
        new_state['filters'] = new_filters
        return new_state

    # If no specific trigger was handled, check for state synchronization issues
    # This handles cases where component values don't match stored state
    if current_state and current_state.get('filters'):
        needs_sync = False
        
        # Check if component values match stored state
        if table_values and len(table_values) == len(current_state['filters']):
            for i, filter_data in enumerate(current_state['filters']):
                stored_table = filter_data.get('table')
                component_table = table_values[i] if i < len(table_values) else None
                stored_column = filter_data.get('column')
                component_column = column_values[i] if i < len(column_values) else None
                
                # If component values don't match stored state, sync them
                if component_table != stored_table or component_column != stored_column:
                    needs_sync = True
                    break
            
        if needs_sync:
            # Force UI update by returning current state unchanged
            # This will trigger render_phenotypic_filters to regenerate UI with correct values
            return current_state

    return dash.no_update


def render_phenotypic_filters(
    filters_state, available_tables, behavioral_columns,
    demo_columns, column_dtypes, column_ranges, merge_keys_dict
):
    """Render the UI for all phenotypic filters."""
    import dash_bootstrap_components as dbc
    from dash import dcc, html
    from utils import MergeKeys, get_unique_column_values, is_numeric_dtype
    from config_manager import get_config
    
    config = get_config()  # Get fresh config

    if not filters_state or not filters_state.get('filters'):
        return html.Div("No phenotypic filters added yet.",
                       className="text-muted font-italic")

    # Ensure we have the necessary data
    if not available_tables:
        available_tables = []
    if not behavioral_columns:
        behavioral_columns = {}
    if not demo_columns:
        demo_columns = []
    if not column_dtypes:
        column_dtypes = {}
    if not column_ranges:
        column_ranges = {}

    merge_keys = MergeKeys.from_dict(merge_keys_dict) if merge_keys_dict else MergeKeys(primary_id="unknown")
    demographics_table_name = config.get_demographics_table_name()

    # Build table options
    table_options = [{'label': demographics_table_name, 'value': demographics_table_name}]
    table_options.extend([{'label': table, 'value': table} for table in available_tables])

    filter_cards = []

    for filter_data in filters_state['filters']:
        filter_id = filter_data['id']
        selected_table = filter_data.get('table')
        selected_column = filter_data.get('column')

        # Build column options for selected table
        column_options = []
        if selected_table:
            if selected_table == demographics_table_name:
                table_columns = demo_columns
            else:
                table_columns = behavioral_columns.get(selected_table, [])

            # Exclude ID columns
            id_cols_to_exclude = {merge_keys.primary_id, merge_keys.session_id, merge_keys.composite_id}
            for col in table_columns:
                if col not in id_cols_to_exclude:
                    column_options.append({'label': col, 'value': col})

        # Build filter component based on column type
        filter_component = html.Div("Select table and column first",
                                  className="text-muted font-italic")

        if selected_table and selected_column:
            # Determine column data type
            # Note: column_dtypes keys are just column names, not prefixed with table aliases
            column_dtype = column_dtypes.get(selected_column)

            if column_dtype and is_numeric_dtype(column_dtype):
                # Numeric column - use range slider
                # Note: column_ranges keys are just column names, not prefixed with table aliases
                if selected_column in column_ranges:
                    min_val, max_val = column_ranges[selected_column]
                    slider_min, slider_max = int(min_val), int(max_val)

                    # Use stored values or default to full range
                    current_min = filter_data.get('min_val')
                    current_max = filter_data.get('max_val')
                    if current_min is not None and current_max is not None:
                        slider_value = [current_min, current_max]
                    else:
                        slider_value = [slider_min, slider_max]

                    filter_component = html.Div([
                        html.P(f"Range: {slider_min} - {slider_max}",
                              className="small text-muted mb-1"),
                        dcc.RangeSlider(
                            id={'type': 'phenotypic-range', 'index': filter_id},
                            min=slider_min, max=slider_max, value=slider_value,
                            tooltip={"placement": "bottom", "always_visible": True},
                            allowCross=False, step=1,
                            marks={slider_min: str(slider_min), slider_max: str(slider_max)}
                        )
                    ])
                else:
                    filter_component = html.Div("No range data available",
                                              className="text-warning")
            else:
                # Categorical column - use dropdown
                try:
                    unique_values, error_msg = get_unique_column_values(
                        data_dir=config.DATA_DIR,
                        table_name=selected_table,
                        column_name=selected_column,
                        demo_table_name=demographics_table_name,
                        demographics_file_name=config.DEMOGRAPHICS_FILE
                    )
                    if error_msg:
                        filter_component = html.Div(f"Error: {error_msg}",
                                                   className="text-danger")
                    elif not unique_values:
                        filter_component = html.Div("No unique values found",
                                                   className="text-warning")
                    else:
                        current_selection = filter_data.get('selected_values', [])
                        filter_component = html.Div([
                            html.P(f"Categorical ({len(unique_values)} values)",
                                  className="small text-muted mb-1"),
                            dcc.Dropdown(
                                id={'type': 'phenotypic-categorical', 'index': filter_id},
                                options=[{'label': str(val), 'value': val} for val in unique_values],
                                value=current_selection,
                                multi=True,
                                placeholder="Select value(s)..."
                            )
                        ])
                except Exception as e:
                    filter_component = html.Div(f"Error: {str(e)}",
                                               className="text-danger")

        # Build the filter card
        filter_card = dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Label("Table:", className="form-label small fw-bold"),
                        dcc.Dropdown(
                            id={'type': 'phenotypic-table', 'index': filter_id},
                            options=table_options,
                            value=selected_table,
                            placeholder="Select Table"
                        )
                    ], width=3),
                    dbc.Col([
                        html.Label("Column:", className="form-label small fw-bold"),
                        dcc.Dropdown(
                            id={'type': 'phenotypic-column', 'index': filter_id},
                            options=column_options,
                            value=selected_column,
                            placeholder="Select Column",
                            disabled=not selected_table
                        )
                    ], width=3),
                    dbc.Col([
                        html.Label("Filter:", className="form-label small fw-bold"),
                        filter_component
                    ], width=5),
                    dbc.Col([
                        html.Div([
                            dbc.Badge(
                                "Active" if filter_data.get('enabled') else "Inactive",
                                color="success" if filter_data.get('enabled') else "secondary",
                                className="mb-1"
                            ),
                            html.Br(),
                            dbc.Button(
                                "Remove",
                                id={'type': 'phenotypic-remove', 'index': filter_id},
                                color="danger", size="sm"
                            )
                        ])
                    ], width=1)
                ])
            ])
        ], className="mb-2")

        filter_cards.append(filter_card)

    return filter_cards


def update_live_participant_count(
    age_range,
    study_site_values, # Study sites from store
    session_values, # Session values from store
    phenotypic_filters_state, # Phenotypic filters state
    merge_keys_dict, available_tables,
    user_session_id # User context for StateManager
):
    """Update live participant count based on current filter settings."""
    import dash
    import dash_bootstrap_components as dbc
    import logging
    from utils import (
        MergeKeys, 
        get_db_connection, 
        _file_access_lock  # temp fix for file access coordination
    )
    from config_manager import get_config
    from query.helpers.data_formatters import convert_phenotypic_to_behavioral_filters
    
    ctx = dash.callback_context

    # Use callback parameters directly to avoid session conflicts
    # The StateManager was causing issues with multiple sessions

    if not ctx.triggered and not merge_keys_dict : # Don't run on initial load if no data yet
        return dbc.Alert("Upload data and select filters to see participant count.", color="info")

    if not merge_keys_dict:
        return dbc.Alert("Merge strategy not determined. Cannot calculate count.", color="warning")

    current_config = get_config()  # Get fresh config instance
    merge_keys = MergeKeys.from_dict(merge_keys_dict)

    demographic_filters = {}
    if age_range:
        demographic_filters['age_range'] = age_range

    # Handle study site filtering
    if study_site_values:
        demographic_filters['substudies'] = study_site_values

    # Handle session filtering
    if session_values:
        demographic_filters['sessions'] = session_values

    # Convert phenotypic filters to behavioral filters for query generation
    behavioral_filters = convert_phenotypic_to_behavioral_filters(phenotypic_filters_state)

    # Determine tables to join: demographics is the main table
    tables_for_query = {current_config.get_demographics_table_name()}

    # Add tables from phenotypic filters
    for p_filter in behavioral_filters:
        tables_for_query.add(p_filter['table'])

    # Add a behavioral table if session filter is active and data is longitudinal,
    # and only demo table is currently selected for query.
    # This logic is simplified here. A more robust way would be to check if session_id column exists in tables.
    if merge_keys.is_longitudinal and demographic_filters.get('sessions') and len(tables_for_query) == 1 and available_tables:
        # Add first available behavioral table to enable session filtering if it's not already there.
        # This assumes session_id might not be in demo table or its filtering is linked to behavioral tables.
        if available_tables[0] not in tables_for_query: # Add first one if not demo
             tables_for_query.add(available_tables[0])

    try:
        # Use secure query generation instead of deprecated functions
        from query.query_factory import QueryMode, get_query_factory

        # Create query factory with secure mode
        query_factory = get_query_factory(mode=QueryMode.SECURE)

        base_query, params = query_factory.get_base_query_logic(
            current_config, merge_keys, demographic_filters, behavioral_filters, list(tables_for_query)
        )
        count_query, count_params = query_factory.get_count_query(base_query, params, merge_keys)

        if count_query:
            # Use cached database connection for improved performance
            con = get_db_connection()
            # temp fix: coordinate file access with pandas operations
            with _file_access_lock:
                count_result = con.execute(count_query, count_params).fetchone()

            if count_result and count_result[0] is not None:
                return dbc.Alert(f"Matching Rows: {count_result[0]}", color="info")
            else:
                return dbc.Alert("Could not retrieve participant count.", color="warning")
        else:
            return dbc.Alert("No query generated for count.", color="info")

    except Exception as e:
        logging.error(f"Error during live count query: {e}")
        logging.error(f"Query attempted: {count_query if 'count_query' in locals() else 'N/A'}")
        logging.error(f"Params: {count_params if 'count_params' in locals() else 'N/A'}")
        return dbc.Alert(f"Error calculating count: {str(e)}", color="danger")


def update_phenotypic_session_notice(filters_state):
    """Show a notice if filters are restored from session storage."""
    import dash_bootstrap_components as dbc
    from dash import html
    
    if not filters_state or not filters_state.get('filters'):
        return None

    num_filters = len(filters_state['filters'])
    if num_filters > 0:
        return dbc.Alert([
            html.I(className="bi bi-info-circle me-2"),
            #f"Note: {num_filters} phenotypic filter{'s' if num_filters != 1 else ''} restored from previous session. ",
            "Select any Table and Column Below to add a filter. ", "Use 'Clear All' (above) to remove all filters if needed."
        ], color="info", className="py-2 mb-2", dismissable=True)

    return None


# All major phenotypic filter callbacks have been extracted from pages/query.py


def register_callbacks(app):
    """Register all filter management callbacks with the Dash app."""
    from dash import Input, Output, State, MATCH
    import dash
    
    # Register update_age_slider
    app.callback(
        [Output('age-slider', 'min'),
         Output('age-slider', 'max'),
         Output('age-slider', 'value'),
         Output('age-slider', 'marks'),
         Output('age-slider', 'disabled'),
         Output('age-slider-info', 'children')],
        [Input('demographics-columns-store', 'data'),
         Input('column-ranges-store', 'data')],
        [State('age-slider-state-store', 'data')]
    )(update_age_slider)
    
    # Register update_dynamic_demographic_filters
    app.callback(
        Output('dynamic-demo-filters-placeholder', 'children'),
        [Input('demographics-columns-store', 'data'),
         Input('session-values-store', 'data'),
         Input('merge-keys-store', 'data'),
         Input('study-site-store', 'data'),
         Input('session-selection-store', 'data')]
    )(update_dynamic_demographic_filters)
    
    # Register manage_phenotypic_filters
    app.callback(
        Output('phenotypic-filters-store', 'data'),
        [Input('phenotypic-add-button', 'n_clicks'),
         Input('phenotypic-clear-button', 'n_clicks'),
         Input({'type': 'phenotypic-remove', 'index': dash.ALL}, 'n_clicks'),
         Input({'type': 'phenotypic-table', 'index': dash.ALL}, 'value'),
         Input({'type': 'phenotypic-column', 'index': dash.ALL}, 'value'),
         Input({'type': 'phenotypic-range', 'index': dash.ALL}, 'value'),
         Input({'type': 'phenotypic-categorical', 'index': dash.ALL}, 'value')],
        State('phenotypic-filters-store', 'data'),
        prevent_initial_call=True
    )(manage_phenotypic_filters)
    
    # Register render_phenotypic_filters
    app.callback(
        Output('phenotypic-filters-list', 'children'),
        [Input('phenotypic-filters-store', 'data'),
         Input('available-tables-store', 'data'),
         Input('behavioral-columns-store', 'data'),
         Input('demographics-columns-store', 'data'),
         Input('column-dtypes-store', 'data'),
         Input('column-ranges-store', 'data'),
         Input('merge-keys-store', 'data')],
        prevent_initial_call=False
    )(render_phenotypic_filters)
    
    # Register update_live_participant_count
    app.callback(
        Output('live-participant-count', 'children'),
        [Input('age-slider', 'value'),
         Input('study-site-store', 'data'), # For Rockland substudies
         Input('session-selection-store', 'data'), # For session filtering
         Input('phenotypic-filters-store', 'data'), # For phenotypic filters
         # Data stores needed for query generation
         Input('merge-keys-store', 'data'),
         Input('available-tables-store', 'data')],
        [State('user-session-id', 'data')] # User context for StateManager
    )(update_live_participant_count)
    
    # Register update_phenotypic_session_notice
    app.callback(
        Output('phenotypic-session-notice', 'children'),
        [Input('phenotypic-filters-store', 'data')],
        prevent_initial_call=False
    )(update_phenotypic_session_notice)