import logging
import time
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, State, callback, dash_table, dcc, html, no_update, MATCH

from analysis.demographics import has_multisite_data
from config_manager import get_config

# Assuming utils.py is in the same directory or accessible in PYTHONPATH
from utils import (
    MergeKeys,
    _file_access_lock,  # temp fix for file access coordination
    enwiden_longitudinal_data,
    export_query_parameters_to_toml,
    generate_export_filename,
    generate_filtering_report,
    generate_final_data_summary,
    get_db_connection,
    get_study_site_values,
    get_table_info,
    get_unique_column_values,
    import_query_parameters_from_toml,
    is_numeric_dtype,
    shorten_path,
    validate_imported_query_parameters,
)

# CENTRALIZED MODULAR CALLBACK REGISTRATION
# Use only the centralized registration system to prevent duplicate registrations
from query.callbacks import register_all_callbacks

dash.register_page(__name__, path='/', title='Query Data')

# Register modular callbacks with proper app instance
try:
    current_app = dash.get_app()
    register_all_callbacks(current_app)
except Exception as e:
    print(f"Warning: Modular callback registration failed: {e}")

# Note: We get fresh config in callbacks to pick up changes from settings

# Import the modular layout
from query.ui.layout import layout





# Note: update_data_status_section() moved to query/callbacks/data_loading.py

# Note: update_age_slider() moved to query/callbacks/filters.py


# Note: update_dynamic_demographic_filters() moved to query/callbacks/filters.py


# Callbacks to update stores when dynamic dropdowns change
@callback(
    Output('study-site-store', 'data'),
    Input('study-site-dropdown', 'value'),
    prevent_initial_call=True
)
def update_study_site_store(study_site_values):
    return study_site_values if study_site_values else []

@callback(
    Output('session-selection-store', 'data'),
    Input('session-dropdown', 'value'),
    prevent_initial_call=True
)
def update_session_selection_store(session_values):
    return session_values if session_values else []


# Phenotypic Filters Management
@callback(
    Output('phenotypic-filters-store', 'data'),
    [Input('phenotypic-add-button', 'n_clicks'),
     Input('phenotypic-clear-button', 'n_clicks'),
     Input({'type': 'phenotypic-remove', 'index': dash.ALL}, 'n_clicks'),
     Input({'type': 'phenotypic-table', 'index': dash.ALL}, 'value'),
     Input({'type': 'phenotypic-column', 'index': dash.ALL}, 'value'),
     Input({'type': 'phenotypic-range', 'index': dash.ALL}, 'value'),
     Input({'type': 'phenotypic-categorical', 'index': dash.ALL}, 'value')],
    State('phenotypic-filters-store', 'data'),
    prevent_initial_call=False
)
def manage_phenotypic_filters(
    add_clicks, clear_clicks, remove_clicks,
    table_values, column_values, range_values, categorical_values,
    current_state
):
    """Single callback to manage all phenotypic filter state changes."""
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


# Render Phenotypic Filters UI
@callback(
    Output('phenotypic-filters-list', 'children'),
    [Input('phenotypic-filters-store', 'data'),
     Input('available-tables-store', 'data'),
     Input('behavioral-columns-store', 'data'),
     Input('demographics-columns-store', 'data'),
     Input('column-dtypes-store', 'data'),
     Input('column-ranges-store', 'data'),
     Input('merge-keys-store', 'data')],
    prevent_initial_call=False
)
def render_phenotypic_filters(
    filters_state, available_tables, behavioral_columns,
    demo_columns, column_dtypes, column_ranges, merge_keys_dict
):
    """Render the UI for all phenotypic filters."""
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



# Session Notice for Phenotypic Filters
@callback(
    Output('phenotypic-session-notice', 'children'),
    [Input('phenotypic-filters-store', 'data')],
    prevent_initial_call=False
)
def update_phenotypic_session_notice(filters_state):
    """Show a notice if filters are restored from session storage."""
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


# Import convert function from helper module to avoid circular imports
from query.helpers.data_formatters import convert_phenotypic_to_behavioral_filters










# Live Participant Count Callback (RESTORED from git history to fix issues)
@callback(
    Output('live-participant-count', 'children'),
    [Input('age-slider', 'value'),
     Input('study-site-store', 'data'), # For Rockland substudies
     Input('session-selection-store', 'data'), # For session filtering
     Input('phenotypic-filters-store', 'data'), # For phenotypic filters
     # Data stores needed for query generation
     Input('merge-keys-store', 'data'),
     Input('available-tables-store', 'data')],
    [State('user-session-id', 'data')] # User context for StateManager
)
def update_live_participant_count(
    age_range,
    rockland_substudy_values, # Rockland substudies from store
    session_values, # Session values from store
    phenotypic_filters_state, # Phenotypic filters state
    merge_keys_dict, available_tables,
    user_session_id # User context for StateManager
):
    """Update live participant count based on current filter settings."""
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

    # Handle Rockland substudy filtering
    if rockland_substudy_values:
        demographic_filters['substudies'] = rockland_substudy_values

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




# Note: load_initial_data_info() moved to query/callbacks/data_loading.py


# Note: update_table_multiselect_options() moved to query/callbacks/data_loading.py

@callback(
    Output('table-multiselect', 'value', allow_duplicate=True),
    Input('available-tables-store', 'data'),
    State('table-multiselect-state-store', 'data'),
    prevent_initial_call=True
)
def restore_table_multiselect_value(available_tables_data, stored_value):
    if stored_value is not None:
        return stored_value
    return []

@callback(
    Output('enwiden-data-checkbox', 'value', allow_duplicate=True),
    Input('merge-keys-store', 'data'),
    State('enwiden-data-checkbox-state-store', 'data'),
    prevent_initial_call=True
)
def restore_enwiden_checkbox_value(merge_keys_dict, stored_value):
    if stored_value is not None:
        return stored_value
    return False

@callback(
    Output('study-site-dropdown', 'value', allow_duplicate=True),
    Input('demographics-columns-store', 'data'),
    State('study-site-store', 'data'),
    prevent_initial_call=True
)
def restore_study_site_dropdown_value(demo_cols, stored_value):
    """Restore study site dropdown value from persistent storage"""
    if stored_value is not None and len(stored_value) > 0:
        return stored_value
    return dash.no_update

@callback(
    Output('session-dropdown', 'value', allow_duplicate=True),
    Input('session-values-store', 'data'),
    State('session-selection-store', 'data'),
    prevent_initial_call=True
)
def restore_session_dropdown_value(session_values, stored_value):
    """Restore session dropdown value from persistent storage"""
    if stored_value is not None and len(stored_value) > 0:
        return stored_value
    return dash.no_update

# Note: update_column_selection_area() moved to query/callbacks/data_loading.py

# Note: update_selected_columns_store() moved to query/callbacks/data_loading.py

# Callback to control "Enwiden Data" checkbox visibility
@callback(
    Output('enwiden-checkbox-wrapper', 'style'), # Target the wrapper div
    Input('merge-keys-store', 'data')
)
def update_enwiden_checkbox_visibility(merge_keys_dict):
    if merge_keys_dict:
        mk = MergeKeys.from_dict(merge_keys_dict)
        if mk.is_longitudinal:
            return {'display': 'block', 'marginTop': '10px'} # Show
    return {'display': 'none'} # Hide

# Note: handle_generate_data() moved to query/callbacks/export.py

# Note: show_data_processing_loading() moved to query/callbacks/export.py

# Note: toggle_filename_modal() moved to query/callbacks/export.py

# Note: download_csv_data() moved to query/callbacks/export.py


# Note: toggle_summary_modal() moved to query/callbacks/export.py

# Note: generate_and_download_summary_reports() moved to query/callbacks/export.py


# Unified callback to save all filter states for persistence across page navigation
@callback(
    [Output('age-slider-state-store', 'data'),
     Output('table-multiselect-state-store', 'data'),
     Output('enwiden-data-checkbox-state-store', 'data')],
    [Input('age-slider', 'value'),
     Input('table-multiselect', 'value'),
     Input('enwiden-data-checkbox', 'value')]
)
def save_all_filter_states(age_value, table_value, enwiden_value):
    return age_value, table_value, enwiden_value


# Export Query Parameters Callbacks

@callback(
    [Output('export-query-modal', 'is_open'),
     Output('export-filename-input', 'value', allow_duplicate=True),
     Output('export-summary-content', 'children')],
    [Input('export-query-button', 'n_clicks'),
     Input('cancel-export-button', 'n_clicks'),
     Input('confirm-export-button', 'n_clicks')],
    [State('export-query-modal', 'is_open'),
     State('age-slider', 'value'),
     State('study-site-store', 'data'),
     State('session-selection-store', 'data'),
     State('phenotypic-filters-store', 'data'),
     State('table-multiselect', 'value'),
     State('selected-columns-per-table-store', 'data'),
     State('enwiden-data-checkbox', 'value'),
     State('consolidate-baseline-checkbox', 'value')],
    prevent_initial_call=True
)
def toggle_export_modal(export_clicks, cancel_clicks, confirm_clicks, is_open,
                       age_range, substudies, sessions, phenotypic_filters,
                       selected_tables, selected_columns, enwiden_longitudinal, consolidate_baseline):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'export-query-button':
        # Generate suggested filename
        timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
        suggested_filename = f"query_parameters_{timestamp}"

        # Generate export summary
        summary_content = []

        # Cohort filters summary
        summary_content.append(html.H6("Cohort Filters:"))
        if age_range:
            summary_content.append(html.Li(f"Age Range: {age_range[0]} - {age_range[1]}"))
        if substudies:
            summary_content.append(html.Li(f"Substudies: {', '.join(substudies)}"))
        if sessions:
            summary_content.append(html.Li(f"Sessions: {', '.join(sessions)}"))

        # Phenotypic filters summary
        if phenotypic_filters and phenotypic_filters.get('filters'):
            logging.info(f"Preview: Processing {len(phenotypic_filters['filters'])} total filters")
            enabled_filters = [f for f in phenotypic_filters['filters'] if f.get('enabled')]
            for i, pf in enumerate(phenotypic_filters['filters']):
                enabled_status = pf.get('enabled')
                table_col = f"{pf.get('table', 'unknown')}.{pf.get('column', 'unknown')}"
                logging.info(f"Preview: Filter {i+1}: {table_col} - enabled: {enabled_status}")
            logging.info(f"Preview: Found {len(enabled_filters)} enabled filters")
            if enabled_filters:
                summary_content.append(html.H6("Phenotypic Filters:", className="mt-3"))
                for i, pf in enumerate(enabled_filters, 1):
                    filter_desc = f"{pf['table']}.{pf['column']}"
                    if pf['filter_type'] == 'numeric':
                        filter_desc += f" ({pf['min_val']} - {pf['max_val']})"
                    elif pf['filter_type'] == 'categorical':
                        selected_vals = pf.get('selected_values', [])
                        if len(selected_vals) <= 3:
                            filter_desc += f" ({', '.join(map(str, selected_vals))})"
                        else:
                            filter_desc += f" ({len(selected_vals)} values selected)"
                    summary_content.append(html.Li(f"Filter {i}: {filter_desc}"))

        # Export selection summary
        summary_content.append(html.H6("Export Selection:", className="mt-3"))
        if selected_tables:
            summary_content.append(html.Li(f"Tables: {', '.join(selected_tables)}"))
        if selected_columns:
            for table, columns in selected_columns.items():
                if columns:
                    column_text = ', '.join(columns) if len(columns) <= 5 else f"{len(columns)} columns"
                    summary_content.append(html.Li(f"{table}: {column_text}"))
        if enwiden_longitudinal:
            summary_content.append(html.Li("Enwiden longitudinal data: Yes"))
        if consolidate_baseline:
            summary_content.append(html.Li("Consolidate baseline sessions: Yes"))

        if not summary_content:
            summary_content = [html.P("No filters or selections to export.", className="text-muted")]

        return True, suggested_filename, summary_content

    elif button_id in ['cancel-export-button', 'confirm-export-button']:
        return False, dash.no_update, dash.no_update

    return is_open, dash.no_update, dash.no_update


@callback(
    Output('download-dataframe-csv', 'data', allow_duplicate=True),
    Input('confirm-export-button', 'n_clicks'),
    [State('export-filename-input', 'value'),
     State('export-notes-input', 'value'),
     State('age-slider', 'value'),
     State('study-site-store', 'data'),
     State('session-selection-store', 'data'),
     State('phenotypic-filters-store', 'data'),
     State('table-multiselect', 'value'),
     State('selected-columns-per-table-store', 'data'),
     State('enwiden-data-checkbox', 'value'),
     State('consolidate-baseline-checkbox', 'value')],
    prevent_initial_call=True
)
def export_query_parameters(confirm_clicks, filename, notes,
                           age_range, substudies, sessions, phenotypic_filters,
                           selected_tables, selected_columns, enwiden_longitudinal, consolidate_baseline):
    if not confirm_clicks or confirm_clicks == 0:
        return dash.no_update

    try:
        # Prepare filename
        if not filename or not filename.strip():
            timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
            filename = f"query_parameters_{timestamp}"

        filename = filename.strip()
        if not filename.endswith('.toml'):
            filename += '.toml'

        # Convert phenotypic filters to proper format
        formatted_phenotypic_filters = []
        if phenotypic_filters and phenotypic_filters.get('filters'):
            logging.info(f"Export: Processing {len(phenotypic_filters['filters'])} total filters")
            for i, pf in enumerate(phenotypic_filters['filters']):
                enabled_status = pf.get('enabled')
                table_col = f"{pf.get('table', 'unknown')}.{pf.get('column', 'unknown')}"
                logging.info(f"Export: Filter {i+1}: {table_col} - enabled: {enabled_status}")
                if enabled_status:
                    formatted_phenotypic_filters.append(pf)
                    logging.info(f"Export: Added filter {i+1} to export list")
                else:
                    logging.info(f"Export: Skipped filter {i+1} (not enabled)")
            logging.info(f"Export: Final formatted_phenotypic_filters count: {len(formatted_phenotypic_filters)}")

        # Generate TOML content
        toml_content = export_query_parameters_to_toml(
            age_range=age_range,
            substudies=substudies,
            sessions=sessions,
            phenotypic_filters=formatted_phenotypic_filters,
            selected_tables=selected_tables,
            selected_columns=selected_columns,
            enwiden_longitudinal=enwiden_longitudinal or False,
            consolidate_baseline=consolidate_baseline or False,
            user_notes=notes or "",
            app_version="1.0.0"  # TODO: Get from actual app version
        )

        return dict(content=toml_content, filename=filename, type="text/plain")

    except Exception as e:
        logging.error(f"Error exporting query parameters: {e}")
        return dash.no_update


# Import Query Parameters Callbacks

@callback(
    Output('import-query-modal', 'is_open'),
    [Input('import-query-button', 'n_clicks'),
     Input('cancel-import-button', 'n_clicks'),
     Input('confirm-import-button', 'n_clicks')],
    State('import-query-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_import_modal(import_clicks, cancel_clicks, confirm_clicks, is_open):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'import-query-button':
        return True
    elif button_id in ['cancel-import-button', 'confirm-import-button']:
        return False

    return is_open


@callback(
    [Output('upload-status', 'children'),
     Output('import-preview-content', 'children'),
     Output('import-preview-content', 'style'),
     Output('import-validation-results', 'children'),
     Output('import-validation-results', 'style'),
     Output('confirm-import-button', 'disabled'),
     Output('imported-file-content-store', 'data'),
     Output('import-validation-results-store', 'data')],
    Input('upload-query-params', 'contents'),
    [State('upload-query-params', 'filename'),
     State('available-tables-store', 'data'),
     State('demographics-columns-store', 'data'),
     State('behavioral-columns-store', 'data')],
    prevent_initial_call=True
)
def handle_file_upload(contents, filename, available_tables, demographics_columns, behavioral_columns):
    if not contents:
        return [
            "",  # upload-status
            "",  # import-preview-content
            {'display': 'none'},  # import-preview-content style
            "",  # import-validation-results
            {'display': 'none'},  # import-validation-results style
            True,  # confirm-import-button disabled
            None,  # imported-file-content-store
            None   # import-validation-results-store
        ]

    try:
        # Decode the uploaded file
        import base64
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        file_content = decoded.decode('utf-8')

        # Parse TOML
        imported_data, parse_errors = import_query_parameters_from_toml(file_content)

        if parse_errors:
            error_content = dbc.Alert([
                html.H6("File Parsing Errors:", className="alert-heading"),
                html.Ul([html.Li(error) for error in parse_errors])
            ], color="danger")

            return [
                dbc.Alert(f"✗ Error parsing {filename}", color="danger"),
                "",  # import-preview-content
                {'display': 'none'},  # import-preview-content style
                error_content,  # import-validation-results
                {'display': 'block'},  # import-validation-results style
                True,  # confirm-import-button disabled
                None,  # imported-file-content-store
                None   # import-validation-results-store
            ]

        # Validate against current dataset
        config = get_config()
        validation_results, validation_errors = validate_imported_query_parameters(
            imported_data, available_tables or [], demographics_columns or [],
            behavioral_columns or {}, config
        )

        # Generate preview content
        preview_content = []
        metadata = imported_data.get('metadata', {})

        # Show metadata
        preview_content.append(html.H6("File Metadata:"))
        preview_content.append(html.Ul([
            html.Li(f"Export Date: {metadata.get('export_timestamp', 'Unknown')}"),
            html.Li(f"App Version: {metadata.get('app_version', 'Unknown')}"),
            html.Li(f"Notes: {metadata.get('user_notes', 'None')}")
        ]))

        # Show what will be imported
        cohort_filters = imported_data.get('cohort_filters', {})
        if cohort_filters:
            preview_content.append(html.H6("Cohort Filters:", className="mt-3"))
            for key, value in cohort_filters.items():
                if key == 'age_range':
                    preview_content.append(html.Li(f"Age Range: {value[0]} - {value[1]}"))
                elif key == 'substudies':
                    preview_content.append(html.Li(f"Substudies: {', '.join(value)}"))
                elif key == 'sessions':
                    preview_content.append(html.Li(f"Sessions: {', '.join(value)}"))

        phenotypic_filters = imported_data.get('phenotypic_filters', [])
        if phenotypic_filters:
            preview_content.append(html.H6("Phenotypic Filters:", className="mt-3"))
            for i, pf in enumerate(phenotypic_filters, 1):
                filter_desc = f"{pf['table']}.{pf['column']}"
                if pf['filter_type'] == 'numeric':
                    filter_desc += f" ({pf['min_val']} - {pf['max_val']})"
                elif pf['filter_type'] == 'categorical':
                    selected_vals = pf.get('selected_values', [])
                    if len(selected_vals) <= 3:
                        filter_desc += f" ({', '.join(map(str, selected_vals))})"
                    else:
                        filter_desc += f" ({len(selected_vals)} values)"
                preview_content.append(html.Li(f"Filter {i}: {filter_desc}"))

        export_selection = imported_data.get('export_selection', {})
        if export_selection:
            preview_content.append(html.H6("Export Selection:", className="mt-3"))
            if export_selection.get('selected_tables'):
                preview_content.append(html.Li(f"Tables: {', '.join(export_selection['selected_tables'])}"))
            if export_selection.get('enwiden_longitudinal'):
                preview_content.append(html.Li("Enwiden longitudinal data: Yes"))
            if export_selection.get('consolidate_baseline'):
                preview_content.append(html.Li("Consolidate baseline sessions: Yes"))

        # Generate validation results display
        validation_content = []

        if validation_errors:
            validation_content.append(dbc.Alert([
                html.H6("Validation Errors:", className="alert-heading"),
                html.Ul([html.Li(error) for error in validation_errors])
            ], color="danger"))

            can_import = False
        else:
            validation_content.append(dbc.Alert([
                html.I(className="bi bi-check-circle me-2"),
                "All parameters validated successfully!"
            ], color="success"))

            can_import = True

        # Show what will be imported vs what will be skipped
        valid_params = validation_results.get('valid_parameters', {})
        invalid_params = validation_results.get('invalid_parameters', {})

        if valid_params:
            validation_content.append(html.H6("Will be imported:", className="text-success mt-3"))
            validation_content.append(html.Ul([
                html.Li(f"Cohort filters: {len(valid_params.get('cohort_filters', {}))} items"),
                html.Li(f"Phenotypic filters: {len(valid_params.get('phenotypic_filters', []))} items"),
                html.Li(f"Export tables: {len(valid_params.get('export_selection', {}).get('selected_tables', []))} items")
            ]))

        if any(invalid_params.values()):
            validation_content.append(html.H6("Will be skipped (invalid):", className="text-danger mt-3"))
            skip_items = []
            if invalid_params.get('cohort_filters'):
                skip_items.append(f"Cohort filters: {len(invalid_params['cohort_filters'])} items")
            if invalid_params.get('phenotypic_filters'):
                skip_items.append(f"Phenotypic filters: {len(invalid_params['phenotypic_filters'])} items")
            if invalid_params.get('export_selection', {}).get('selected_tables'):
                skip_items.append(f"Export tables: {len(invalid_params['export_selection']['selected_tables'])} items")

            if skip_items:
                validation_content.append(html.Ul([html.Li(item) for item in skip_items]))

        upload_status = dbc.Alert(f"✓ Successfully parsed {filename}", color="success")

        # Add filename to validation results for later use
        validation_results['filename'] = filename

        return [
            upload_status,
            preview_content,
            {'display': 'block'},  # show preview
            validation_content,
            {'display': 'block'},  # show validation
            not can_import,  # disable import button if validation failed
            file_content,  # store file content
            validation_results  # store validation results
        ]

    except Exception as e:
        logging.error(f"Error processing uploaded file: {e}")
        error_content = dbc.Alert([
            html.H6("File Processing Error:", className="alert-heading"),
            html.P(f"Could not process the uploaded file: {str(e)}")
        ], color="danger")

        return [
            dbc.Alert(f"✗ Error processing {filename}", color="danger"),
            "",  # import-preview-content
            {'display': 'none'},  # import-preview-content style
            error_content,  # import-validation-results
            {'display': 'block'},  # import-validation-results style
            True,  # confirm-import-button disabled
            None,  # imported-file-content-store
            None   # import-validation-results-store
        ]


@callback(
    [Output('age-slider', 'value', allow_duplicate=True),
     Output('study-site-store', 'data', allow_duplicate=True),
     Output('session-selection-store', 'data', allow_duplicate=True),
     Output('phenotypic-filters-store', 'data', allow_duplicate=True),
     Output('table-multiselect', 'value', allow_duplicate=True),
     Output('selected-columns-per-table-store', 'data', allow_duplicate=True),
     Output('enwiden-data-checkbox', 'value', allow_duplicate=True),
     Output('consolidate-baseline-checkbox', 'value', allow_duplicate=True),
     Output('merged-dataframe-store', 'data', allow_duplicate=True),
     Output('data-preview-area', 'children', allow_duplicate=True),
     Output('current-query-metadata-store', 'data')],
    Input('confirm-import-button', 'n_clicks'),
    [State('import-validation-results-store', 'data'),
     State('imported-file-content-store', 'data')],
    prevent_initial_call=True
)
def apply_imported_parameters(confirm_clicks, validation_results, file_content):
    if not confirm_clicks or confirm_clicks == 0 or not validation_results or not file_content:
        return [dash.no_update] * 11

    try:
        # Re-parse the file content to get the imported data
        imported_data, _ = import_query_parameters_from_toml(file_content)
        valid_params = validation_results.get('valid_parameters', {})

        # Reset age slider to default first
        config = get_config()
        age_value = [config.DEFAULT_AGE_SELECTION[0], config.DEFAULT_AGE_SELECTION[1]]

        # Apply valid cohort filters
        substudies_value = []
        sessions_value = []

        cohort_filters = valid_params.get('cohort_filters', {})
        if 'age_range' in cohort_filters:
            age_value = cohort_filters['age_range']
        if 'substudies' in cohort_filters:
            substudies_value = cohort_filters['substudies']
        if 'sessions' in cohort_filters:
            sessions_value = cohort_filters['sessions']

        # Apply valid phenotypic filters
        valid_phenotypic_filters = valid_params.get('phenotypic_filters', [])
        phenotypic_store_data = {'filters': [], 'next_id': 1}

        for i, pf in enumerate(valid_phenotypic_filters):
            filter_data = {
                'id': i + 1,
                'table': pf['table'],
                'column': pf['column'],
                'filter_type': pf['filter_type'],
                'enabled': True,
                'expanded': False
            }

            if pf['filter_type'] == 'numeric':
                filter_data.update({
                    'min_val': pf.get('min_val'),
                    'max_val': pf.get('max_val'),
                    'range_min': pf.get('min_val'),
                    'range_max': pf.get('max_val'),
                    'selected_values': [],
                    'available_values': []
                })
            elif pf['filter_type'] == 'categorical':
                filter_data.update({
                    'min_val': None,
                    'max_val': None,
                    'range_min': None,
                    'range_max': None,
                    'selected_values': pf.get('selected_values', []),
                    'available_values': pf.get('selected_values', [])
                })

            phenotypic_store_data['filters'].append(filter_data)

        phenotypic_store_data['next_id'] = len(valid_phenotypic_filters) + 1

        # Apply valid export selection
        export_selection = valid_params.get('export_selection', {})
        selected_tables = export_selection.get('selected_tables', [])
        selected_columns = export_selection.get('selected_columns', {})
        enwiden_value = export_selection.get('enwiden_longitudinal', False)
        consolidate_baseline_value = export_selection.get('consolidate_baseline', False)

        # Clear existing data preview and merged data store
        preview_content = dbc.Alert([
            html.I(className="bi bi-check-circle me-2"),
            "Query parameters imported successfully. ",
            html.Strong("Previous query results have been cleared."),
            " Use 'Generate Merged Data' to create new results with the imported parameters."
        ], color="success")

        # Prepare query metadata for storage
        metadata = imported_data.get('metadata', {})
        filename = validation_results.get('filename', 'Unknown')

        query_metadata = {
            'filename': filename,
            'metadata': metadata,
            'full_toml_content': file_content,
            'import_timestamp': datetime.now().isoformat()
        }

        return [
            age_value,                  # age-slider value
            substudies_value,           # study-site-store
            sessions_value,             # session-selection-store
            phenotypic_store_data,      # phenotypic-filters-store
            selected_tables,            # table-multiselect value
            selected_columns,           # selected-columns-per-table-store
            enwiden_value,              # enwiden-data-checkbox value
            consolidate_baseline_value, # consolidate-baseline-checkbox value
            None,                       # merged-dataframe-store (clear existing)
            preview_content,            # data-preview-area (show success message)
            query_metadata              # current-query-metadata-store
        ]

    except Exception as e:
        logging.error(f"Error applying imported parameters: {e}")
        error_content = dbc.Alert([
            html.I(className="bi bi-exclamation-triangle me-2"),
            f"Error applying imported parameters: {str(e)}"
        ], color="danger")

        return [dash.no_update] * 9 + [error_content, dash.no_update]


# Query Management Callbacks

@callback(
    [Output('current-query-display-text', 'children'),
     Output('current-query-dropdown-button', 'disabled'),
     Output('current-query-container', 'style')],
    Input('current-query-metadata-store', 'data'),
    prevent_initial_call=True
)
def update_query_dropdown_display(query_metadata):
    """Update the query dropdown button text and visibility when metadata is loaded"""
    if not query_metadata:
        return "", True, {'display': 'none'}

    filename = query_metadata.get('filename', 'Unknown')
    # Remove .toml extension if present
    display_name = filename.replace('.toml', '') if filename.endswith('.toml') else filename
    return f"Current query: {display_name}", False, {'display': 'block', 'margin-top': '0.5rem'}


@callback(
    Output('query-details-modal', 'is_open'),
    [Input('current-query-dropdown-button', 'n_clicks'),
     Input('close-query-details-button', 'n_clicks')],
    State('query-details-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_query_details_modal(dropdown_clicks, close_clicks, is_open):
    """Toggle the query details modal"""
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'current-query-dropdown-button':
        return not is_open
    elif button_id == 'close-query-details-button':
        return False

    return is_open


@callback(
    Output('query-details-content', 'children'),
    Input('query-details-modal', 'is_open'),
    State('current-query-metadata-store', 'data'),
    prevent_initial_call=True
)
def populate_query_details_content(is_open, query_metadata):
    """Populate the query details modal content"""
    if not is_open or not query_metadata:
        return ""

    try:
        filename = query_metadata.get('filename', 'Unknown')
        metadata = query_metadata.get('metadata', {})
        import_timestamp = query_metadata.get('import_timestamp', 'Unknown')
        toml_content = query_metadata.get('full_toml_content', '')

        # Parse the TOML content to display formatted details
        from query.query_parameters import import_query_parameters_from_toml
        imported_data, _ = import_query_parameters_from_toml(toml_content)

        content = []

        # File Information
        content.append(html.H5("File Information", className="mb-3"))
        content.append(dbc.Row([
            dbc.Col([
                html.Strong("Filename:"), f" {filename}"
            ], width=12),
            dbc.Col([
                html.Strong("Import Date:"), f" {import_timestamp[:19] if import_timestamp != 'Unknown' else 'Unknown'}"
            ], width=12, className="mt-2"),
            dbc.Col([
                html.Strong("Export Date:"), f" {metadata.get('export_timestamp', 'Unknown')}"
            ], width=12, className="mt-2"),
            dbc.Col([
                html.Strong("App Version:"), f" {metadata.get('app_version', 'Unknown')}"
            ], width=12, className="mt-2")
        ]))

        # User Notes
        if metadata.get('user_notes'):
            content.append(html.H5("Notes", className="mb-3 mt-4"))
            content.append(dbc.Card([
                dbc.CardBody([
                    html.P(metadata['user_notes'], className="mb-0", style={'color': 'black'})
                ], style={'background-color': 'white'})
            ], className="mb-3", style={'background-color': 'white'}))

        # Query Parameters
        content.append(html.H5("Query Parameters", className="mb-3 mt-4"))

        # Cohort Filters
        cohort_filters = imported_data.get('cohort_filters', {})
        if cohort_filters:
            content.append(html.H6("Cohort Filters", className="mt-3"))
            filter_items = []
            for key, value in cohort_filters.items():
                if key == 'age_range':
                    filter_items.append(html.Li(f"Age Range: {value[0]} - {value[1]}"))
                elif key == 'substudies':
                    filter_items.append(html.Li(f"Substudies: {', '.join(value)}"))
                elif key == 'sessions':
                    filter_items.append(html.Li(f"Sessions: {', '.join(value)}"))
            content.append(html.Ul(filter_items))

        # Phenotypic Filters
        phenotypic_filters = imported_data.get('phenotypic_filters', [])
        if phenotypic_filters:
            content.append(html.H6("Phenotypic Filters", className="mt-3"))
            filter_items = []
            for i, pf in enumerate(phenotypic_filters, 1):
                filter_desc = f"Filter {i}: {pf['table']}.{pf['column']}"
                if pf['filter_type'] == 'numeric':
                    filter_desc += f" ({pf['min_val']} - {pf['max_val']})"
                elif pf['filter_type'] == 'categorical':
                    selected_vals = pf.get('selected_values', [])
                    if len(selected_vals) <= 3:
                        filter_desc += f" ({', '.join(map(str, selected_vals))})"
                    else:
                        filter_desc += f" ({len(selected_vals)} values)"
                filter_items.append(html.Li(filter_desc))
            content.append(html.Ul(filter_items))

        # Export Selection
        export_selection = imported_data.get('export_selection', {})
        if export_selection:
            content.append(html.H6("Export Selection", className="mt-3"))
            export_items = []
            if export_selection.get('selected_tables'):
                export_items.append(html.Li(f"Tables: {', '.join(export_selection['selected_tables'])}"))
            if export_selection.get('enwiden_longitudinal'):
                export_items.append(html.Li("Enwiden longitudinal data: Yes"))
            if export_selection.get('consolidate_baseline'):
                export_items.append(html.Li("Consolidate baseline sessions: Yes"))
            if export_items:
                content.append(html.Ul(export_items))

        return content

    except Exception as e:
        return dbc.Alert([
            html.H6("Error displaying query details:", className="alert-heading"),
            html.P(f"Could not parse query details: {str(e)}")
        ], color="danger")


# Configuration Change Listener Callback
@callback(
    [Output('demographics-columns-store', 'data', allow_duplicate=True),
     Output('column-ranges-store', 'data', allow_duplicate=True),
     Output('merge-keys-store', 'data', allow_duplicate=True)],
    [Input('app-config-store', 'data')],
    prevent_initial_call=True
)
def refresh_data_stores_on_config_change(config_data):
    """Refresh data stores when configuration changes from settings page."""
    if not config_data:
        return no_update, no_update, no_update

    try:
        # Force refresh of table info to pick up config changes
        config = get_config()
        (behavioral_tables, demographics_cols, behavioral_cols_by_table,
         col_dtypes, col_ranges, merge_keys_dict,
         actions_taken, session_vals, is_empty, messages) = get_table_info(config)

        logging.info("Data stores refreshed due to configuration change")
        return demographics_cols, col_ranges, merge_keys_dict
    except Exception as e:
        logging.warning(f"Failed to refresh data stores after config change: {e}")
        return no_update, no_update, no_update


# Callback to populate merge strategy info with data path
@callback(
    Output('merge-strategy-info', 'children'),
    Input('merge-keys-store', 'data')
)
def update_merge_strategy_info(merge_keys_dict):
    """Display shortened data path and merge strategy information."""
    config = get_config()  # Get fresh config

    # Get shortened data path
    shortened_data_path = shorten_path(config.DATA_DIR)

    # Create the data path display
    children = [
        html.Div([
            html.Strong("Data Directory: "), #f8f9fa
            html.Code(shortened_data_path, style={'background-color': '#f8f9fa', 'padding': '2px 4px', 'border-radius': '3px'})
        ], style={'margin-bottom': '10px'})
    ]

    # Add merge strategy information if available
    if merge_keys_dict:
        merge_keys = MergeKeys.from_dict(merge_keys_dict)

        if merge_keys.is_longitudinal:
            children.append(html.Div([
                html.Strong("Merge Strategy: "),
                html.Span("Longitudinal Data", style={'color': '#28a745', 'font-weight': 'bold'}),
                html.Ul([
                    html.Li(f"Primary ID: {merge_keys.primary_id}"),
                    html.Li(f"Session ID: {merge_keys.session_id}") if merge_keys.session_id else None,
                    html.Li(f"Composite ID: {merge_keys.composite_id}") if merge_keys.composite_id else None
                ], style={'margin-top': '5px', 'margin-bottom': '0px'})
            ]))
        else:
            children.append(html.Div([
                html.Strong("Merge Strategy: "),
                html.Span("Cross-sectional Data", style={'color': '#007bff', 'font-weight': 'bold'}),
                html.Ul([
                    html.Li(f"Primary ID: {merge_keys.primary_id}")
                ], style={'margin-top': '5px', 'margin-bottom': '0px'})
            ]))
    else:
        children.append(html.Div([
            html.Strong("Merge Strategy: "),
            html.Span("Not determined", style={'color': '#6c757d', 'font-style': 'italic'})
        ]))

    return children
