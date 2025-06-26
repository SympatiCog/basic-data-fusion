import logging
import time
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, State, callback, dash_table, dcc, html, no_update

from config_manager import get_config
from state_manager import get_state_manager

# Assuming utils.py is in the same directory or accessible in PYTHONPATH
from utils import (
    MergeKeys,
    enwiden_longitudinal_data,
    generate_base_query_logic,
    generate_count_query,
    generate_data_query,
    generate_export_filename,
    generate_filtering_report,
    generate_final_data_summary,
    get_db_connection,
    get_table_info,
    get_unique_column_values,
    has_multisite_data,
    is_numeric_column,
)

dash.register_page(__name__, path='/', title='Query Data')

# Note: We get fresh config in callbacks to pick up changes from settings

layout = dbc.Container([
    # Data status and import link section
    html.Div(id='query-data-status-section'),
    dbc.Row([
        dbc.Col([
            html.H3("Merge Strategy"),
            html.Div(id='merge-strategy-info'),
        ], width=6), # Left column for merge strategy info
        dbc.Col([
            html.Div([
                html.Img(
                    src="/assets/TBS_Logo_Wide_sm.png",
                    style={
                        'width': '100%',
                        'height': 'auto',
                        'maxWidth': '800px'
                    }
                )
            ], className="d-flex justify-content-center align-items-center", style={'height': '100%'})
        ], width=6) # Right column for logo
    ]),
    dbc.Row([
        dbc.Col([
            html.H3("Live Participant Count"),
            html.Div(id='live-participant-count'), # Placeholder for participant count
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            html.H3("Define Cohort Filters"),
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.H4("Demographic Filters", className="card-title"),
                dbc.Row([
                    dbc.Col(html.Div([
                        html.Label("Age Range:"),
                        dcc.RangeSlider(id='age-slider', disabled=True, allowCross=False, step=1, tooltip={"placement": "bottom", "always_visible": True}),
                        html.Div(id='age-slider-info') # To show min/max or if disabled
                    ]), md=6),
                ]),
                html.Div(id='dynamic-demo-filters-placeholder', style={'marginTop': '20px'}), # For Rockland substudies and Sessions
            ])),
        ], md=6), # Left column for demographic filters
        dbc.Col([
            # Phenotypic Filters Card
            dbc.Card(dbc.CardBody([
                html.H4("Phenotypic Filters", className="card-title"),
                html.P("Filter participants based on data from any table and column.", className="card-text text-muted"),
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "Add Phenotypic Filter",
                            id="phenotypic-add-button",
                            color="primary",
                            size="sm"
                        )
                    ], width="auto"),
                    dbc.Col([
                        dbc.Button(
                            "Clear All",
                            id="phenotypic-clear-button",
                            color="outline-secondary",
                            size="sm",
                            title="Remove all phenotypic filters (including saved filters from previous sessions)"
                        )
                    ], width="auto")
                ], className="mb-3"),
                html.Div(id="phenotypic-session-notice", className="mb-2"),
                html.Div(id="phenotypic-filters-list")
            ])),
        ], md=6) # Right column for phenotypic filters
    ]),
    dbc.Row([
        dbc.Col([
            html.H3("Select Data for Export"),
            html.Div([
                html.H4("Select Tables:"),
                dcc.Dropdown(
                    id='table-multiselect',
                    multi=True,
                    placeholder="Select tables for export..."
                ),
                html.Div(id='column-selection-area'),
                html.Div([
                    dbc.Checkbox(
                        id='enwiden-data-checkbox',
                        label='Enwiden longitudinal data (pivot sessions to columns)',
                        value=False
                    )
                ], id='enwiden-checkbox-wrapper', style={'display': 'none', 'marginTop': '10px'})
            ], id='table-column-selector-container')
        ], md=12) # Full width for data export selection
    ]),
    dbc.Row([
        dbc.Col([
            html.H3("Query Results"),
            html.Div([
                dbc.Button(
                    "Generate Merged Data",
                    id='generate-data-button',
                    n_clicks=0,
                    color="primary",
                    className="mb-3"
                ),
                # Loading spinner for data processing
                dcc.Loading(
                    id="data-processing-loading",
                    type="default",
                    children=html.Div(id="data-processing-loading-output")
                ),
                html.Div(id='data-preview-area'),
                dcc.Download(id='download-dataframe-csv')
            ], id='results-container')
        ], width=12)
    ])
], fluid=True)





# Callback to populate data status section
@callback(
    Output('query-data-status-section', 'children'),
    [Input('available-tables-store', 'data'),
     Input('merge-keys-store', 'data')]
)
def update_data_status_section(available_tables, merge_keys_dict):
    """Show data status and import link if needed"""
    if not available_tables:
        # No data available - show import prompt
        return dbc.Row([
            dbc.Col([
                dbc.Alert([
                    html.I(className="bi bi-info-circle me-2"),
                    html.Strong("No data found. "),
                    html.A("Import CSV files", href="/import", className="alert-link"),
                    " to get started with data analysis."
                ], color="info")
            ], width=12)
        ], className="mb-4")
    else:
        # Data available - show quick summary
        num_tables = len(available_tables)
        return dbc.Row([
            dbc.Col([
                dbc.Alert([
                    html.I(className="bi bi-check-circle me-2"),
                    f"Data loaded: {num_tables} behavioral tables available. ",
                    html.A("Import more data", href="/import", className="alert-link"),
                    " if needed."
                ], color="info")
            ], width=12)
        ], className="mb-4")

# Callback to update Age Slider properties
@callback(
    [Output('age-slider', 'min'),
     Output('age-slider', 'max'),
     Output('age-slider', 'value'),
     Output('age-slider', 'marks'),
     Output('age-slider', 'disabled'),
     Output('age-slider-info', 'children')],
    [Input('demographics-columns-store', 'data'),
     Input('column-ranges-store', 'data')],
    [State('age-slider-state-store', 'data')]
)
def update_age_slider(demo_cols, col_ranges, stored_age_value):
    config = get_config()  # Get fresh config
    if not demo_cols or config.AGE_COLUMN not in demo_cols or not col_ranges:
        return 0, 100, [0, 100], {}, True, f"Age filter disabled: '{config.AGE_COLUMN}' column not found in demographics or ranges not available."

    # Use 'demo' as the alias for demographics table, consistent with get_table_alias() in utils.py
    age_col_key = f"demo.{config.AGE_COLUMN}" # Construct the key for column_ranges

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
        if min_age not in marks: marks[min_age] = str(min_age)
        if max_age not in marks: marks[max_age] = str(max_age)

        return min_age, max_age, value, marks, False, f"Age range: {min_age}-{max_age}"
    else:
        # Fallback if age column is in demo_cols but no range found (should ideally not happen if get_table_info is robust)
        return 0, 100, [config.DEFAULT_AGE_SELECTION[0], config.DEFAULT_AGE_SELECTION[1]], {}, True, f"Age filter disabled: Range for '{config.AGE_COLUMN}' column not found."


# Callback to populate dynamic demographic filters (Rockland substudies, Sessions)
@callback(
    Output('dynamic-demo-filters-placeholder', 'children'),
    [Input('demographics-columns-store', 'data'),
     Input('session-values-store', 'data'),
     Input('merge-keys-store', 'data')],
    [State('rockland-substudy-store', 'data'),
     State('session-selection-store', 'data')]
)
def update_dynamic_demographic_filters(demo_cols, session_values, merge_keys_dict,
                                     stored_rockland_values, stored_session_values):
    config = get_config()  # Get fresh config
    if not demo_cols:
        return html.P("Demographic information not yet available to populate dynamic filters.")

    children = []

    # Multisite/Multistudy Filters
    if has_multisite_data(demo_cols, config.STUDY_SITE_COLUMN):
        children.append(html.H5("Substudy/Site Selection", style={'marginTop': '15px'}))
        # Use stored values if available, otherwise use default
        rockland_value = stored_rockland_values if stored_rockland_values else config.DEFAULT_ROCKLAND_STUDIES
        children.append(
            dcc.Dropdown(
                id='rockland-substudy-dropdown',
                options=[{'label': s, 'value': s} for s in config.ROCKLAND_BASE_STUDIES],
                value=rockland_value,
                multi=True,
                placeholder="Select Rockland substudies..."
            )
        )

    # Session Filters
    if merge_keys_dict:
        mk = MergeKeys.from_dict(merge_keys_dict)
        if mk.is_longitudinal and mk.session_id and session_values:
            children.append(html.H5("Session/Visit Selection", style={'marginTop': '15px'}))
            # Use stored values if available, otherwise default to all available sessions
            session_value = stored_session_values if stored_session_values else session_values
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


# Callbacks to update stores when dynamic dropdowns change
@callback(
    Output('rockland-substudy-store', 'data'),
    Input('rockland-substudy-dropdown', 'value'),
    prevent_initial_call=True
)
def update_rockland_substudy_store(rockland_values):
    return rockland_values if rockland_values else []

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
    prevent_initial_call=True
)
def manage_phenotypic_filters(
    add_clicks, clear_clicks, remove_clicks,
    table_values, column_values, range_values, categorical_values,
    current_state
):
    """Single callback to manage all phenotypic filter state changes."""
    ctx = dash.callback_context

    if not ctx.triggered:
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
     Input('merge-keys-store', 'data')]
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
            table_alias = 'demo' if selected_table == demographics_table_name else selected_table
            dtype_key = f"{table_alias}.{selected_column}"
            column_dtype = column_dtypes.get(dtype_key)

            if column_dtype and is_numeric_column(column_dtype):
                # Numeric column - use range slider
                range_key = f"{table_alias}.{selected_column}"
                if range_key in column_ranges:
                    min_val, max_val = column_ranges[range_key]
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


def convert_phenotypic_to_behavioral_filters(phenotypic_filters_state):
    """Convert phenotypic filters to behavioral filters format for query generation."""
    if not phenotypic_filters_state or not phenotypic_filters_state.get('filters'):
        return []

    behavioral_filters = []
    for filter_data in phenotypic_filters_state['filters']:
        if not filter_data.get('enabled'):
            continue

        if filter_data.get('table') and filter_data.get('column') and filter_data.get('filter_type'):
            behavioral_filter = {
                'table': filter_data['table'],
                'column': filter_data['column'],
                'filter_type': filter_data['filter_type']
            }

            if filter_data['filter_type'] == 'numeric':
                behavioral_filter['min_val'] = filter_data.get('min_val')
                behavioral_filter['max_val'] = filter_data.get('max_val')
            elif filter_data['filter_type'] == 'categorical':
                behavioral_filter['selected_values'] = filter_data.get('selected_values', [])

            behavioral_filters.append(behavioral_filter)

    return behavioral_filters










# Live Participant Count Callback (optimized with cached DB connection)
@callback(
    Output('live-participant-count', 'children'),
    [Input('age-slider', 'value'),
     Input('rockland-substudy-store', 'data'), # For Rockland substudies
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
    ctx = dash.callback_context
    
    # Try to get data from StateManager if available (hybrid approach)
    state_manager = get_state_manager()
    if user_session_id:
        state_manager.set_user_context(user_session_id)
        
        # Try StateManager first, fallback to callback parameters
        server_merge_keys = state_manager.get_store_data('merge-keys-store')
        server_available_tables = state_manager.get_store_data('available-tables-store')
        
        # Use server data if available and not client-managed
        if server_merge_keys and server_merge_keys != "CLIENT_MANAGED":
            merge_keys_dict = server_merge_keys
        if server_available_tables and server_available_tables != "CLIENT_MANAGED":
            available_tables = server_available_tables
    
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
        base_query, params = generate_base_query_logic(
            current_config, merge_keys, demographic_filters, behavioral_filters, list(tables_for_query)
        )
        count_query, count_params = generate_count_query(base_query, params, merge_keys)

        if count_query:
            # Use cached database connection for improved performance
            con = get_db_connection()
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




@callback(
    [Output('available-tables-store', 'data'),
     Output('demographics-columns-store', 'data'),
     Output('behavioral-columns-store', 'data'),
     Output('column-dtypes-store', 'data'),
     Output('column-ranges-store', 'data'),
     Output('merge-keys-store', 'data'),
     Output('session-values-store', 'data'),
     Output('all-messages-store', 'data'), # To display errors from get_table_info
     Output('merge-strategy-info', 'children')],
    [Input('query-data-status-section', 'id')], # Trigger on page load
    [State('user-session-id', 'data')] # User context for StateManager
)
def load_initial_data_info(_, user_session_id): # Trigger on page load
    # We need to use the global config instance that was loaded/created when query.py was imported.
    # Or, if config can change dynamically (e.g. via UI), it needs to be managed in a dcc.Store

    # Re-fetch config if it could have changed (e.g., if settings were editable in another part of the app)
    # For now, assume config loaded at app start is sufficient, or re-initialize.
    current_config = get_config()  # Get fresh config instance

    (behavioral_tables, demographics_cols, behavioral_cols_by_table,
     col_dtypes, col_ranges, merge_keys_dict,
     actions_taken, session_vals, is_empty, messages) = get_table_info(current_config)

    info_messages = []
    if messages: # 'messages' is 'all_messages' from get_table_info, which is List[str]
        for msg_text in messages:
            color = "black" # Default color
            msg_lower = msg_text.lower()
            if "error" in msg_lower:
                color = "red"
            elif "warning" in msg_lower or "warn" in msg_lower: # Catch 'warning' or 'warn'
                color = "orange"
            elif "info" in msg_lower or "note" in msg_lower: # Catch 'info' or 'note'
                color = "blue"
            elif "✅" in msg_text or "success" in msg_lower: # Catch success indicators
                color = "green"

            info_messages.append(html.P(msg_text, style={'color': color}))

    if actions_taken:
        # Summarize dataset preparation actions instead of showing all
        fixed_count = sum(1 for action in actions_taken if "Fixed inconsistent" in action)
        added_count = sum(1 for action in actions_taken if "Added" in action)

        if fixed_count > 0 or added_count > 0:
            info_messages.append(html.H5("Dataset Preparation:", style={'marginTop': '10px'}))
            if added_count > 0:
                info_messages.append(html.P(f"✅ Added composite IDs to {added_count} file(s)", style={'color': 'green', 'marginBottom': '2px'}))
            if fixed_count > 0:
                info_messages.append(html.P(f"🔧 Fixed inconsistent IDs in {fixed_count} file(s)", style={'color': 'orange', 'marginBottom': '2px'}))

        # Show other non-ID related actions individually
        other_actions = [action for action in actions_taken if not ("Fixed inconsistent" in action or "Added" in action)]
        for action in other_actions:
            info_messages.append(html.P(action))

    merge_strategy_display = [html.H5("Merge Strategy:", style={'marginTop': '8px', 'marginBottom': '8px'})]
    if merge_keys_dict:
        mk = MergeKeys.from_dict(merge_keys_dict)
        if mk.is_longitudinal:
            merge_strategy_display.append(html.P("Detected: Longitudinal data.", style={'marginBottom': '0px'}))
            merge_strategy_display.append(html.P(f"Primary ID: {mk.primary_id}", style={'marginBottom': '0px'}))
            merge_strategy_display.append(html.P(f"Session ID: {mk.session_id}", style={'marginBottom': '0px'}))
            merge_strategy_display.append(html.P(f"Composite ID (for merge): {mk.composite_id}", style={'marginBottom': '8px'}))
        else:
            merge_strategy_display.append(html.P("Detected: Cross-sectional data.", style={'marginBottom': '4px'}))
            merge_strategy_display.append(html.P(f"Primary ID (for merge): {mk.primary_id}", style={'marginBottom': '8px'}))
    else:
        merge_strategy_display.append(html.P("Merge strategy not determined yet. Upload data or check configuration.", style={'marginBottom': '4px'}))

    # Combine messages from get_table_info with other status messages
    # Place dataset preparation actions and merge strategy info after general messages.
    status_display_content = info_messages + merge_strategy_display
    status_display = html.Div(status_display_content)


    # Store critical data in StateManager for server-side state management
    state_manager = get_state_manager()
    if user_session_id:
        state_manager.set_user_context(user_session_id)
        
        # Store critical stores in StateManager (hybrid approach)
        try:
            state_manager.set_store_data('merge-keys-store', merge_keys_dict)
            state_manager.set_store_data('available-tables-store', behavioral_tables)
            state_manager.set_store_data('demographics-columns-store', demographics_cols)
            logging.info(f"Stored critical data in StateManager for user {user_session_id[:8]}...")
        except Exception as e:
            logging.error(f"Failed to store data in StateManager: {e}")

    return (behavioral_tables, demographics_cols, behavioral_cols_by_table,
            col_dtypes, col_ranges, merge_keys_dict, session_vals,
            messages, # Store raw messages from get_table_info for potential detailed display
            status_display) # This now goes to 'merge-strategy-info' Div


# Callbacks for Table and Column Selection
@callback(
    Output('table-multiselect', 'options'),
    Input('available-tables-store', 'data')
)
def update_table_multiselect_options(available_tables_data):
    if not available_tables_data:
        return []
    # available_tables_data is a list of table names (strings)
    return [{'label': table, 'value': table} for table in available_tables_data]

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
    Output('column-selection-area', 'children'),
    Input('table-multiselect', 'value'), # List of selected table names
    State('demographics-columns-store', 'data'),
    State('behavioral-columns-store', 'data'),
    State('merge-keys-store', 'data'),
    State('selected-columns-per-table-store', 'data') # To pre-populate selections
)
def update_column_selection_area(selected_tables, demo_cols, behavioral_cols, merge_keys_dict, stored_selections):
    if not selected_tables:
        return dbc.Alert("Select tables above to choose columns for export.", color="info")

    if not demo_cols: demo_cols = []
    if not behavioral_cols: behavioral_cols = {}
    if not stored_selections: stored_selections = {}

    config = get_config()  # Get fresh config
    merge_keys = MergeKeys.from_dict(merge_keys_dict) if merge_keys_dict else MergeKeys(primary_id="unknown")
    id_cols_to_exclude = {merge_keys.primary_id, merge_keys.session_id, merge_keys.composite_id}
    demographics_table_name = config.get_demographics_table_name()

    cards = []
    for table_name in selected_tables:
        options = []
        actual_cols_for_table = []
        is_demographics_table = (table_name == demographics_table_name)

        if is_demographics_table:
            actual_cols_for_table = demo_cols
        elif table_name in behavioral_cols:
            actual_cols_for_table = behavioral_cols[table_name]

        for col in actual_cols_for_table:
            if col not in id_cols_to_exclude: # Exclude ID columns from selection
                options.append({'label': col, 'value': col})

        # Get previously selected columns for this table, if any
        current_selection_for_table = stored_selections.get(table_name, [])

        card_body_content = [
            dcc.Dropdown(
                id={'type': 'column-select-dropdown', 'table': table_name},
                options=options,
                value=current_selection_for_table, # Pre-populate with stored selections
                multi=True,
                placeholder=f"Select columns from {table_name}..."
            )
        ]
        # If it's the demographics table, add a note that ID columns are auto-included
        if is_demographics_table:
            card_body_content.insert(0, html.P(f"All demographics columns (including {merge_keys.get_merge_column()}) will be included by default. You can select additional ones if needed, or deselect to only include IDs/merge keys.", className="small text-muted"))


        cards.append(dbc.Card([
            dbc.CardHeader(f"Columns for: {table_name}"),
            dbc.CardBody(card_body_content)
        ], className="mb-3"))

    return cards

@callback(
    Output('selected-columns-per-table-store', 'data'),
    Input({'type': 'column-select-dropdown', 'table': dash.ALL}, 'value'), # Values from all column dropdowns
    State({'type': 'column-select-dropdown', 'table': dash.ALL}, 'id'), # IDs of all column dropdowns
    State('selected-columns-per-table-store', 'data') # Current stored data
)
def update_selected_columns_store(all_column_values, all_column_ids, current_stored_data):
    ctx = dash.callback_context

    # Make a copy to modify, or initialize if None
    updated_selections = current_stored_data.copy() if current_stored_data else {}

    # Only update if callback was actually triggered by user interaction
    # This prevents overwriting stored data on initial page load
    if ctx.triggered and all_column_ids and all_column_values:
        for i, component_id_dict in enumerate(all_column_ids):
            table_name = component_id_dict['table']
            selected_cols_for_table = all_column_values[i]

            if selected_cols_for_table is not None: # An empty selection is an empty list, None means no interaction yet
                updated_selections[table_name] = selected_cols_for_table
            elif table_name in updated_selections and selected_cols_for_table is None:
                # This case might occur if a table is deselected from table-multiselect,
                # its column dropdown might fire a final None value.
                # However, the update_column_selection_area callback should remove the dropdown.
                # If a user manually clears a dropdown, it becomes an empty list.
                pass # No change if value is None and table already existed or didn't.
    else:
        # Return no_update to preserve stored data when callback isn't triggered by user interaction
        return no_update

    # This ensures that if a table is deselected from 'table-multiselect',
    # its column selections are removed from the store.
    # We get the list of currently *rendered* tables from the IDs.
    # Any table in 'updated_selections' NOT in this list should be removed.
    current_rendered_tables = {comp_id['table'] for comp_id in all_column_ids}
    keys_to_remove = [table_key for table_key in updated_selections if table_key not in current_rendered_tables]
    for key in keys_to_remove:
        del updated_selections[key]

    return updated_selections

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

# Callback for Data Generation
@callback(
    [Output('data-preview-area', 'children'),
     Output('merged-dataframe-store', 'data'),
     Output('data-processing-loading-output', 'children')], # Store for profiling page
    Input('generate-data-button', 'n_clicks'),
    [State('age-slider', 'value'),
     State('rockland-substudy-store', 'data'),
     State('session-selection-store', 'data'),
     State('phenotypic-filters-store', 'data'),
     State('selected-columns-per-table-store', 'data'),
     State('enwiden-data-checkbox', 'value'), # Boolean value (True when checked, False when unchecked)
     State('merge-keys-store', 'data'),
     State('available-tables-store', 'data'), # Needed for tables_to_join logic
     State('table-multiselect', 'value')] # Explicitly selected tables for export
)
def handle_generate_data(
    n_clicks,
    age_range,
    rockland_substudy_values, session_filter_values,
    phenotypic_filters_state, selected_columns_per_table,
    enwiden_checkbox_value, merge_keys_dict, available_tables, tables_selected_for_export
):
    if n_clicks == 0 or not merge_keys_dict:
        return dbc.Alert("Click 'Generate Merged Data' after selecting filters and columns.", color="info"), None, ""

    current_config = get_config()  # Get fresh config instance
    merge_keys = MergeKeys.from_dict(merge_keys_dict)

    # --- Collect Demographic Filters ---
    demographic_filters = {}
    if age_range: demographic_filters['age_range'] = age_range

    # Handle Rockland substudy filtering
    if rockland_substudy_values:
        demographic_filters['substudies'] = rockland_substudy_values

    # Handle session filtering
    if session_filter_values:
        demographic_filters['sessions'] = session_filter_values

    # Convert phenotypic filters to behavioral filters for query generation
    behavioral_filters = convert_phenotypic_to_behavioral_filters(phenotypic_filters_state)

    # --- Determine Tables to Join ---
    # Start with tables explicitly selected for export
    tables_for_query = set(tables_selected_for_export if tables_selected_for_export else [])
    tables_for_query.add(current_config.get_demographics_table_name()) # Always include demographics

    # Add tables from phenotypic filters
    for p_filter in behavioral_filters:
        tables_for_query.add(p_filter['table'])

    # Note: Session-based table selection logic would go here when session filters are available
    # if merge_keys.is_longitudinal and demographic_filters.get('sessions') and \
    #    len(tables_for_query.intersection(set(available_tables if available_tables else []))) == 0 and \
    #    current_config.get_demographics_table_name() in tables_for_query and \
    #    len(tables_for_query) == 1 and available_tables:
    #     tables_for_query.add(available_tables[0]) # Add a behavioral table if needed for session join

    # --- Selected Columns for Query ---
    # If no columns are selected for a table in tables_selected_for_export, select all its non-ID columns.

    query_selected_columns = selected_columns_per_table.copy() if selected_columns_per_table else {}
    # For tables selected for export but with no specific columns chosen, this implies "all columns" for that table.
    # The generate_data_query handles this: if a table is in selected_tables (i.e. tables_for_query here)
    # and has entries in selected_columns, those are used. If demo.* is default, other tables need explicit columns.
    # For this implementation, we assume selected_columns_per_table_store correctly reflects user choices.
    # If a table is in tables_selected_for_export, it should be in query_selected_columns.
    # If user wants all columns from table X, they should use "select all" in its column dropdown (not yet implemented).
    # For now, only explicitly selected columns via UI are passed. generate_data_query adds demo.*

    try:
        # Start timing the query + merge operation
        start_time = time.time()

        base_query, params = generate_base_query_logic(
            current_config, merge_keys, demographic_filters, behavioral_filters, list(tables_for_query)
        )
        data_query, data_params = generate_data_query(
            base_query, params, list(tables_for_query), query_selected_columns
        )

        if not data_query:
            return dbc.Alert("Could not generate data query.", color="warning"), None, ""

        # Use cached database connection for improved performance
        con = get_db_connection()
        result_df = con.execute(data_query, data_params).fetchdf()

        original_row_count = len(result_df)

        if enwiden_checkbox_value and merge_keys.is_longitudinal:
            result_df = enwiden_longitudinal_data(result_df, merge_keys)
            enwiden_info = f" (enwidened from {original_row_count} rows to {len(result_df)} rows)"
        else:
            enwiden_info = ""

        # Calculate elapsed time
        elapsed_time = time.time() - start_time

        if result_df.empty:
            return dbc.Alert("No data found for the selected criteria.", color="info"), None, ""

        # Prepare for DataTable
        dt_columns = [{"name": i, "id": i} for i in result_df.columns]
        # For performance, only show head in preview
        dt_data = result_df.head(current_config.MAX_DISPLAY_ROWS).to_dict('records')

        preview_table = dash_table.DataTable(
            data=dt_data,
            columns=dt_columns,
            page_size=10,
            style_table={'overflowX': 'auto'},
            filter_action="native",
            sort_action="native",
        )

        return (
            html.Div([
                dbc.Alert(f"Filter/query/merge successful in {elapsed_time:.2f} seconds. Displaying first {min(len(result_df), current_config.MAX_DISPLAY_ROWS)} of {len(result_df)} total rows{enwiden_info}.", color="success"),
                preview_table,
                html.Hr(),
                dbc.Row([
                    dbc.Col([
                        dbc.Button("Download CSV", id="download-csv-button", color="success", className="mt-2")
                    ], width="auto"),
                    dbc.Col([
                        dbc.Button("Download with Custom Name", id="download-custom-csv-button", color="outline-success", className="mt-2")
                    ], width="auto"),
                    dbc.Col([
                        dbc.Button("Generate Summary", id="generate-summary-button", color="info", className="mt-2")
                    ], width="auto")
                ], className="g-2"),
                # Modal for custom filename
                dbc.Modal([
                    dbc.ModalHeader(dbc.ModalTitle("Download CSV with Custom Filename")),
                    dbc.ModalBody([
                        html.P("Enter a filename for your CSV export:"),
                        dbc.InputGroup([
                            dbc.Input(
                                id="custom-filename-input",
                                placeholder="Enter filename (without .csv extension)",
                                value="",
                                type="text"
                            ),
                            dbc.InputGroupText(".csv")
                        ], className="mb-3"),
                        html.P("Suggested filename based on your selection:", className="text-muted small"),
                        html.Code(id="suggested-filename", className="text-muted small")
                    ]),
                    dbc.ModalFooter([
                        dbc.Button("Cancel", id="cancel-download-button", color="secondary", className="me-2"),
                        dbc.Button("Download", id="confirm-download-button", color="success")
                    ])
                ], id="filename-modal", is_open=False),
                # Modal for summary generation
                dbc.Modal([
                    dbc.ModalHeader(dbc.ModalTitle("Generate Filtering Summary Report")),
                    dbc.ModalBody([
                        html.P("This will generate two CSV files:"),
                        html.Ul([
                            html.Li("filtering_report.csv - Shows filter application steps and sample size impact"),
                            html.Li("final_data_summary.csv - Descriptive statistics for the final filtered dataset")
                        ]),
                        html.Hr(),
                        html.P("Enter a prefix for your report files (optional):"),
                        dbc.InputGroup([
                            dbc.Input(
                                id="summary-filename-prefix-input",
                                placeholder="Enter filename prefix (optional)",
                                value="",
                                type="text"
                            ),
                            dbc.InputGroupText("_filtering_report.csv / _final_data_summary.csv")
                        ], className="mb-3"),
                        html.P("Default filenames will be used if no prefix is provided.", className="text-muted small"),
                        html.Code(id="suggested-summary-filenames", className="text-muted small")
                    ]),
                    dbc.ModalFooter([
                        dbc.Button("Cancel", id="cancel-summary-button", color="secondary", className="me-2"),
                        dbc.Button("Generate Reports", id="confirm-summary-button", color="info")
                    ])
                ], id="summary-modal", is_open=False)
            ]),
            {
                'row_count': len(result_df),
                'column_count': len(result_df.columns),
                'columns': result_df.columns.tolist(),
                'full_data': result_df.to_dict('records'),  # Store complete dataset for plotting/profiling
                'filters_applied': {
                    'age_range': age_range,
                    'phenotypic_filters': phenotypic_filters_state,
                    'session_filters': session_filter_values
                },
                'data_size_mb': round(len(str(result_df.to_dict('records'))) / (1024*1024), 2)  # Track data size
            }, # Store complete dataset for plotting and profiling pages
            ""  # Clear loading message
        )

    except Exception as e:
        logging.error(f"Error during data generation: {e}")
        logging.error(f"Query attempted: {data_query if 'data_query' in locals() else 'N/A'}")
        return dbc.Alert(f"Error generating data: {str(e)}", color="danger"), None, ""

# Callback to show loading message during data processing
@callback(
    Output('data-processing-loading-output', 'children', allow_duplicate=True),
    Input('generate-data-button', 'n_clicks'),
    prevent_initial_call=True
)
def show_data_processing_loading(n_clicks):
    if n_clicks and n_clicks > 0:
        return html.Div([
            html.P("Processing data query and generating results...",
                   className="text-info text-center"),
            html.P("This may take a moment for large datasets.",
                   className="text-muted text-center small")
        ])
    return ""

# Callback to open filename modal and populate suggested filename
@callback(
    [Output('filename-modal', 'is_open'),
     Output('suggested-filename', 'children'),
     Output('custom-filename-input', 'value')],
    [Input('download-custom-csv-button', 'n_clicks'),
     Input('cancel-download-button', 'n_clicks'),
     Input('confirm-download-button', 'n_clicks')],
    [State('filename-modal', 'is_open'),
     State('table-multiselect', 'value'),
     State('enwiden-data-checkbox', 'value')],
    prevent_initial_call=True
)
def toggle_filename_modal(custom_clicks, cancel_clicks, confirm_clicks, is_open, selected_tables, is_enwidened):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update

    # Check if any button was actually clicked (not just initialized)
    if ((custom_clicks is None or custom_clicks == 0) and
        (cancel_clicks is None or cancel_clicks == 0) and
        (confirm_clicks is None or confirm_clicks == 0)):
        return dash.no_update, dash.no_update, dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'download-custom-csv-button':
        # Generate suggested filename
        current_config = get_config()
        demographics_table_name = current_config.get_demographics_table_name()
        all_tables = [demographics_table_name] + (selected_tables or [])
        suggested_filename = generate_export_filename(all_tables, demographics_table_name, is_enwidened or False)
        # Remove .csv extension for input field
        suggested_name_without_ext = suggested_filename.replace('.csv', '')
        return True, suggested_filename, suggested_name_without_ext

    elif button_id in ['cancel-download-button', 'confirm-download-button']:
        return False, dash.no_update, dash.no_update

    return is_open, dash.no_update, dash.no_update


# Callback for direct CSV Download Button (uses smart filename)
@callback(
    Output('download-dataframe-csv', 'data'),
    [Input('download-csv-button', 'n_clicks'),
     Input('confirm-download-button', 'n_clicks')],
    [State('merged-dataframe-store', 'data'),
     State('table-multiselect', 'value'),
     State('enwiden-data-checkbox', 'value'),
     State('custom-filename-input', 'value')],
    prevent_initial_call=True
)
def download_csv_data(direct_clicks, custom_clicks, stored_data, selected_tables, is_enwidened, custom_filename):
    # Only proceed if we have actual button clicks and data
    if not stored_data:
        return dash.no_update

    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    # Check if any button was actually clicked (not just initialized)
    if (direct_clicks is None or direct_clicks == 0) and (custom_clicks is None or custom_clicks == 0):
        return dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    # Additional safety check - only proceed if a download button was clicked
    if button_id not in ['download-csv-button', 'confirm-download-button']:
        return dash.no_update

    # Regenerate the data instead of using stored dataframe
    try:
        # Use the full dataset for download
        full_data = stored_data.get('full_data', [])
        if not full_data:
            raise ValueError("No data available for download")

        df = pd.DataFrame(full_data)
    except Exception as e:
        logging.error(f"Error preparing download data: {e}")
        return dash.no_update

    # Determine filename based on which button was clicked
    if button_id == 'download-csv-button':
        # Direct download with smart filename
        current_config = get_config()
        demographics_table_name = current_config.get_demographics_table_name()
        all_tables = [demographics_table_name] + (selected_tables or [])
        filename = generate_export_filename(all_tables, demographics_table_name, is_enwidened or False)
    elif button_id == 'confirm-download-button':
        # Custom filename from modal
        if custom_filename and custom_filename.strip():
            # Ensure .csv extension
            filename = custom_filename.strip()
            if not filename.endswith('.csv'):
                filename += '.csv'
            # Secure the filename
            from utils import secure_filename
            filename = secure_filename(filename)
        else:
            # Fallback to smart filename if custom name is empty
            current_config = get_config()
            demographics_table_name = current_config.get_demographics_table_name()
            all_tables = [demographics_table_name] + (selected_tables or [])
            filename = generate_export_filename(all_tables, demographics_table_name, is_enwidened or False)
    else:
        return dash.no_update

    return dcc.send_data_frame(df.to_csv, filename, index=False)


# Callback to open summary modal
@callback(
    [Output('summary-modal', 'is_open'),
     Output('suggested-summary-filenames', 'children'),
     Output('summary-filename-prefix-input', 'value')],
    [Input('generate-summary-button', 'n_clicks'),
     Input('cancel-summary-button', 'n_clicks'),
     Input('confirm-summary-button', 'n_clicks')],
    [State('summary-modal', 'is_open')],
    prevent_initial_call=True
)
def toggle_summary_modal(generate_clicks, cancel_clicks, confirm_clicks, is_open):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update

    # Check if any button was actually clicked (not just initialized)
    if ((generate_clicks is None or generate_clicks == 0) and
        (cancel_clicks is None or cancel_clicks == 0) and
        (confirm_clicks is None or confirm_clicks == 0)):
        return dash.no_update, dash.no_update, dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'generate-summary-button':
        # Generate suggested filenames with timestamp
        timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
        suggested_filtering = f"filtering_report_{timestamp}.csv"
        suggested_summary = f"final_data_summary_{timestamp}.csv"
        suggested_text = f"Default: {suggested_filtering} and {suggested_summary}"
        return True, suggested_text, ""

    elif button_id in ['cancel-summary-button', 'confirm-summary-button']:
        return False, dash.no_update, dash.no_update

    return is_open, dash.no_update, dash.no_update


# Callback for summary report generation and download
@callback(
    [Output('download-dataframe-csv', 'data', allow_duplicate=True),
     Output('summary-modal', 'is_open', allow_duplicate=True)],
    Input('confirm-summary-button', 'n_clicks'),
    [State('age-slider', 'value'),
     State('rockland-substudy-store', 'data'),
     State('session-selection-store', 'data'),
     State('phenotypic-filters-store', 'data'),
     State('merged-dataframe-store', 'data'),
     State('merge-keys-store', 'data'),
     State('available-tables-store', 'data'),
     State('table-multiselect', 'value'),
     State('summary-filename-prefix-input', 'value')],
    prevent_initial_call=True
)
def generate_and_download_summary_reports(
    confirm_clicks, age_range, rockland_substudy_values, session_filter_values,
    phenotypic_filters_state, merged_data_store, merge_keys_dict, available_tables,
    tables_selected_for_export, filename_prefix
):
    # Only proceed if we have actual button clicks and data
    if not confirm_clicks or not merged_data_store or not merge_keys_dict:
        return dash.no_update, dash.no_update

    ctx = dash.callback_context
    if not ctx.triggered or confirm_clicks == 0:
        return dash.no_update, dash.no_update

    try:
        current_config = get_config()
        merge_keys = MergeKeys.from_dict(merge_keys_dict)

        # Collect demographic and behavioral filters (same logic as in handle_generate_data)
        demographic_filters = {}
        if age_range:
            demographic_filters['age_range'] = age_range
        if rockland_substudy_values:
            demographic_filters['substudies'] = rockland_substudy_values
        if session_filter_values:
            demographic_filters['sessions'] = session_filter_values

        # Convert phenotypic filters to behavioral filters
        behavioral_filters = convert_phenotypic_to_behavioral_filters(phenotypic_filters_state)

        # Determine tables for query
        demographics_table_name = current_config.get_demographics_table_name()
        tables_for_query = set(tables_selected_for_export if tables_selected_for_export else [])
        tables_for_query.add(demographics_table_name)

        for bf in behavioral_filters:
            if bf.get('table'):
                tables_for_query.add(bf['table'])

        # Generate filtering report
        filtering_report_df = generate_filtering_report(
            current_config, merge_keys, demographic_filters, behavioral_filters, list(tables_for_query)
        )

        # Generate final data summary from stored data
        full_data = merged_data_store.get('full_data', [])
        if not full_data:
            raise ValueError("No data available for summary generation")

        final_df = pd.DataFrame(full_data)
        final_summary_df = generate_final_data_summary(final_df, merge_keys)

        # Handle longitudinal data special cases
        if merge_keys.is_longitudinal and len(final_df) > 0:
            # For longitudinal data, create session-specific summaries if requested
            if merge_keys.session_id and merge_keys.session_id in final_df.columns:
                sessions = final_df[merge_keys.session_id].dropna().unique()
                if len(sessions) > 1:
                    # Add session-specific summaries
                    session_summaries = []
                    for session in sorted(sessions):
                        session_data = final_df[final_df[merge_keys.session_id] == session]
                        if not session_data.empty:
                            session_summary = generate_final_data_summary(session_data, merge_keys)
                            # Add session identifier to variable names
                            session_summary['variable_name'] = session_summary['variable_name'].apply(
                                lambda x: f"{x}_session_{session}"
                            )
                            session_summaries.append(session_summary)

                    if session_summaries:
                        # Combine with main summary
                        all_summaries = [final_summary_df] + session_summaries
                        final_summary_df = pd.concat(all_summaries, ignore_index=True)

        # Create filenames
        timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
        if filename_prefix and filename_prefix.strip():
            prefix = filename_prefix.strip() + "_"
        else:
            prefix = ""

        filtering_filename = f"{prefix}filtering_report_{timestamp}.csv"
        summary_filename = f"{prefix}final_data_summary_{timestamp}.csv"

        # For now, download the filtering report first
        # Note: Dash can only trigger one download at a time, so we'll need a different approach
        # We could create a ZIP file or handle downloads sequentially

        # Create a combined download approach - we'll put both reports in a single ZIP
        import io
        import zipfile

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Add filtering report
            filtering_csv = filtering_report_df.to_csv(index=False)
            zip_file.writestr(filtering_filename, filtering_csv)

            # Add final data summary
            summary_csv = final_summary_df.to_csv(index=False)
            zip_file.writestr(summary_filename, summary_csv)

        zip_buffer.seek(0)
        zip_filename = f"{prefix}summary_reports_{timestamp}.zip"

        return dcc.send_bytes(zip_buffer.read(), zip_filename), False

    except Exception as e:
        logging.error(f"Error generating summary reports: {e}")
        return dash.no_update, dash.no_update


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
