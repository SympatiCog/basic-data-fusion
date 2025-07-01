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

# Import modular query callbacks to ensure they are registered
from query.callbacks import register_all_callbacks

dash.register_page(__name__, path='/', title='Query Data')

# Register modular callbacks with the current app
try:
    register_all_callbacks(dash.get_app())
except Exception as e:
    print(f"Note: Modular callback registration will occur when app is fully initialized: {e}")

# Note: We get fresh config in callbacks to pick up changes from settings

layout = dbc.Container([
    # Data status and import link section
    html.Div(id='query-data-status-section'),
    dbc.Row([
        dbc.Col([
            html.H3("Data Overview"),
            html.Div(id='merge-strategy-info'),
        ], width=6), # Left column for merge strategy info
        dbc.Col([
            html.Div([
                html.Img(
                    src="/assets/TBS_Logo_Wide_sm.png",
                    style={
                        'width': '100%',
                        'height': 'auto',
                        'maxWidth': '250px'
                    }
                )
            ], className="d-flex justify-content-center align-items-center", style={'height': '100%'})
        ], width=5) # Right column for logo
    ]),
    html.Br(), ## Standard spacing between sections
    dbc.Row([
        dbc.Col([
            html.H3("Live Participant Count"),
            html.Div(id='live-participant-count'), # Placeholder for participant count
        ], width=6),
        dbc.Col([
            html.H3("Query Management"),
            dbc.Row([
                dbc.Col([
                    dbc.Button(
                        [html.I(className="bi bi-upload me-2"), "Import Query Parameters"],
                        id="import-query-button",
                        color="info",
                        size="sm",
                        className="w-100"
                    )
                ], width=6),
                dbc.Col([
                    dbc.Button(
                        [html.I(className="bi bi-download me-2"), "Export Query Parameters"],
                        id="export-query-button",
                        color="success",
                        size="sm",
                        className="w-100"
                    )
                ], width=6),
            ], className="g-2"),
            html.Div([
                dbc.Button(
                    [
                        html.Span(id="current-query-display-text", children=""),
                        html.I(className="bi bi-chevron-down ms-2")
                    ],
                    id="current-query-dropdown-button",
                    color="light",
                    outline=True,
                    size="sm",
                    className="w-100 mt-2 text-start",
                    disabled=True
                )
            ], id="current-query-container", style={'display': 'none'})
        ], width=6)
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
            html.H3("Data Exports"),
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
                    ),
                    dbc.Checkbox(
                        id='consolidate-baseline-checkbox',
                        label='Consolidate baseline sessions (BAS1, BAS2, BAS3 → BAS)',
                        value=True,
                        style={'marginTop': '5px'}
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
    ]),

    # Export Query Parameters Modal
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Export Query Parameters")),
        dbc.ModalBody([
            html.P("Export your current query configuration to a TOML file that can be shared with others or imported later."),
            html.Hr(),
            html.H6("File Details"),
            dbc.Row([
                dbc.Col([
                    html.Label("Filename:", className="form-label"),
                    dbc.InputGroup([
                        dbc.Input(
                            id="export-filename-input",
                            placeholder="Enter filename",
                            value="",
                            type="text"
                        ),
                        dbc.InputGroupText(".toml")
                    ])
                ], width=12),
                dbc.Col([
                    html.Label("Notes (optional):", className="form-label mt-3"),
                    dbc.Textarea(
                        id="export-notes-input",
                        placeholder="Add notes about this query configuration...",
                        rows=3
                    )
                ], width=12)
            ]),
            html.Hr(),
            html.H6("Export Summary"),
            html.Div(id="export-summary-content"),
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel", id="cancel-export-button", color="secondary", className="me-2"),
            dbc.Button("Export", id="confirm-export-button", color="success")
        ])
    ], id="export-query-modal", is_open=False, size="lg"),

    # Import Query Parameters Modal
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Import Query Parameters")),
        dbc.ModalBody([
            html.P("Import previously exported query configuration from a TOML file. This will replace your current filter settings."),
            dcc.Upload(
                id='upload-query-params',
                children=html.Div([
                    html.I(className="bi bi-cloud-upload me-2"),
                    'Drag and Drop or ',
                    html.A('Select a TOML File')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=False,
                accept='.toml'
            ),
            html.Div(id='upload-status'),
            html.Hr(),
            html.Div(id='import-preview-content', style={'display': 'none'}),
            html.Div(id='import-validation-results', style={'display': 'none'})
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel", id="cancel-import-button", color="secondary", className="me-2"),
            dbc.Button("Import", id="confirm-import-button", color="primary", disabled=True)
        ])
    ], id="import-query-modal", is_open=False, size="lg"),

    # Query Details Modal
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Query Details")),
        dbc.ModalBody([
            html.Div(id='query-details-content')
        ]),
        dbc.ModalFooter([
            dbc.Button("Close", id="close-query-details-button", color="secondary")
        ])
    ], id="query-details-modal", is_open=False, size="lg")
], fluid=True)





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


def convert_phenotypic_to_behavioral_filters(phenotypic_filters_state):
    """Convert phenotypic filters to behavioral filters format for query generation."""
    if not phenotypic_filters_state or not phenotypic_filters_state.get('filters'):
        logging.info("No phenotypic filters to convert")
        return []

    behavioral_filters = []
    logging.info(f"Converting {len(phenotypic_filters_state['filters'])} phenotypic filters")

    for filter_data in phenotypic_filters_state['filters']:
        if not filter_data.get('enabled'):
            logging.debug(f"Skipping disabled filter: {filter_data.get('table', 'unknown')}.{filter_data.get('column', 'unknown')}")
            continue

        if filter_data.get('table') and filter_data.get('column') and filter_data.get('filter_type'):
            # Map to the format expected by secure query functions
            behavioral_filter = {
                'table': filter_data['table'],
                'column': filter_data['column'],
                'type': filter_data['filter_type']  # Use 'type' not 'filter_type'
            }

            if filter_data['filter_type'] == 'numeric':
                # For numeric filters, use 'value' as [min, max] tuple
                min_val = filter_data.get('min_val')
                max_val = filter_data.get('max_val')
                if min_val is not None and max_val is not None:
                    behavioral_filter['value'] = [min_val, max_val]
                    behavioral_filter['type'] = 'range'  # Secure query expects 'range' not 'numeric'
                    logging.info(f"Added numeric filter: {filter_data['table']}.{filter_data['column']} BETWEEN {min_val} AND {max_val}")
            elif filter_data['filter_type'] == 'categorical':
                # For categorical filters, use 'value' directly
                selected_values = filter_data.get('selected_values', [])
                if selected_values:
                    behavioral_filter['value'] = selected_values
                    logging.info(f"Added categorical filter: {filter_data['table']}.{filter_data['column']} IN {selected_values}")

            # Only add the filter if it has a valid value
            if 'value' in behavioral_filter:
                behavioral_filters.append(behavioral_filter)
        else:
            logging.warning(f"Incomplete filter data: table={filter_data.get('table')}, column={filter_data.get('column')}, type={filter_data.get('filter_type')}")

    logging.info(f"Converted to {len(behavioral_filters)} behavioral filters")
    return behavioral_filters










# Note: update_live_participant_count() moved to query/callbacks/filters.py
# (Large function - 100+ lines - removed to avoid duplicate callback output)
"""
Original function signature:
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
def update_live_participant_count_REMOVED(...):
    # Function body removed - see query/callbacks/filters.py
    pass
"""




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

# Callback for Data Generation
@callback(
    [Output('data-preview-area', 'children'),
     Output('merged-dataframe-store', 'data'),
     Output('data-processing-loading-output', 'children')], # Store for profiling page
    Input('generate-data-button', 'n_clicks'),
    [State('age-slider', 'value'),
     State('study-site-store', 'data'),
     State('session-selection-store', 'data'),
     State('phenotypic-filters-store', 'data'),
     State('selected-columns-per-table-store', 'data'),
     State('enwiden-data-checkbox', 'value'), # Boolean value (True when checked, False when unchecked)
     State('consolidate-baseline-checkbox', 'value'), # Boolean value for baseline consolidation
     State('merge-keys-store', 'data'),
     State('available-tables-store', 'data'), # Needed for tables_to_join logic
     State('table-multiselect', 'value')] # Explicitly selected tables for export
)
def handle_generate_data(
    n_clicks,
    age_range,
    rockland_substudy_values, session_filter_values,
    phenotypic_filters_state, selected_columns_per_table,
    enwiden_checkbox_value, consolidate_baseline_value, merge_keys_dict, available_tables, tables_selected_for_export
):
    if n_clicks == 0 or not merge_keys_dict:
        return dbc.Alert("Click 'Generate Merged Data' after selecting filters and columns.", color="info"), no_update, ""

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

        # Use secure query generation instead of deprecated functions
        from query.query_factory import QueryMode, get_query_factory

        # Create query factory with secure mode
        query_factory = get_query_factory(mode=QueryMode.SECURE)

        # If we need to enwiden longitudinal data, ensure session column is included
        modified_selected_columns = query_selected_columns.copy()
        if enwiden_checkbox_value and merge_keys.is_longitudinal and merge_keys.session_id:
            # Add session column to demographics table selection
            demographics_table = current_config.get_demographics_table_name()
            if demographics_table not in modified_selected_columns:
                modified_selected_columns[demographics_table] = []
            if merge_keys.session_id not in modified_selected_columns[demographics_table]:
                modified_selected_columns[demographics_table].append(merge_keys.session_id)

        base_query, params = query_factory.get_base_query_logic(
            current_config, merge_keys, demographic_filters, behavioral_filters, list(tables_for_query)
        )
        data_query, data_params = query_factory.get_data_query(
            base_query, params, list(tables_for_query), modified_selected_columns
        )

        if not data_query:
            return dbc.Alert("Could not generate data query.", color="warning"), None, ""
        # Use cached database connection for improved performance
        con = get_db_connection()
        # temp fix: coordinate file access with pandas operations
        with _file_access_lock:
            result_df = con.execute(data_query, data_params).fetchdf()

        original_row_count = len(result_df)

        if enwiden_checkbox_value and merge_keys.is_longitudinal:
            result_df = enwiden_longitudinal_data(result_df, merge_keys, consolidate_baseline_value)
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
                        dbc.Button("Download CSV", id="download-custom-csv-button", color="primary", className="mt-2")
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
    [Input('confirm-download-button', 'n_clicks')],
    [State('merged-dataframe-store', 'data'),
     State('table-multiselect', 'value'),
     State('enwiden-data-checkbox', 'value'),
     State('custom-filename-input', 'value')],
    prevent_initial_call=True
)
def download_csv_data(custom_clicks, stored_data, selected_tables, is_enwidened, custom_filename):
    # Only proceed if we have actual button clicks and data
    if not stored_data:
        return dash.no_update

    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    # Check if any button was actually clicked (not just initialized)
    if (custom_clicks is None or custom_clicks == 0):
        return dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    # Additional safety check - only proceed if a download button was clicked
    if button_id not in ['confirm-download-button']:
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
    if button_id == 'confirm-download-button':
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
     State('study-site-store', 'data'),
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
