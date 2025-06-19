import dash
from dash import html, dcc, callback, Input, Output, State, dash_table, no_update
import dash_bootstrap_components as dbc
import base64 # For decoding file contents
import io # For converting bytes to file-like object for pandas
import json # For JSON parsing
import logging
import duckdb
import pandas as pd
from datetime import datetime

# Assuming utils.py is in the same directory or accessible in PYTHONPATH
from utils import (
    Config,
    MergeKeys,
    validate_csv_file,
    save_uploaded_files_to_data_dir,
    get_table_info,
    detect_rs1_format,
    detect_rockland_format,
    is_numeric_column,
    generate_base_query_logic,
    generate_count_query,
    generate_data_query,
    enwiden_longitudinal_data
)

dash.register_page(__name__, path='/', title='Query Data')

# Initialize config instance
config = Config() # Loads from or creates config.toml

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Upload CSV Files"),
            dcc.Upload(
                id='upload-data',
                children=html.Div([
                    'Drag and Drop or ',
                    html.A('Select Files')
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
                multiple=True # Allow multiple files to be uploaded
            ),
            html.Div(id='upload-status-container'), # Container for collapsible upload messages
            dcc.Store(id='upload-trigger-store'), # To trigger updates after successful uploads
# All persistent stores are now defined in the main app layout for cross-page access
            dcc.Store(id='rs1-checkbox-ids-store'), # To store IDs of dynamically generated RS1 checkboxes
            dash_table.DataTable(id='data-preview-table', style_table={'display': 'none'})
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            html.H3("Merge Strategy"),
            html.Div(id='merge-strategy-info'),
        ], width=12)
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
            dbc.Card(dbc.CardBody([
                html.H4("Demographic Filters", className="card-title"),
                dbc.Row([
                    dbc.Col(html.Div([
                        html.Label("Age Range:"),
                        dcc.RangeSlider(id='age-slider', disabled=True, allowCross=False, step=1, tooltip={"placement": "bottom", "always_visible": True}),
                        html.Div(id='age-slider-info') # To show min/max or if disabled
                    ]), md=6),
                    dbc.Col(html.Div([
                        html.Label("Sex:"),
                        dcc.Dropdown(id='sex-dropdown', multi=True, disabled=True, placeholder="Select sex...")
                    ]), md=6),
                ]),
                html.Div(id='dynamic-demo-filters-placeholder', style={'marginTop': '20px'}), # For RS1, Rockland, Sessions
            ]), style={'marginTop': '20px'}),
            
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
                            size="sm"
                        )
                    ], width="auto")
                ], className="mb-3"),
                html.Div(id="phenotypic-filters-list")
            ]), style={'marginTop': '20px'}),

        ], md=6), # Left column for filters
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
        ], md=6) # Right column for selections
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
                html.Div(id='data-preview-area'),
                dcc.Download(id='download-dataframe-csv')
            ], id='results-container')
        ], width=12)
    ])
], fluid=True)


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
    if not demo_cols or 'age' not in demo_cols or not col_ranges:
        return 0, 100, [0, 100], {}, True, "Age filter disabled: 'age' column not found in demographics or ranges not available."

    # Use 'demo' as the alias for demographics table, consistent with get_table_alias() in utils.py
    age_col_key = "demo.age" # Construct the key for column_ranges

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
        # Fallback if 'age' column is in demo_cols but no range found (should ideally not happen if get_table_info is robust)
        return 0, 100, [config.DEFAULT_AGE_SELECTION[0], config.DEFAULT_AGE_SELECTION[1]], {}, True, "Age filter disabled: Range for 'age' column not found."

# Callback to update Sex Dropdown properties
@callback(
    [Output('sex-dropdown', 'options'),
     Output('sex-dropdown', 'value'),
     Output('sex-dropdown', 'disabled')],
    [Input('demographics-columns-store', 'data')],
    [State('sex-dropdown-state-store', 'data')]
)
def update_sex_dropdown(demo_cols, stored_sex_value):
    if not demo_cols or 'sex' not in demo_cols:
        return [], None, True

    # Options from config.SEX_OPTIONS
    options = [{'label': s, 'value': s} for s in config.SEX_OPTIONS]
    
    # Use stored value if available, otherwise use default
    value = stored_sex_value if stored_sex_value is not None else config.DEFAULT_SEX_SELECTION
    return options, value, False

# Callback to populate dynamic demographic filters (RS1, Rockland, Sessions)
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
    if not demo_cols:
        return html.P("Demographic information not yet available to populate dynamic filters.")

    children = []

    # RS1 Study Filters
    if detect_rs1_format(demo_cols, config): # utils.detect_rs1_format needs config
        children.append(html.H5("RS1 Study Selection", style={'marginTop': '15px'}))
        rs1_checkboxes = []
        for study_col, study_label in config.RS1_STUDY_LABELS.items():
            rs1_checkboxes.append(
                dbc.Checkbox(
                    id={'type': 'rs1-study-checkbox', 'index': study_col},
                    label=study_label,
                    value=study_col in config.DEFAULT_STUDY_SELECTION # Default checked based on config
                )
            )
        children.append(dbc.Form(rs1_checkboxes))

    # Rockland Substudy Filters
    if detect_rockland_format(demo_cols): # utils.detect_rockland_format
        children.append(html.H5("Substudy Selection", style={'marginTop': '15px'}))
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
            children.append(html.H5(f"{mk.session_id} Selection", style={'marginTop': '15px'}))
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
    
    triggered_id = ctx.triggered_id
    
    # Handle add filter
    if triggered_id == 'phenotypic-add-button':
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
    if triggered_id == 'phenotypic-clear-button':
        return {'filters': [], 'next_id': 1}
    
    # Handle remove filter
    if isinstance(triggered_id, dict) and triggered_id.get('type') == 'phenotypic-remove':
        filter_id = triggered_id['index']
        new_filters = [f for f in current_state['filters'] if f['id'] != filter_id]
        new_state = current_state.copy()
        new_state['filters'] = new_filters
        return new_state
    
    # Handle table/column/value changes
    if isinstance(triggered_id, dict) and triggered_id['type'].startswith('phenotypic-'):
        filter_id = triggered_id['index']
        component_type = triggered_id['type']
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
    [Input('phenotypic-filters-store', 'data')],
    [State('available-tables-store', 'data'),
     State('behavioral-columns-store', 'data'),
     State('demographics-columns-store', 'data'),
     State('column-dtypes-store', 'data'),
     State('column-ranges-store', 'data'),
     State('merge-keys-store', 'data')]
)
def render_phenotypic_filters(
    filters_state, available_tables, behavioral_columns, 
    demo_columns, column_dtypes, column_ranges, merge_keys_dict
):
    """Render the UI for all phenotypic filters."""
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










# Live Participant Count Callback
@callback(
    Output('live-participant-count', 'children'),
    [Input('age-slider', 'value'),
     Input('sex-dropdown', 'value'),
     Input({'type': 'rs1-study-checkbox', 'index': dash.ALL}, 'value'), # For RS1 studies
     Input('rockland-substudy-store', 'data'), # For Rockland substudies
     Input('session-selection-store', 'data'), # For session filtering
     Input('phenotypic-filters-store', 'data'), # For phenotypic filters
     # Data stores needed for query generation
     Input('merge-keys-store', 'data'),
     Input('available-tables-store', 'data')]
)
def update_live_participant_count(
    age_range, selected_sex,
    rs1_study_values, # RS1 studies from dynamic checkboxes
    rockland_substudy_values, # Rockland substudies from store
    session_values, # Session values from store
    phenotypic_filters_state, # Phenotypic filters state
    merge_keys_dict, available_tables
):
    ctx = dash.callback_context
    if not ctx.triggered and not merge_keys_dict : # Don't run on initial load if no data yet
        return dbc.Alert("Upload data and select filters to see participant count.", color="info")

    if not merge_keys_dict:
        return dbc.Alert("Merge strategy not determined. Cannot calculate count.", color="warning")

    current_config = config # Use global config instance
    merge_keys = MergeKeys.from_dict(merge_keys_dict)

    demographic_filters = {}
    if age_range:
        demographic_filters['age_range'] = age_range
    if selected_sex:
        demographic_filters['sex'] = selected_sex

    # Collect RS1 study selections
    # Assuming rs1_study_values corresponds to the 'value' (boolean) of dbc.Checkbox
    # and ctx.inputs_list[2] gives us the list of component states that includes their IDs.
    # This part is a bit complex due to dynamic component IDs.
    # A simpler way if IDs are predictable:
    selected_rs1_studies = []
    if ctx.inputs_list and len(ctx.inputs_list) > 2:
        rs1_input_states = ctx.inputs_list[2] # List of {'id': {'index': 'is_DS', 'type': 'rs1-study-checkbox'}, 'value': True/False}
        for i, state in enumerate(rs1_input_states):
            # The actual value comes from rs1_study_values[i]
            # The id comes from state['id']['index']
            if rs1_study_values[i]: # if checkbox is checked
                 selected_rs1_studies.append(state['id']['index'])
    if selected_rs1_studies:
        demographic_filters['studies'] = selected_rs1_studies

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
            # Establish a new connection for each callback execution for safety in threaded Dash environment
            # For very high frequency updates, a shared connection with appropriate locking might be considered.
            with duckdb.connect(database=':memory:', read_only=False) as con:
                count_result = con.execute(count_query, count_params).fetchone()

            if count_result and count_result[0] is not None:
                return dbc.Alert(f"Matching Rows: {count_result[0]}", color="success")
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
    [Output('upload-status-container', 'children'),
     Output('upload-trigger-store', 'data')],
    [Input('upload-data', 'contents')],
    [State('upload-data', 'filename'),
     State('upload-data', 'last_modified')],
    prevent_initial_call=True
)
def create_collapsible_upload_messages(messages, num_files=0):
    """Create a collapsible component for upload messages"""
    if not messages:
        return html.Div()
    
    # Count different message types
    validation_msgs = [msg for msg in messages if hasattr(msg, 'children') and 'is valid' in str(msg.children)]
    save_msgs = [msg for msg in messages if hasattr(msg, 'children') and ('Saved' in str(msg.children) or 'Error' in str(msg.children))]
    error_msgs = [msg for msg in messages if hasattr(msg, 'style') and msg.style.get('color') == 'red']
    
    # Summary line
    summary_text = f"Processed {num_files} files"
    if error_msgs:
        summary_text += f" ({len(error_msgs)} errors)"
    
    summary_color = "danger" if error_msgs else "success"
    
    return dbc.Card([
        dbc.CardHeader([
            html.H5([
                html.I(className="fas fa-upload me-2"),
                summary_text,
                dbc.Button(
                    [html.I(className="fas fa-chevron-down")],
                    id="upload-messages-toggle",
                    color="link",
                    size="sm",
                    className="float-end p-0",
                    style={"border": "none"}
                ),
                dbc.Button(
                    [html.I(className="fas fa-times")],
                    id="upload-messages-dismiss",
                    color="link",
                    size="sm", 
                    className="float-end p-0 me-2",
                    style={"border": "none", "color": "red"}
                )
            ], className="mb-0")
        ], className=f"bg-{summary_color} text-white"),
        dbc.Collapse([
            dbc.CardBody([
                html.Div(messages, style={"max-height": "300px", "overflow-y": "auto"})
            ])
        ], id="upload-messages-collapse", is_open=False)
    ], className="mb-3")

def handle_file_uploads(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is None:
        return html.Div("No files uploaded."), dash.no_update

    messages = []
    all_files_valid = True
    saved_file_names = []
    file_byte_contents = []

    if list_of_names:
        for i, (c, n, d) in enumerate(zip(list_of_contents, list_of_names, list_of_dates)):
            try:
                content_type, content_string = c.split(',')
                decoded = base64.b64decode(content_string)

                # Validate each file
                validation_errors, df = validate_csv_file(decoded, n) # Using the new signature

                if validation_errors:
                    all_files_valid = False
                    for error in validation_errors:
                        messages.append(html.Div(f"Error with {n}: {error}", style={'color': 'red'}))
                else:
                    messages.append(html.Div(f"File {n} is valid.", style={'color': 'green'}))
                    file_byte_contents.append(decoded)
                    saved_file_names.append(n) # Keep track of names for saving

            except Exception as e:
                all_files_valid = False
                messages.append(html.Div(f"Error processing file {n}: {str(e)}", style={'color': 'red'}))
                continue # Skip to next file if this one errors out during processing

    if all_files_valid and file_byte_contents:
        # Save valid files
        # utils.save_uploaded_files_to_data_dir expects lists of contents and names
        success_msgs, error_msgs = save_uploaded_files_to_data_dir(file_byte_contents, saved_file_names, config.DATA_DIR)
        for msg in success_msgs:
            messages.append(html.Div(msg, style={'color': 'green'}))
        for err_msg in error_msgs:
            messages.append(html.Div(err_msg, style={'color': 'red'}))

        num_files = len(list_of_names) if list_of_names else 0
        collapsible_messages = create_collapsible_upload_messages(messages, num_files)
        
        if not error_msgs: # Only trigger if all saves were successful
             # Trigger downstream updates by changing the store's data
            return collapsible_messages, {'timestamp': datetime.now().isoformat()}
        else:
            return collapsible_messages, dash.no_update

    elif not file_byte_contents: # No valid files to save
        messages.append(html.Div("No valid files were processed to save.", style={'color': 'orange'}))
        num_files = len(list_of_names) if list_of_names else 0
        collapsible_messages = create_collapsible_upload_messages(messages, num_files)
        return collapsible_messages, dash.no_update
    else: # Some files were invalid
        num_files = len(list_of_names) if list_of_names else 0
        collapsible_messages = create_collapsible_upload_messages(messages, num_files)
        return collapsible_messages, dash.no_update


# Callbacks for collapsible upload messages
@callback(
    Output('upload-messages-collapse', 'is_open'),
    [Input('upload-messages-toggle', 'n_clicks')],
    [State('upload-messages-collapse', 'is_open')],
    prevent_initial_call=True
)
def toggle_upload_messages(n_clicks, is_open):
    if n_clicks:
        return not is_open
    return is_open

@callback(
    Output('upload-status-container', 'children', allow_duplicate=True),
    [Input('upload-messages-dismiss', 'n_clicks')],
    prevent_initial_call=True
)
def dismiss_upload_messages(n_clicks):
    if n_clicks:
        return html.Div()
    return dash.no_update


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
    [Input('upload-trigger-store', 'data'), # Triggered by successful file upload
     Input('upload-data', 'contents')] # Also trigger on initial page load if files are "already there" (less common for upload)
)
def load_initial_data_info(trigger_data, _): # trigger_data from upload-trigger-store, _ for upload-data contents (initial)
    # We need to use the global config instance that was loaded/created when query.py was imported.
    # Or, if config can change dynamically (e.g. via UI), it needs to be managed in a dcc.Store

    # Re-fetch config if it could have changed (e.g., if settings were editable in another part of the app)
    # For now, assume config loaded at app start is sufficient, or re-initialize.
    current_config = config # Use global config instance

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
        info_messages.append(html.H5("Dataset Preparation Actions:", style={'marginTop': '10px'}))
        for action in actions_taken:
            info_messages.append(html.P(action))

    merge_strategy_display = [html.H5("Merge Strategy:", style={'marginTop': '10px'})]
    if merge_keys_dict:
        mk = MergeKeys.from_dict(merge_keys_dict)
        if mk.is_longitudinal:
            merge_strategy_display.append(html.P(f"Detected: Longitudinal data."))
            merge_strategy_display.append(html.P(f"Primary ID: {mk.primary_id}"))
            merge_strategy_display.append(html.P(f"Session ID: {mk.session_id}"))
            merge_strategy_display.append(html.P(f"Composite ID (for merge): {mk.composite_id}"))
        else:
            merge_strategy_display.append(html.P(f"Detected: Cross-sectional data."))
            merge_strategy_display.append(html.P(f"Primary ID (for merge): {mk.primary_id}"))
    else:
        merge_strategy_display.append(html.P("Merge strategy not determined yet. Upload data or check configuration."))

    # Combine messages from get_table_info with other status messages
    # Place dataset preparation actions and merge strategy info after general messages.
    status_display_content = info_messages + merge_strategy_display
    status_display = html.Div(status_display_content)


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
     Output('merged-dataframe-store', 'data')], # Store for profiling page
    Input('generate-data-button', 'n_clicks'),
    [State('age-slider', 'value'),
     State('sex-dropdown', 'value'),
     State({'type': 'rs1-study-checkbox', 'index': dash.ALL}, 'value'),
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
    age_range, selected_sex, rs1_study_values,
    rockland_substudy_values, session_filter_values,
    phenotypic_filters_state, selected_columns_per_table,
    enwiden_checkbox_value, merge_keys_dict, available_tables, tables_selected_for_export
):
    if n_clicks == 0 or not merge_keys_dict:
        return dbc.Alert("Click 'Generate Merged Data' after selecting filters and columns.", color="info"), None

    current_config = config # Use global config instance
    merge_keys = MergeKeys.from_dict(merge_keys_dict)

    # --- Collect Demographic Filters ---
    demographic_filters = {}
    if age_range: demographic_filters['age_range'] = age_range
    if selected_sex: demographic_filters['sex'] = selected_sex

    selected_rs1_studies = []
    # Accessing rs1_study_values directly as it's a list of booleans from the checkboxes
    # Need to map these back to the study column names using the order from config.RS1_STUDY_LABELS
    # This assumes the order of checkboxes matches RS1_STUDY_LABELS.items()
    # A more robust way would be to get the component IDs if they were static or use ctx.inputs_list carefully.
    # For dynamically generated checkboxes via a callback, their state needs to be carefully managed.
    # Let's assume the `update_dynamic_demographic_filters` callback ensures IDs are {'type': 'rs1-study-checkbox', 'index': study_col}
    # and rs1_study_values is a list of their `value` properties (True/False)

    # Simplified: If rs1_study_values is available and True, get its corresponding ID.
    # This requires that the Input for rs1_study_values provides enough context or
    # that we fetch rs1_study_ids from another source (e.g. a hidden store updated by dynamic filter callback)
    # For now, we'll rely on the direct values if they are simple lists of selected items.
    # If rs1_study_values is a list of booleans, we need to associate them with the study names.
    # This part is tricky with dash.ALL for dynamically generated checkboxes if not handled carefully.
    # A common pattern is to have the callback that generates these also store their IDs/relevant info.
    # Given the current structure, we assume rs1_study_values are the *values* of the checked items,
    # and we need their corresponding *IDs* (study column names).
    # This part of the logic might need refinement based on how `{'type': 'rs1-study-checkbox', 'index': dash.ALL}` actually passes data.
    # It typically passes a list of the `property` specified (here, 'value').
    # We need the 'index' part of the ID for those that are True.
    # This requires iterating through `dash.callback_context.inputs_list` or `triggered` if it's an Input.
    # For now, let's assume `rs1_study_values` contains the `study_col` if checked.
    # This would be true if the `value` property of dbc.Checkbox was set to `study_col` itself.
    # Rechecking the dynamic filter callback: value is set to `study_col in config.DEFAULT_STUDY_SELECTION`
    # This means rs1_study_values is a list of booleans.
    # We need the corresponding 'index' from the ID for those that are True.
     # This is now handled by taking rs1_checkbox_ids_store as State.

    # Correctly accessing RS1 study selections:
    # For now, get the study names from config based on checkbox values
    if rs1_study_values:
        study_cols = list(config.RS1_STUDY_LABELS.keys())
        selected_rs1_studies = [study_cols[i] for i, checked in enumerate(rs1_study_values) if i < len(study_cols) and checked]
        if selected_rs1_studies:
            demographic_filters['studies'] = selected_rs1_studies

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
        base_query, params = generate_base_query_logic(
            current_config, merge_keys, demographic_filters, behavioral_filters, list(tables_for_query)
        )
        data_query, data_params = generate_data_query(
            base_query, params, list(tables_for_query), query_selected_columns
        )

        if not data_query:
            return dbc.Alert("Could not generate data query.", color="warning"), None

        with duckdb.connect(database=':memory:', read_only=False) as con:
            result_df = con.execute(data_query, data_params).fetchdf()

        original_row_count = len(result_df)

        if enwiden_checkbox_value and merge_keys.is_longitudinal:
            result_df = enwiden_longitudinal_data(result_df, merge_keys)
            enwiden_info = f" (enwidened from {original_row_count} rows to {len(result_df)} rows)"
        else:
            enwiden_info = ""

        if result_df.empty:
            return dbc.Alert("No data found for the selected criteria.", color="info"), None

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
                dbc.Alert(f"Query successful. Displaying first {min(len(result_df), current_config.MAX_DISPLAY_ROWS)} of {len(result_df)} total rows{enwiden_info}.", color="success"),
                preview_table,
                html.Hr(),
                dbc.Button("Download CSV", id="download-csv-button", color="success", className="mt-2")
            ]),
            result_df.to_dict('records') # Store full data for profiling page (consider size limits)
        )

    except Exception as e:
        logging.error(f"Error during data generation: {e}")
        logging.error(f"Query attempted: {data_query if 'data_query' in locals() else 'N/A'}")
        return dbc.Alert(f"Error generating data: {str(e)}", color="danger"), None


# Callback for CSV Download Button
@callback(
    Output('download-dataframe-csv', 'data'),
    Input('download-csv-button', 'n_clicks'),
    [State('merged-dataframe-store', 'data'),
     State('age-slider', 'value')],
    prevent_initial_call=True
)
def download_csv_data(n_clicks, stored_data, age_range):
    if n_clicks is None or not stored_data:
        return dash.no_update
    
    # Convert stored data back to DataFrame
    df = pd.DataFrame(stored_data)
    
    # Create a filename
    filename_parts = ["merged_data"]
    if age_range: 
        filename_parts.append(f"age{age_range[0]}-{age_range[1]}")
    filename = "_".join(filename_parts) + ".csv"
    
    return dcc.send_data_frame(df.to_csv, filename, index=False)


# Unified callback to save all filter states for persistence across page navigation
@callback(
    [Output('age-slider-state-store', 'data'),
     Output('sex-dropdown-state-store', 'data'),
     Output('table-multiselect-state-store', 'data'),
     Output('enwiden-data-checkbox-state-store', 'data')],
    [Input('age-slider', 'value'),
     Input('sex-dropdown', 'value'),
     Input('table-multiselect', 'value'),
     Input('enwiden-data-checkbox', 'value')]
)
def save_all_filter_states(age_value, sex_value, table_value, enwiden_value):
    return age_value, sex_value, table_value, enwiden_value
