"""
Data Plotting Page - Interactive visualization and exploration of research datasets
"""

import base64
import io
import logging
from datetime import datetime

import dash
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, State, callback, dcc, html, no_update
from scipy import stats

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

dash.register_page(__name__, path='/plotting', title='Plot Data')

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Interactive Data Plotting"),
            dbc.Card(dbc.CardBody([
                html.H4("Data Source", className="card-title"),
                html.Div(id='plotting-data-source-status', children="No data loaded."),
                dcc.Upload(
                    id='upload-plotting-csv',
                    children=html.Div(['Drag and Drop or ', html.A('Select CSV File to Plot')]),
                    style={
                        'width': '100%', 'height': '60px', 'lineHeight': '60px',
                        'borderWidth': '1px', 'borderStyle': 'dashed', 'borderRadius': '5px',
                        'textAlign': 'center', 'margin': '10px 0'
                    },
                    multiple=False
                ),
            ]))
        ], width=12)
    ]),

    dbc.Row([
        # Left Panel - Plot Configuration
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.H4("Plot Configuration", className="card-title"),

                # Plot Type Selector
                html.Div([
                    html.Label("Plot Type:", className="fw-bold mb-2"),
                    dcc.Dropdown(
                        id='plot-type-dropdown',
                        options=[
                            {'label': 'Scatter Plot', 'value': 'scatter'},
                            {'label': 'Histogram', 'value': 'histogram'},
                            {'label': 'Box Plot', 'value': 'box'},
                            {'label': 'Violin Plot', 'value': 'violin'},
                            {'label': 'Density Heatmap', 'value': 'density_heatmap'}
                        ],
                        value='scatter',
                        clearable=False,
                        className="mb-3"
                    )
                ]),

                # Dynamic Plot Property Mappers
                html.Div(id='plot-property-mappers'),

                # Dropdown Controls Section
                html.Div(id='dropdown-controls-section', className="mt-3"),

                # Update Plot Button
                dbc.Button(
                    "Update Plot",
                    id='update-plot-button',
                    n_clicks=0,
                    className="mt-3 w-100",
                    color="primary",
                    disabled=True
                ),
                
                # Always-present analysis checkboxes (shown/hidden based on plot type)
                html.Hr(className="mt-4"),
                html.Div([
                    html.Label("Analysis Options:", className="fw-bold mb-2"),
                    
                    # OLS options for scatter plots
                    html.Div([
                        dbc.Checklist(
                            id='ols-analysis-checkboxes',
                            options=[
                                {'label': 'Show OLS Trendline', 'value': 'show_trendline'},
                                {'label': 'Show Statistical Summary', 'value': 'show_summary'}
                            ],
                            value=[],
                            className="mb-2"
                        )
                    ], id='ols-checkbox-container', style={'display': 'none'}),
                    
                    # Histogram options  
                    html.Div([
                        dbc.Checklist(
                            id='histogram-analysis-checkboxes',
                            options=[
                                {'label': 'Show Statistical Summary', 'value': 'show_summary'},
                                {'label': 'Show Mean Line', 'value': 'show_mean'},
                                {'label': 'Show Median Line', 'value': 'show_median'},
                                {'label': 'Show KDE Curve', 'value': 'show_kde'}
                            ],
                            value=[],
                            className="mb-2"
                        )
                    ], id='histogram-checkbox-container', style={'display': 'none'}),
                    
                    # Box/Violin plot options (for future)
                    html.Div([
                        dbc.Checklist(
                            id='boxviolin-analysis-checkboxes',
                            options=[
                                {'label': 'Show Statistical Summary', 'value': 'show_summary'},
                                {'label': 'Show ANOVA Results', 'value': 'show_anova'}
                            ],
                            value=[],
                            className="mb-2"
                        )
                    ], id='boxviolin-checkbox-container', style={'display': 'none'})
                ], id='analysis-options-section'),
            ]))
        ], width=3),

        # Main Content - Plot Display
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.H4("Data Visualization", className="card-title"),
                dcc.Loading(
                    id="loading-plot",
                    children=[
                        dcc.Graph(
                            id='main-plot',
                            style={'height': '600px'},
                            config={
                                'modeBarButtonsToAdd': ['select2d', 'lasso2d'],
                                'displayModeBar': True,
                                'displaylogo': False,
                                'toImageButtonOptions': {
                                    'format': 'png',
                                    'filename': 'data_plot',
                                    'height': 600,
                                    'width': 800,
                                    'scale': 2
                                }
                            }
                        )
                    ],
                    type="default"
                ),
                # OLS Regression Summary (only visible for scatter plots with trendline)
                html.Div(id='ols-summary-container', className="mt-3")
            ]))
        ], width=9)
    ], className="mt-3"),

    dbc.Row([
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.H4("Selected Data Points", className="card-title"),
                html.P("Use box select or lasso select on the plot above to filter the data table below.",
                       className="text-muted"),
                html.Div(id='selected-data-info', className="mb-2"),
                dcc.Loading(
                    id="loading-data-table",
                    children=[
                        html.Div(id='selected-data-table-container')
                    ],
                    type="default"
                ),
                html.Div([
                    dbc.Button(
                        "Export Selected Data to CSV",
                        id='export-selected-data-button',
                        n_clicks=0,
                        className="mt-2",
                        color="success",
                        disabled=True
                    )
                ], className="d-flex justify-content-end")
            ]))
        ], width=12)
    ], className="mt-3"),

    # Export Filename Modal
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Export Selected Data")),
        dbc.ModalBody([
            html.P("Enter a custom filename for your exported data:"),
            dbc.Input(
                id='export-filename-input',
                type='text',
                placeholder='Enter filename (without .csv extension)',
                value='',
                className='mb-3'
            ),
            html.Small("The .csv extension will be added automatically.", className="text-muted")
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel", id="export-cancel-button", className="me-2", color="secondary"),
            dbc.Button("Export", id="export-confirm-button", color="success")
        ])
    ], id="export-filename-modal", is_open=False),

    # Stores
    dcc.Store(id='plotting-df-store'),  # Stores the dataframe for plotting
    dcc.Store(id='filtered-plot-df-store'),  # Stores the filtered dataframe actually used for plotting
    dcc.Store(id='selected-points-store'),  # Stores selected points from plot
    dcc.Store(id='plot-config-store'),  # Stores current plot configuration
    dcc.Store(id='ols-results-store'),  # Stores OLS regression results
    dcc.Store(id='histogram-stats-store'),  # Stores histogram statistical analysis results

    # Download component (invisible)
    dcc.Download(id='download-selected-data'),

    # Hidden dropdown components that are always present to avoid callback errors
    html.Div([
        dcc.Dropdown(id='x-axis-dropdown', style={'display': 'none'}),
        dcc.Dropdown(id='y-axis-dropdown', style={'display': 'none'}),
        dcc.Dropdown(id='color-dropdown', style={'display': 'none'}),
        dcc.Dropdown(id='size-dropdown', style={'display': 'none'}),
        dcc.Dropdown(id='facet-dropdown', style={'display': 'none'}),
        dcc.Dropdown(id='variable-dropdown', style={'display': 'none'}),
        dcc.Dropdown(id='categorical-axis-dropdown', style={'display': 'none'}),
        dcc.Dropdown(id='value-axis-dropdown', style={'display': 'none'}),
        dbc.Checklist(id='ols-trendline-checkbox-hidden', options=[], value=[], style={'display': 'none'}),
        dbc.Checklist(id='histogram-stats-checkbox-hidden', options=[], value=[], style={'display': 'none'})
    ], style={'display': 'none'})
], fluid=True)

# --- Callbacks ---

# Callback to Load Data from Query Page or Upload
@callback(
    [Output('plotting-df-store', 'data'),
     Output('plotting-data-source-status', 'children')],
    [Input('merged-dataframe-store', 'data'),
     Input('upload-plotting-csv', 'contents')],
    [State('upload-plotting-csv', 'filename')],
    prevent_initial_call=False
)
def load_data_for_plotting(merged_data, upload_contents, upload_filename):
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

    # Debug logging
    logging.info(f"Plotting callback triggered by: {triggered_id}, merged_data available: {merged_data is not None}")

    if triggered_id == 'upload-plotting-csv' and upload_contents:
        try:
            content_type, content_string = upload_contents.split(',')
            decoded = base64.b64decode(content_string)
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
            status_message = f"Data loaded from uploaded file: {upload_filename} ({len(df)} rows × {len(df.columns)} columns)"
            logging.info(status_message)
            return df.to_dict('records'), status_message
        except Exception as e:
            error_message = f"Error processing uploaded CSV: {str(e)}"
            logging.error(error_message)
            return None, dbc.Alert(error_message, color="danger")

    elif merged_data:  # Check for merged data regardless of trigger
        try:
            # Handle new metadata structure from query page
            if isinstance(merged_data, dict) and 'full_data' in merged_data:
                full_data = merged_data['full_data']
                column_count = merged_data.get('column_count', 0)
                df = pd.DataFrame(full_data)
                status_message = f"Data loaded from Query Page ({len(df)} rows × {column_count} columns)"
            else:
                # Fallback for old format (if any)
                df = pd.DataFrame(merged_data)
                status_message = f"Data loaded from Query Page ({len(df)} rows × {len(df.columns)} columns)"

            logging.info(status_message)
            return df.to_dict('records'), status_message
        except Exception as e:
            error_message = f"Error processing data from Query Page: {str(e)}"
            logging.error(error_message)
            return None, dbc.Alert(error_message, color="danger")

    # Check for existing data on page load
    if upload_contents:
        try:
            content_type, content_string = upload_contents.split(',')
            decoded = base64.b64decode(content_string)
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
            status_message = f"Data loaded from uploaded file: {upload_filename} ({len(df)} rows × {len(df.columns)} columns)"
            logging.info(status_message)
            return df.to_dict('records'), status_message
        except Exception as e:
            logging.error(f"Error processing uploaded CSV on refresh: {str(e)}")

    if merged_data:
        try:
            # Handle new metadata structure from query page (same logic as above)
            if isinstance(merged_data, dict) and 'full_data' in merged_data:
                full_data = merged_data['full_data']
                column_count = merged_data.get('column_count', 0)
                df = pd.DataFrame(full_data)
                status_message = f"Data loaded from Query Page ({len(df)} rows × {column_count} columns)"
            else:
                # Fallback for old format (if any)
                df = pd.DataFrame(merged_data)
                status_message = f"Data loaded from Query Page ({len(df)} rows × {len(df.columns)} columns)"

            logging.info(status_message)
            return df.to_dict('records'), status_message
        except Exception as e:
            logging.error(f"Error processing data from Query Page on refresh: {str(e)}")

    return None, html.Div([
        "No data available for plotting.",
        html.Br(),
        "• Generate data on the Query page and it will automatically appear here, or",
        html.Br(),
        "• Upload a CSV file above to plot it directly"
    ])

# Callback to Generate Dynamic Plot Property Mappers and update hidden dropdowns
@callback(
    [Output('plot-property-mappers', 'children'),
     Output('x-axis-dropdown', 'options'),
     Output('y-axis-dropdown', 'options'),
     Output('color-dropdown', 'options'),
     Output('size-dropdown', 'options'),
     Output('facet-dropdown', 'options'),
     Output('variable-dropdown', 'options'),
     Output('categorical-axis-dropdown', 'options'),
     Output('value-axis-dropdown', 'options')],
    [Input('plot-type-dropdown', 'value'),
     Input('plotting-df-store', 'data')]
)
def generate_plot_property_mappers(plot_type, df_data):
    if not df_data:
        empty_options = []
        return (html.Div("Load data to see plot configuration options.", className="text-muted"),
                empty_options, empty_options, empty_options, empty_options, empty_options,
                empty_options, empty_options, empty_options)

    df = pd.DataFrame(df_data)
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    categorical_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
    all_columns = df.columns.tolist()

    # Create column options
    numeric_options = [{'label': col, 'value': col} for col in numeric_columns]
    categorical_options = [{'label': col, 'value': col} for col in categorical_columns]
    all_options = [{'label': col, 'value': col} for col in all_columns]
    empty_options = []

    mappers = []

    if plot_type == 'scatter':
        mappers = [
            html.Div([
                html.Label("X-Axis:", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Y-Axis:", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Color (optional):", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Size (optional):", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Facet Column (optional):", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3")
        ]
        return (html.Div(mappers), numeric_options, numeric_options, all_options,
                numeric_options, categorical_options, empty_options, empty_options, empty_options)

    elif plot_type == 'histogram':
        mappers = [
            html.Div([
                html.Label("Variable:", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Color (optional):", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Facet Column (optional):", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3")
        ]
        return (html.Div(mappers), empty_options, empty_options, categorical_options,
                empty_options, categorical_options, numeric_options, empty_options, empty_options)

    elif plot_type in ['box', 'violin']:
        mappers = [
            html.Div([
                html.Label("Categorical Axis:", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Value Axis:", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Color (optional):", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Facet Column (optional):", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3")
        ]
        return (html.Div(mappers), empty_options, empty_options, categorical_options,
                empty_options, categorical_options, empty_options, categorical_options, numeric_options)

    elif plot_type == 'density_heatmap':
        mappers = [
            html.Div([
                html.Label("X-Axis:", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Y-Axis:", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3"),
            html.Div([
                html.Label("Facet Column (optional):", className="fw-bold"),
                html.P("Select from dropdown below", className="small text-muted")
            ], className="mb-3")
        ]
        return (html.Div(mappers), numeric_options, numeric_options, empty_options,
                empty_options, categorical_options, empty_options, empty_options, empty_options)

    return (html.Div("Select a plot type above"), empty_options, empty_options, empty_options,
            empty_options, empty_options, empty_options, empty_options, empty_options)

# Callback to populate the dropdown controls section with visible dropdowns
@callback(
    Output('dropdown-controls-section', 'children'),
    [Input('plot-type-dropdown', 'value'),
     Input('plotting-df-store', 'data')]
)
def populate_dropdown_controls(plot_type, df_data):
    if not df_data or not plot_type:
        return html.Div()

    df = pd.DataFrame(df_data)
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    categorical_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
    all_columns = df.columns.tolist()

    numeric_options = [{'label': col, 'value': col} for col in numeric_columns]
    categorical_options = [{'label': col, 'value': col} for col in categorical_columns]
    all_options = [{'label': col, 'value': col} for col in all_columns]

    controls = []

    if plot_type == 'scatter':
        controls = [
            html.Label("X-Axis:", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'x-axis'},
                        options=numeric_options, placeholder="Select X variable", className="mb-2"),
            html.Label("Y-Axis:", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'y-axis'},
                        options=numeric_options, placeholder="Select Y variable", className="mb-2"),
            html.Label("Color (optional):", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'color'},
                        options=all_options, placeholder="Select color variable", className="mb-2"),
            html.Label("Size (optional):", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'size'},
                        options=numeric_options, placeholder="Select size variable", className="mb-2"),
            html.Label("Facet (optional):", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'facet'},
                        options=categorical_options, placeholder="Select facet variable", className="mb-2"),
        ]
    elif plot_type == 'histogram':
        controls = [
            html.Label("Variable:", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'variable'},
                        options=numeric_options, placeholder="Select variable", className="mb-2"),
            html.Label("Color (optional):", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'color'},
                        options=categorical_options, placeholder="Select color variable", className="mb-2"),
            html.Label("Facet (optional):", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'facet'},
                        options=categorical_options, placeholder="Select facet variable", className="mb-2"),
        ]
    elif plot_type in ['box', 'violin']:
        controls = [
            html.Label("Categorical Axis:", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'categorical-axis'},
                        options=categorical_options, placeholder="Select categorical variable", className="mb-2"),
            html.Label("Value Axis:", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'value-axis'},
                        options=numeric_options, placeholder="Select value variable", className="mb-2"),
            html.Label("Color (optional):", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'color'},
                        options=categorical_options, placeholder="Select color variable", className="mb-2"),
            html.Label("Facet (optional):", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'facet'},
                        options=categorical_options, placeholder="Select facet variable", className="mb-2")
        ]
    elif plot_type == 'density_heatmap':
        controls = [
            html.Label("X-Axis:", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'x-axis'},
                        options=numeric_options, placeholder="Select X variable", className="mb-2"),
            html.Label("Y-Axis:", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'y-axis'},
                        options=numeric_options, placeholder="Select Y variable", className="mb-2"),
            html.Label("Facet (optional):", className="fw-bold"),
            dcc.Dropdown(id={'type': 'visible-dropdown', 'target': 'facet'},
                        options=categorical_options, placeholder="Select facet variable", className="mb-2")
        ]

    return html.Div(controls)

# Callback to Enable/Disable Update Plot Button - simplified to avoid referencing non-existent dropdowns
@callback(
    Output('update-plot-button', 'disabled'),
    [Input('plotting-df-store', 'data'),
     Input('plot-type-dropdown', 'value')],
    prevent_initial_call=True
)
def control_update_button(df_data, plot_type):
    if not df_data or not plot_type:
        return True

    # Enable button when data and plot type are available
    # Validation will happen in the plot generation callback
    return False

# Callback to sync visible dropdowns with hidden ones
@callback(
    [Output('x-axis-dropdown', 'value'),
     Output('y-axis-dropdown', 'value'),
     Output('color-dropdown', 'value'),
     Output('size-dropdown', 'value'),
     Output('facet-dropdown', 'value'),
     Output('variable-dropdown', 'value'),
     Output('categorical-axis-dropdown', 'value'),
     Output('value-axis-dropdown', 'value')],
    [Input({'type': 'visible-dropdown', 'target': dash.ALL}, 'value')],
    [State({'type': 'visible-dropdown', 'target': dash.ALL}, 'id')],
    prevent_initial_call=True
)
def sync_dropdown_values(dropdown_values, all_ids):
    # Initialize all dropdown values
    values = {
        'x-axis': None,
        'y-axis': None,
        'color': None,
        'size': None,
        'facet': None,
        'variable': None,
        'categorical-axis': None,
        'value-axis': None
    }

    # Update with values from visible dropdowns
    for i, dropdown_id in enumerate(all_ids):
        target = dropdown_id['target']
        if i < len(dropdown_values) and dropdown_values[i]:
            values[target] = dropdown_values[i]

    return (values['x-axis'], values['y-axis'], values['color'], values['size'],
            values['facet'], values['variable'], values['categorical-axis'], values['value-axis'])

# Callback to show/hide analysis checkboxes based on plot type
@callback(
    [Output('ols-checkbox-container', 'style'),
     Output('histogram-checkbox-container', 'style'),
     Output('boxviolin-checkbox-container', 'style')],
    [Input('plot-type-dropdown', 'value')]
)
def toggle_analysis_checkboxes(plot_type):
    # Default all hidden
    ols_style = {'display': 'none'}
    hist_style = {'display': 'none'}
    boxviolin_style = {'display': 'none'}
    
    # Show appropriate checkboxes based on plot type
    if plot_type == 'scatter':
        ols_style = {'display': 'block'}
    elif plot_type == 'histogram':
        hist_style = {'display': 'block'}
    elif plot_type in ['box', 'violin']:
        boxviolin_style = {'display': 'block'}
    
    return ols_style, hist_style, boxviolin_style

# Plot generation callback - pure plotting logic only
@callback(
    [Output('main-plot', 'figure'),
     Output('filtered-plot-df-store', 'data')],
    [Input('update-plot-button', 'n_clicks')],
    [State('plotting-df-store', 'data'),
     State('plot-type-dropdown', 'value'),
     State('x-axis-dropdown', 'value'),
     State('y-axis-dropdown', 'value'),
     State('color-dropdown', 'value'),
     State('size-dropdown', 'value'),
     State('facet-dropdown', 'value'),
     State('variable-dropdown', 'value'),
     State('categorical-axis-dropdown', 'value'),
     State('value-axis-dropdown', 'value')],
    prevent_initial_call=True
)
def generate_plot(n_clicks, df_data, plot_type, x_axis, y_axis, color, size, facet, variable, categorical_axis, value_axis):
    if not df_data or n_clicks == 0:
        return {}, None

    # Create a simple error message plot for missing data
    def create_error_plot(message):
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False,
            font={'size': 16, 'color': "orange"}
        )
        fig.update_layout(
            title="Configuration Required",
            showlegend=False,
            xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
            yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False}
        )
        return fig

    try:
        df = pd.DataFrame(df_data)

        # Generate plot based on type using selected dropdown values
        fig = None

        if plot_type == 'scatter':
            if not (x_axis and y_axis):
                return create_error_plot("Scatter plot requires X-Axis and Y-Axis selections. Please select both variables."), None

            # Filter out rows with NaN values in essential columns
            essential_cols = [x_axis, y_axis]
            if size:
                essential_cols.append(size)

            # Create a copy of dataframe with non-null values for essential columns
            plot_df = df.dropna(subset=essential_cols)

            if len(plot_df) == 0:
                return create_error_plot(f"No valid data points found after removing NaN values from selected columns: {', '.join(essential_cols)}"), None

            # Handle size column transformation for negative values
            size_adjusted = False
            if size:
                size_values = plot_df[size]
                min_size = size_values.min()
                if min_size < 0:
                    # Shift values to make them positive, add 1 to ensure no zero sizes
                    plot_df = plot_df.copy()
                    plot_df[f'{size}_adjusted'] = size_values - min_size + 1
                    size_column = f'{size}_adjusted'
                    size_adjusted = True
                    logging.info(f"Adjusted size column '{size}' for negative values: shifted by {-min_size + 1}")
                else:
                    # Add small constant to avoid zero sizes
                    plot_df = plot_df.copy()
                    plot_df[f'{size}_adjusted'] = size_values + 1
                    size_column = f'{size}_adjusted'
                    size_adjusted = True

            # Build scatter plot parameters
            plot_params = {
                'data_frame': plot_df,
                'x': x_axis,
                'y': y_axis,
                'title': f'Scatter Plot: {y_axis} vs {x_axis}'
            }

            if color:
                plot_params['color'] = color
                plot_params['title'] += f' (colored by {color})'
            if size:
                plot_params['size'] = size_column if size_adjusted else size
                size_note = f' (sized by {size}' + (' - adjusted for visualization)' if size_adjusted else ')')
                plot_params['title'] += size_note
            if facet:
                plot_params['facet_col'] = facet
                plot_params['title'] += f' (faceted by {facet})'

            # Log data filtering info
            if len(plot_df) < len(df):
                logging.info(f"Filtered scatter plot data: {len(df)} -> {len(plot_df)} points (removed {len(df) - len(plot_df)} points with NaN values)")

            fig = px.scatter(**plot_params)

        elif plot_type == 'histogram':
            if not variable:
                return create_error_plot("Histogram requires a Variable selection. Please select a variable."), None

            # Filter out NaN values for histogram
            hist_df = df.dropna(subset=[variable])

            if len(hist_df) == 0:
                return create_error_plot(f"No valid data points found for variable '{variable}' after removing NaN values."), None

            plot_params = {
                'data_frame': hist_df,
                'x': variable,
                'title': f'Histogram: {variable}'
            }

            if color:
                plot_params['color'] = color
                plot_params['title'] += f' (colored by {color})'
            if facet:
                plot_params['facet_col'] = facet
                plot_params['title'] += f' (faceted by {facet})'

            fig = px.histogram(**plot_params)

        elif plot_type == 'box':
            if not (categorical_axis and value_axis):
                return create_error_plot("Box plot requires both Categorical Axis and Value Axis selections."), None

            plot_params = {
                'data_frame': df,
                'x': categorical_axis,
                'y': value_axis,
                'title': f'Box Plot: {value_axis} by {categorical_axis}'
            }

            if color:
                plot_params['color'] = color
                plot_params['title'] += f' (colored by {color})'
            if facet:
                plot_params['facet_col'] = facet
                plot_params['title'] += f' (faceted by {facet})'

            fig = px.box(**plot_params)

        elif plot_type == 'violin':
            if not (categorical_axis and value_axis):
                return create_error_plot("Violin plot requires both Categorical Axis and Value Axis selections."), None

            plot_params = {
                'data_frame': df,
                'x': categorical_axis,
                'y': value_axis,
                'title': f'Violin Plot: {value_axis} by {categorical_axis}'
            }

            if color:
                plot_params['color'] = color
                plot_params['title'] += f' (colored by {color})'
            if facet:
                plot_params['facet_col'] = facet
                plot_params['title'] += f' (faceted by {facet})'

            fig = px.violin(**plot_params)

        elif plot_type == 'density_heatmap':
            if not (x_axis and y_axis):
                return create_error_plot("Density heatmap requires X-Axis and Y-Axis selections. Please select both variables."), None

            plot_params = {
                'data_frame': df,
                'x': x_axis,
                'y': y_axis,
                'title': f'Density Heatmap: {y_axis} vs {x_axis}'
            }

            if facet:
                plot_params['facet_col'] = facet
                plot_params['title'] += f' (faceted by {facet})'

            fig = px.density_heatmap(**plot_params)

        else:
            return create_error_plot(f"Plot type '{plot_type}' not implemented."), None

        if fig:
            # Configure layout for better interactivity
            fig.update_layout(
                height=600,
                hovermode='closest',
                dragmode='select',  # Enable selection by default
                margin={'l': 50, 'r': 50, 't': 80, 'b': 50},
                font={'size': 12}
            )

            # Enhanced hover templates showing relevant data
            if plot_type == 'scatter':
                hover_template = f'<b>{x_axis}</b>: %{{x}}<br><b>{y_axis}</b>: %{{y}}<br>'
                if color and color in df.columns:
                    hover_template += f'<b>{color}</b>: %{{marker.color}}<br>'
                hover_template += '<extra></extra>'
                fig.update_traces(hovertemplate=hover_template)

            logging.info(f"Generated {plot_type} plot successfully using selected columns: {[v for v in [x_axis, y_axis, variable, categorical_axis, value_axis] if v]}")

            # Store the filtered dataframe that was actually used for plotting
            filtered_df_data = None
            if plot_type == 'scatter' and 'plot_df' in locals():
                filtered_df_data = plot_df.to_dict('records')
            elif plot_type == 'histogram' and 'hist_df' in locals():
                filtered_df_data = hist_df.to_dict('records')
            else:
                # For other plot types, use the original dataframe
                filtered_df_data = df.to_dict('records')

            return fig, filtered_df_data

        return create_error_plot("Could not generate plot with current configuration."), None

    except Exception as e:
        logging.error(f"Error generating plot: {e}")
        # Return an error plot
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error generating plot: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False,
            font={'size': 16, 'color': "red"}
        )
        fig.update_layout(
            title="Plot Generation Error",
            showlegend=False,
            xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
            yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False}
        )
        return fig, None

# Simple approach: always show analysis results when available
# Checkbox functionality will be added in a separate iteration

# Callback to add overlays to plots based on analysis results
@callback(
    Output('main-plot', 'figure', allow_duplicate=True),
    [Input('ols-results-store', 'data'),
     Input('histogram-stats-store', 'data'),
     Input('ols-analysis-checkboxes', 'value'),
     Input('histogram-analysis-checkboxes', 'value')],
    [State('main-plot', 'figure'),
     State('plot-type-dropdown', 'value')],
    prevent_initial_call=True
)
def add_plot_overlays(ols_results, histogram_stats, ols_checkboxes, hist_checkboxes, current_figure, plot_type):
    if not current_figure:
        return dash.no_update
    
    try:
        fig = go.Figure(current_figure)
        
        # Add OLS trendline for scatter plots when checkbox is checked
        if plot_type == 'scatter' and ols_results and ols_checkboxes:
            show_trendline = 'show_trendline' in ols_checkboxes
            
            if show_trendline:
                x_range = np.linspace(ols_results['x_range'][0], ols_results['x_range'][1], 100)
                y_trend = ols_results['slope'] * x_range + ols_results['intercept']
                
                fig.add_trace(go.Scatter(
                    x=x_range,
                    y=y_trend,
                    mode='lines',
                    name=f'OLS Trendline (R² = {ols_results["r_squared"]:.3f})',
                    line={'color': 'red', 'width': 2, 'dash': 'dash'},
                    hovertemplate='<b>Trendline</b><br>' +
                                f'<b>{ols_results["x_var"]}</b>: %{{x}}<br>' +
                                f'<b>{ols_results["y_var"]}</b>: %{{y}}<br>' +
                                '<extra></extra>'
                ))
        
        # Add histogram overlays when checkboxes are selected
        elif plot_type == 'histogram' and histogram_stats and hist_checkboxes:
            show_mean = 'show_mean' in hist_checkboxes
            show_median = 'show_median' in hist_checkboxes
            show_kde = 'show_kde' in hist_checkboxes
            
            # Add mean line if requested
            if show_mean:
                fig.add_vline(
                    x=histogram_stats['mean'],
                    line_dash="dash",
                    line_color="red",
                    annotation_text=f"Mean: {histogram_stats['mean']:.3f}",
                    annotation_position="top"
                )
            
            # Add median line if requested  
            if show_median:
                fig.add_vline(
                    x=histogram_stats['median'],
                    line_dash="dash", 
                    line_color="orange",
                    annotation_text=f"Median: {histogram_stats['median']:.3f}",
                    annotation_position="top"
                )
            
            # Add KDE if requested and data available
            if show_kde and 'kde_data' in histogram_stats:
                # Calculate KDE using stored data
                try:
                    kde = stats.gaussian_kde(histogram_stats['kde_data'])
                    x_range = np.linspace(histogram_stats['min'], histogram_stats['max'], 100)
                    kde_values = kde(x_range)
                    
                    # Scale KDE to match histogram scale (approximate)
                    hist_counts = np.histogram(histogram_stats['kde_data'], bins=30)[0]
                    bin_width = (histogram_stats['max'] - histogram_stats['min']) / 30
                    kde_scaled = kde_values * histogram_stats['n_points'] * bin_width
                    
                    fig.add_trace(go.Scatter(
                        x=x_range,
                        y=kde_scaled,
                        mode='lines',
                        name='KDE',
                        line={'color': 'purple', 'width': 2},
                        hovertemplate='<b>KDE</b><br>' +
                                    f'<b>{histogram_stats["variable"]}</b>: %{{x}}<br>' +
                                    '<b>Density</b>: %{{y}}<br>' +
                                    '<extra></extra>'
                    ))
                except Exception as e:
                    logging.warning(f"KDE calculation failed: {e}")
        
        return fig
        
    except Exception as e:
        logging.error(f"Error adding plot overlays: {e}")
        return dash.no_update

# Separate OLS Analysis Callback for Scatter Plots
@callback(
    Output('ols-results-store', 'data'),
    [Input('filtered-plot-df-store', 'data'),
     Input('plot-type-dropdown', 'value')],
    [State('x-axis-dropdown', 'value'),
     State('y-axis-dropdown', 'value')],
    prevent_initial_call=True
)
def calculate_ols_analysis(filtered_df_data, plot_type, x_axis, y_axis):
    # Only calculate for scatter plots
    if plot_type != 'scatter' or not filtered_df_data:
        return None
    
    if not (x_axis and y_axis):
        return None
    
    try:
        df = pd.DataFrame(filtered_df_data)
        
        # Get clean data for regression
        x_vals = df[x_axis].dropna()
        y_vals = df[y_axis].dropna()
        
        # Ensure we have matching indices
        common_idx = x_vals.index.intersection(y_vals.index)
        x_clean = x_vals.loc[common_idx]
        y_clean = y_vals.loc[common_idx]
        
        if len(x_clean) >= 2:  # Need at least 2 points for regression
            slope, intercept, r_value, p_value, std_err = stats.linregress(x_clean, y_clean)
            
            # Store OLS results
            ols_results = {
                'slope': slope,
                'intercept': intercept,
                'r_squared': r_value**2,
                'r_value': r_value,
                'p_value': p_value,
                'std_err': std_err,
                'n_points': len(x_clean),
                'x_var': x_axis,
                'y_var': y_axis,
                'x_range': [x_clean.min(), x_clean.max()],
                'trendline_y': [slope * x_clean.min() + intercept, slope * x_clean.max() + intercept]
            }
            
            logging.info(f"OLS regression calculated: R² = {r_value**2:.3f}, p = {p_value:.3f}")
            return ols_results
        else:
            logging.warning("Insufficient data points for OLS regression")
            return None
            
    except Exception as e:
        logging.error(f"Error calculating OLS regression: {e}")
        return None

# Separate Distribution Analysis Callback for Histograms
@callback(
    Output('histogram-stats-store', 'data'),
    [Input('filtered-plot-df-store', 'data'),
     Input('plot-type-dropdown', 'value')],
    [State('variable-dropdown', 'value')],
    prevent_initial_call=True
)
def calculate_histogram_analysis(filtered_df_data, plot_type, variable):
    # Only calculate for histograms
    if plot_type != 'histogram' or not filtered_df_data:
        return None
    
    if not variable:
        return None
    
    try:
        df = pd.DataFrame(filtered_df_data)
        
        # Get clean data for the variable
        data_values = df[variable].dropna().values
        
        if len(data_values) >= 3:  # Need at least 3 points for meaningful statistics
            # Calculate comprehensive statistics
            mean_val = np.mean(data_values)
            median_val = np.median(data_values)
            std_val = np.std(data_values, ddof=1)  # Sample standard deviation
            var_val = np.var(data_values, ddof=1)  # Sample variance
            skew_val = stats.skew(data_values)
            kurt_val = stats.kurtosis(data_values)
            min_val = np.min(data_values)
            max_val = np.max(data_values)
            q25 = np.percentile(data_values, 25)
            q75 = np.percentile(data_values, 75)
            iqr_val = q75 - q25
            range_val = max_val - min_val
            
            # Normality tests (only for reasonable sample sizes)
            normality_results = {}
            if 3 <= len(data_values) <= 5000:
                try:
                    shapiro_stat, shapiro_p = stats.shapiro(data_values)
                    normality_results['shapiro'] = {
                        'statistic': shapiro_stat,
                        'p_value': shapiro_p,
                        'test_name': 'Shapiro-Wilk'
                    }
                except Exception as e:
                    logging.warning(f"Shapiro-Wilk test failed: {e}")
            
            if len(data_values) >= 8:
                try:
                    anderson_stat, anderson_crit, anderson_sig = stats.anderson(data_values, dist='norm')
                    # Anderson-Darling gives critical values, we'll use the 5% level (index 2)
                    anderson_p_approx = "< 0.05" if anderson_stat > anderson_crit[2] else "> 0.05"
                    normality_results['anderson'] = {
                        'statistic': anderson_stat,
                        'critical_5pct': anderson_crit[2],
                        'p_value_approx': anderson_p_approx,
                        'test_name': 'Anderson-Darling'
                    }
                except Exception as e:
                    logging.warning(f"Anderson-Darling test failed: {e}")
            
            # Store comprehensive statistics including raw data for KDE
            histogram_stats = {
                'variable': variable,
                'n_points': len(data_values),
                'mean': mean_val,
                'median': median_val,
                'std': std_val,
                'variance': var_val,
                'skewness': skew_val,
                'kurtosis': kurt_val,
                'min': min_val,
                'max': max_val,
                'range': range_val,
                'q25': q25,
                'q75': q75,
                'iqr': iqr_val,
                'normality_tests': normality_results,
                'kde_data': data_values.tolist()  # Store data for KDE calculation
            }
            
            logging.info(f"Histogram statistics calculated for '{variable}': n={len(data_values)}, mean={mean_val:.3f}, std={std_val:.3f}")
            return histogram_stats
        else:
            logging.warning("Insufficient data points for histogram statistics")
            return None
            
    except Exception as e:
        logging.error(f"Error calculating histogram statistics: {e}")
        return None


# Callback to display OLS regression summary and histogram statistics
@callback(
    Output('ols-summary-container', 'children'),
    [Input('ols-results-store', 'data'),
     Input('histogram-stats-store', 'data'),
     Input('plot-type-dropdown', 'value'),
     Input('ols-analysis-checkboxes', 'value'),
     Input('histogram-analysis-checkboxes', 'value')],
    prevent_initial_call=True
)
def display_ols_and_histogram_summary(ols_results, histogram_stats, plot_type, ols_checkboxes, hist_checkboxes):
    # Display histogram statistics when checkbox is checked
    if plot_type == 'histogram' and histogram_stats and hist_checkboxes:
        show_summary = 'show_summary' in hist_checkboxes
        if show_summary:
            return display_histogram_results(histogram_stats)

    # Display OLS results when checkbox is checked
    elif plot_type == 'scatter' and ols_results and ols_checkboxes:
        show_summary = 'show_summary' in ols_checkboxes
        if show_summary:
            return display_ols_results(ols_results)

    # Return empty div for other cases
    return html.Div()

def display_ols_results(ols_results):

    try:
        # Create regression summary table
        summary_data = [
            {'Statistic': 'R-squared', 'Value': f"{ols_results['r_squared']:.4f}"},
            {'Statistic': 'Correlation (r)', 'Value': f"{ols_results['r_value']:.4f}"},
            {'Statistic': 'Slope', 'Value': f"{ols_results['slope']:.4f}"},
            {'Statistic': 'Intercept', 'Value': f"{ols_results['intercept']:.4f}"},
            {'Statistic': 'P-value', 'Value': f"{ols_results['p_value']:.4e}" if ols_results['p_value'] < 0.001 else f"{ols_results['p_value']:.4f}"},
            {'Statistic': 'Standard Error', 'Value': f"{ols_results['std_err']:.4f}"},
            {'Statistic': 'Sample Size (n)', 'Value': f"{ols_results['n_points']}"},
        ]

        # Regression equation
        slope = ols_results['slope']
        intercept = ols_results['intercept']
        x_var = ols_results['x_var']
        y_var = ols_results['y_var']

        if intercept >= 0:
            equation = f"{y_var} = {slope:.4f} × {x_var} + {intercept:.4f}"
        else:
            equation = f"{y_var} = {slope:.4f} × {x_var} - {abs(intercept):.4f}"

        # Significance interpretation
        p_val = ols_results['p_value']
        if p_val < 0.001:
            sig_text = "highly significant (p < 0.001)"
        elif p_val < 0.01:
            sig_text = "very significant (p < 0.01)"
        elif p_val < 0.05:
            sig_text = "significant (p < 0.05)"
        else:
            sig_text = "not significant (p ≥ 0.05)"

        # Create the summary display
        summary_content = dbc.Card([
            dbc.CardHeader(html.H5("OLS Regression Analysis", className="mb-0")),
            dbc.CardBody([
                html.Div([
                    html.H6("Regression Equation:", className="fw-bold"),
                    html.P(equation, className="font-monospace mb-3"),

                    dbc.Row([
                        dbc.Col([
                            html.H6("Model Summary:", className="fw-bold"),
                            dbc.Table.from_dataframe(
                                pd.DataFrame(summary_data),
                                striped=True,
                                bordered=True,
                                hover=True,
                                size="sm",
                                className="mb-3"
                            )
                        ], width=8),
                        dbc.Col([
                            html.H6("Interpretation:", className="fw-bold"),
                            html.P([
                                f"The relationship is {sig_text}",
                                html.Br(),
                                f"R² = {ols_results['r_squared']:.1%} of variance explained",
                                html.Br(),
                                f"Effect size: {abs(ols_results['r_value']):.3f} {'(strong)' if abs(ols_results['r_value']) > 0.7 else '(moderate)' if abs(ols_results['r_value']) > 0.3 else '(weak)'}"
                            ], className="small text-muted")
                        ], width=4)
                    ])
                ])
            ])
        ], className="border-primary")

        return summary_content

    except Exception as e:
        logging.error(f"Error displaying OLS summary: {e}")
        return dbc.Alert(f"Error displaying regression summary: {str(e)}", color="warning")

def display_histogram_results(histogram_stats):
    try:
        # Descriptive statistics table
        descriptive_data = [
            {'Statistic': 'Sample Size (n)', 'Value': f"{histogram_stats['n_points']}"},
            {'Statistic': 'Mean', 'Value': f"{histogram_stats['mean']:.4f}"},
            {'Statistic': 'Median', 'Value': f"{histogram_stats['median']:.4f}"},
            {'Statistic': 'Standard Deviation', 'Value': f"{histogram_stats['std']:.4f}"},
            {'Statistic': 'Variance', 'Value': f"{histogram_stats['variance']:.4f}"},
            {'Statistic': 'Minimum', 'Value': f"{histogram_stats['min']:.4f}"},
            {'Statistic': 'Maximum', 'Value': f"{histogram_stats['max']:.4f}"},
            {'Statistic': 'Range', 'Value': f"{histogram_stats['range']:.4f}"},
        ]

        # Shape statistics table
        shape_data = [
            {'Statistic': 'Skewness', 'Value': f"{histogram_stats['skewness']:.4f}"},
            {'Statistic': 'Kurtosis', 'Value': f"{histogram_stats['kurtosis']:.4f}"},
            {'Statistic': '25th Percentile', 'Value': f"{histogram_stats['q25']:.4f}"},
            {'Statistic': '75th Percentile', 'Value': f"{histogram_stats['q75']:.4f}"},
            {'Statistic': 'Interquartile Range', 'Value': f"{histogram_stats['iqr']:.4f}"},
        ]

        # Interpretation
        skew = histogram_stats['skewness']
        kurt = histogram_stats['kurtosis']

        # Skewness interpretation
        if abs(skew) < 0.5:
            skew_interp = "approximately symmetric"
        elif abs(skew) < 1:
            skew_interp = "moderately skewed"
        else:
            skew_interp = "highly skewed"

        if skew > 0:
            skew_direction = "right (positive)"
        elif skew < 0:
            skew_direction = "left (negative)"
        else:
            skew_direction = "symmetric"

        # Kurtosis interpretation (excess kurtosis)
        if abs(kurt) < 0.5:
            kurt_interp = "approximately normal"
        elif kurt > 0.5:
            kurt_interp = "heavy-tailed (leptokurtic)"
        else:
            kurt_interp = "light-tailed (platykurtic)"

        # Normality test results
        normality_content = []
        if histogram_stats['normality_tests']:
            normality_content.append(html.H6("Normality Tests:", className="fw-bold mt-3"))

            for test_name, results in histogram_stats['normality_tests'].items():
                if test_name == 'shapiro':
                    p_val = results['p_value']
                    test_result = "suggests normality" if p_val > 0.05 else "suggests non-normality"
                    normality_content.extend([
                        html.P([
                            html.Strong("Shapiro-Wilk Test: "),
                            f"W = {results['statistic']:.4f}, p = {p_val:.4f}",
                            html.Br(),
                            html.Small(f"Result: {test_result} (α = 0.05)", className="text-muted")
                        ], className="mb-2")
                    ])
                elif test_name == 'anderson':
                    p_approx = results['p_value_approx']
                    test_result = "suggests normality" if ">" in p_approx else "suggests non-normality"
                    normality_content.extend([
                        html.P([
                            html.Strong("Anderson-Darling Test: "),
                            f"A² = {results['statistic']:.4f}, p {p_approx}",
                            html.Br(),
                            html.Small(f"Result: {test_result} (α = 0.05)", className="text-muted")
                        ], className="mb-2")
                    ])
        else:
            normality_content.append(
                html.P("Normality tests not performed (insufficient sample size)",
                       className="text-muted small mt-3")
            )

        # Create the summary display
        summary_content = dbc.Card([
            dbc.CardHeader(html.H5(f"Distribution Analysis: {histogram_stats['variable']}", className="mb-0")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.H6("Descriptive Statistics:", className="fw-bold"),
                        dbc.Table.from_dataframe(
                            pd.DataFrame(descriptive_data),
                            striped=True,
                            bordered=True,
                            hover=True,
                            size="sm",
                            className="mb-3"
                        )
                    ], width=6),
                    dbc.Col([
                        html.H6("Shape & Quartiles:", className="fw-bold"),
                        dbc.Table.from_dataframe(
                            pd.DataFrame(shape_data),
                            striped=True,
                            bordered=True,
                            hover=True,
                            size="sm",
                            className="mb-3"
                        )
                    ], width=6)
                ]),

                dbc.Row([
                    dbc.Col([
                        html.H6("Distribution Interpretation:", className="fw-bold"),
                        html.P([
                            html.Strong("Shape: "),
                            f"Distribution is {skew_interp}, skewed {skew_direction}",
                            html.Br(),
                            html.Strong("Tail behavior: "),
                            f"Distribution is {kurt_interp}",
                            html.Br(),
                            html.Strong("Central tendency: "),
                            f"Mean = {histogram_stats['mean']:.3f}, Median = {histogram_stats['median']:.3f}"
                        ], className="small text-muted")
                    ], width=8),
                    dbc.Col([
                        html.Div(normality_content)
                    ], width=4)
                ])
            ])
        ], className="border-info")

        return summary_content

    except Exception as e:
        logging.error(f"Error displaying histogram summary: {e}")
        return dbc.Alert(f"Error displaying distribution summary: {str(e)}", color="warning")

# Cross-filtering: Update data table based on plot selections
@callback(
    [Output('selected-data-table-container', 'children'),
     Output('selected-data-info', 'children'),
     Output('export-selected-data-button', 'disabled'),
     Output('selected-points-store', 'data')],
    [Input('main-plot', 'selectedData')],
    [State('filtered-plot-df-store', 'data'),
     State('plotting-df-store', 'data')],
    prevent_initial_call=True
)
def update_selected_data_table(selected_data, filtered_df_data, df_data):
    if not df_data:
        return html.Div(), "", True, None

    # Use the filtered dataframe that was actually used for plotting if available
    if filtered_df_data:
        df = pd.DataFrame(filtered_df_data)
        logging.info(f"Using filtered dataframe for cross-filtering: {len(df)} rows")
    else:
        df = pd.DataFrame(df_data)
        logging.info(f"Using original dataframe for cross-filtering: {len(df)} rows")

    # If no selection made, show empty table or first few rows
    if not selected_data or not selected_data.get('points'):
        info_text = "No points selected. Use box select or lasso select on the plot to filter data."
        empty_table = dag.AgGrid(
            id='selected-data-ag-grid',
            rowData=[],
            columnDefs=[{'field': col, 'headerName': col} for col in df.columns[:10]],  # Limit columns for display
            style={'height': '300px'},
            className="ag-theme-alpine-dark"
        )
        return empty_table, info_text, True, None

    try:
        # Extract selected point indices
        selected_points = selected_data['points']
        point_indices = [point.get('pointIndex', point.get('pointNumber')) for point in selected_points if point.get('pointIndex') is not None or point.get('pointNumber') is not None]

        if not point_indices:
            # Try to get custom data if point indices aren't available
            custom_data = [point.get('customdata') for point in selected_points if point.get('customdata') is not None]
            if custom_data:
                # For now, fall back to showing all data if we can't match indices
                selected_df = df.head(100)  # Limit to first 100 rows for performance
                info_text = f"Selection detected but unable to filter precisely. Showing first {len(selected_df)} rows."
            else:
                info_text = "No valid selection found."
                return dag.AgGrid(rowData=[], columnDefs=[]), info_text, True, None
        else:
            # Filter dataframe based on selected indices
            selected_df = df.iloc[point_indices].copy()
            info_text = f"Selected {len(selected_df)} data points from the plot."

        # Prepare data for AgGrid
        column_defs = []
        for col in selected_df.columns:
            col_def = {
                'field': col,
                'headerName': col,
                'sortable': True,
                'filter': True,
                'resizable': True,
                'width': 150
            }

            # Format numeric columns
            if pd.api.types.is_numeric_dtype(selected_df[col]):
                col_def['type'] = 'numericColumn'
                col_def['valueFormatter'] = {'function': 'd3.format(",.2f")(params.value)'}

            column_defs.append(col_def)

        # Convert to records for AgGrid
        row_data = selected_df.round(3).to_dict('records')  # Round numeric values for display

        data_table = dag.AgGrid(
            id='selected-data-ag-grid',
            rowData=row_data,
            columnDefs=column_defs,
            style={'height': '400px'},
            className="ag-theme-alpine-dark",
            dashGridOptions={
                'pagination': True,
                'paginationPageSize': 20,
                'domLayout': 'normal'
            }
        )

        return data_table, info_text, False, selected_df.to_dict('records')

    except Exception as e:
        logging.error(f"Error updating selected data table: {e}")
        error_text = f"Error processing selection: {str(e)}"
        return html.Div(error_text, style={'color': 'red'}), error_text, True, None

# Open export modal when export button is clicked
@callback(
    [Output('export-filename-modal', 'is_open'),
     Output('export-filename-input', 'value')],
    [Input('export-selected-data-button', 'n_clicks'),
     Input('export-cancel-button', 'n_clicks'),
     Input('export-confirm-button', 'n_clicks')],
    [State('export-filename-modal', 'is_open'),
     State('selected-points-store', 'data')],
    prevent_initial_call=True
)
def toggle_export_modal(export_btn_clicks, cancel_clicks, confirm_clicks, is_open, selected_points):
    ctx = dash.callback_context
    if not ctx.triggered:
        return False, ""

    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if triggered_id == 'export-selected-data-button' and selected_points:
        # Generate default filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        default_filename = f"selected_data_{timestamp}"
        return True, default_filename
    elif triggered_id in ['export-cancel-button', 'export-confirm-button']:
        return False, ""

    return is_open, no_update

# Export selected data to CSV with custom filename
@callback(
    Output('download-selected-data', 'data'),
    [Input('export-confirm-button', 'n_clicks')],
    [State('selected-points-store', 'data'),
     State('export-filename-input', 'value')],
    prevent_initial_call=True
)
def export_selected_data(n_clicks, selected_points, custom_filename):
    if not selected_points or n_clicks == 0:
        return no_update

    try:
        # Convert selected points back to DataFrame
        selected_df = pd.DataFrame(selected_points)

        # Generate CSV string
        csv_string = selected_df.to_csv(index=False)

        # Use custom filename or default
        if custom_filename and custom_filename.strip():
            filename = f"{custom_filename.strip()}.csv"
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"selected_data_{timestamp}.csv"

        logging.info(f"Exporting {len(selected_df)} selected data points to CSV as '{filename}'")

        return {
            "content": csv_string,
            "filename": filename,
            "base64": False,
            "type": "text/csv"
        }

    except Exception as e:
        logging.error(f"Error exporting selected data: {e}")
        return no_update
