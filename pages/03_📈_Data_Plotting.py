"""
Data Plotting Page - Interactive visualization and exploration of research datasets
"""

import dash
from dash import html, dcc, callback, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import base64
import io
import logging
from datetime import datetime

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

    # Stores
    dcc.Store(id='plotting-df-store'),  # Stores the dataframe for plotting
    dcc.Store(id='selected-points-store'),  # Stores selected points from plot
    dcc.Store(id='plot-config-store'),  # Stores current plot configuration
    
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
        dcc.Dropdown(id='value-axis-dropdown', style={'display': 'none'})
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

    elif triggered_id == 'merged-dataframe-store' and merged_data:
        try:
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
                        options=categorical_options, placeholder="Select facet variable", className="mb-2")
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
                        options=categorical_options, placeholder="Select facet variable", className="mb-2")
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
def sync_dropdown_values(all_values, all_ids):
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
        if i < len(all_values) and all_values[i]:
            values[target] = all_values[i]
    
    return (values['x-axis'], values['y-axis'], values['color'], values['size'],
            values['facet'], values['variable'], values['categorical-axis'], values['value-axis'])

# Plot generation callback using hidden dropdown values
@callback(
    Output('main-plot', 'figure'),
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
        return {}
    
    # Create a simple error message plot for missing data
    def create_error_plot(message):
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16, color="orange")
        )
        fig.update_layout(
            title="Configuration Required",
            showlegend=False,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
        return fig
    
    try:
        df = pd.DataFrame(df_data)
        
        # Generate plot based on type using selected dropdown values
        fig = None
        
        if plot_type == 'scatter':
            if not (x_axis and y_axis):
                return create_error_plot("Scatter plot requires X-Axis and Y-Axis selections. Please select both variables.")
            
            # Build scatter plot parameters
            plot_params = {
                'data_frame': df,
                'x': x_axis,
                'y': y_axis,
                'title': f'Scatter Plot: {y_axis} vs {x_axis}'
            }
            
            if color:
                plot_params['color'] = color
                plot_params['title'] += f' (colored by {color})'
            if size:
                plot_params['size'] = size
                plot_params['title'] += f' (sized by {size})'
            if facet:
                plot_params['facet_col'] = facet
                plot_params['title'] += f' (faceted by {facet})'
            
            fig = px.scatter(**plot_params)
        
        elif plot_type == 'histogram':
            if not variable:
                return create_error_plot("Histogram requires a Variable selection. Please select a variable.")
            
            plot_params = {
                'data_frame': df,
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
                return create_error_plot("Box plot requires both Categorical Axis and Value Axis selections.")
            
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
                return create_error_plot("Violin plot requires both Categorical Axis and Value Axis selections.")
            
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
                return create_error_plot("Density heatmap requires X-Axis and Y-Axis selections. Please select both variables.")
            
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
            return create_error_plot(f"Plot type '{plot_type}' not implemented.")
        
        if fig:
            # Configure layout for better interactivity
            fig.update_layout(
                height=600,
                hovermode='closest',
                dragmode='select',  # Enable selection by default
                margin=dict(l=50, r=50, t=80, b=50),
                font=dict(size=12)
            )
            
            # Enhanced hover templates showing relevant data
            if plot_type == 'scatter':
                hover_template = f'<b>{x_axis}</b>: %{{x}}<br><b>{y_axis}</b>: %{{y}}<br>'
                if color and color in df.columns:
                    hover_template += f'<b>{color}</b>: %{{marker.color}}<br>'
                hover_template += '<extra></extra>'
                fig.update_traces(hovertemplate=hover_template)
            
            logging.info(f"Generated {plot_type} plot successfully using selected columns: {[v for v in [x_axis, y_axis, variable, categorical_axis, value_axis] if v]}")
            return fig
        
        return create_error_plot("Could not generate plot with current configuration.")
    
    except Exception as e:
        logging.error(f"Error generating plot: {e}")
        # Return an error plot
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error generating plot: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16, color="red")
        )
        fig.update_layout(
            title="Plot Generation Error",
            showlegend=False,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
        return fig

# Cross-filtering: Update data table based on plot selections
@callback(
    [Output('selected-data-table-container', 'children'),
     Output('selected-data-info', 'children'),
     Output('export-selected-data-button', 'disabled'),
     Output('selected-points-store', 'data')],
    [Input('main-plot', 'selectedData')],
    [State('plotting-df-store', 'data')],
    prevent_initial_call=True
)
def update_selected_data_table(selected_data, df_data):
    if not df_data:
        return html.Div(), "", True, None
    
    df = pd.DataFrame(df_data)
    
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

# Export selected data to CSV
@callback(
    Output('download-selected-data', 'data'),
    [Input('export-selected-data-button', 'n_clicks')],
    [State('selected-points-store', 'data')],
    prevent_initial_call=True
)
def export_selected_data(n_clicks, selected_points):
    if not selected_points or n_clicks == 0:
        return no_update
    
    try:
        # Convert selected points back to DataFrame
        selected_df = pd.DataFrame(selected_points)
        
        # Generate CSV string
        csv_string = selected_df.to_csv(index=False)
        
        # Create filename with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"selected_data_{timestamp}.csv"
        
        logging.info(f"Exporting {len(selected_df)} selected data points to CSV")
        
        return dict(
            content=csv_string,
            filename=filename,
            base64=False,
            type="text/csv"
        )
    
    except Exception as e:
        logging.error(f"Error exporting selected data: {e}")
        return no_update