import base64
import io
import logging
import warnings

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, State, callback, dcc, html, no_update

# Suppress imghdr deprecation warning from ydata-profiling/visions dependency
# This is safe until the upstream library fixes the issue in a future release
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message=".*imghdr.*deprecated.*", category=DeprecationWarning)
    from ydata_profiling import ProfileReport

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

dash.register_page(__name__, path='/profiling', title='Profile Data')

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H2("Data Profiling", style={'display': 'inline-block', 'margin-right': '40px'}),
                html.Img(
                    src="/assets/profiling.png",
                    style={
                        'height': '150px',
                        'display': 'inline-block',
                        'vertical-align': 'middle'
                    }
                )
            ], style={'display': 'flex', 'align-items': 'center'}, className="mb-4"),
            dbc.Card(dbc.CardBody([
                html.H4("Data Source", className="card-title"),
                html.Div(id='profiling-data-source-status', children="No data loaded."),
                dcc.Upload(
                    id='upload-profiling-csv',
                    children=html.Div(['Drag and Drop or ', html.A('Select CSV File to Profile')]),
                    style={
                        'width': '100%', 'height': '60px', 'lineHeight': '60px',
                        'borderWidth': '1px', 'borderStyle': 'dashed', 'borderRadius': '5px',
                        'textAlign': 'center', 'margin': '10px 0'
                    },
                    multiple=False # Allow only single file upload for profiling
                ),
            ]))
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.H4("Profiling Options", className="card-title"),
                dbc.Row([
                    dbc.Col(dcc.Dropdown(
                        id='profiling-report-type-dropdown',
                        options=[
                            {'label': 'Full Report', 'value': 'full'},
                            {'label': 'Minimal Report', 'value': 'minimal'},
                            {'label': 'Explorative Report', 'value': 'explorative'}
                        ],
                        value='full', # Default to full report
                        clearable=False
                    ), md=4),
                    dbc.Col(dbc.Checkbox(
                        id='profiling-use-sample-checkbox',
                        label='Use sample for large dataset',
                        value=False # Default to not using sample
                    ), md=4, align="center"),
                    dbc.Col(html.Div([ # Wrapper for slider
                        dcc.Slider(
                            id='profiling-sample-size-slider',
                            min=1000, max=20000, step=1000, value=5000,
                            marks={i: str(i) for i in range(1000, 20001, 2000)},
                            disabled=True # Initially disabled
                        )], id='profiling-sample-slider-wrapper', style={'display': 'none'}), # Initially hidden
                    md=4),
                ]),
                dbc.Button(
                    "Generate Profiling Report",
                    id='generate-profiling-report-button',
                    n_clicks=0,
                    className="mt-3",
                    color="primary"
                ),
            ]))
        ], width=12, style={'marginTop': '20px'})
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.H4("Profiling Report", className="card-title"),
                dcc.Loading(
                    id="loading-profiling-report",
                    children=[html.Iframe(id='profiling-report-iframe', style={'width': '100%', 'height': '800px', 'border': 'none'})],
                    type="default"
                ),
                html.Div(id='profiling-download-buttons-area', className="mt-3")
            ]))
        ], width=12, style={'marginTop': '20px'})
    ]),

    # Stores
    dcc.Store(id='profiling-df-store'), # Stores the dataframe for profiling (as dict)
    dcc.Store(id='profiling-report-html-store'), # Stores the generated HTML report string
    dcc.Store(id='profiling-report-json-store'), # Stores the generated JSON summary string

    # Download components (invisible)
    dcc.Download(id='download-profiling-html'),
    dcc.Download(id='download-profiling-json')
], fluid=True)

# --- Callbacks ---

# State Persistence Callbacks

# Save profiling options state
@callback(
    Output('profiling-options-state-store', 'data'),
    [Input('profiling-report-type-dropdown', 'value'),
     Input('profiling-use-sample-checkbox', 'value'),
     Input('profiling-sample-size-slider', 'value')]
)
def save_profiling_options_state(report_type, use_sample, sample_size):
    return {
        'report_type': report_type,
        'use_sample': use_sample,
        'sample_size': sample_size
    }

# Restore profiling options
@callback(
    [Output('profiling-report-type-dropdown', 'value', allow_duplicate=True),
     Output('profiling-use-sample-checkbox', 'value', allow_duplicate=True),
     Output('profiling-sample-size-slider', 'value', allow_duplicate=True)],
    Input('profiling-options-state-store', 'data'),
    prevent_initial_call=True
)
def restore_profiling_options(stored_options):
    if stored_options:
        return (
            stored_options.get('report_type', 'full'),
            stored_options.get('use_sample', False),
            stored_options.get('sample_size', 5000)
        )
    return 'full', False, 5000

# Callback to Load Data from Query Page (merged-dataframe-store) or Upload
@callback(
    [Output('profiling-df-store', 'data'),
     Output('profiling-data-source-status', 'children')],
    [Input('merged-dataframe-store', 'data'), # From query page
     Input('upload-profiling-csv', 'contents')],
    [State('upload-profiling-csv', 'filename')],
    prevent_initial_call=False # Allow initial call to check for existing data
)
def load_data_for_profiling(merged_data, upload_contents, upload_filename):
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

    # Debug logging
    logging.info(f"Profiling callback triggered by: {triggered_id}, merged_data available: {merged_data is not None}")

    if triggered_id == 'upload-profiling-csv' and upload_contents:
        try:
            content_type, content_string = upload_contents.split(',')
            decoded = base64.b64decode(content_string)
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
            status_message = f"Data loaded from uploaded file: {upload_filename} ({len(df)} rows)"
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
                df = pd.DataFrame(full_data)
                status_message = f"Data loaded from Query Page ({len(df)} rows)."
            else:
                # Fallback for old format (if any)
                df = pd.DataFrame(merged_data)
                status_message = f"Data loaded from Query Page ({len(df)} rows)."

            logging.info(status_message)
            return df.to_dict('records'), status_message
        except Exception as e:
            error_message = f"Error processing data from Query Page: {str(e)}"
            logging.error(error_message)
            return None, dbc.Alert(error_message, color="danger")

    # If no specific trigger but one of them has data (e.g. page refresh with store data)
    if upload_contents: # Prioritize uploaded file if present (e.g. after refresh)
        try:
            content_type, content_string = upload_contents.split(',')
            decoded = base64.b64decode(content_string)
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
            status_message = f"Data loaded from uploaded file: {upload_filename} ({len(df)} rows) - (on refresh/no specific trigger)"
            logging.info(status_message)
            return df.to_dict('records'), status_message
        except Exception as e:
            logging.error(f"Error processing uploaded CSV on refresh: {str(e)}")
            # Fall through to check merged_data or show no data

    if merged_data: # Check merged_data if upload didn't happen or failed
        try:
            # Handle new metadata structure from query page (same logic as above)
            if isinstance(merged_data, dict) and 'full_data' in merged_data:
                full_data = merged_data['full_data']
                df = pd.DataFrame(full_data)
                status_message = f"Data loaded from Query Page ({len(df)} rows) - (on refresh/no specific trigger)"
            else:
                # Fallback for old format (if any)
                df = pd.DataFrame(merged_data)
                status_message = f"Data loaded from Query Page ({len(df)} rows) - (on refresh/no specific trigger)"

            logging.info(status_message)
            return df.to_dict('records'), status_message
        except Exception as e:
            logging.error(f"Error processing data from Query Page on refresh: {str(e)}")
            # Fall through to no data state

    return None, html.Div([
        "No data available for profiling.",
        html.Br(),
        "• Generate data on the Query page and it will automatically appear here, or",
        html.Br(),
        "• Upload a CSV file above to profile it directly"
    ])


# Callback to Control Sample Slider Visibility and Disabled State
@callback(
    [Output('profiling-sample-slider-wrapper', 'style'),
     Output('profiling-sample-size-slider', 'disabled')],
    [Input('profiling-use-sample-checkbox', 'value')], # 'value' is boolean for dbc.Checkbox
    [State('profiling-df-store', 'data')]
)
def control_sample_slider_visibility(use_sample_checked, df_data):
    slider_style = {'display': 'none'}
    slider_disabled = True

    if use_sample_checked:
        slider_style = {'display': 'block', 'marginTop': '10px'} # Show slider
        slider_disabled = False # Enable slider
        # Optionally, adjust slider max based on df_data length if df_data is not None
        # For now, just enable/show it. Logic for actual sampling happens at report generation.

    return slider_style, slider_disabled


# Callback to enable/disable report generation button
@callback(
    Output('generate-profiling-report-button', 'disabled'),
    Input('profiling-df-store', 'data')
)
def control_generate_button_disabled_state(df_data):
    return not bool(df_data) # Disabled if no data, enabled if data exists


# Callback for Report Generation and Display
@callback(
    [Output('profiling-report-iframe', 'srcDoc'),
     Output('profiling-download-buttons-area', 'children'),
     Output('profiling-report-html-store', 'data'),
     Output('profiling-report-json-store', 'data')],
    [Input('generate-profiling-report-button', 'n_clicks')],
    [State('profiling-df-store', 'data'),
     State('profiling-report-type-dropdown', 'value'),
     State('profiling-use-sample-checkbox', 'value'), # dbc.Checkbox 'value' is boolean
     State('profiling-sample-size-slider', 'value')]
)
def generate_and_display_profiling_report(n_clicks, df_data, report_type_value, use_sample, sample_size):
    if n_clicks == 0 or not df_data:
        return "Please load data and click 'Generate Profiling Report'.", html.Div(), None, None

    try:
        df = pd.DataFrame(df_data)
        if df.empty:
            return "The provided dataset is empty.", html.Div(), None, None

        logging.info(f"Generating profiling report. Original df size: {len(df)} rows.")

        df_to_profile = df
        if use_sample and len(df) > sample_size:
            logging.info(f"Using sample of {sample_size} rows for profiling.")
            df_to_profile = df.sample(n=sample_size, random_state=42) # Added random_state for reproducibility

        logging.info(f"Profiling dataframe with {len(df_to_profile)} rows. Report type: {report_type_value}")

        # Create ProfileReport with appropriate parameters based on report type
        if report_type_value == 'minimal':
            profile = ProfileReport(
                df_to_profile,
                title="Data Profiling Report (Minimal)",
                minimal=True,
                lazy=False
            )
        elif report_type_value == 'explorative':
            profile = ProfileReport(
                df_to_profile,
                title="Data Profiling Report (Explorative)",
                explorative=True,
                lazy=False
            )
        else:  # 'full' is default
            profile = ProfileReport(
                df_to_profile,
                title="Data Profiling Report",
                lazy=False
            )

        report_html = profile.to_html()
        # Attempt to get JSON data, handle if not available for certain reports/versions
        try:
            report_json = profile.to_json()
        except Exception as e_json:
            logging.warning(f"Could not generate JSON report: {e_json}")
            report_json = None # Or provide a default JSON error message string

        logging.info("Profiling report generated successfully.")

        download_buttons = html.Div([
            dbc.Button("Download HTML Report", id='download-profiling-html-button', color="success", className="me-2"),
            dbc.Button("Download JSON Summary", id='download-profiling-json-button', color="info", disabled=not bool(report_json))
        ])

        return report_html, download_buttons, report_html, report_json

    except Exception as e:
        error_html = f"""
        <html><body>
        <h2>Error Generating Profiling Report</h2>
        <p>An error occurred: {str(e)}</p>
        <p>Please check the data or try different profiling options.</p>
        </body></html>
        """
        logging.error(f"Error generating profiling report: {e}", exc_info=True)
        return error_html, html.Div(dbc.Alert(f"Error: {str(e)}", color="danger")), None, None

# Callback for HTML Report Download
@callback(
    Output('download-profiling-html', 'data'),
    Input('download-profiling-html-button', 'n_clicks'),
    State('profiling-report-html-store', 'data'),
    prevent_initial_call=True
)
def download_html_report(n_clicks, html_data):
    if not html_data:
        return no_update
    return {"content": html_data, "filename": "profiling_report.html", "base64": False, "type": "text/html"}

# Callback for JSON Summary Download
@callback(
    Output('download-profiling-json', 'data'),
    Input('download-profiling-json-button', 'n_clicks'),
    State('profiling-report-json-store', 'data'),
    prevent_initial_call=True
)
def download_json_summary(n_clicks, json_data):
    if not json_data:
        return no_update
    return {"content": json_data, "filename": "profiling_summary.json", "base64": False, "type": "application/json"}
