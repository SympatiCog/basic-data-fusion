import base64
import json

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import (
    Input,
    Output,
    State,
    callback,
    clientside_callback,
    dcc,
    html,
    no_update,
)

from config_manager import get_config, refresh_config
from utils import save_uploaded_files_to_data_dir, validate_csv_file

dash.register_page(__name__, path='/onboarding', name='Setup')

layout = dbc.Container([
    # Page header
    html.Div([
        html.H1("Application Setup", className="text-center mb-4"),
        html.P("Welcome! Let's set up your data analysis environment step by step.",
               className="text-center text-muted mb-5")
    ]),

    # Store components for managing state
    dcc.Store(id='onboarding-demographics-data', storage_type='session'),
    dcc.Store(id='onboarding-step-state', storage_type='session', data={
        'project_setup_complete': False,
        'demographics_loaded': False,
        'config_ready': False
    }),

    # Alert container
    html.Div(id='onboarding-alerts', className="mb-4"),

    # Main content area with the four widgets
    dbc.Row([
        # Left column - Project Setup and Demo Configuration
        dbc.Col([
            # Project Setup Widget
            dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.I(className="bi bi-1-circle-fill text-success me-2"),
                        html.H4("Project Setup", className="mb-0 d-inline"),
                        html.Span(" (Active)", className="badge bg-success ms-2")
                    ], className="d-flex align-items-center")
                ], className="bg-success text-white", id='project-setup-header'),
                dbc.CardBody([
                    # Demographics file selection
                    html.Label("Choose your demographics CSV file", className="fw-bold mb-2"),
                    dcc.Upload(
                        id='onboarding-demographics-upload',
                        children=html.Div([
                            html.Button('Choose File', className='btn btn-outline-secondary'),
                        ]),
                        className="mb-3"
                    ),
                    html.Div(id='onboarding-demographics-filename', className="mb-3"),

                    # Data directory selection
                    html.Label("Data Folder (optional)*", className="fw-bold mb-2"),
                    dbc.Input(
                        id='onboarding-data-dir-input',
                        value='data',
                        placeholder='data',
                        className="mb-2"
                    ),
                    html.Small("*Note: This is the location where data will be stored. You can use the default folder (data); all of your source data folder contents will be copied here.", className="text-muted mb-3 d-block"),

                    # Load Configuration File button
                    html.Hr(),
                    dbc.Button("Load Configuration File", id="onboarding-load-config-btn",
                              color="secondary", className="w-100"),
                    dcc.Upload(
                        id='onboarding-config-upload',
                        children=html.Div(),
                        style={'display': 'none'}
                    )
                ], id='project-setup-body')
            ], className="mb-4 border-success shadow-sm", id='project-setup-card'),

            # Demo Configuration Widget
            dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.I(className="bi bi-2-circle text-muted me-2"),
                        html.H4("Demo Configuration", className="mb-0 d-inline text-muted"),
                        html.Span(" (Waiting)", className="badge bg-secondary ms-2")
                    ], className="d-flex align-items-center")
                ], className="bg-light", id='demo-config-header'),
                dbc.CardBody([
                    # Age Column
                    html.Label("Age Column", className="fw-bold mb-2"),
                    dcc.Dropdown(
                        id='onboarding-age-column-dropdown',
                        placeholder="Select age column",
                        className="mb-3",
                        disabled=True
                    ),

                    # Sex Column
                    html.Label("Sex Column", className="fw-bold mb-2"),
                    dcc.Dropdown(
                        id='onboarding-sex-column-dropdown',
                        placeholder="Select sex column",
                        className="mb-3",
                        disabled=True
                    ),

                    # Sex Mapping
                    html.Label("Sex Mapping", className="fw-bold mb-2"),
                    html.Div(id='onboarding-sex-mapping-container', children=[
                        html.P("Load demographics file to configure sex mapping", className="text-muted")
                    ]),

                    dbc.Button("Add Sex Mapping", id="onboarding-add-sex-mapping-btn",
                              color="secondary", size="sm", className="mt-2", disabled=True)
                ], id='demo-config-body', style={'opacity': '0.5'})
            ], className="mb-4 border-secondary", id='demo-config-card', style={'opacity': '0.7'})
        ], width=6),

        # Right column - CSV Linking and Drag & Drop
        dbc.Col([
            # CSV Linking Information Widget
            dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.I(className="bi bi-3-circle text-muted me-2"),
                        html.H4("CSV Linking Information", className="mb-0 d-inline text-muted"),
                        html.Span(" (Waiting)", className="badge bg-secondary ms-2")
                    ], className="d-flex align-items-center")
                ], className="bg-light", id='csv-linking-header'),
                dbc.CardBody([
                    # Primary ID Column
                    html.Label("Primary ID Column", className="fw-bold mb-2"),
                    dcc.Dropdown(
                        id='onboarding-primary-id-dropdown',
                        placeholder="Select primary ID column",
                        className="mb-3",
                        disabled=True
                    ),

                    # Session Column (optional)
                    html.Label("Session Column (optional)*", className="fw-bold mb-2"),
                    dcc.Dropdown(
                        id='onboarding-session-column-dropdown',
                        placeholder="None",
                        className="mb-3",
                        disabled=True
                    ),

                    # Study/Site Column (optional)
                    html.Label("Study/Site Column (optional)*", className="fw-bold mb-2"),
                    dcc.Dropdown(
                        id='onboarding-study-site-dropdown',
                        placeholder="None",
                        className="mb-3",
                        disabled=True
                    ),

                    # Composite ID (optional)
                    html.Label("Composite ID (optional)*", className="fw-bold mb-2"),
                    dbc.Input(
                        id='onboarding-composite-id-input',
                        value='customID',
                        placeholder='customID',
                        className="mb-2",
                        disabled=True
                    ),
                    html.Small("**These values are only necessary if you have multi-session and/or multiple sites/studies in your database - e.g. Rockland Sample Datasets", className="text-muted")
                ], id='csv-linking-body', style={'opacity': '0.5'})
            ], className="mb-4 border-secondary", id='csv-linking-card', style={'opacity': '0.7'}),

            # Drag and Drop Widget
            dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.I(className="bi bi-4-circle text-muted me-2"),
                        html.H4("Data Upload", className="mb-0 d-inline text-muted"),
                        html.Span(" (Waiting)", className="badge bg-secondary ms-2")
                    ], className="d-flex align-items-center")
                ], className="bg-light", id='drag-drop-header'),
                dbc.CardBody([
                    dcc.Upload(
                        id='onboarding-final-upload',
                        children=html.Div([
                            html.H4("Drag and Drop Data Files - or Click to Browse",
                                   className="text-center text-muted mb-0"),
                        ], className="text-center p-4"),
                        style={
                            'width': '100%',
                            'height': '120px',
                            'borderWidth': '2px',
                            'borderStyle': 'dashed',
                            'borderRadius': '10px',
                            'borderColor': '#ccc',
                            'backgroundColor': '#f8f9fa',
                            'cursor': 'not-allowed'
                        },
                        multiple=True,
                        disabled=True
                    )
                ], id='drag-drop-body', style={'opacity': '0.5'})
            ], className="border-secondary", id='drag-drop-card', style={'opacity': '0.7'})
        ], width=6)
    ])
], fluid=True)

# Callback to handle demographics file upload and parsing
@callback(
    [Output('onboarding-demographics-filename', 'children'),
     Output('onboarding-demographics-data', 'data'),
     Output('onboarding-age-column-dropdown', 'options'),
     Output('onboarding-age-column-dropdown', 'disabled'),
     Output('onboarding-sex-column-dropdown', 'options'),
     Output('onboarding-sex-column-dropdown', 'disabled'),
     Output('onboarding-primary-id-dropdown', 'options'),
     Output('onboarding-primary-id-dropdown', 'disabled'),
     Output('onboarding-session-column-dropdown', 'options'),
     Output('onboarding-session-column-dropdown', 'disabled'),
     Output('onboarding-study-site-dropdown', 'options'),
     Output('onboarding-study-site-dropdown', 'disabled'),
     Output('onboarding-composite-id-input', 'disabled'),
     Output('demo-config-header', 'children'),
     Output('demo-config-header', 'className'),
     Output('demo-config-body', 'style'),
     Output('csv-linking-header', 'children'),
     Output('csv-linking-header', 'className'),
     Output('csv-linking-body', 'style'),
     Output('project-setup-header', 'children'),
     Output('project-setup-header', 'className'),
     Output('onboarding-step-state', 'data'),
     Output('onboarding-alerts', 'children')],
    [Input('onboarding-demographics-upload', 'contents')],
    [State('onboarding-demographics-upload', 'filename'),
     State('onboarding-step-state', 'data')]
)
def handle_demographics_upload(contents, filename, step_state):
    if not contents or not filename:
        return (no_update, no_update, no_update, no_update, no_update, no_update,
                no_update, no_update, no_update, no_update, no_update, no_update,
                no_update, no_update, no_update, no_update, no_update, no_update,
                no_update, no_update, no_update, no_update, no_update)

    try:
        # Decode and validate the uploaded file
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)

        # Validate the CSV file
        errors, df = validate_csv_file(decoded, filename)

        if errors:
            alert = dbc.Alert([
                html.H5("File Validation Error", className="alert-heading"),
                html.Ul([html.Li(error) for error in errors])
            ], color="danger", dismissable=True)
            return (no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, no_update, no_update, no_update, alert)

        if df is None:
            alert = dbc.Alert("Failed to read the CSV file.", color="danger", dismissable=True)
            return (no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, no_update, no_update, no_update, alert)

        # Get column names
        columns = df.columns.tolist()
        column_options = [{'label': col, 'value': col} for col in columns]

        # Add "None" option for optional fields
        optional_column_options = [{'label': 'None', 'value': ''}] + column_options

        # Success message
        success_alert = dbc.Alert([
            html.I(className="bi bi-check-circle-fill me-2"),
            f"Successfully loaded '{filename}' with {len(columns)} columns and {len(df)} rows."
        ], color="success", dismissable=True)

        # Update step state
        updated_step_state = step_state.copy()
        updated_step_state['demographics_loaded'] = True

        # Store the dataframe data for later use
        demographics_data = {
            'filename': filename,
            'columns': columns,
            'data': df.to_dict('records')[:5]  # Store first 5 rows for preview
        }

        return (
            html.Div([
                html.I(className="bi bi-file-earmark-check text-success me-2"),
                html.Span(f"Selected: {filename}", className="fw-bold")
            ]),
            demographics_data,
            column_options, False,  # age column dropdown
            column_options, False,  # sex column dropdown
            column_options, False,  # primary ID dropdown
            optional_column_options, False,  # session column dropdown
            optional_column_options, False,  # study/site dropdown
            False,  # composite ID input enabled
            # Demo Configuration header - now enabled
            html.Div([
                html.I(className="bi bi-2-circle-fill text-success me-2"),
                html.H4("Demo Configuration", className="mb-0 d-inline"),
                html.Span(" (Active)", className="badge bg-success ms-2")
            ], className="d-flex align-items-center"),
            "bg-success text-white",  # demo config header class
            {'opacity': '1'},  # demo config body enabled
            # CSV Linking header - now enabled
            html.Div([
                html.I(className="bi bi-3-circle-fill text-info me-2"),
                html.H4("CSV Linking Information", className="mb-0 d-inline"),
                html.Span(" (Active)", className="badge bg-info ms-2")
            ], className="d-flex align-items-center"),
            "bg-info text-white",  # csv linking header class
            {'opacity': '1'},  # csv linking body enabled
            # Project Setup header - now completed
            html.Div([
                html.I(className="bi bi-1-circle-fill text-success me-2"),
                html.H4("Project Setup", className="mb-0 d-inline"),
                html.Span(" (Complete)", className="badge bg-success ms-2")
            ], className="d-flex align-items-center"),
            "bg-success text-white",  # project setup header class
            updated_step_state,
            success_alert
        )

    except Exception as e:
        alert = dbc.Alert(f"Error processing file: {str(e)}", color="danger", dismissable=True)
        return (no_update, no_update, no_update, no_update, no_update, no_update,
                no_update, no_update, no_update, no_update, no_update, no_update,
                no_update, no_update, no_update, no_update, no_update, no_update,
                no_update, no_update, no_update, no_update, alert)

# Callback to handle sex mapping based on selected sex column
@callback(
    [Output('onboarding-sex-mapping-container', 'children'),
     Output('onboarding-add-sex-mapping-btn', 'disabled')],
    [Input('onboarding-sex-column-dropdown', 'value')],
    [State('onboarding-demographics-data', 'data')]
)
def update_sex_mapping(sex_column, demographics_data):
    if not sex_column or not demographics_data:
        return html.P("Select a sex column to configure mapping", className="text-muted"), True

    try:
        # Get unique values from the selected sex column
        df = pd.DataFrame(demographics_data['data'])
        if sex_column in df.columns:
            unique_values = df[sex_column].dropna().unique().tolist()

            # Create default mapping components
            mapping_components = []
            default_mappings = {'Female': 1, 'Male': 2, 'Other': 3, 'F': 1, 'M': 2}

            for i, value in enumerate(unique_values):
                default_num = default_mappings.get(str(value), 0)

                mapping_row = dbc.Row([
                    dbc.Col([
                        dbc.Button(str(value), color="light", size="sm", className="w-100")
                    ], width=4),
                    dbc.Col([
                        dbc.Input(
                            id={'type': 'sex-mapping-input', 'index': i},
                            value=default_num,
                            type="number",
                            size="sm"
                        )
                    ], width=3),
                    dbc.Col([
                        dbc.Button("×", id={'type': 'sex-mapping-remove', 'index': i},
                                  color="danger", size="sm", outline=True)
                    ], width=2)
                ], className="mb-2")

                mapping_components.append(mapping_row)

            return mapping_components, False
    except Exception as e:
        return html.P(f"Error: {str(e)}", className="text-danger"), True

    return html.P("No data found for selected column", className="text-muted"), True

# Callback to enable drag & drop when configuration is ready
@callback(
    [Output('onboarding-final-upload', 'disabled'),
     Output('onboarding-final-upload', 'style'),
     Output('drag-drop-body', 'style'),
     Output('drag-drop-header', 'children'),
     Output('drag-drop-header', 'className')],
    [Input('onboarding-age-column-dropdown', 'value'),
     Input('onboarding-sex-column-dropdown', 'value'),
     Input('onboarding-primary-id-dropdown', 'value')],
    [State('onboarding-step-state', 'data')]
)
def enable_drag_drop(age_column, sex_column, primary_id, step_state):
    if not step_state.get('demographics_loaded'):
        return True, {
            'width': '100%',
            'height': '120px',
            'borderWidth': '2px',
            'borderStyle': 'dashed',
            'borderRadius': '10px',
            'borderColor': '#ccc',
            'backgroundColor': '#f8f9fa',
            'cursor': 'not-allowed'
        }, {'opacity': '0.5'}, html.Div([
            html.I(className="bi bi-4-circle text-muted me-2"),
            html.H4("Data Upload", className="mb-0 d-inline text-muted"),
            html.Span(" (Waiting)", className="badge bg-secondary ms-2")
        ], className="d-flex align-items-center"), "bg-light"

    # Check if required fields are filled
    if age_column and sex_column and primary_id:
        return False, {
            'width': '100%',
            'height': '120px',
            'borderWidth': '2px',
            'borderStyle': 'dashed',
            'borderRadius': '10px',
            'borderColor': '#28a745',
            'backgroundColor': '#f8f9fa',
            'cursor': 'pointer'
        }, {'opacity': '1'}, html.Div([
            html.I(className="bi bi-4-circle-fill text-success me-2"),
            html.H4("Data Upload", className="mb-0 d-inline"),
            html.Span(" (Ready)", className="badge bg-success ms-2")
        ], className="d-flex align-items-center"), "bg-success text-white"

    return True, {
        'width': '100%',
        'height': '120px',
        'borderWidth': '2px',
        'borderStyle': 'dashed',
        'borderRadius': '10px',
        'borderColor': '#ffc107',
        'backgroundColor': '#fff3cd',
        'cursor': 'not-allowed'
    }, {'opacity': '0.7'}, html.Div([
        html.I(className="bi bi-4-circle text-warning me-2"),
        html.H4("Data Upload", className="mb-0 d-inline text-warning"),
        html.Span(" (Incomplete)", className="badge bg-warning ms-2")
    ], className="d-flex align-items-center"), "bg-warning"

# Callback to handle final file upload and configuration saving
@callback(
    [Output('onboarding-alerts', 'children', allow_duplicate=True),
     Output('onboarding-final-upload', 'children')],
    [Input('onboarding-final-upload', 'contents')],
    [State('onboarding-final-upload', 'filename'),
     State('onboarding-demographics-data', 'data'),
     State('onboarding-data-dir-input', 'value'),
     State('onboarding-age-column-dropdown', 'value'),
     State('onboarding-sex-column-dropdown', 'value'),
     State('onboarding-primary-id-dropdown', 'value'),
     State('onboarding-session-column-dropdown', 'value'),
     State('onboarding-composite-id-input', 'value'),
     State('onboarding-demographics-upload', 'contents'),
     State('onboarding-demographics-upload', 'filename')],
    prevent_initial_call=True
)
def handle_final_upload(contents_list, filenames_list, demographics_data, data_dir,
                       age_column, sex_column, primary_id, session_column, composite_id,
                       demographics_contents, demographics_filename):

    if not contents_list:
        return no_update, no_update

    try:
        # Prepare configuration
        config = get_config()

        # Update configuration with user selections
        config.DATA_DIR = data_dir or 'data'
        config.DEMOGRAPHICS_FILE = demographics_filename or 'demographics.csv'
        config.AGE_COLUMN = age_column
        config.SEX_COLUMN = sex_column
        config.PRIMARY_ID_COLUMN = primary_id
        config.SESSION_COLUMN = session_column or 'session_num'
        config.COMPOSITE_ID_COLUMN = composite_id or 'customID'

        # Save configuration
        config.save_config()
        refresh_config()

        # Prepare file contents for saving
        all_file_contents = []
        all_filenames = []

        # Add demographics file first
        if demographics_contents and demographics_filename:
            content_type, content_string = demographics_contents.split(',')
            demo_decoded = base64.b64decode(content_string)
            all_file_contents.append(demo_decoded)
            all_filenames.append(demographics_filename)

        # Add other uploaded files
        if isinstance(contents_list, list):
            for content, filename in zip(contents_list, filenames_list):
                content_type, content_string = content.split(',')
                decoded = base64.b64decode(content_string)
                all_file_contents.append(decoded)
                all_filenames.append(filename)
        else:
            # Single file
            content_type, content_string = contents_list.split(',')
            decoded = base64.b64decode(content_string)
            all_file_contents.append(decoded)
            all_filenames.append(filenames_list)

        # Save all files
        success_messages, error_messages = save_uploaded_files_to_data_dir(
            all_file_contents, all_filenames, config.DATA_DIR
        )

        if error_messages:
            alert = dbc.Alert([
                html.H5("Upload Errors", className="alert-heading"),
                html.Ul([html.Li(error) for error in error_messages])
            ], color="warning", dismissable=True)
        else:
            # Success - redirect to main app
            alert = dbc.Alert([
                html.H5("Setup Complete!", className="alert-heading"),
                html.P("Your configuration has been saved and files have been uploaded successfully."),
                html.Hr(),
                html.P([
                    "Redirecting to the main application... ",
                    dcc.Link("Click here if not redirected", href="/", className="alert-link")
                ])
            ], color="success")

            # Add client-side redirect
            alert.children.append(
                dcc.Interval(id='redirect-interval', interval=2000, n_intervals=0, max_intervals=1)
            )

        upload_status = html.Div([
            html.H4("Upload Complete!", className="text-success text-center mb-0"),
            html.P(f"Successfully uploaded {len(all_filenames)} files", className="text-center text-muted")
        ], className="text-center p-4")

        return alert, upload_status

    except Exception as e:
        alert = dbc.Alert(f"Error during setup: {str(e)}", color="danger", dismissable=True)
        return alert, no_update

# Client-side callback for redirect after successful setup
clientside_callback(
    """
    function(n_intervals) {
        if (n_intervals > 0) {
            window.location.href = '/';
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output('redirect-interval', 'n_intervals'),
    Input('redirect-interval', 'n_intervals'),
    prevent_initial_call=True
)

# Callback to handle Load Configuration File button
@callback(
    Output('onboarding-config-upload', 'style'),
    [Input('onboarding-load-config-btn', 'n_clicks')],
    prevent_initial_call=True
)
def trigger_config_upload(n_clicks):
    if n_clicks:
        # This is a workaround to trigger the hidden upload component
        return {'display': 'block', 'visibility': 'hidden', 'height': '0px'}
    return {'display': 'none'}

# Callback to handle config file loading
@callback(
    [Output('onboarding-alerts', 'children', allow_duplicate=True),
     Output('onboarding-data-dir-input', 'value'),
     Output('onboarding-composite-id-input', 'value')],
    [Input('onboarding-config-upload', 'contents')],
    [State('onboarding-config-upload', 'filename')],
    prevent_initial_call=True
)
def handle_config_upload(contents, filename):
    if not contents:
        return no_update, no_update, no_update

    try:
        # Decode the file
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)

        if filename.endswith('.toml'):
            import toml
            config_data = toml.loads(decoded.decode('utf-8'))
        elif filename.endswith('.json'):
            config_data = json.loads(decoded.decode('utf-8'))
        else:
            raise ValueError("Unsupported file format. Please upload a .toml or .json configuration file.")

        # Update current configuration
        config = get_config()

        # Apply loaded configuration
        config.DATA_DIR = config_data.get('data_dir', config.DATA_DIR)
        config.DEMOGRAPHICS_FILE = config_data.get('demographics_file', config.DEMOGRAPHICS_FILE)
        config.PRIMARY_ID_COLUMN = config_data.get('primary_id_column', config.PRIMARY_ID_COLUMN)
        config.SESSION_COLUMN = config_data.get('session_column', config.SESSION_COLUMN)
        config.COMPOSITE_ID_COLUMN = config_data.get('composite_id_column', config.COMPOSITE_ID_COLUMN)
        config.AGE_COLUMN = config_data.get('age_column', config.AGE_COLUMN)
        config.SEX_COLUMN = config_data.get('sex_column', config.SEX_COLUMN)

        # Save the updated configuration
        config.save_config()
        refresh_config()

        alert = dbc.Alert([
            html.I(className="bi bi-check-circle-fill me-2"),
            f"Successfully loaded configuration from '{filename}'"
        ], color="success", dismissable=True)

        return alert, config.DATA_DIR, config.COMPOSITE_ID_COLUMN

    except Exception as e:
        alert = dbc.Alert(f"Error loading configuration: {str(e)}", color="danger", dismissable=True)
        return alert, no_update, no_update

# Callback to update card styling based on progress
@callback(
    [Output('demo-config-card', 'className'),
     Output('demo-config-card', 'style'),
     Output('csv-linking-card', 'className'),
     Output('csv-linking-card', 'style'),
     Output('drag-drop-card', 'className'),
     Output('drag-drop-card', 'style')],
    [Input('onboarding-step-state', 'data'),
     Input('onboarding-age-column-dropdown', 'value'),
     Input('onboarding-sex-column-dropdown', 'value'),
     Input('onboarding-primary-id-dropdown', 'value')]
)
def update_card_styling(step_state, age_column, sex_column, primary_id):
    # Default styles (inactive)
    demo_class = "mb-4 border-secondary"
    demo_style = {'opacity': '0.7'}
    csv_class = "mb-4 border-secondary"
    csv_style = {'opacity': '0.7'}
    drag_class = "border-secondary"
    drag_style = {'opacity': '0.7'}

    # If demographics loaded, activate step 2 and 3
    if step_state and step_state.get('demographics_loaded'):
        demo_class = "mb-4 border-success shadow-sm"
        demo_style = {'opacity': '1'}
        csv_class = "mb-4 border-info shadow-sm"
        csv_style = {'opacity': '1'}

        # If required fields filled, activate step 4
        if age_column and sex_column and primary_id:
            drag_class = "border-success shadow-sm"
            drag_style = {'opacity': '1'}

    return demo_class, demo_style, csv_class, csv_style, drag_class, drag_style
