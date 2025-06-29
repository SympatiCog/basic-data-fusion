import base64
import json
import logging

import dash
import dash_bootstrap_components as dbc
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
        html.Div([
            html.Img(src="/assets/onboarding_left.png", style={"height": "90px", "width": "auto"}, 
                     className="d-inline-block align-middle me-3"),
            html.H1("Application Setup", className="d-inline-block align-middle mb-0"),
            html.Img(src="/assets/onboarding_right.png", style={"height": "90px", "width": "auto"}, 
                     className="d-inline-block align-middle ms-3")
        ], className="text-center mb-4"),
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

    # Loading spinner for upload process
    dcc.Loading(
        id="upload-loading",
        type="default",
        children=html.Div(id="upload-loading-output")
    ),

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
                            dbc.Button('Choose File', color='success', size='sm'),  # TODO: Evaluate for better highlighting choice
                        ]),
                        className="mb-3"
                    ),
                    html.Div(id='onboarding-demographics-filename', className="mb-3"),

                    # Data directory selection
                    html.Label("Data Folder*", className="fw-bold mb-2"),
                    dbc.Input(
                        id='onboarding-data-dir-input',
                        value='data',
                        placeholder='data',
                        className="mb-2"
                    ),
                    html.Small("*Note: This is the location where data will be stored. You can use the default folder (data); all of your source data folder contents will be copied here.", className="text-muted mb-3 d-block"),

                    # Load Configuration File button
                    html.Hr(),
                    dcc.Upload(
                        id='onboarding-config-upload',
                        children=dbc.Button("Load Configuration File", color="secondary", className="w-100"),
                        accept=".toml,.json"
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
                                   className="text-center text-muted mb-2"),
                            html.P("Note: Duplicate demographics files will be automatically skipped",
                                   className="text-center text-muted small mb-0"),
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
                no_update, no_update, no_update)

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
                    no_update, no_update, alert)

        if df is None:
            alert = dbc.Alert("Failed to read the CSV file.", color="danger", dismissable=True)
            return (no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, no_update, alert)

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
                no_update, no_update, alert)


# Callback to enable drag & drop when configuration is ready
@callback(
    [Output('onboarding-final-upload', 'disabled'),
     Output('onboarding-final-upload', 'style'),
     Output('drag-drop-body', 'style'),
     Output('drag-drop-header', 'children'),
     Output('drag-drop-header', 'className')],
    [Input('onboarding-age-column-dropdown', 'value'),
     Input('onboarding-primary-id-dropdown', 'value')],
    [State('onboarding-step-state', 'data')]
)
def enable_drag_drop(age_column, primary_id, step_state):
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
    if age_column and primary_id:
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
     Output('onboarding-final-upload', 'children'),
     Output('upload-loading-output', 'children')],
    [Input('onboarding-final-upload', 'contents')],
    [State('onboarding-final-upload', 'filename'),
     State('onboarding-demographics-data', 'data'),
     State('onboarding-data-dir-input', 'value'),
     State('onboarding-age-column-dropdown', 'value'),
     State('onboarding-primary-id-dropdown', 'value'),
     State('onboarding-session-column-dropdown', 'value'),
     State('onboarding-study-site-dropdown', 'value'),
     State('onboarding-composite-id-input', 'value'),
     State('onboarding-demographics-upload', 'contents'),
     State('onboarding-demographics-upload', 'filename')],
    prevent_initial_call=True
)
def handle_final_upload(contents_list, filenames_list, demographics_data, data_dir,
                       age_column, primary_id, session_column, study_site_column, composite_id,
                       demographics_contents, demographics_filename):

    if not contents_list:
        return no_update, no_update, ""

    try:
        # Prepare configuration
        config = get_config()

        # Update configuration with user selections
        config.DATA_DIR = data_dir or 'data'
        config.DEMOGRAPHICS_FILE = demographics_filename or 'demographics.csv'
        config.AGE_COLUMN = age_column
        config.PRIMARY_ID_COLUMN = primary_id
        config.SESSION_COLUMN = session_column if session_column else None
        config.STUDY_SITE_COLUMN = study_site_column if study_site_column else None
        config.COMPOSITE_ID_COLUMN = composite_id or 'customID'

        # Save configuration and refresh merge detection
        config.save_config()
        config.refresh_merge_detection()
        refresh_config()

        # Force dataset preparation for longitudinal data if session column is configured
        if session_column:
            try:
                merge_keys = config.get_merge_keys()
                if merge_keys.is_longitudinal:
                    success, prep_actions = config.get_merge_strategy().prepare_datasets(config.DATA_DIR, merge_keys)
                    if success and prep_actions:
                        # Log the preparation actions for debugging
                        for action in prep_actions:
                            logging.info(f"Onboarding dataset preparation: {action}")
            except Exception as e:
                logging.error(f"Failed to prepare datasets during onboarding: {e}")

        # Prepare file contents for saving
        all_file_contents = []
        all_filenames = []

        # Add demographics file first
        if demographics_contents and demographics_filename:
            content_type, content_string = demographics_contents.split(',')
            demo_decoded = base64.b64decode(content_string)
            all_file_contents.append(demo_decoded)
            all_filenames.append(demographics_filename)

        # Add other uploaded files (excluding demographics file duplicates)
        demographics_filename_lower = demographics_filename.lower() if demographics_filename else None
        skipped_files = []

        if isinstance(contents_list, list):
            for content, filename in zip(contents_list, filenames_list):
                # Skip if this file has the same name as the demographics file
                if demographics_filename_lower and filename.lower() == demographics_filename_lower:
                    skipped_files.append(filename)
                    continue

                content_type, content_string = content.split(',')
                decoded = base64.b64decode(content_string)
                all_file_contents.append(decoded)
                all_filenames.append(filename)
        else:
            # Single file
            if demographics_filename_lower and filenames_list.lower() == demographics_filename_lower:
                skipped_files.append(filenames_list)
            else:
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
            success_content = [
                html.H5("Setup Complete!", className="alert-heading"),
                html.P("Your configuration has been saved and files have been uploaded successfully.")
            ]

            # Add information about skipped duplicate files
            if skipped_files:
                success_content.extend([
                    html.Hr(),
                    html.P([
                        html.Strong("Note: "),
                        f"Skipped {len(skipped_files)} duplicate file(s) that matched your demographics file: ",
                        ", ".join(skipped_files)
                    ], style={"color": "black"})
                ])

            success_content.extend([
                html.Hr(),
                html.P([
                    "Redirecting to the main application. Please wait a few seconds...",
                    dcc.Link("Click here if not redirected", href="/", className="alert-link")
                ])
            ])

            alert = dbc.Alert(success_content, color="success")

            # Add client-side redirect with verification
            alert.children.extend([
                dcc.Interval(id='redirect-interval', interval=1000, n_intervals=0, max_intervals=5),
                dcc.Store(id='config-verification-store', data={'verified': False})
            ])

        upload_status = html.Div([
            html.H4("Upload Complete!", className="text-success text-center mb-0"),
            html.P(f"Successfully uploaded {len(all_filenames)} files", className="text-center text-muted")
        ], className="text-center p-4")

        return alert, upload_status, ""

    except Exception as e:
        alert = dbc.Alert(f"Error during setup: {str(e)}", color="danger", dismissable=True)
        return alert, no_update, ""

# Callback to show loading message during upload
@callback(
    Output('upload-loading-output', 'children', allow_duplicate=True),
    Input('onboarding-final-upload', 'contents'),
    prevent_initial_call=True
)
def show_upload_loading(contents):
    if contents:
        return html.Div([
            html.P("Processing files and saving configuration...",
                   className="text-info text-center"),
            html.P("Please wait, this may take a moment.",
                   className="text-muted text-center small")
        ])
    return ""

# Verify configuration is properly updated before redirect
@callback(
    Output('config-verification-store', 'data'),
    Input('redirect-interval', 'n_intervals'),
    prevent_initial_call=True
)
def verify_config_update(n_intervals):
    """Verify that configuration has been properly updated with session column"""
    if n_intervals > 0:
        try:
            from config_manager import get_config
            from utils import get_table_info

            # Get fresh config and check if longitudinal detection works
            config = get_config()
            merge_keys = config.get_merge_keys()

            # Try to get table info to see if it detects longitudinal properly
            _, _, _, _, _, merge_keys_dict, _, _, _, _ = get_table_info(config)

            # Check if session column is properly configured and detected
            is_properly_configured = (
                config.SESSION_COLUMN and
                merge_keys.is_longitudinal and
                merge_keys_dict.get('is_longitudinal', False)
            )

            return {'verified': is_properly_configured, 'attempts': n_intervals}

        except Exception as e:
            return {'verified': False, 'attempts': n_intervals, 'error': str(e)}

    return {'verified': False, 'attempts': 0}

# Clear session stores before redirect
@callback(
    [Output('available-tables-store', 'clear_data'),
     Output('demographics-columns-store', 'clear_data'),
     Output('behavioral-columns-store', 'clear_data'),
     Output('column-dtypes-store', 'clear_data'),
     Output('column-ranges-store', 'clear_data'),
     Output('merge-keys-store', 'clear_data'),
     Output('session-values-store', 'clear_data'),
     Output('all-messages-store', 'clear_data')],
    Input('config-verification-store', 'data'),
    prevent_initial_call=True
)
def clear_stores_before_redirect(verification_data):
    """Clear session stores when config is verified"""
    if verification_data and verification_data.get('verified', False):
        return True, True, True, True, True, True, True, True
    return False, False, False, False, False, False, False, False

# Client-side callback for redirect after successful setup and verification
clientside_callback(
    """
    function(verification_data) {
        if (verification_data && verification_data.verified) {
            // Config is verified, redirect now using location.replace to avoid back button issues
            setTimeout(function() {
                window.location.replace('/');
            }, 500);
        } else if (verification_data && verification_data.attempts >= 5) {
            // Max attempts reached, redirect anyway with warning
            console.warn('Configuration verification failed, redirecting anyway');
            setTimeout(function() {
                window.location.replace('/');
            }, 500);
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output('config-verification-store', 'id'),  # Dummy output
    Input('config-verification-store', 'data'),
    prevent_initial_call=True
)


# Callback to handle config file loading
@callback(
    [Output('onboarding-alerts', 'children', allow_duplicate=True),
     Output('onboarding-data-dir-input', 'value'),
     Output('onboarding-composite-id-input', 'value'),
     Output('onboarding-age-column-dropdown', 'value'),
     Output('onboarding-primary-id-dropdown', 'value'),
     Output('onboarding-session-column-dropdown', 'value'),
     Output('onboarding-study-site-dropdown', 'value')],
    [Input('onboarding-config-upload', 'contents')],
    [State('onboarding-config-upload', 'filename')],
    prevent_initial_call=True
)
def handle_config_upload(contents, filename):
    if not contents:
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update

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
        config.STUDY_SITE_COLUMN = config_data.get('study_site_column', config.STUDY_SITE_COLUMN)

        # Save the updated configuration and refresh merge detection
        config.save_config()
        config.refresh_merge_detection()
        refresh_config()

        # Force dataset preparation for longitudinal data if session column is configured
        if config.SESSION_COLUMN:
            try:
                merge_keys = config.get_merge_keys()
                if merge_keys.is_longitudinal:
                    success, prep_actions = config.get_merge_strategy().prepare_datasets(config.DATA_DIR, merge_keys)
                    if success and prep_actions:
                        # Log the preparation actions for debugging
                        for action in prep_actions:
                            logging.info(f"Config upload dataset preparation: {action}")
            except Exception as e:
                logging.error(f"Failed to prepare datasets during config upload: {e}")

        alert = dbc.Alert([
            html.I(className="bi bi-check-circle-fill me-2"),
            f"Successfully loaded configuration from '{filename}'"
        ], color="success", dismissable=True)

        return (alert, config.DATA_DIR, config.COMPOSITE_ID_COLUMN,
                config.AGE_COLUMN, config.PRIMARY_ID_COLUMN,
                config.SESSION_COLUMN, config.STUDY_SITE_COLUMN)

    except Exception as e:
        alert = dbc.Alert(f"Error loading configuration: {str(e)}", color="danger", dismissable=True)
        return alert, no_update, no_update, no_update, no_update, no_update, no_update

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
     Input('onboarding-primary-id-dropdown', 'value')]
)
def update_card_styling(step_state, age_column, primary_id):
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
        if age_column and primary_id:
            drag_class = "border-success shadow-sm"
            drag_style = {'opacity': '1'}

    return demo_class, demo_style, csv_class, csv_style, drag_class, drag_style
