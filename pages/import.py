import base64

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html

from config_manager import get_config

# Import utility functions
from utils import (
    FileActionChoice,
    check_for_duplicate_files,
    get_table_info,
    save_uploaded_files_to_data_dir,
    validate_csv_file,
)

dash.register_page(__name__, path='/import', title='Import Data')

# Note: We get fresh config in callbacks to pick up changes from settings

layout = dbc.Container([
    html.Div([
        html.H1("Data Import", style={'display': 'inline-block', 'margin-right': '40px'}),
        # html.Img(
        #     src="/assets/importer.png",
        #     style={
        #         'height': '75px',
        #         'display': 'inline-block',
        #         'vertical-align': 'middle'
        #     }
        # )
    ], style={'display': 'flex', 'align-items': 'center'}, className="mb-4"),

    # Instructions Card
    dbc.Card([
        dbc.CardHeader(html.H4("How to Import Data")),
        dbc.CardBody([
            html.P("Upload your CSV data files to get started. The system will:"),
            html.Ul([
                html.Li("Validate file formats and structure"),
                html.Li("Check for required ID columns based on your configuration"),
                html.Li("Automatically detect longitudinal vs cross-sectional data"),
                html.Li("Create composite IDs for longitudinal data if needed"),
                html.Li("Generate merge strategy information")
            ]),
            html.Div(id="import-config-info")
        ])
    ], className="mb-4"),

    # Upload Area
    dbc.Card([
        dbc.CardHeader(html.H4("Upload CSV Files")),
        dbc.CardBody([
            dcc.Upload(
                id='import-upload-data',
                children=html.Div([
                    html.I(className="bi bi-cloud-upload display-1 text-primary mb-3"),
                    html.H5("Drag and Drop or Click to Select Files"),
                    html.P("Supports multiple CSV files", className="text-muted")
                ], className="text-center p-5"),
                style={
                    'width': '100%',
                    'height': '200px',
                    'lineHeight': '60px',
                    'borderWidth': '2px',
                    'borderStyle': 'dashed',
                    'borderRadius': '10px',
                    'borderColor': '#007bff',
                    'textAlign': 'center',
                    'cursor': 'pointer'
                },
                style_active={
                    'borderColor': '#28a745',
                    'backgroundColor': '#f8f9fa'
                },
                multiple=True
            )
        ])
    ], className="mb-4"),

    # Upload Status
    html.Div(id='import-upload-status'),

    # Duplicate Files Modal
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Duplicate Files Found")),
        dbc.ModalBody([
            html.P("Some files you're uploading already exist. Please choose how to handle each duplicate:"),
            html.Div(id='duplicate-files-list')
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel All", id="duplicate-cancel-all-btn", color="secondary", className="me-2"),
            dbc.Button("Apply Choices", id="duplicate-apply-btn", color="primary")
        ])
    ], id="duplicate-files-modal", is_open=False, size="lg"),

    # Current Data Summary
    dbc.Card([
        dbc.CardHeader(html.H4("Current Data Status")),
        dbc.CardBody([
            html.Div(id='import-data-summary')
        ])
    ], className="mb-4"),

    # Data Management Actions
    dbc.Card([
        dbc.CardHeader(html.H4("Data Management")),
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    dbc.Button(
                        [html.I(className="bi bi-arrow-clockwise me-2"), "Refresh Data Status"],
                        id="import-refresh-btn",
                        color="primary",
                        outline=True
                    )
                ], width="auto"),
                dbc.Col([
                    dbc.Button(
                        [html.I(className="bi bi-gear me-2"), "Go to Settings"],
                        href="/settings",
                        color="secondary",
                        outline=True,
                        external_link=True
                    )
                ], width="auto"),
                dbc.Col([
                    dbc.Button(
                        [html.I(className="bi bi-search me-2"), "Go to Query"],
                        href="/",
                        color="success",
                        external_link=True,
                        disabled=True,
                        id="import-start-analysis-btn"
                    )
                ], width="auto")
            ])
        ])
    ]),

    # Data stores for managing upload state
    dcc.Store(id='upload-data-store', data={}),
    dcc.Store(id='duplicate-choices-store', data={})

], fluid=True)


@callback(
    Output("import-config-info", "children"),
    Input("import-config-info", "id")
)
def update_config_info(_):
    """Display current configuration info"""
    config = get_config()
    return dbc.Alert([
        html.I(className="bi bi-info-circle me-2"),
        html.Strong("Configuration: "),
        f"Make sure your data has columns named '{config.PRIMARY_ID_COLUMN}' for subject IDs"
        f"{f' and {config.SESSION_COLUMN} for session data' if config.SESSION_COLUMN else ''}. "
        f"You can change these column names in the Settings page."
    ], color="info", className="mt-3")


def create_upload_status_card(messages, num_files=0):
    """Create a status card for upload results"""
    if not messages:
        return html.Div()

    # Count success and error messages
    error_count = sum(1 for msg in messages if 'alert-danger' in str(msg) or 'color: red' in str(msg))
    success_count = sum(1 for msg in messages if 'alert-success' in str(msg) or 'color: green' in str(msg))

    # Determine card color and styling based on results
    if error_count > 0 and success_count > 0:
        # Partial success - some files succeeded, some failed
        card_color = "warning"
        header_icon = html.I(className="bi bi-exclamation-triangle-fill me-2")
        header_text = f"Upload Results ({num_files} files) - {success_count} successful, {error_count} failed"
    elif error_count > 0:
        # All failed
        card_color = "danger"
        header_icon = html.I(className="bi bi-x-circle-fill me-2")
        header_text = f"Upload Results ({num_files} files) - All failed"
    else:
        # All successful
        card_color = "success"
        header_icon = html.I(className="bi bi-check-circle-fill me-2")
        header_text = f"Upload Results ({num_files} files) - All successful"

    return dbc.Card([
        dbc.CardHeader([
            html.H5([
                header_icon,
                header_text
            ], className="mb-0"),
        ]),
        dbc.CardBody(messages)
    ], color=card_color, outline=True, className="mb-3")


# Step 1: Handle initial upload and check for duplicates
@callback(
    [Output('upload-data-store', 'data'),
     Output('duplicate-files-modal', 'is_open'),
     Output('duplicate-files-list', 'children'),
     Output('import-upload-status', 'children')],
    Input('import-upload-data', 'contents'),
    [State('import-upload-data', 'filename'),
     State('import-upload-data', 'last_modified')],
    prevent_initial_call=True
)
def handle_initial_upload(list_of_contents, list_of_names, list_of_dates):
    """Handle initial file upload and check for duplicates"""
    if not list_of_contents:
        return {}, False, [], html.Div()

    config = get_config()
    messages = []
    saved_file_names = []
    file_byte_contents = []

    # Prepare config parameters for validation
    config_params = {
        'primary_id_column': config.PRIMARY_ID_COLUMN,
        'session_column': config.SESSION_COLUMN,
        'composite_id_column': config.COMPOSITE_ID_COLUMN,
        'age_column': config.AGE_COLUMN,
        'sex_column': config.SEX_COLUMN,
        'study_site_column': config.STUDY_SITE_COLUMN
    }
    
    # Validate all files first
    for c, n in zip(list_of_contents, list_of_names):
        try:
            content_type, content_string = c.split(',')
            decoded = base64.b64decode(content_string)

            # Basic CSV validation
            validation_errors, df = validate_csv_file(decoded, n)

            if validation_errors:
                for error in validation_errors:
                    messages.append(dbc.Alert(f"Error with {n}: {error}", color="danger", className="mb-1"))
            elif df is not None:
                # Additional config-based column validation
                from file_handling.csv_utils import validate_csv_columns_against_config
                column_valid, column_errors = validate_csv_columns_against_config(df, n, config_params)
                
                if not column_valid:
                    for error in column_errors:
                        messages.append(dbc.Alert(f"Error with {n}: {error}", color="danger", className="mb-1"))
                else:
                    messages.append(dbc.Alert(f"File {n} is valid.", color="success", className="mb-1"))
                    file_byte_contents.append(decoded)
                    saved_file_names.append(n)
            else:
                messages.append(dbc.Alert(f"Error with {n}: Could not parse CSV file", color="danger", className="mb-1"))

        except Exception as e:
            messages.append(dbc.Alert(f"Error processing file {n}: {str(e)}", color="danger", className="mb-1"))
            continue

    # Continue with valid files even if some files failed validation
    if not file_byte_contents:
        # No valid files to process
        num_files = len(list_of_names) if list_of_names else 0
        upload_status = create_upload_status_card(messages, num_files)
        return {}, False, [], upload_status

    # Check for duplicate files
    duplicates, non_duplicate_indices = check_for_duplicate_files(file_byte_contents, saved_file_names, config.DATA_DIR)

    # Store upload data for later use
    upload_data = {
        'file_contents': [base64.b64encode(content).decode() for content in file_byte_contents],
        'filenames': saved_file_names,
        'duplicates': [
            {
                'original_filename': dup.original_filename,
                'safe_filename': dup.safe_filename,
                'existing_path': dup.existing_path
            } for dup in duplicates
        ],
        'non_duplicate_indices': non_duplicate_indices
    }

    if duplicates:
        # Show duplicate handling modal
        duplicate_components = []
        for dup in duplicates:
            duplicate_components.append(
                dbc.Card([
                    dbc.CardHeader(html.H6(f"File: {dup.original_filename}")),
                    dbc.CardBody([
                        html.P(f"A file named '{dup.safe_filename}' already exists in the data directory."),
                        dbc.RadioItems(
                            options=[
                                {"label": "Replace existing file", "value": "replace"},
                                {"label": "Save with new name", "value": "rename"},
                                {"label": "Cancel this file", "value": "cancel"}
                            ],
                            value="rename",
                            id={"type": "duplicate-choice", "filename": dup.original_filename},
                            className="mb-2"
                        ),
                        dbc.Input(
                            id={"type": "new-filename", "filename": dup.original_filename},
                            placeholder="Enter new filename (including .csv)",
                            value=f"{dup.safe_filename[:-4]}_new.csv",
                            style={"display": "block"}  # Will be controlled by callback
                        )
                    ])
                ], className="mb-3")
            )

        return upload_data, True, duplicate_components, html.Div()
    else:
        # No duplicates, proceed with saving
        # Prepare config parameters for validation and composite ID creation
        config_params = {
            'primary_id_column': config.PRIMARY_ID_COLUMN,
            'session_column': config.SESSION_COLUMN,
            'composite_id_column': config.COMPOSITE_ID_COLUMN,
            'age_column': config.AGE_COLUMN,
            'sex_column': config.SEX_COLUMN,
            'study_site_column': config.STUDY_SITE_COLUMN
        }
        
        success_msgs, error_msgs = save_uploaded_files_to_data_dir(
            file_byte_contents, 
            saved_file_names, 
            config.DATA_DIR, 
            duplicate_actions=None, 
            sanitize_columns=True, 
            config_params=config_params
        )

        for msg in success_msgs:
            messages.append(dbc.Alert(msg, color="success", className="mb-1"))
        for err_msg in error_msgs:
            messages.append(dbc.Alert(err_msg, color="danger", className="mb-1"))

        num_files = len(saved_file_names)
        upload_status = create_upload_status_card(messages, num_files)

        return {}, False, [], upload_status

# Step 2: Handle duplicate choices and save files
@callback(
    [Output('import-upload-status', 'children', allow_duplicate=True),
     Output('duplicate-files-modal', 'is_open', allow_duplicate=True),
     Output('import-data-summary', 'children')],
    [Input('duplicate-apply-btn', 'n_clicks'),
     Input('duplicate-cancel-all-btn', 'n_clicks'),
     Input('import-refresh-btn', 'n_clicks')],
    [State('upload-data-store', 'data'),
     State({"type": "duplicate-choice", "filename": dash.dependencies.ALL}, "value"),
     State({"type": "new-filename", "filename": dash.dependencies.ALL}, "value"),
     State({"type": "duplicate-choice", "filename": dash.dependencies.ALL}, "id")],
    prevent_initial_call=True
)
def handle_duplicate_choices_and_refresh(apply_clicks, cancel_clicks, refresh_clicks, upload_data, choices, new_filenames, choice_ids):
    """Handle user's duplicate file choices and refresh data summary"""
    config = get_config()
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

    upload_status = html.Div()

    # Handle duplicate file choices
    if triggered_id in ['duplicate-apply-btn', 'duplicate-cancel-all-btn'] and upload_data:
        messages = []

        if triggered_id == 'duplicate-cancel-all-btn':
            messages.append(dbc.Alert("Upload cancelled by user.", color="info", className="mb-1"))
        else:
            # Process duplicate choices
            duplicate_actions = {}

            for i, choice_id in enumerate(choice_ids):
                filename = choice_id['filename']
                choice = choices[i] if i < len(choices) else 'cancel'
                new_filename = new_filenames[i] if i < len(new_filenames) and choice == 'rename' else None

                duplicate_actions[filename] = FileActionChoice(
                    action=choice,
                    new_filename=new_filename
                )

            # Reconstruct file contents from base64
            file_contents = [base64.b64decode(content) for content in upload_data['file_contents']]
            filenames = upload_data['filenames']

            # Prepare config parameters for validation and composite ID creation
            config_params = {
                'primary_id_column': config.PRIMARY_ID_COLUMN,
                'session_column': config.SESSION_COLUMN,
                'composite_id_column': config.COMPOSITE_ID_COLUMN,
                'age_column': config.AGE_COLUMN,
                'sex_column': config.SEX_COLUMN,
                'study_site_column': config.STUDY_SITE_COLUMN
            }
            
            # Save files with user choices
            success_msgs, error_msgs = save_uploaded_files_to_data_dir(
                file_contents, filenames, config.DATA_DIR, duplicate_actions=duplicate_actions, config_params=config_params
            )

            for msg in success_msgs:
                messages.append(dbc.Alert(msg, color="success", className="mb-1"))
            for err_msg in error_msgs:
                messages.append(dbc.Alert(err_msg, color="danger", className="mb-1"))

        num_files = len(upload_data.get('filenames', []))
        upload_status = create_upload_status_card(messages, num_files)

    # Get current data summary
    try:
        (behavioral_tables, demographics_columns, behavioral_columns_by_table,
         col_dtypes, col_ranges, merge_keys_dict, actions_taken,
         session_values, is_empty_state, all_messages) = get_table_info(config)

        if is_empty_state:
            data_summary = dbc.Alert([
                html.I(className="bi bi-exclamation-triangle me-2"),
                html.Strong("No data found. "),
                "Upload CSV files above to get started."
            ], color="warning")
        else:
            # Create summary
            total_tables = len(behavioral_tables) + (1 if demographics_columns else 0)

            summary_items = [
                html.H6("Data Overview", className="mb-3"),
                html.P(f"📊 Total tables: {total_tables}"),
                html.P(f"👥 Demographics columns: {len(demographics_columns)}"),
                html.P(f"📈 Behavioral tables: {len(behavioral_tables)}")
            ]

            if merge_keys_dict:
                from utils import MergeKeys
                mk = MergeKeys.from_dict(merge_keys_dict)
                summary_items.extend([
                    html.Hr(),
                    html.H6("Merge Strategy", className="mb-2"),
                    html.P(f"🔗 Type: {'Longitudinal' if mk.is_longitudinal else 'Cross-sectional'}"),
                    html.P(f"🆔 Primary ID: {mk.primary_id}"),
                ])
                if mk.is_longitudinal:
                    summary_items.append(html.P(f"📅 Session ID: {mk.session_id}"))
                    if session_values:
                        summary_items.append(html.P(f"📋 Sessions found: {len(session_values)}"))

            if actions_taken:
                summary_items.extend([
                    html.Hr(),
                    html.H6("Recent Actions", className="mb-2"),
                    html.Ul([html.Li(action) for action in actions_taken])
                ])

            data_summary = html.Div(summary_items)

    except Exception as e:
        data_summary = dbc.Alert(f"Error loading data status: {str(e)}", color="danger")

    # Close modal if it was a duplicate choice action
    modal_open = False if triggered_id in ['duplicate-apply-btn', 'duplicate-cancel-all-btn'] else dash.no_update

    return upload_status, modal_open, data_summary

# Step 3: Control new filename input visibility
@callback(
    Output({"type": "new-filename", "filename": dash.dependencies.MATCH}, "style"),
    Input({"type": "duplicate-choice", "filename": dash.dependencies.MATCH}, "value"),
    prevent_initial_call=True
)
def toggle_filename_input(choice):
    """Show/hide new filename input based on user choice"""
    if choice == "rename":
        return {"display": "block"}
    else:
        return {"display": "none"}


@callback(
    Output('import-start-analysis-btn', 'disabled'),
    Input('import-data-summary', 'children'),
    prevent_initial_call=False
)
def update_analysis_button(data_summary):
    """Enable analysis button when data is available"""
    # Simple check - if data_summary doesn't contain "No data found", enable the button
    if data_summary and isinstance(data_summary, dict):
        return False  # Enable if we have data summary content
    return True  # Keep disabled if no data
