import json
import os

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import toml
from dash import Input, Output, State, callback, dcc, html

from config_manager import get_config, refresh_config

dash.register_page(__name__, path='/settings', name='Settings')

def create_settings_layout():
    """Create the settings page layout"""
    current_config = get_config()

    return dbc.Container([
        html.Div([
            html.H1("Application Settings", style={'display': 'inline-block', 'margin-right': '40px'}),
            html.Img(
                src="/assets/settings.png",
                style={
                    'height': '150px',
                    'display': 'inline-block',
                    'vertical-align': 'middle'
                }
            )
        ], style={'display': 'flex', 'align-items': 'center'}, className="mb-4"),

        # Success/Error alerts
        html.Div(id="settings-alerts"),

        # Data Directory Settings
        dbc.Card([
            dbc.CardHeader(html.H4("Data Directory Settings")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Data Directory Path"),
                        dbc.Input(
                            id="data-dir-input",
                            value=current_config.DATA_DIR,
                            placeholder="Path to CSV data files",
                            type="text"
                        ),
                        dbc.FormText("Directory containing your CSV data files")
                    ], width=6),
                    dbc.Col([
                        dbc.Label("Demographics File"),
                        dbc.Input(
                            id="demographics-file-input",
                            value=current_config.DEMOGRAPHICS_FILE,
                            placeholder="demographics.csv",
                            type="text"
                        ),
                        dbc.FormText("Main demographics file name")
                    ], width=6)
                ])
            ])
        ], className="mb-4"),

        # Column Mapping Settings
        dbc.Card([
            dbc.CardHeader(html.H4("Column Mapping Settings")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Primary ID Column"),
                        dbc.Input(
                            id="primary-id-input",
                            value=current_config.PRIMARY_ID_COLUMN,
                            placeholder="ursi",
                            type="text"
                        ),
                        dbc.FormText("Subject identifier column (e.g., ursi, subject_id)")
                    ], width=4),
                    dbc.Col([
                        dbc.Label("Session Column"),
                        dbc.Input(
                            id="session-column-input",
                            value=current_config.SESSION_COLUMN,
                            placeholder="session_num",
                            type="text"
                        ),
                        dbc.FormText("Session identifier for longitudinal data [optional]")
                    ], width=4),
                    dbc.Col([
                        dbc.Label("Study Site Column"),
                        dbc.Input(
                            id="study-site-column-input",
                            value=current_config.STUDY_SITE_COLUMN or "",
                            placeholder="site",
                            type="text"
                        ),
                        dbc.FormText("Study site/substudy identifier [optional]")
                    ], width=4)
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Composite ID Column"),
                        dbc.Input(
                            id="composite-id-input",
                            value=current_config.COMPOSITE_ID_COLUMN,
                            placeholder="customID",
                            type="text"
                        ),
                        dbc.FormText("Composite ID column name (auto-generated for longitudinal data) [optional]")
                    ], width=4)
                ])
            ])
        ], className="mb-4"),

        # Data Column Settings
        dbc.Card([
            dbc.CardHeader(html.H4("Data Column Configuration")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Age Column Name"),
                        dbc.Input(
                            id="age-column-input",
                            value=current_config.AGE_COLUMN,
                            placeholder="age",
                            type="text"
                        ),
                        dbc.FormText("Column name containing age data (e.g., age, Age, age_years)")
                    ], width=6)
                ])
            ])
        ], className="mb-4"),

        # Default Filter Settings
        dbc.Card([
            dbc.CardHeader(html.H4("Default Filter Settings")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Default Age Range"),
                        html.Div([
                            dcc.RangeSlider(
                                id="age-range-slider",
                                min=0,
                                max=120,
                                step=1,
                                value=[current_config.DEFAULT_AGE_SELECTION[0], current_config.DEFAULT_AGE_SELECTION[1]],
                                marks={i: str(i) for i in range(0, 121, 20)},
                                tooltip={"placement": "bottom", "always_visible": True}
                            )
                        ], className="mb-3"),
                        dbc.FormText("Default age range for filtering")
                    ], width=12)
                ])
            ])
        ], className="mb-4"),


        # Study Configuration
        dbc.Card([
            dbc.CardHeader(html.H4("Study Configuration")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Display Settings"),
                        dbc.Input(
                            id="max-display-rows",
                            value=current_config.MAX_DISPLAY_ROWS,
                            type="number",
                            min=10,
                            max=1000,
                            step=10
                        ),
                        dbc.FormText("Maximum rows to display in tables")
                    ], width=6),
                    dbc.Col([
                        html.P("Sessions are automatically detected from your data files.",
                               className="text-muted mt-4"),
                        html.P("No session configuration needed.",
                               className="text-muted")
                    ], width=6)
                ])
            ])
        ], className="mb-4"),

        # Action Buttons
        dbc.Card([
            dbc.CardHeader(html.H4("Settings Actions")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Import Settings File"),
                        dcc.Upload(
                            id="import-settings-upload",
                            children=dbc.Button("Choose File", color="primary", size="lg"),
                            multiple=False,
                            accept=".toml,.json"
                        ),
                        dbc.FormText("Upload a TOML or JSON configuration file")
                    ], width=4),
                    dbc.Col([
                        dbc.Label("Save Current Settings"),
                        html.Br(),
                        dbc.Button("Save Settings", id="save-settings-btn", color="primary", size="lg", className="w-100")
                    ], width=4),
                    dbc.Col([
                        dbc.Label("Reset to Defaults"),
                        html.Br(),
                        dbc.Button("Reset to Defaults", id="reset-settings-btn", color="warning", size="lg", className="w-100")
                    ], width=4)
                ])
            ])
        ], className="mb-4"),

        # Configuration Preview
        dbc.Card([
            dbc.CardHeader(html.H4("Current Configuration Preview")),
            dbc.CardBody([
                html.Pre(id="config-preview", style={"background-color": "#f8f9fa", "padding": "1rem"})
            ])
        ])

    ], fluid=True)


# Layout
layout = create_settings_layout()


# Callback for updating config preview
@callback(
    Output("config-preview", "children"),
    [Input("data-dir-input", "value"),
     Input("demographics-file-input", "value"),
     Input("primary-id-input", "value"),
     Input("session-column-input", "value"),
     Input("study-site-column-input", "value"),
     Input("composite-id-input", "value"),
     Input("age-column-input", "value"),
     Input("age-range-slider", "value"),
     Input("max-display-rows", "value")]
)
def update_config_preview(data_dir, demo_file, primary_id, session_col, study_site_col, composite_id,
                         age_col, age_range, max_rows):
    """Update the configuration preview"""
    current_config = get_config()
    preview_config = {
        "data_dir": data_dir or current_config.DATA_DIR,
        "demographics_file": demo_file or current_config.DEMOGRAPHICS_FILE,
        "primary_id_column": primary_id or current_config.PRIMARY_ID_COLUMN,
        "session_column": session_col or current_config.SESSION_COLUMN,
        "study_site_column": study_site_col or current_config.STUDY_SITE_COLUMN,
        "composite_id_column": composite_id or current_config.COMPOSITE_ID_COLUMN,
        "age_column": age_col or current_config.AGE_COLUMN,
        "default_age_min": (age_range or list(current_config.DEFAULT_AGE_SELECTION))[0],
        "default_age_max": (age_range or list(current_config.DEFAULT_AGE_SELECTION))[1],
        "max_display_rows": max_rows or current_config.MAX_DISPLAY_ROWS
    }

    return toml.dumps(preview_config)

# Callback for saving settings
@callback(
    [Output("settings-alerts", "children"),
     Output("app-config-store", "data")],
    [Input("save-settings-btn", "n_clicks"),
     Input("reset-settings-btn", "n_clicks")],
    [State("data-dir-input", "value"),
     State("demographics-file-input", "value"),
     State("primary-id-input", "value"),
     State("session-column-input", "value"),
     State("study-site-column-input", "value"),
     State("composite-id-input", "value"),
     State("age-column-input", "value"),
     State("age-range-slider", "value"),
     State("max-display-rows", "value")],
    prevent_initial_call=True
)
def handle_settings_actions(save_clicks, reset_clicks,
                           data_dir, demo_file, primary_id, session_col, study_site_col, composite_id,
                           age_col, age_range, max_rows):
    """Handle save and reset actions"""
    ctx = dash.callback_context
    if not ctx.triggered:
        return "", dash.no_update

    trigger = ctx.triggered[0]["prop_id"]
    current_config = get_config()

    if "save-settings-btn" in trigger:
        try:
            # Update config object
            current_config.DATA_DIR = data_dir or current_config.DATA_DIR
            current_config.DEMOGRAPHICS_FILE = demo_file or current_config.DEMOGRAPHICS_FILE
            current_config.PRIMARY_ID_COLUMN = primary_id or current_config.PRIMARY_ID_COLUMN
            current_config.SESSION_COLUMN = session_col or current_config.SESSION_COLUMN
            current_config.STUDY_SITE_COLUMN = study_site_col if study_site_col else None
            current_config.COMPOSITE_ID_COLUMN = composite_id or current_config.COMPOSITE_ID_COLUMN
            current_config.AGE_COLUMN = age_col or current_config.AGE_COLUMN

            if age_range:
                current_config.DEFAULT_AGE_SELECTION = tuple(age_range)

            if max_rows:
                current_config.MAX_DISPLAY_ROWS = max_rows


            # Save to file
            current_config.save_config()
            current_config.refresh_merge_detection()
            # Refresh the global config instance to pick up changes
            refresh_config()

            # Create config data for store
            config_data = {
                "data_dir": current_config.DATA_DIR,
                "demographics_file": current_config.DEMOGRAPHICS_FILE,
                "primary_id_column": current_config.PRIMARY_ID_COLUMN,
                "session_column": current_config.SESSION_COLUMN,
                "study_site_column": current_config.STUDY_SITE_COLUMN,
                "composite_id_column": current_config.COMPOSITE_ID_COLUMN,
                "age_column": current_config.AGE_COLUMN,
                "default_age_selection": list(current_config.DEFAULT_AGE_SELECTION),
                "max_display_rows": current_config.MAX_DISPLAY_ROWS,
                "timestamp": str(pd.Timestamp.now())
            }

            return dbc.Alert("Settings saved successfully!", color="success", dismissable=True), config_data

        except Exception as e:
            return dbc.Alert(f"Error saving settings: {str(e)}", color="danger", dismissable=True), dash.no_update

    elif "reset-settings-btn" in trigger:
        try:
            # Reset to defaults by deleting config file and reloading
            if os.path.exists(current_config.CONFIG_FILE_PATH):
                os.remove(current_config.CONFIG_FILE_PATH)

            # Reinitialize with defaults
            fresh_config = refresh_config()

            # Create fresh config data for store
            config_data = {
                "data_dir": fresh_config.DATA_DIR,
                "demographics_file": fresh_config.DEMOGRAPHICS_FILE,
                "primary_id_column": fresh_config.PRIMARY_ID_COLUMN,
                "session_column": fresh_config.SESSION_COLUMN,
                "study_site_column": fresh_config.STUDY_SITE_COLUMN,
                "composite_id_column": fresh_config.COMPOSITE_ID_COLUMN,
                "age_column": fresh_config.AGE_COLUMN,
                "default_age_selection": list(fresh_config.DEFAULT_AGE_SELECTION),
                "max_display_rows": fresh_config.MAX_DISPLAY_ROWS,
                "timestamp": str(pd.Timestamp.now())
            }

            return dbc.Alert("Settings reset to defaults!", color="info", dismissable=True), config_data

        except Exception as e:
            return dbc.Alert(f"Error resetting settings: {str(e)}", color="danger", dismissable=True), dash.no_update

    return "", dash.no_update

# Callback to initialize settings from config store only on first load
@callback(
    [Output("data-dir-input", "value"),
     Output("demographics-file-input", "value"),
     Output("primary-id-input", "value"),
     Output("session-column-input", "value"),
     Output("study-site-column-input", "value"),
     Output("composite-id-input", "value"),
     Output("age-column-input", "value"),
     Output("age-range-slider", "value"),
     Output("max-display-rows", "value")],
    [Input("data-dir-input", "id")],  # Use a dummy input that only triggers on page load
    prevent_initial_call=False
)
def initialize_settings_from_config(dummy_input):
    """Initialize settings form with values from current config on page load only"""
    current_config = get_config()
    return (
        current_config.DATA_DIR,
        current_config.DEMOGRAPHICS_FILE,
        current_config.PRIMARY_ID_COLUMN,
        current_config.SESSION_COLUMN,
        current_config.STUDY_SITE_COLUMN or "",
        current_config.COMPOSITE_ID_COLUMN,
        current_config.AGE_COLUMN,
        list(current_config.DEFAULT_AGE_SELECTION),
        current_config.MAX_DISPLAY_ROWS
    )

# Callback to update form values after successful save/reset/import (without interfering with typing)
@callback(
    [Output("data-dir-input", "value", allow_duplicate=True),
     Output("demographics-file-input", "value", allow_duplicate=True),
     Output("primary-id-input", "value", allow_duplicate=True),
     Output("session-column-input", "value", allow_duplicate=True),
     Output("study-site-column-input", "value", allow_duplicate=True),
     Output("composite-id-input", "value", allow_duplicate=True),
     Output("age-column-input", "value", allow_duplicate=True),
     Output("age-range-slider", "value", allow_duplicate=True),
     Output("max-display-rows", "value", allow_duplicate=True)],
    [Input("save-settings-btn", "n_clicks"),
     Input("reset-settings-btn", "n_clicks"),
     Input("import-settings-upload", "contents")],
    prevent_initial_call=True
)
def refresh_form_after_action(save_clicks, reset_clicks, import_contents):
    """Refresh form values only after explicit save/reset/import actions"""
    ctx = dash.callback_context
    if not ctx.triggered:
        return [dash.no_update] * 9

    # Only update if one of the action buttons was actually clicked
    trigger = ctx.triggered[0]["prop_id"]
    if any(action in trigger for action in ["save-settings-btn", "reset-settings-btn", "import-settings-upload"]):
        current_config = get_config()
        return (
            current_config.DATA_DIR,
            current_config.DEMOGRAPHICS_FILE,
            current_config.PRIMARY_ID_COLUMN,
            current_config.SESSION_COLUMN,
            current_config.STUDY_SITE_COLUMN or "",
            current_config.COMPOSITE_ID_COLUMN,
            current_config.AGE_COLUMN,
            list(current_config.DEFAULT_AGE_SELECTION),
            current_config.MAX_DISPLAY_ROWS
        )
    else:
        return [dash.no_update] * 9

# Callback for importing settings from file
@callback(
    [Output("settings-alerts", "children", allow_duplicate=True),
     Output("app-config-store", "data", allow_duplicate=True)],
    [Input("import-settings-upload", "contents")],
    [State("import-settings-upload", "filename")],
    prevent_initial_call=True
)
def import_settings_from_file(contents, filename):
    """Import settings from uploaded file"""
    if contents is None:
        return "", dash.no_update

    try:
        import base64

        # Decode the file contents
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)

        # Parse based on file extension
        if filename.endswith('.json'):
            imported_data = json.loads(decoded.decode('utf-8'))
        elif filename.endswith('.toml'):
            imported_data = toml.loads(decoded.decode('utf-8'))
        else:
            return dbc.Alert("Unsupported file format. Please use JSON or TOML files.",
                           color="danger", dismissable=True), dash.no_update

        # Update config
        current_config = get_config()
        if 'data_dir' in imported_data:
            current_config.DATA_DIR = imported_data['data_dir']
        if 'demographics_file' in imported_data:
            current_config.DEMOGRAPHICS_FILE = imported_data['demographics_file']
        if 'primary_id_column' in imported_data:
            current_config.PRIMARY_ID_COLUMN = imported_data['primary_id_column']
        if 'session_column' in imported_data:
            current_config.SESSION_COLUMN = imported_data['session_column']
        if 'study_site_column' in imported_data:
            current_config.STUDY_SITE_COLUMN = imported_data['study_site_column'] if imported_data['study_site_column'] else None
        if 'composite_id_column' in imported_data:
            current_config.COMPOSITE_ID_COLUMN = imported_data['composite_id_column']
        if 'age_column' in imported_data:
            current_config.AGE_COLUMN = imported_data['age_column']

        # Handle age range (could be in different formats)
        if 'default_age_selection' in imported_data:
            current_config.DEFAULT_AGE_SELECTION = tuple(imported_data['default_age_selection'])
        elif 'default_age_min' in imported_data and 'default_age_max' in imported_data:
            current_config.DEFAULT_AGE_SELECTION = (imported_data['default_age_min'], imported_data['default_age_max'])

        if 'max_display_rows' in imported_data:
            current_config.MAX_DISPLAY_ROWS = imported_data['max_display_rows']

        # Save the imported config
        current_config.save_config()
        current_config.refresh_merge_detection()
        # Refresh the global config instance to pick up changes
        refresh_config()

        # Create config data for store
        config_data = {
            "data_dir": current_config.DATA_DIR,
            "demographics_file": current_config.DEMOGRAPHICS_FILE,
            "primary_id_column": current_config.PRIMARY_ID_COLUMN,
            "session_column": current_config.SESSION_COLUMN,
            "study_site_column": current_config.STUDY_SITE_COLUMN,
            "composite_id_column": current_config.COMPOSITE_ID_COLUMN,
            "age_column": current_config.AGE_COLUMN,
            "default_age_selection": list(current_config.DEFAULT_AGE_SELECTION),
            "max_display_rows": current_config.MAX_DISPLAY_ROWS,
            "timestamp": str(pd.Timestamp.now())
        }

        return dbc.Alert(f"Settings imported successfully from {filename}!",
                        color="success", dismissable=True), config_data

    except Exception as e:
        return dbc.Alert(f"Error importing settings: {str(e)}",
                        color="danger", dismissable=True), dash.no_update
