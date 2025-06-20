import dash
from dash import dcc, html, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
from pathlib import Path
import json
import toml
import os
import pandas as pd
from utils import Config

dash.register_page(__name__, path='/settings', name='Settings')

# Initialize config instance
def get_config():
    """Get current config instance"""
    return Config()

config = get_config()

def create_settings_layout():
    """Create the settings page layout"""
    current_config = get_config()
    
    return dbc.Container([
        html.H1("Application Settings", className="mb-4"),
        
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
                        dbc.FormText("Session identifier for longitudinal data")
                    ], width=4),
                    dbc.Col([
                        dbc.Label("Composite ID Column"),
                        dbc.Input(
                            id="composite-id-input",
                            value=current_config.COMPOSITE_ID_COLUMN,
                            placeholder="customID",
                            type="text"
                        ),
                        dbc.FormText("Composite ID column name (auto-generated)")
                    ], width=4)
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
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Default Sex Selection"),
                        dcc.Checklist(
                            id="sex-selection-checklist",
                            options=[
                                {"label": sex, "value": sex} 
                                for sex in current_config.SEX_OPTIONS
                            ],
                            value=current_config.DEFAULT_SEX_SELECTION,
                            inline=True
                        ),
                        dbc.FormText("Default sex categories to include")
                    ], width=12)
                ])
            ])
        ], className="mb-4"),
        
        # Sex Mapping Settings
        dbc.Card([
            dbc.CardHeader(html.H4("Sex Mapping Settings")),
            dbc.CardBody([
                html.P("Configure how string sex values map to numeric codes:"),
                html.Div(id="sex-mapping-container"),
                dbc.Button("Add Sex Mapping", id="add-sex-mapping-btn", color="secondary", size="sm", className="mt-2")
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
                            children=dbc.Button("Choose File", color="secondary", outline=True, size="lg"),
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

def create_sex_mapping_row(sex_key, sex_value, row_id):
    """Create a row for sex mapping editing"""
    return dbc.Row([
        dbc.Col([
            dbc.Input(
                id={"type": "sex-key", "index": row_id},
                value=sex_key,
                placeholder="Sex label (e.g., Female)"
            )
        ], width=5),
        dbc.Col([
            dbc.Input(
                id={"type": "sex-value", "index": row_id},
                value=sex_value,
                type="number",
                step=0.1,
                placeholder="Numeric code (e.g., 1.0)"
            )
        ], width=5),
        dbc.Col([
            dbc.Button("×", id={"type": "remove-sex-mapping", "index": row_id}, 
                      color="danger", size="sm")
        ], width=2)
    ], className="mb-2")

# Layout
layout = create_settings_layout()

# Callbacks for sex mapping management
@callback(
    Output("sex-mapping-container", "children"),
    [Input("add-sex-mapping-btn", "n_clicks"),
     Input({"type": "remove-sex-mapping", "index": dash.dependencies.ALL}, "n_clicks")],
    [State("sex-mapping-container", "children")],
    prevent_initial_call=False
)
def manage_sex_mappings(add_clicks, remove_clicks, current_children):
    """Manage adding and removing sex mapping rows"""
    ctx = dash.callback_context
    current_config = get_config()
    
    if not current_children:
        # Initialize with current sex mappings
        current_children = []
        for i, (sex_key, sex_value) in enumerate(current_config.SEX_MAPPING.items()):
            current_children.append(create_sex_mapping_row(sex_key, sex_value, i))
    
    if ctx.triggered:
        trigger = ctx.triggered[0]["prop_id"]
        if "add-sex-mapping-btn" in trigger:
            # Add new row
            new_id = len(current_children)
            current_children.append(create_sex_mapping_row("", 0.0, new_id))
        elif "remove-sex-mapping" in trigger:
            # Remove specific row
            trigger_data = json.loads(trigger.split(".")[0])
            row_to_remove = trigger_data["index"]
            current_children = [child for i, child in enumerate(current_children) if i != row_to_remove]
            # Re-index remaining children
            for i, child in enumerate(current_children):
                child["props"]["children"][0]["props"]["children"]["props"]["id"]["index"] = i
                child["props"]["children"][1]["props"]["children"]["props"]["id"]["index"] = i
                child["props"]["children"][2]["props"]["children"]["props"]["id"]["index"] = i
    
    return current_children

# Callback for updating config preview
@callback(
    Output("config-preview", "children"),
    [Input("data-dir-input", "value"),
     Input("demographics-file-input", "value"),
     Input("primary-id-input", "value"),
     Input("session-column-input", "value"),
     Input("composite-id-input", "value"),
     Input("age-range-slider", "value"),
     Input("sex-selection-checklist", "value"),
     Input("max-display-rows", "value")]
)
def update_config_preview(data_dir, demo_file, primary_id, session_col, composite_id, 
                         age_range, sex_selection, max_rows):
    """Update the configuration preview"""
    current_config = get_config()
    preview_config = {
        "data_dir": data_dir or current_config.DATA_DIR,
        "demographics_file": demo_file or current_config.DEMOGRAPHICS_FILE,
        "primary_id_column": primary_id or current_config.PRIMARY_ID_COLUMN,
        "session_column": session_col or current_config.SESSION_COLUMN,
        "composite_id_column": composite_id or current_config.COMPOSITE_ID_COLUMN,
        "default_age_min": (age_range or list(current_config.DEFAULT_AGE_SELECTION))[0],
        "default_age_max": (age_range or list(current_config.DEFAULT_AGE_SELECTION))[1],
        "default_sex_selection": sex_selection or current_config.DEFAULT_SEX_SELECTION,
        "max_display_rows": max_rows or current_config.MAX_DISPLAY_ROWS,
        "sex_mapping": current_config.SEX_MAPPING
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
     State("composite-id-input", "value"),
     State("age-range-slider", "value"),
     State("sex-selection-checklist", "value"),
     State("max-display-rows", "value"),
     State({"type": "sex-key", "index": dash.dependencies.ALL}, "value"),
     State({"type": "sex-value", "index": dash.dependencies.ALL}, "value")],
    prevent_initial_call=True
)
def handle_settings_actions(save_clicks, reset_clicks,
                           data_dir, demo_file, primary_id, session_col, composite_id,
                           age_range, sex_selection, max_rows,
                           sex_keys, sex_values):
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
            current_config.COMPOSITE_ID_COLUMN = composite_id or current_config.COMPOSITE_ID_COLUMN
            
            if age_range:
                current_config.DEFAULT_AGE_SELECTION = tuple(age_range)
            
            if sex_selection:
                current_config.DEFAULT_SEX_SELECTION = sex_selection
            
            if max_rows:
                current_config.MAX_DISPLAY_ROWS = max_rows
            
            # Update sex mapping
            if sex_keys and sex_values:
                new_sex_mapping = {}
                for key, value in zip(sex_keys, sex_values):
                    if key and value is not None:
                        new_sex_mapping[key] = float(value)
                if new_sex_mapping:
                    current_config.SEX_MAPPING = new_sex_mapping
            
            # Save to file
            current_config.save_config()
            current_config.refresh_merge_detection()
            
            # Create config data for store
            config_data = {
                "data_dir": current_config.DATA_DIR,
                "demographics_file": current_config.DEMOGRAPHICS_FILE,
                "primary_id_column": current_config.PRIMARY_ID_COLUMN,
                "session_column": current_config.SESSION_COLUMN,
                "composite_id_column": current_config.COMPOSITE_ID_COLUMN,
                "default_age_selection": list(current_config.DEFAULT_AGE_SELECTION),
                "default_sex_selection": current_config.DEFAULT_SEX_SELECTION,
                "sex_mapping": current_config.SEX_MAPPING,
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
            fresh_config = Config()
            
            # Create fresh config data for store
            config_data = {
                "data_dir": fresh_config.DATA_DIR,
                "demographics_file": fresh_config.DEMOGRAPHICS_FILE,
                "primary_id_column": fresh_config.PRIMARY_ID_COLUMN,
                "session_column": fresh_config.SESSION_COLUMN,
                "composite_id_column": fresh_config.COMPOSITE_ID_COLUMN,
                "default_age_selection": list(fresh_config.DEFAULT_AGE_SELECTION),
                "default_sex_selection": fresh_config.DEFAULT_SEX_SELECTION,
                "sex_mapping": fresh_config.SEX_MAPPING,
                "max_display_rows": fresh_config.MAX_DISPLAY_ROWS,
                "timestamp": str(pd.Timestamp.now())
            }
            
            return dbc.Alert("Settings reset to defaults!", color="info", dismissable=True), config_data
        
        except Exception as e:
            return dbc.Alert(f"Error resetting settings: {str(e)}", color="danger", dismissable=True), dash.no_update
    
    return "", dash.no_update

# Callback to initialize settings from config store
@callback(
    [Output("data-dir-input", "value"),
     Output("demographics-file-input", "value"),
     Output("primary-id-input", "value"),
     Output("session-column-input", "value"),
     Output("composite-id-input", "value"),
     Output("age-range-slider", "value"),
     Output("sex-selection-checklist", "value"),
     Output("max-display-rows", "value")],
    [Input("app-config-store", "data")],
    prevent_initial_call=False
)
def initialize_settings_from_store(config_data):
    """Initialize settings form with values from config store or current config"""
    current_config = get_config()
    if config_data:
        return (
            config_data.get("data_dir", current_config.DATA_DIR),
            config_data.get("demographics_file", current_config.DEMOGRAPHICS_FILE),
            config_data.get("primary_id_column", current_config.PRIMARY_ID_COLUMN),
            config_data.get("session_column", current_config.SESSION_COLUMN),
            config_data.get("composite_id_column", current_config.COMPOSITE_ID_COLUMN),
            config_data.get("default_age_selection", list(current_config.DEFAULT_AGE_SELECTION)),
            config_data.get("default_sex_selection", current_config.DEFAULT_SEX_SELECTION),
            config_data.get("max_display_rows", current_config.MAX_DISPLAY_ROWS)
        )
    else:
        # Use current config values
        return (
            current_config.DATA_DIR,
            current_config.DEMOGRAPHICS_FILE,
            current_config.PRIMARY_ID_COLUMN,
            current_config.SESSION_COLUMN,
            current_config.COMPOSITE_ID_COLUMN,
            list(current_config.DEFAULT_AGE_SELECTION),
            current_config.DEFAULT_SEX_SELECTION,
            current_config.MAX_DISPLAY_ROWS
        )

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
        if 'composite_id_column' in imported_data:
            current_config.COMPOSITE_ID_COLUMN = imported_data['composite_id_column']
        
        # Handle age range (could be in different formats)
        if 'default_age_selection' in imported_data:
            current_config.DEFAULT_AGE_SELECTION = tuple(imported_data['default_age_selection'])
        elif 'default_age_min' in imported_data and 'default_age_max' in imported_data:
            current_config.DEFAULT_AGE_SELECTION = (imported_data['default_age_min'], imported_data['default_age_max'])
        
        if 'sex_mapping' in imported_data:
            current_config.SEX_MAPPING = imported_data['sex_mapping']
        if 'default_sex_selection' in imported_data:
            current_config.DEFAULT_SEX_SELECTION = imported_data['default_sex_selection']
        if 'max_display_rows' in imported_data:
            current_config.MAX_DISPLAY_ROWS = imported_data['max_display_rows']
        
        # Save the imported config
        current_config.save_config()
        current_config.refresh_merge_detection()
        
        # Create config data for store
        config_data = {
            "data_dir": current_config.DATA_DIR,
            "demographics_file": current_config.DEMOGRAPHICS_FILE,
            "primary_id_column": current_config.PRIMARY_ID_COLUMN,
            "session_column": current_config.SESSION_COLUMN,
            "composite_id_column": current_config.COMPOSITE_ID_COLUMN,
            "default_age_selection": list(current_config.DEFAULT_AGE_SELECTION),
            "default_sex_selection": current_config.DEFAULT_SEX_SELECTION,
            "sex_mapping": current_config.SEX_MAPPING,
            "max_display_rows": current_config.MAX_DISPLAY_ROWS,
            "timestamp": str(pd.Timestamp.now())
        }
        
        return dbc.Alert(f"Settings imported successfully from {filename}!", 
                        color="success", dismissable=True), config_data
        
    except Exception as e:
        return dbc.Alert(f"Error importing settings: {str(e)}", 
                        color="danger", dismissable=True), dash.no_update