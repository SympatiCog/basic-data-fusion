import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import toml
import os
from utils import Config

dash.register_page(__name__, path='/settings', title='Settings')

# Initialize config instance
config = Config()

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Application Settings"),
            html.P("Configure application behavior and data processing options.", 
                  className="text-muted mb-4"),
            
            # Configuration File Info
            dbc.Card([
                dbc.CardHeader([
                    html.H4("Configuration File", className="mb-0")
                ]),
                dbc.CardBody([
                    html.P(f"Configuration file location: {config.CONFIG_FILE_PATH}", 
                          className="font-monospace small"),
                    html.P("Changes are automatically saved to config.toml", 
                          className="text-muted small")
                ])
            ], className="mb-4"),
            
            # Data Directory Settings
            dbc.Card([
                dbc.CardHeader([
                    html.H4("Data Directory Settings", className="mb-0")
                ]),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Data Directory:", className="form-label"),
                            dbc.Input(
                                id="data-dir-input",
                                type="text",
                                value=config.DATA_DIR,
                                placeholder="Path to data directory"
                            ),
                            html.Small("Directory containing CSV files for analysis", 
                                     className="form-text text-muted")
                        ], width=6),
                        dbc.Col([
                            html.Label("Demographics File:", className="form-label"),
                            dbc.Input(
                                id="demographics-file-input",
                                type="text", 
                                value=config.DEMOGRAPHICS_FILE,
                                placeholder="demographics.csv"
                            ),
                            html.Small("Primary demographics file name", 
                                     className="form-text text-muted")
                        ], width=6)
                    ], className="mb-3"),
                ])
            ], className="mb-4"),
            
            # Column Configuration
            dbc.Card([
                dbc.CardHeader([
                    html.H4("Column Configuration", className="mb-0")
                ]),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Primary ID Column:", className="form-label"),
                            dbc.Input(
                                id="primary-id-input",
                                type="text",
                                value=config.PRIMARY_ID_COLUMN,
                                placeholder="ursi"
                            ),
                            html.Small("Primary subject identifier column", 
                                     className="form-text text-muted")
                        ], width=4),
                        dbc.Col([
                            html.Label("Session Column:", className="form-label"),
                            dbc.Input(
                                id="session-column-input",
                                type="text",
                                value=config.SESSION_COLUMN, 
                                placeholder="session_num"
                            ),
                            html.Small("Session identifier for longitudinal data", 
                                     className="form-text text-muted")
                        ], width=4),
                        dbc.Col([
                            html.Label("Composite ID Column:", className="form-label"),
                            dbc.Input(
                                id="composite-id-input",
                                type="text",
                                value=config.COMPOSITE_ID_COLUMN,
                                placeholder="customID"
                            ),
                            html.Small("Composite ID column name", 
                                     className="form-text text-muted")
                        ], width=4)
                    ], className="mb-3"),
                ])
            ], className="mb-4"),
            
            # Display Settings
            dbc.Card([
                dbc.CardHeader([
                    html.H4("Display Settings", className="mb-0")
                ]),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Max Display Rows:", className="form-label"),
                            dbc.Input(
                                id="max-display-rows-input",
                                type="number",
                                value=config.MAX_DISPLAY_ROWS,
                                min=10,
                                max=10000,
                                step=10
                            ),
                            html.Small("Maximum rows to display in data preview", 
                                     className="form-text text-muted")
                        ], width=6),
                        dbc.Col([
                            html.Label("Default Age Range:", className="form-label"),
                            dcc.RangeSlider(
                                id="default-age-range-slider",
                                min=0,
                                max=100,
                                step=1,
                                value=list(config.DEFAULT_AGE_SELECTION),
                                marks={i: str(i) for i in range(0, 101, 20)},
                                tooltip={"placement": "bottom", "always_visible": True}
                            ),
                            html.Small("Default age range selection", 
                                     className="form-text text-muted")
                        ], width=6)
                    ], className="mb-3"),
                ])
            ], className="mb-4"),
            
            # Actions
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Button(
                                "Save Settings",
                                id="save-settings-btn",
                                color="primary",
                                className="me-2"
                            ),
                            dbc.Button(
                                "Reset to Defaults",
                                id="reset-settings-btn", 
                                color="outline-secondary"
                            )
                        ], width="auto"),
                        dbc.Col([
                            html.Div(id="settings-status-message")
                        ])
                    ], align="center")
                ])
            ])
            
        ], width=12)
    ])
], fluid=True)


@callback(
    Output('settings-status-message', 'children'),
    [Input('save-settings-btn', 'n_clicks'),
     Input('reset-settings-btn', 'n_clicks')],
    [State('data-dir-input', 'value'),
     State('demographics-file-input', 'value'),
     State('primary-id-input', 'value'),
     State('session-column-input', 'value'),
     State('composite-id-input', 'value'),
     State('max-display-rows-input', 'value'),
     State('default-age-range-slider', 'value')],
    prevent_initial_call=True
)
def handle_settings_actions(save_clicks, reset_clicks, data_dir, demographics_file,
                          primary_id, session_column, composite_id, max_display_rows,
                          default_age_range):
    ctx = dash.callback_context
    if not ctx.triggered:
        return ""
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'save-settings-btn' and save_clicks:
        try:
            # Update config values
            config.DATA_DIR = data_dir or 'data'
            config.DEMOGRAPHICS_FILE = demographics_file or 'demographics.csv'
            config.PRIMARY_ID_COLUMN = primary_id or 'ursi'
            config.SESSION_COLUMN = session_column or 'session_num'
            config.COMPOSITE_ID_COLUMN = composite_id or 'customID'
            config.MAX_DISPLAY_ROWS = max_display_rows or 1000
            config.DEFAULT_AGE_SELECTION = tuple(default_age_range) if default_age_range else (18, 90)
            
            # Save to file
            config.save_config()
            
            return dbc.Alert("Settings saved successfully!", color="success", 
                           dismissable=True, duration=3000)
        except Exception as e:
            return dbc.Alert(f"Error saving settings: {str(e)}", color="danger",
                           dismissable=True)
    
    elif button_id == 'reset-settings-btn' and reset_clicks:
        # This will trigger a page refresh to reset all inputs
        return dbc.Alert("Please refresh the page to see default values.", 
                        color="info", dismissable=True)
    
    return ""


@callback(
    [Output('data-dir-input', 'value'),
     Output('demographics-file-input', 'value'),
     Output('primary-id-input', 'value'),
     Output('session-column-input', 'value'),
     Output('composite-id-input', 'value'),
     Output('max-display-rows-input', 'value'),
     Output('default-age-range-slider', 'value')],
    Input('reset-settings-btn', 'n_clicks'),
    prevent_initial_call=True
)
def reset_settings_inputs(reset_clicks):
    if reset_clicks:
        # Reset to default values
        default_config = Config()
        return (
            'data',
            'demographics.csv', 
            'ursi',
            'session_num',
            'customID',
            1000,
            [18, 90]
        )
    return dash.no_update