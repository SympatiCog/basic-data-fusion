"""
Reusable UI components for the Query interface.

This module contains functions that generate common UI components used
throughout the query page, promoting consistency and reusability.
"""

import dash_bootstrap_components as dbc
from dash import dcc, html

from .styles import COLORS, STYLES, CLASSES, ICONS, MODAL_SIZES


def create_demographic_filters_card():
    """Create the demographic filters card component."""
    return dbc.Card(dbc.CardBody([
        html.H4("Demographic Filters", className="card-title"),
        dbc.Row([
            dbc.Col(html.Div([
                html.Label("Age Range:"),
                dcc.RangeSlider(
                    id='age-slider', 
                    disabled=True, 
                    allowCross=False, 
                    step=1, 
                    tooltip={"placement": "bottom", "always_visible": True}
                ),
                html.Div(id='age-slider-info') # To show min/max or if disabled
            ]), md=6),
        ]),
        html.Div(
            id='dynamic-demo-filters-placeholder', 
            style=STYLES['card_body_spacing']
        ), # For Rockland substudies and Sessions
    ]))


def create_phenotypic_filters_card():
    """Create the phenotypic filters card component."""
    return dbc.Card(dbc.CardBody([
        html.H4("Phenotypic Filters", className="card-title"),
        html.P(
            "Filter participants based on data from any table and column.", 
            className=CLASSES['text_muted']
        ),
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
                    size="sm",
                    title="Remove all phenotypic filters (including saved filters from previous sessions)"
                )
            ], width="auto")
        ], className=CLASSES['button_spacing']),
        html.Div(id="phenotypic-session-notice", className="mb-2"),
        html.Div(id="phenotypic-filters-list")
    ]))


def create_data_export_card():
    """Create the data export selection card component."""
    return html.Div([
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
            ),
            dbc.Checkbox(
                id='consolidate-baseline-checkbox',
                label='Consolidate baseline sessions (BAS1, BAS2, BAS3 → BAS)',
                value=True,
                style=STYLES['filter_spacing']
            )
        ], id='enwiden-checkbox-wrapper', style=STYLES['enwiden_checkbox_wrapper'])
    ], id='table-column-selector-container')


def create_query_results_card():
    """Create the query results section component."""
    return html.Div([
        dbc.Button(
            "Generate Merged Data",
            id='generate-data-button',
            n_clicks=0,
            color="primary",
            className=CLASSES['margin_bottom']
        ),
        # Loading spinner for data processing
        dcc.Loading(
            id="data-processing-loading",
            type="default",
            children=html.Div(id="data-processing-loading-output")
        ),
        html.Div(id='data-preview-area'),
        dcc.Download(id='download-dataframe-csv')
    ], id='results-container')


def create_export_parameters_modal():
    """Create the export query parameters modal component."""
    return dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Export Query Parameters")),
        dbc.ModalBody([
            html.P("Export your current query configuration to a TOML file that can be shared with others or imported later."),
            html.Hr(),
            html.H6("File Details"),
            dbc.Row([
                dbc.Col([
                    html.Label("Filename:", className="form-label"),
                    dbc.InputGroup([
                        dbc.Input(
                            id="export-filename-input",
                            placeholder="Enter filename",
                            value="",
                            type="text"
                        ),
                        dbc.InputGroupText(".toml")
                    ])
                ], width=12),
                dbc.Col([
                    html.Label("Notes (optional):", className="form-label mt-3"),
                    dbc.Textarea(
                        id="export-notes-input",
                        placeholder="Add notes about this query configuration...",
                        rows=3
                    )
                ], width=12)
            ]),
            html.Hr(),
            html.H6("Export Summary"),
            html.Div(id="export-summary-content"),
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel", id="cancel-export-button", color="secondary", className="me-2"),
            dbc.Button("Export", id="confirm-export-button", color="success")
        ])
    ], id="export-query-modal", is_open=False, size=MODAL_SIZES['large'])


def create_import_parameters_modal():
    """Create the import query parameters modal component."""
    return dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Import Query Parameters")),
        dbc.ModalBody([
            html.P("Import previously exported query configuration from a TOML file. This will replace your current filter settings."),
            dcc.Upload(
                id='upload-query-params',
                children=html.Div([
                    html.I(className=ICONS['cloud_upload']),
                    'Drag and Drop or ',
                    html.A('Select a TOML File')
                ]),
                style=STYLES['upload_area'],
                multiple=False,
                accept='.toml'
            ),
            html.Div(id='upload-status'),
            html.Hr(),
            html.Div(id='import-preview-content', style=STYLES['hidden']),
            html.Div(id='import-validation-results', style=STYLES['hidden'])
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel", id="cancel-import-button", color="secondary", className="me-2"),
            dbc.Button("Import", id="confirm-import-button", color="primary", disabled=True)
        ])
    ], id="import-query-modal", is_open=False, size=MODAL_SIZES['large'])


def create_query_details_modal():
    """Create the query details modal component."""
    return dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Query Details")),
        dbc.ModalBody([
            html.Div(id='query-details-content')
        ]),
        dbc.ModalFooter([
            dbc.Button("Close", id="close-query-details-button", color="secondary")
        ])
    ], id="query-details-modal", is_open=False, size=MODAL_SIZES['large'])


def create_data_status_section():
    """Create placeholder for the data status section."""
    return html.Div(id='query-data-status-section')


def create_logo_section():
    """Create the logo display section."""
    return html.Div([
        html.Img(
            src="/assets/TBS_Logo_Wide_sm.png",
            style=STYLES['logo']
        )
    ], className=CLASSES['flex_center'], style={'height': '100%'})


def create_query_management_section():
    """Create the query management buttons section."""
    return html.Div([
        html.H3("Query Management"),
        dbc.Row([
            dbc.Col([
                dbc.Button(
                    [html.I(className=ICONS['upload']), "Import Query Parameters"],
                    id="import-query-button",
                    color="info",
                    size="sm",
                    className=CLASSES['full_width']
                )
            ], width=6),
            dbc.Col([
                dbc.Button(
                    [html.I(className=ICONS['download']), "Export Query Parameters"],
                    id="export-query-button",
                    color="success",
                    size="sm",
                    className=CLASSES['full_width']
                )
            ], width=6),
        ], className=CLASSES['button_spacing']),
        html.Div([
            dbc.Button(
                [
                    html.Span(id="current-query-display-text", children=""),
                    html.I(className=ICONS['chevron_down'])
                ],
                id="current-query-dropdown-button",
                color="light",
                outline=True,
                size="sm",
                className=f"{CLASSES['full_width']} {CLASSES['margin_top']} {CLASSES['text_start']}",
                disabled=True
            )
        ], id="current-query-container", style=STYLES['hidden'])
    ])


def create_live_participant_count_section():
    """Create the live participant count section."""
    return html.Div([
        html.H3("Live Participant Count"),
        html.Div(id='live-participant-count'), # Placeholder for participant count
    ])