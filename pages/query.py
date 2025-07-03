import logging
import time
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, State, callback, dash_table, dcc, html, no_update, MATCH

from analysis.demographics import has_multisite_data
from config_manager import get_config

# Assuming utils.py is in the same directory or accessible in PYTHONPATH
from utils import (
    MergeKeys,
    _file_access_lock,  # temp fix for file access coordination
    enwiden_longitudinal_data,
    export_query_parameters_to_toml,
    generate_export_filename,
    generate_filtering_report,
    generate_final_data_summary,
    get_db_connection,
    get_study_site_values,
    get_table_info,
    get_unique_column_values,
    import_query_parameters_from_toml,
    is_numeric_dtype,
    shorten_path,
    validate_imported_query_parameters,
)

# CENTRALIZED MODULAR CALLBACK REGISTRATION
# Use only the centralized registration system to prevent duplicate registrations
from query.callbacks import register_all_callbacks

dash.register_page(__name__, path='/', title='Query Data')

# Register modular callbacks with proper app instance
try:
    current_app = dash.get_app()
    register_all_callbacks(current_app)
except Exception as e:
    print(f"Warning: Modular callback registration failed: {e}")

# Note: We get fresh config in callbacks to pick up changes from settings

# Import the modular layout
from query.ui.layout import layout





# Note: All phenotypic filter callbacks are now handled by the modular registration system.
# See query/callbacks/filters.py for implementation details.

# Note: All other callbacks (data loading, state, export, etc.) are also handled
# by the modular registration system in their respective files.


# Query Management Callbacks

@callback(
    [Output('current-query-display-text', 'children'),
     Output('current-query-dropdown-button', 'disabled'),
     Output('current-query-container', 'style')],
    Input('current-query-metadata-store', 'data'),
    prevent_initial_call=True
)
def update_query_dropdown_display(query_metadata):
    """Update the query dropdown button text and visibility when metadata is loaded"""
    if not query_metadata:
        return "", True, {'display': 'none'}

    filename = query_metadata.get('filename', 'Unknown')
    # Remove .toml extension if present
    display_name = filename.replace('.toml', '') if filename.endswith('.toml') else filename
    return f"Current query: {display_name}", False, {'display': 'block', 'margin-top': '0.5rem'}


@callback(
    Output('query-details-modal', 'is_open'),
    [Input('current-query-dropdown-button', 'n_clicks'),
     Input('close-query-details-button', 'n_clicks')],
    State('query-details-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_query_details_modal(dropdown_clicks, close_clicks, is_open):
    """Toggle the query details modal"""
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'current-query-dropdown-button':
        return not is_open
    elif button_id == 'close-query-details-button':
        return False

    return is_open


@callback(
    Output('query-details-content', 'children'),
    Input('query-details-modal', 'is_open'),
    State('current-query-metadata-store', 'data'),
    prevent_initial_call=True
)
def populate_query_details_content(is_open, query_metadata):
    """Populate the query details modal content"""
    if not is_open or not query_metadata:
        return ""

    try:
        from query.helpers.ui_builders import build_query_details_content
        return build_query_details_content(query_metadata)

    except Exception as e:
        return dbc.Alert([
            html.H6("Error displaying query details:", className="alert-heading"),
            html.P(f"Could not parse query details: {str(e)}")
        ], color="danger")


# Configuration Change Listener Callback
@callback(
    [Output('demographics-columns-store', 'data', allow_duplicate=True),
     Output('column-ranges-store', 'data', allow_duplicate=True),
     Output('merge-keys-store', 'data', allow_duplicate=True)],
    [Input('app-config-store', 'data')],
    prevent_initial_call=True
)
def refresh_data_stores_on_config_change(config_data):
    """Refresh data stores when configuration changes from settings page."""
    if not config_data:
        return no_update, no_update, no_update

    try:
        # Force refresh of table info to pick up config changes
        config = get_config()
        (behavioral_tables, demographics_cols, behavioral_cols_by_table,
         col_dtypes, col_ranges, merge_keys_dict,
         actions_taken, session_vals, is_empty, messages) = get_table_info(config)

        logging.info("Data stores refreshed due to configuration change")
        return demographics_cols, col_ranges, merge_keys_dict
    except Exception as e:
        logging.warning(f"Failed to refresh data stores after config change: {e}")
        return no_update, no_update, no_update


# Callback to populate merge strategy info with data path
@callback(
    Output('merge-strategy-info', 'children'),
    Input('merge-keys-store', 'data')
)
def update_merge_strategy_info(merge_keys_dict):
    """Display shortened data path and merge strategy information."""
    config = get_config()  # Get fresh config

    # Get shortened data path
    shortened_data_path = shorten_path(config.DATA_DIR)

    # Create the data path display
    children = [
        html.Div([
            html.Strong("Data Directory: "), #f8f9fa
            html.Code(shortened_data_path, style={'background-color': '#f8f9fa', 'padding': '2px 4px', 'border-radius': '3px'})
        ], style={'margin-bottom': '10px'})
    ]

    # Add merge strategy information if available
    if merge_keys_dict:
        merge_keys = MergeKeys.from_dict(merge_keys_dict)

        if merge_keys.is_longitudinal:
            children.append(html.Div([
                html.Strong("Merge Strategy: "),
                html.Span("Longitudinal Data", style={'color': '#28a745', 'font-weight': 'bold'}),
                html.Ul([
                    html.Li(f"Primary ID: {merge_keys.primary_id}"),
                    html.Li(f"Session ID: {merge_keys.session_id}") if merge_keys.session_id else None,
                    html.Li(f"Composite ID: {merge_keys.composite_id}") if merge_keys.composite_id else None
                ], style={'margin-top': '5px', 'margin-bottom': '0px'})
            ]))
        else:
            children.append(html.Div([
                html.Strong("Merge Strategy: "),
                html.Span("Cross-sectional Data", style={'color': '#007bff', 'font-weight': 'bold'}),
                html.Ul([
                    html.Li(f"Primary ID: {merge_keys.primary_id}")
                ], style={'margin-top': '5px', 'margin-bottom': '0px'})
            ]))
    else:
        children.append(html.Div([
            html.Strong("Merge Strategy: "),
            html.Span("Not determined", style={'color': '#6c757d', 'font-style': 'italic'})
        ]))

    return children
