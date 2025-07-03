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





# Note: update_data_status_section() moved to query/callbacks/data_loading.py


# Note: Live participant count callback exists below at line ~650


# Note: update_study_site_store() and update_session_selection_store() callbacks 
# moved to query/callbacks/state.py during Phase 2.4 refactoring


# Note: manage_phenotypic_filters() callback moved to query/callbacks/filters.py
# during Phase 2.2 refactoring - see query/callbacks/filters.py


# Note: render_phenotypic_filters() callback moved to query/callbacks/filters.py
# during Phase 2.2 refactoring - see query/callbacks/filters.py



# Note: update_phenotypic_session_notice() callback moved to query/callbacks/filters.py
# during Phase 2.2 refactoring - see query/callbacks/filters.py

# Import convert function from helper module to avoid circular imports
from query.helpers.data_formatters import convert_phenotypic_to_behavioral_filters










# Note: update_live_participant_count() callback moved to query/callbacks/filters.py
# during Phase 2.2 refactoring - see query/callbacks/filters.py




# Note: load_initial_data_info() moved to query/callbacks/data_loading.py


# Note: update_table_multiselect_options() moved to query/callbacks/data_loading.py

# Note: All restore_*_value() callbacks moved to query/callbacks/state.py 
# during Phase 2.4 refactoring - see query/callbacks/state.py

# Note: update_enwiden_checkbox_visibility() callback moved to query/callbacks/state.py
# during Phase 2.4 refactoring - see query/callbacks/state.py

# Note: handle_generate_data() moved to query/callbacks/export.py

# Note: show_data_processing_loading() moved to query/callbacks/export.py

# Note: toggle_filename_modal() moved to query/callbacks/export.py

# Note: download_csv_data() moved to query/callbacks/export.py


# Note: toggle_summary_modal() moved to query/callbacks/export.py

# Note: generate_and_download_summary_reports() moved to query/callbacks/export.py


# Note: save_all_filter_states() callback moved to query/callbacks/state.py
# during Phase 2.4 refactoring - see query/callbacks/state.py


# Note: Export Query Parameters callbacks (toggle_export_modal, export_query_parameters)
# moved to query/callbacks/state.py during Phase 2.4 refactoring


# Note: Import Query Parameters callbacks (toggle_import_modal, handle_file_upload, apply_imported_parameters)
# moved to query/callbacks/state.py during Phase 2.4 refactoring


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
        filename = query_metadata.get('filename', 'Unknown')
        metadata = query_metadata.get('metadata', {})
        import_timestamp = query_metadata.get('import_timestamp', 'Unknown')
        toml_content = query_metadata.get('full_toml_content', '')

        # Parse the TOML content to display formatted details
        from query.query_parameters import import_query_parameters_from_toml
        imported_data, _ = import_query_parameters_from_toml(toml_content)

        content = []

        # File Information
        content.append(html.H5("File Information", className="mb-3"))
        content.append(dbc.Row([
            dbc.Col([
                html.Strong("Filename:"), f" {filename}"
            ], width=12),
            dbc.Col([
                html.Strong("Import Date:"), f" {import_timestamp[:19] if import_timestamp != 'Unknown' else 'Unknown'}"
            ], width=12, className="mt-2"),
            dbc.Col([
                html.Strong("Export Date:"), f" {metadata.get('export_timestamp', 'Unknown')}"
            ], width=12, className="mt-2"),
            dbc.Col([
                html.Strong("App Version:"), f" {metadata.get('app_version', 'Unknown')}"
            ], width=12, className="mt-2")
        ]))

        # User Notes
        if metadata.get('user_notes'):
            content.append(html.H5("Notes", className="mb-3 mt-4"))
            content.append(dbc.Card([
                dbc.CardBody([
                    html.P(metadata['user_notes'], className="mb-0", style={'color': 'black'})
                ], style={'background-color': 'white'})
            ], className="mb-3", style={'background-color': 'white'}))

        # Query Parameters
        content.append(html.H5("Query Parameters", className="mb-3 mt-4"))

        # Cohort Filters
        cohort_filters = imported_data.get('cohort_filters', {})
        if cohort_filters:
            content.append(html.H6("Cohort Filters", className="mt-3"))
            filter_items = []
            for key, value in cohort_filters.items():
                if key == 'age_range':
                    filter_items.append(html.Li(f"Age Range: {value[0]} - {value[1]}"))
                elif key == 'substudies':
                    filter_items.append(html.Li(f"Substudies: {', '.join(value)}"))
                elif key == 'sessions':
                    filter_items.append(html.Li(f"Sessions: {', '.join(value)}"))
            content.append(html.Ul(filter_items))

        # Phenotypic Filters
        phenotypic_filters = imported_data.get('phenotypic_filters', [])
        if phenotypic_filters:
            content.append(html.H6("Phenotypic Filters", className="mt-3"))
            filter_items = []
            for i, pf in enumerate(phenotypic_filters, 1):
                filter_desc = f"Filter {i}: {pf['table']}.{pf['column']}"
                if pf['filter_type'] == 'numeric':
                    filter_desc += f" ({pf['min_val']} - {pf['max_val']})"
                elif pf['filter_type'] == 'categorical':
                    selected_vals = pf.get('selected_values', [])
                    if len(selected_vals) <= 3:
                        filter_desc += f" ({', '.join(map(str, selected_vals))})"
                    else:
                        filter_desc += f" ({len(selected_vals)} values)"
                filter_items.append(html.Li(filter_desc))
            content.append(html.Ul(filter_items))

        # Export Selection
        export_selection = imported_data.get('export_selection', {})
        if export_selection:
            content.append(html.H6("Export Selection", className="mt-3"))
            export_items = []
            if export_selection.get('selected_tables'):
                export_items.append(html.Li(f"Tables: {', '.join(export_selection['selected_tables'])}"))
            if export_selection.get('enwiden_longitudinal'):
                export_items.append(html.Li("Enwiden longitudinal data: Yes"))
            if export_selection.get('consolidate_baseline'):
                export_items.append(html.Li("Consolidate baseline sessions: Yes"))
            if export_items:
                content.append(html.Ul(export_items))

        return content

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
