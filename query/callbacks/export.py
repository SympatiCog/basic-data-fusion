"""
Data export and generation callbacks for the Query interface.

This module contains callbacks responsible for:
- Data generation and merging
- Export functionality
- Download handling
- Export/import parameter modals
"""

import io
import logging
import time
import zipfile
from datetime import datetime
import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, State, callback, dcc, html, no_update, dash_table

from config_manager import get_config
from utils import (
    MergeKeys,
    _file_access_lock,  # temp fix for file access coordination
    enwiden_longitudinal_data,
    export_query_parameters_to_toml,
    generate_export_filename,
    generate_filtering_report,
    generate_final_data_summary,
    get_db_connection,
    import_query_parameters_from_toml,
    secure_filename,
    validate_imported_query_parameters,
)

# Import convert function from helper module to avoid circular imports
from query.helpers.data_formatters import convert_phenotypic_to_behavioral_filters


# === EXPORT CALLBACKS ===


# Callback for Data Generation  
def handle_generate_data(
    n_clicks,
    age_range,
    rockland_substudy_values, session_filter_values,
    phenotypic_filters_state, selected_columns_per_table,
    enwiden_checkbox_value, consolidate_baseline_value, merge_keys_dict, available_tables, tables_selected_for_export
):
    """Generate merged data based on current filter and table selections."""
    if n_clicks == 0 or not merge_keys_dict:
        return dbc.Alert("Click 'Generate Merged Data' after selecting filters and columns.", color="info"), no_update, ""

    current_config = get_config()  # Get fresh config instance
    merge_keys = MergeKeys.from_dict(merge_keys_dict)

    # --- Collect Demographic Filters ---
    demographic_filters = {}
    if age_range: demographic_filters['age_range'] = age_range

    # Handle Rockland substudy filtering
    if rockland_substudy_values:
        demographic_filters['substudies'] = rockland_substudy_values

    # Handle session filtering
    if session_filter_values:
        demographic_filters['sessions'] = session_filter_values

    # Convert phenotypic filters to behavioral filters for query generation
    behavioral_filters = convert_phenotypic_to_behavioral_filters(phenotypic_filters_state)

    # --- Determine Tables to Join ---
    # Start with tables explicitly selected for export
    tables_for_query = set(tables_selected_for_export if tables_selected_for_export else [])
    tables_for_query.add(current_config.get_demographics_table_name()) # Always include demographics

    # Add tables from phenotypic filters
    for p_filter in behavioral_filters:
        tables_for_query.add(p_filter['table'])

    # Note: Session-based table selection logic would go here when session filters are available
    # if merge_keys.is_longitudinal and demographic_filters.get('sessions') and \
    #    len(tables_for_query.intersection(set(available_tables if available_tables else []))) == 0 and \
    #    current_config.get_demographics_table_name() in tables_for_query and \
    #    len(tables_for_query) == 1 and available_tables:
    #     tables_for_query.add(available_tables[0]) # Add a behavioral table if needed for session join

    # --- Selected Columns for Query ---
    # If no columns are selected for a table in tables_selected_for_export, select all its non-ID columns.

    query_selected_columns = selected_columns_per_table.copy() if selected_columns_per_table else {}
    # For tables selected for export but with no specific columns chosen, this implies "all columns" for that table.
    # The generate_data_query handles this: if a table is in selected_tables (i.e. tables_for_query here)
    # and has entries in selected_columns, those are used. If demo.* is default, other tables need explicit columns.
    # For this implementation, we assume selected_columns_per_table_store correctly reflects user choices.
    # If a table is in tables_selected_for_export, it should be in query_selected_columns.
    # If user wants all columns from table X, they should use "select all" in its column dropdown (not yet implemented).
    # For now, only explicitly selected columns via UI are passed. generate_data_query adds demo.*

    try:
        # Start timing the query + merge operation
        start_time = time.time()

        # Use secure query generation instead of deprecated functions
        from query.query_factory import QueryMode, get_query_factory

        # Create query factory with secure mode
        query_factory = get_query_factory(mode=QueryMode.SECURE)

        # If we need to enwiden longitudinal data, ensure session column is included
        modified_selected_columns = query_selected_columns.copy()
        if enwiden_checkbox_value and merge_keys.is_longitudinal and merge_keys.session_id:
            # Add session column to demographics table selection
            demographics_table = current_config.get_demographics_table_name()
            if demographics_table not in modified_selected_columns:
                modified_selected_columns[demographics_table] = []
            if merge_keys.session_id not in modified_selected_columns[demographics_table]:
                modified_selected_columns[demographics_table].append(merge_keys.session_id)

        base_query, params = query_factory.get_base_query_logic(
            current_config, merge_keys, demographic_filters, behavioral_filters, list(tables_for_query)
        )
        data_query, data_params = query_factory.get_data_query(
            base_query, params, list(tables_for_query), modified_selected_columns
        )

        if not data_query:
            return dbc.Alert("Could not generate data query.", color="warning"), None, ""
        # Use cached database connection for improved performance
        con = get_db_connection()
        # temp fix: coordinate file access with pandas operations
        with _file_access_lock:
            result_df = con.execute(data_query, data_params).fetchdf()

        original_row_count = len(result_df)

        if enwiden_checkbox_value and merge_keys.is_longitudinal:
            result_df = enwiden_longitudinal_data(result_df, merge_keys, consolidate_baseline_value)
            enwiden_info = f" (enwidened from {original_row_count} rows to {len(result_df)} rows)"
        else:
            enwiden_info = ""

        # Calculate elapsed time
        elapsed_time = time.time() - start_time

        if result_df.empty:
            return dbc.Alert("No data found for the selected criteria.", color="info"), None, ""

        # Prepare for DataTable
        dt_columns = [{"name": i, "id": i} for i in result_df.columns]
        # For performance, only show head in preview
        dt_data = result_df.head(current_config.MAX_DISPLAY_ROWS).to_dict('records')

        preview_table = dash_table.DataTable(
            data=dt_data,
            columns=dt_columns,
            page_size=10,
            style_table={'overflowX': 'auto'},
            filter_action="native",
            sort_action="native",
        )

        return (
            html.Div([
                dbc.Alert(f"Filter/query/merge successful in {elapsed_time:.2f} seconds. Displaying first {min(len(result_df), current_config.MAX_DISPLAY_ROWS)} of {len(result_df)} total rows{enwiden_info}.", color="success"),
                preview_table,
                html.Hr(),
                dbc.Row([
                    dbc.Col([
                        dbc.Button("Download CSV", id="download-custom-csv-button", color="primary", className="mt-2")
                    ], width="auto"),
                    dbc.Col([
                        dbc.Button("Generate Summary", id="generate-summary-button", color="info", className="mt-2")
                    ], width="auto")
                ], className="g-2"),
                # Modal for custom filename
                dbc.Modal([
                    dbc.ModalHeader(dbc.ModalTitle("Download CSV with Custom Filename")),
                    dbc.ModalBody([
                        html.P("Enter a filename for your CSV export:"),
                        dbc.InputGroup([
                            dbc.Input(
                                id="custom-filename-input",
                                placeholder="Enter filename (without .csv extension)",
                                value="",
                                type="text"
                            ),
                            dbc.InputGroupText(".csv")
                        ], className="mb-3"),
                        html.P("Suggested filename based on your selection:", className="text-muted small"),
                        html.Code(id="suggested-filename", className="text-muted small")
                    ]),
                    dbc.ModalFooter([
                        dbc.Button("Cancel", id="cancel-download-button", color="secondary", className="me-2"),
                        dbc.Button("Download", id="confirm-download-button", color="success")
                    ])
                ], id="filename-modal", is_open=False),
                # Modal for summary generation
                dbc.Modal([
                    dbc.ModalHeader(dbc.ModalTitle("Generate Filtering Summary Report")),
                    dbc.ModalBody([
                        html.P("This will generate two CSV files:"),
                        html.Ul([
                            html.Li("filtering_report.csv - Shows filter application steps and sample size impact"),
                            html.Li("final_data_summary.csv - Descriptive statistics for the final filtered dataset")
                        ]),
                        html.Hr(),
                        html.P("Enter a prefix for your report files (optional):"),
                        dbc.InputGroup([
                            dbc.Input(
                                id="summary-filename-prefix-input",
                                placeholder="Enter filename prefix (optional)",
                                value="",
                                type="text"
                            ),
                            dbc.InputGroupText("_filtering_report.csv / _final_data_summary.csv")
                        ], className="mb-3"),
                        html.P("Default filenames will be used if no prefix is provided.", className="text-muted small"),
                        html.Code(id="suggested-summary-filenames", className="text-muted small")
                    ]),
                    dbc.ModalFooter([
                        dbc.Button("Cancel", id="cancel-summary-button", color="secondary", className="me-2"),
                        dbc.Button("Generate Reports", id="confirm-summary-button", color="info")
                    ])
                ], id="summary-modal", is_open=False)
            ]),
            {
                'row_count': len(result_df),
                'column_count': len(result_df.columns),
                'columns': result_df.columns.tolist(),
                'full_data': result_df.to_dict('records'),  # Store complete dataset for plotting/profiling
                'filters_applied': {
                    'age_range': age_range,
                    'phenotypic_filters': phenotypic_filters_state,
                    'session_filters': session_filter_values
                },
                'data_size_mb': round(len(str(result_df.to_dict('records'))) / (1024*1024), 2)  # Track data size
            }, # Store complete dataset for plotting and profiling pages
            ""  # Clear loading message
        )

    except Exception as e:
        logging.error(f"Error during data generation: {e}")
        logging.error(f"Query attempted: {data_query if 'data_query' in locals() else 'N/A'}")
        return dbc.Alert(f"Error generating data: {str(e)}", color="danger"), None, ""


def show_data_processing_loading(n_clicks):
    """Show loading message during data processing."""
    if n_clicks and n_clicks > 0:
        return html.Div([
            html.P("Processing data query and generating results...",
                   className="text-info text-center"),
            html.P("This may take a moment for large datasets.",
                   className="text-muted text-center small")
        ])
    return ""


def toggle_filename_modal(custom_clicks, cancel_clicks, confirm_clicks, is_open, selected_tables, is_enwidened):
    """Toggle filename modal and populate suggested filename."""
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update

    # Check if any button was actually clicked (not just initialized)
    if ((custom_clicks is None or custom_clicks == 0) and
        (cancel_clicks is None or cancel_clicks == 0) and
        (confirm_clicks is None or confirm_clicks == 0)):
        return dash.no_update, dash.no_update, dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'download-custom-csv-button':
        # Generate suggested filename
        current_config = get_config()
        demographics_table_name = current_config.get_demographics_table_name()
        all_tables = [demographics_table_name] + (selected_tables or [])
        suggested_filename = generate_export_filename(all_tables, demographics_table_name, is_enwidened or False)
        # Remove .csv extension for input field
        suggested_name_without_ext = suggested_filename.replace('.csv', '')
        return True, suggested_filename, suggested_name_without_ext

    elif button_id in ['cancel-download-button', 'confirm-download-button']:
        return False, dash.no_update, dash.no_update

    return is_open, dash.no_update, dash.no_update


def download_csv_data(custom_clicks, stored_data, selected_tables, is_enwidened, custom_filename):
    """Handle CSV download with custom filename."""
    # Only proceed if we have actual button clicks and data
    if not stored_data:
        return dash.no_update

    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    # Check if any button was actually clicked (not just initialized)
    if (custom_clicks is None or custom_clicks == 0):
        return dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    # Additional safety check - only proceed if a download button was clicked
    if button_id not in ['confirm-download-button']:
        return dash.no_update

    # Regenerate the data instead of using stored dataframe
    try:
        # Use the full dataset for download
        full_data = stored_data.get('full_data', [])
        if not full_data:
            raise ValueError("No data available for download")

        df = pd.DataFrame(full_data)
    except Exception as e:
        logging.error(f"Error preparing download data: {e}")
        return dash.no_update

    # Determine filename based on which button was clicked
    if button_id == 'confirm-download-button':
        # Custom filename from modal
        if custom_filename and custom_filename.strip():
            # Ensure .csv extension
            filename = custom_filename.strip()
            if not filename.endswith('.csv'):
                filename += '.csv'
            # Secure the filename
            filename = secure_filename(filename)
        else:
            # Fallback to smart filename if custom name is empty
            current_config = get_config()
            demographics_table_name = current_config.get_demographics_table_name()
            all_tables = [demographics_table_name] + (selected_tables or [])
            filename = generate_export_filename(all_tables, demographics_table_name, is_enwidened or False)
    else:
        return dash.no_update

    return dcc.send_data_frame(df.to_csv, filename, index=False)


def toggle_summary_modal(generate_clicks, cancel_clicks, confirm_clicks, is_open):
    """Toggle summary modal and populate suggested filenames."""
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update

    # Check if any button was actually clicked (not just initialized)
    if ((generate_clicks is None or generate_clicks == 0) and
        (cancel_clicks is None or cancel_clicks == 0) and
        (confirm_clicks is None or confirm_clicks == 0)):
        return dash.no_update, dash.no_update, dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'generate-summary-button':
        # Generate suggested filenames with timestamp
        timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
        suggested_filtering = f"filtering_report_{timestamp}.csv"
        suggested_summary = f"final_data_summary_{timestamp}.csv"
        suggested_text = f"Default: {suggested_filtering} and {suggested_summary}"
        return True, suggested_text, ""

    elif button_id in ['cancel-summary-button', 'confirm-summary-button']:
        return False, dash.no_update, dash.no_update

    return is_open, dash.no_update, dash.no_update


def generate_and_download_summary_reports(
    confirm_clicks, age_range, rockland_substudy_values, session_filter_values,
    phenotypic_filters_state, merged_data_store, merge_keys_dict, available_tables,
    tables_selected_for_export, filename_prefix
):
    """Generate and download summary reports as ZIP file."""
    # Only proceed if we have actual button clicks and data
    if not confirm_clicks or not merged_data_store or not merge_keys_dict:
        return dash.no_update, dash.no_update

    ctx = dash.callback_context
    if not ctx.triggered or confirm_clicks == 0:
        return dash.no_update, dash.no_update

    try:
        current_config = get_config()
        merge_keys = MergeKeys.from_dict(merge_keys_dict)

        # Collect demographic and behavioral filters (same logic as in handle_generate_data)
        demographic_filters = {}
        if age_range:
            demographic_filters['age_range'] = age_range
        if rockland_substudy_values:
            demographic_filters['substudies'] = rockland_substudy_values
        if session_filter_values:
            demographic_filters['sessions'] = session_filter_values

        # Convert phenotypic filters to behavioral filters
        behavioral_filters = convert_phenotypic_to_behavioral_filters(phenotypic_filters_state)

        # Determine tables for query
        demographics_table_name = current_config.get_demographics_table_name()
        tables_for_query = set(tables_selected_for_export if tables_selected_for_export else [])
        tables_for_query.add(demographics_table_name)

        for bf in behavioral_filters:
            if bf.get('table'):
                tables_for_query.add(bf['table'])

        # Generate filtering report
        filtering_report_df = generate_filtering_report(
            current_config, merge_keys, demographic_filters, behavioral_filters, list(tables_for_query)
        )

        # Generate final data summary from stored data
        full_data = merged_data_store.get('full_data', [])
        if not full_data:
            raise ValueError("No data available for summary generation")

        final_df = pd.DataFrame(full_data)
        final_summary_df = generate_final_data_summary(final_df, merge_keys)

        # Handle longitudinal data special cases
        if merge_keys.is_longitudinal and len(final_df) > 0:
            # For longitudinal data, create session-specific summaries if requested
            if merge_keys.session_id and merge_keys.session_id in final_df.columns:
                sessions = final_df[merge_keys.session_id].dropna().unique()
                if len(sessions) > 1:
                    # Add session-specific summaries
                    session_summaries = []
                    for session in sorted(sessions):
                        session_data = final_df[final_df[merge_keys.session_id] == session]
                        if not session_data.empty:
                            session_summary = generate_final_data_summary(session_data, merge_keys)
                            # Add session identifier to variable names
                            session_summary['variable_name'] = session_summary['variable_name'].apply(
                                lambda x: f"{x}_session_{session}"
                            )
                            session_summaries.append(session_summary)

                    if session_summaries:
                        # Combine with main summary
                        all_summaries = [final_summary_df] + session_summaries
                        final_summary_df = pd.concat(all_summaries, ignore_index=True)

        # Create filenames
        timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
        if filename_prefix and filename_prefix.strip():
            prefix = filename_prefix.strip() + "_"
        else:
            prefix = ""

        filtering_filename = f"{prefix}filtering_report_{timestamp}.csv"
        summary_filename = f"{prefix}final_data_summary_{timestamp}.csv"

        # For now, download the filtering report first
        # Note: Dash can only trigger one download at a time, so we'll need a different approach
        # We could create a ZIP file or handle downloads sequentially

        # Create a combined download approach - we'll put both reports in a single ZIP
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Add filtering report
            filtering_csv = filtering_report_df.to_csv(index=False)
            zip_file.writestr(filtering_filename, filtering_csv)

            # Add final data summary
            summary_csv = final_summary_df.to_csv(index=False)
            zip_file.writestr(summary_filename, summary_csv)

        zip_buffer.seek(0)
        zip_filename = f"{prefix}summary_reports_{timestamp}.zip"

        return dcc.send_bytes(zip_buffer.read(), zip_filename), False

    except Exception as e:
        logging.error(f"Error generating summary reports: {e}")
        return dash.no_update, dash.no_update


def register_callbacks(app):
    """Register all export and generation callbacks with the Dash app."""
    
    # Data Generation Callback
    app.callback(
        [Output('data-preview-area', 'children'),
         Output('merged-dataframe-store', 'data'),
         Output('data-processing-loading-output', 'children')], # Store for profiling page
        Input('generate-data-button', 'n_clicks'),
        [State('age-slider', 'value'),
         State('study-site-store', 'data'),
         State('session-selection-store', 'data'),
         State('phenotypic-filters-store', 'data'),
         State('selected-columns-per-table-store', 'data'),
         State('enwiden-data-checkbox', 'value'), # Boolean value (True when checked, False when unchecked)
         State('consolidate-baseline-checkbox', 'value'), # Boolean value for baseline consolidation
         State('merge-keys-store', 'data'),
         State('available-tables-store', 'data'), # Needed for tables_to_join logic
         State('table-multiselect', 'value')] # Explicitly selected tables for export
    )(handle_generate_data)
    
    # Data Processing Loading Callback
    app.callback(
        Output('data-processing-loading-output', 'children', allow_duplicate=True),
        Input('generate-data-button', 'n_clicks'),
        prevent_initial_call=True
    )(show_data_processing_loading)
    
    # Filename Modal Callback
    app.callback(
        [Output('filename-modal', 'is_open'),
         Output('suggested-filename', 'children'),
         Output('custom-filename-input', 'value')],
        [Input('download-custom-csv-button', 'n_clicks'),
         Input('cancel-download-button', 'n_clicks'),
         Input('confirm-download-button', 'n_clicks')],
        [State('filename-modal', 'is_open'),
         State('table-multiselect', 'value'),
         State('enwiden-data-checkbox', 'value')],
        prevent_initial_call=True
    )(toggle_filename_modal)
    
    # CSV Download Callback
    app.callback(
        Output('download-dataframe-csv', 'data'),
        [Input('confirm-download-button', 'n_clicks')],
        [State('merged-dataframe-store', 'data'),
         State('table-multiselect', 'value'),
         State('enwiden-data-checkbox', 'value'),
         State('custom-filename-input', 'value')],
        prevent_initial_call=True
    )(download_csv_data)
    
    # Summary Modal Callback
    app.callback(
        [Output('summary-modal', 'is_open'),
         Output('suggested-summary-filenames', 'children'),
         Output('summary-filename-prefix-input', 'value')],
        [Input('generate-summary-button', 'n_clicks'),
         Input('cancel-summary-button', 'n_clicks'),
         Input('confirm-summary-button', 'n_clicks')],
        [State('summary-modal', 'is_open')],
        prevent_initial_call=True
    )(toggle_summary_modal)
    
    # Summary Reports Generation Callback
    app.callback(
        [Output('download-dataframe-csv', 'data', allow_duplicate=True),
         Output('summary-modal', 'is_open', allow_duplicate=True)],
        Input('confirm-summary-button', 'n_clicks'),
        [State('age-slider', 'value'),
         State('study-site-store', 'data'),
         State('session-selection-store', 'data'),
         State('phenotypic-filters-store', 'data'),
         State('merged-dataframe-store', 'data'),
         State('merge-keys-store', 'data'),
         State('available-tables-store', 'data'),
         State('table-multiselect', 'value'),
         State('summary-filename-prefix-input', 'value')],
        prevent_initial_call=True
    )(generate_and_download_summary_reports)