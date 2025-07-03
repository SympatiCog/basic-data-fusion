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


# Callbacks to update stores when dynamic dropdowns change
@callback(
    Output('study-site-store', 'data'),
    Input('study-site-dropdown', 'value'),
    prevent_initial_call=True
)
def update_study_site_store(study_site_values):
    return study_site_values if study_site_values else []

@callback(
    Output('session-selection-store', 'data'),
    Input('session-dropdown', 'value'),
    prevent_initial_call=True
)
def update_session_selection_store(session_values):
    return session_values if session_values else []


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

@callback(
    Output('table-multiselect', 'value', allow_duplicate=True),
    Input('available-tables-store', 'data'),
    State('table-multiselect-state-store', 'data'),
    prevent_initial_call=True
)
def restore_table_multiselect_value(available_tables_data, stored_value):
    if stored_value is not None:
        return stored_value
    return []

@callback(
    Output('enwiden-data-checkbox', 'value', allow_duplicate=True),
    Input('merge-keys-store', 'data'),
    State('enwiden-data-checkbox-state-store', 'data'),
    prevent_initial_call=True
)
def restore_enwiden_checkbox_value(merge_keys_dict, stored_value):
    if stored_value is not None:
        return stored_value
    return False

@callback(
    Output('study-site-dropdown', 'value', allow_duplicate=True),
    Input('demographics-columns-store', 'data'),
    State('study-site-store', 'data'),
    prevent_initial_call=True
)
def restore_study_site_dropdown_value(demo_cols, stored_value):
    """Restore study site dropdown value from persistent storage"""
    if stored_value is not None and len(stored_value) > 0:
        return stored_value
    return dash.no_update

@callback(
    Output('session-dropdown', 'value', allow_duplicate=True),
    Input('session-values-store', 'data'),
    State('session-selection-store', 'data'),
    prevent_initial_call=True
)
def restore_session_dropdown_value(session_values, stored_value):
    """Restore session dropdown value from persistent storage"""
    if stored_value is not None and len(stored_value) > 0:
        return stored_value
    return dash.no_update

# Note: update_column_selection_area() moved to query/callbacks/data_loading.py

# Note: update_selected_columns_store() moved to query/callbacks/data_loading.py

# Callback to control "Enwiden Data" checkbox visibility
@callback(
    Output('enwiden-checkbox-wrapper', 'style'), # Target the wrapper div
    Input('merge-keys-store', 'data')
)
def update_enwiden_checkbox_visibility(merge_keys_dict):
    if merge_keys_dict:
        mk = MergeKeys.from_dict(merge_keys_dict)
        if mk.is_longitudinal:
            return {'display': 'block', 'marginTop': '10px'} # Show
    return {'display': 'none'} # Hide

# Note: handle_generate_data() moved to query/callbacks/export.py

# Note: show_data_processing_loading() moved to query/callbacks/export.py

# Note: toggle_filename_modal() moved to query/callbacks/export.py

# Note: download_csv_data() moved to query/callbacks/export.py


# Note: toggle_summary_modal() moved to query/callbacks/export.py

# Note: generate_and_download_summary_reports() moved to query/callbacks/export.py


# Unified callback to save all filter states for persistence across page navigation
@callback(
    [Output('age-slider-state-store', 'data'),
     Output('table-multiselect-state-store', 'data'),
     Output('enwiden-data-checkbox-state-store', 'data')],
    [Input('age-slider', 'value'),
     Input('table-multiselect', 'value'),
     Input('enwiden-data-checkbox', 'value')]
)
def save_all_filter_states(age_value, table_value, enwiden_value):
    return age_value, table_value, enwiden_value


# Export Query Parameters Callbacks

@callback(
    [Output('export-query-modal', 'is_open'),
     Output('export-filename-input', 'value', allow_duplicate=True),
     Output('export-summary-content', 'children')],
    [Input('export-query-button', 'n_clicks'),
     Input('cancel-export-button', 'n_clicks'),
     Input('confirm-export-button', 'n_clicks')],
    [State('export-query-modal', 'is_open'),
     State('age-slider', 'value'),
     State('study-site-store', 'data'),
     State('session-selection-store', 'data'),
     State('phenotypic-filters-store', 'data'),
     State('table-multiselect', 'value'),
     State('selected-columns-per-table-store', 'data'),
     State('enwiden-data-checkbox', 'value'),
     State('consolidate-baseline-checkbox', 'value')],
    prevent_initial_call=True
)
def toggle_export_modal(export_clicks, cancel_clicks, confirm_clicks, is_open,
                       age_range, substudies, sessions, phenotypic_filters,
                       selected_tables, selected_columns, enwiden_longitudinal, consolidate_baseline):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'export-query-button':
        # Generate suggested filename
        timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
        suggested_filename = f"query_parameters_{timestamp}"

        # Generate export summary
        summary_content = []

        # Cohort filters summary
        summary_content.append(html.H6("Cohort Filters:"))
        if age_range:
            summary_content.append(html.Li(f"Age Range: {age_range[0]} - {age_range[1]}"))
        if substudies:
            summary_content.append(html.Li(f"Substudies: {', '.join(substudies)}"))
        if sessions:
            summary_content.append(html.Li(f"Sessions: {', '.join(sessions)}"))

        # Phenotypic filters summary
        if phenotypic_filters and phenotypic_filters.get('filters'):
            logging.info(f"Preview: Processing {len(phenotypic_filters['filters'])} total filters")
            enabled_filters = [f for f in phenotypic_filters['filters'] if f.get('enabled')]
            for i, pf in enumerate(phenotypic_filters['filters']):
                enabled_status = pf.get('enabled')
                table_col = f"{pf.get('table', 'unknown')}.{pf.get('column', 'unknown')}"
                logging.info(f"Preview: Filter {i+1}: {table_col} - enabled: {enabled_status}")
            logging.info(f"Preview: Found {len(enabled_filters)} enabled filters")
            if enabled_filters:
                summary_content.append(html.H6("Phenotypic Filters:", className="mt-3"))
                for i, pf in enumerate(enabled_filters, 1):
                    filter_desc = f"{pf['table']}.{pf['column']}"
                    if pf['filter_type'] == 'numeric':
                        filter_desc += f" ({pf['min_val']} - {pf['max_val']})"
                    elif pf['filter_type'] == 'categorical':
                        selected_vals = pf.get('selected_values', [])
                        if len(selected_vals) <= 3:
                            filter_desc += f" ({', '.join(map(str, selected_vals))})"
                        else:
                            filter_desc += f" ({len(selected_vals)} values selected)"
                    summary_content.append(html.Li(f"Filter {i}: {filter_desc}"))

        # Export selection summary
        summary_content.append(html.H6("Export Selection:", className="mt-3"))
        if selected_tables:
            summary_content.append(html.Li(f"Tables: {', '.join(selected_tables)}"))
        if selected_columns:
            for table, columns in selected_columns.items():
                if columns:
                    column_text = ', '.join(columns) if len(columns) <= 5 else f"{len(columns)} columns"
                    summary_content.append(html.Li(f"{table}: {column_text}"))
        if enwiden_longitudinal:
            summary_content.append(html.Li("Enwiden longitudinal data: Yes"))
        if consolidate_baseline:
            summary_content.append(html.Li("Consolidate baseline sessions: Yes"))

        if not summary_content:
            summary_content = [html.P("No filters or selections to export.", className="text-muted")]

        return True, suggested_filename, summary_content

    elif button_id in ['cancel-export-button', 'confirm-export-button']:
        return False, dash.no_update, dash.no_update

    return is_open, dash.no_update, dash.no_update


@callback(
    Output('download-dataframe-csv', 'data', allow_duplicate=True),
    Input('confirm-export-button', 'n_clicks'),
    [State('export-filename-input', 'value'),
     State('export-notes-input', 'value'),
     State('age-slider', 'value'),
     State('study-site-store', 'data'),
     State('session-selection-store', 'data'),
     State('phenotypic-filters-store', 'data'),
     State('table-multiselect', 'value'),
     State('selected-columns-per-table-store', 'data'),
     State('enwiden-data-checkbox', 'value'),
     State('consolidate-baseline-checkbox', 'value')],
    prevent_initial_call=True
)
def export_query_parameters(confirm_clicks, filename, notes,
                           age_range, substudies, sessions, phenotypic_filters,
                           selected_tables, selected_columns, enwiden_longitudinal, consolidate_baseline):
    if not confirm_clicks or confirm_clicks == 0:
        return dash.no_update

    try:
        # Prepare filename
        if not filename or not filename.strip():
            timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
            filename = f"query_parameters_{timestamp}"

        filename = filename.strip()
        if not filename.endswith('.toml'):
            filename += '.toml'

        # Convert phenotypic filters to proper format
        formatted_phenotypic_filters = []
        if phenotypic_filters and phenotypic_filters.get('filters'):
            logging.info(f"Export: Processing {len(phenotypic_filters['filters'])} total filters")
            for i, pf in enumerate(phenotypic_filters['filters']):
                enabled_status = pf.get('enabled')
                table_col = f"{pf.get('table', 'unknown')}.{pf.get('column', 'unknown')}"
                logging.info(f"Export: Filter {i+1}: {table_col} - enabled: {enabled_status}")
                if enabled_status:
                    formatted_phenotypic_filters.append(pf)
                    logging.info(f"Export: Added filter {i+1} to export list")
                else:
                    logging.info(f"Export: Skipped filter {i+1} (not enabled)")
            logging.info(f"Export: Final formatted_phenotypic_filters count: {len(formatted_phenotypic_filters)}")

        # Generate TOML content
        toml_content = export_query_parameters_to_toml(
            age_range=age_range,
            substudies=substudies,
            sessions=sessions,
            phenotypic_filters=formatted_phenotypic_filters,
            selected_tables=selected_tables,
            selected_columns=selected_columns,
            enwiden_longitudinal=enwiden_longitudinal or False,
            consolidate_baseline=consolidate_baseline or False,
            user_notes=notes or "",
            app_version="1.0.0"  # TODO: Get from actual app version
        )

        return dict(content=toml_content, filename=filename, type="text/plain")

    except Exception as e:
        logging.error(f"Error exporting query parameters: {e}")
        return dash.no_update


# Import Query Parameters Callbacks

@callback(
    Output('import-query-modal', 'is_open'),
    [Input('import-query-button', 'n_clicks'),
     Input('cancel-import-button', 'n_clicks'),
     Input('confirm-import-button', 'n_clicks')],
    State('import-query-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_import_modal(import_clicks, cancel_clicks, confirm_clicks, is_open):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'import-query-button':
        return True
    elif button_id in ['cancel-import-button', 'confirm-import-button']:
        return False

    return is_open


@callback(
    [Output('upload-status', 'children'),
     Output('import-preview-content', 'children'),
     Output('import-preview-content', 'style'),
     Output('import-validation-results', 'children'),
     Output('import-validation-results', 'style'),
     Output('confirm-import-button', 'disabled'),
     Output('imported-file-content-store', 'data'),
     Output('import-validation-results-store', 'data')],
    Input('upload-query-params', 'contents'),
    [State('upload-query-params', 'filename'),
     State('available-tables-store', 'data'),
     State('demographics-columns-store', 'data'),
     State('behavioral-columns-store', 'data')],
    prevent_initial_call=True
)
def handle_file_upload(contents, filename, available_tables, demographics_columns, behavioral_columns):
    if not contents:
        return [
            "",  # upload-status
            "",  # import-preview-content
            {'display': 'none'},  # import-preview-content style
            "",  # import-validation-results
            {'display': 'none'},  # import-validation-results style
            True,  # confirm-import-button disabled
            None,  # imported-file-content-store
            None   # import-validation-results-store
        ]

    try:
        # Decode the uploaded file
        import base64
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        file_content = decoded.decode('utf-8')

        # Parse TOML
        imported_data, parse_errors = import_query_parameters_from_toml(file_content)

        if parse_errors:
            error_content = dbc.Alert([
                html.H6("File Parsing Errors:", className="alert-heading"),
                html.Ul([html.Li(error) for error in parse_errors])
            ], color="danger")

            return [
                dbc.Alert(f"✗ Error parsing {filename}", color="danger"),
                "",  # import-preview-content
                {'display': 'none'},  # import-preview-content style
                error_content,  # import-validation-results
                {'display': 'block'},  # import-validation-results style
                True,  # confirm-import-button disabled
                None,  # imported-file-content-store
                None   # import-validation-results-store
            ]

        # Validate against current dataset
        config = get_config()
        validation_results, validation_errors = validate_imported_query_parameters(
            imported_data, available_tables or [], demographics_columns or [],
            behavioral_columns or {}, config
        )

        # Generate preview content
        preview_content = []
        metadata = imported_data.get('metadata', {})

        # Show metadata
        preview_content.append(html.H6("File Metadata:"))
        preview_content.append(html.Ul([
            html.Li(f"Export Date: {metadata.get('export_timestamp', 'Unknown')}"),
            html.Li(f"App Version: {metadata.get('app_version', 'Unknown')}"),
            html.Li(f"Notes: {metadata.get('user_notes', 'None')}")
        ]))

        # Show what will be imported
        cohort_filters = imported_data.get('cohort_filters', {})
        if cohort_filters:
            preview_content.append(html.H6("Cohort Filters:", className="mt-3"))
            for key, value in cohort_filters.items():
                if key == 'age_range':
                    preview_content.append(html.Li(f"Age Range: {value[0]} - {value[1]}"))
                elif key == 'substudies':
                    preview_content.append(html.Li(f"Substudies: {', '.join(value)}"))
                elif key == 'sessions':
                    preview_content.append(html.Li(f"Sessions: {', '.join(value)}"))

        phenotypic_filters = imported_data.get('phenotypic_filters', [])
        if phenotypic_filters:
            preview_content.append(html.H6("Phenotypic Filters:", className="mt-3"))
            for i, pf in enumerate(phenotypic_filters, 1):
                filter_desc = f"{pf['table']}.{pf['column']}"
                if pf['filter_type'] == 'numeric':
                    filter_desc += f" ({pf['min_val']} - {pf['max_val']})"
                elif pf['filter_type'] == 'categorical':
                    selected_vals = pf.get('selected_values', [])
                    if len(selected_vals) <= 3:
                        filter_desc += f" ({', '.join(map(str, selected_vals))})"
                    else:
                        filter_desc += f" ({len(selected_vals)} values)"
                preview_content.append(html.Li(f"Filter {i}: {filter_desc}"))

        export_selection = imported_data.get('export_selection', {})
        if export_selection:
            preview_content.append(html.H6("Export Selection:", className="mt-3"))
            if export_selection.get('selected_tables'):
                preview_content.append(html.Li(f"Tables: {', '.join(export_selection['selected_tables'])}"))
            if export_selection.get('enwiden_longitudinal'):
                preview_content.append(html.Li("Enwiden longitudinal data: Yes"))
            if export_selection.get('consolidate_baseline'):
                preview_content.append(html.Li("Consolidate baseline sessions: Yes"))

        # Generate validation results display
        validation_content = []

        if validation_errors:
            validation_content.append(dbc.Alert([
                html.H6("Validation Errors:", className="alert-heading"),
                html.Ul([html.Li(error) for error in validation_errors])
            ], color="danger"))

            can_import = False
        else:
            validation_content.append(dbc.Alert([
                html.I(className="bi bi-check-circle me-2"),
                "All parameters validated successfully!"
            ], color="success"))

            can_import = True

        # Show what will be imported vs what will be skipped
        valid_params = validation_results.get('valid_parameters', {})
        invalid_params = validation_results.get('invalid_parameters', {})

        if valid_params:
            validation_content.append(html.H6("Will be imported:", className="text-success mt-3"))
            validation_content.append(html.Ul([
                html.Li(f"Cohort filters: {len(valid_params.get('cohort_filters', {}))} items"),
                html.Li(f"Phenotypic filters: {len(valid_params.get('phenotypic_filters', []))} items"),
                html.Li(f"Export tables: {len(valid_params.get('export_selection', {}).get('selected_tables', []))} items")
            ]))

        if any(invalid_params.values()):
            validation_content.append(html.H6("Will be skipped (invalid):", className="text-danger mt-3"))
            skip_items = []
            if invalid_params.get('cohort_filters'):
                skip_items.append(f"Cohort filters: {len(invalid_params['cohort_filters'])} items")
            if invalid_params.get('phenotypic_filters'):
                skip_items.append(f"Phenotypic filters: {len(invalid_params['phenotypic_filters'])} items")
            if invalid_params.get('export_selection', {}).get('selected_tables'):
                skip_items.append(f"Export tables: {len(invalid_params['export_selection']['selected_tables'])} items")

            if skip_items:
                validation_content.append(html.Ul([html.Li(item) for item in skip_items]))

        upload_status = dbc.Alert(f"✓ Successfully parsed {filename}", color="success")

        # Add filename to validation results for later use
        validation_results['filename'] = filename

        return [
            upload_status,
            preview_content,
            {'display': 'block'},  # show preview
            validation_content,
            {'display': 'block'},  # show validation
            not can_import,  # disable import button if validation failed
            file_content,  # store file content
            validation_results  # store validation results
        ]

    except Exception as e:
        logging.error(f"Error processing uploaded file: {e}")
        error_content = dbc.Alert([
            html.H6("File Processing Error:", className="alert-heading"),
            html.P(f"Could not process the uploaded file: {str(e)}")
        ], color="danger")

        return [
            dbc.Alert(f"✗ Error processing {filename}", color="danger"),
            "",  # import-preview-content
            {'display': 'none'},  # import-preview-content style
            error_content,  # import-validation-results
            {'display': 'block'},  # import-validation-results style
            True,  # confirm-import-button disabled
            None,  # imported-file-content-store
            None   # import-validation-results-store
        ]


@callback(
    [Output('age-slider', 'value', allow_duplicate=True),
     Output('study-site-store', 'data', allow_duplicate=True),
     Output('session-selection-store', 'data', allow_duplicate=True),
     Output('phenotypic-filters-store', 'data', allow_duplicate=True),
     Output('table-multiselect', 'value', allow_duplicate=True),
     Output('selected-columns-per-table-store', 'data', allow_duplicate=True),
     Output('enwiden-data-checkbox', 'value', allow_duplicate=True),
     Output('consolidate-baseline-checkbox', 'value', allow_duplicate=True),
     Output('merged-dataframe-store', 'data', allow_duplicate=True),
     Output('data-preview-area', 'children', allow_duplicate=True),
     Output('current-query-metadata-store', 'data')],
    Input('confirm-import-button', 'n_clicks'),
    [State('import-validation-results-store', 'data'),
     State('imported-file-content-store', 'data')],
    prevent_initial_call=True
)
def apply_imported_parameters(confirm_clicks, validation_results, file_content):
    if not confirm_clicks or confirm_clicks == 0 or not validation_results or not file_content:
        return [dash.no_update] * 11

    try:
        # Re-parse the file content to get the imported data
        imported_data, _ = import_query_parameters_from_toml(file_content)
        valid_params = validation_results.get('valid_parameters', {})

        # Reset age slider to default first
        config = get_config()
        age_value = [config.DEFAULT_AGE_SELECTION[0], config.DEFAULT_AGE_SELECTION[1]]

        # Apply valid cohort filters
        substudies_value = []
        sessions_value = []

        cohort_filters = valid_params.get('cohort_filters', {})
        if 'age_range' in cohort_filters:
            age_value = cohort_filters['age_range']
        if 'substudies' in cohort_filters:
            substudies_value = cohort_filters['substudies']
        if 'sessions' in cohort_filters:
            sessions_value = cohort_filters['sessions']

        # Apply valid phenotypic filters
        valid_phenotypic_filters = valid_params.get('phenotypic_filters', [])
        phenotypic_store_data = {'filters': [], 'next_id': 1}

        for i, pf in enumerate(valid_phenotypic_filters):
            filter_data = {
                'id': i + 1,
                'table': pf['table'],
                'column': pf['column'],
                'filter_type': pf['filter_type'],
                'enabled': True,
                'expanded': False
            }

            if pf['filter_type'] == 'numeric':
                filter_data.update({
                    'min_val': pf.get('min_val'),
                    'max_val': pf.get('max_val'),
                    'range_min': pf.get('min_val'),
                    'range_max': pf.get('max_val'),
                    'selected_values': [],
                    'available_values': []
                })
            elif pf['filter_type'] == 'categorical':
                filter_data.update({
                    'min_val': None,
                    'max_val': None,
                    'range_min': None,
                    'range_max': None,
                    'selected_values': pf.get('selected_values', []),
                    'available_values': pf.get('selected_values', [])
                })

            phenotypic_store_data['filters'].append(filter_data)

        phenotypic_store_data['next_id'] = len(valid_phenotypic_filters) + 1

        # Apply valid export selection
        export_selection = valid_params.get('export_selection', {})
        selected_tables = export_selection.get('selected_tables', [])
        selected_columns = export_selection.get('selected_columns', {})
        enwiden_value = export_selection.get('enwiden_longitudinal', False)
        consolidate_baseline_value = export_selection.get('consolidate_baseline', False)

        # Clear existing data preview and merged data store
        preview_content = dbc.Alert([
            html.I(className="bi bi-check-circle me-2"),
            "Query parameters imported successfully. ",
            html.Strong("Previous query results have been cleared."),
            " Use 'Generate Merged Data' to create new results with the imported parameters."
        ], color="success")

        # Prepare query metadata for storage
        metadata = imported_data.get('metadata', {})
        filename = validation_results.get('filename', 'Unknown')

        query_metadata = {
            'filename': filename,
            'metadata': metadata,
            'full_toml_content': file_content,
            'import_timestamp': datetime.now().isoformat()
        }

        return [
            age_value,                  # age-slider value
            substudies_value,           # study-site-store
            sessions_value,             # session-selection-store
            phenotypic_store_data,      # phenotypic-filters-store
            selected_tables,            # table-multiselect value
            selected_columns,           # selected-columns-per-table-store
            enwiden_value,              # enwiden-data-checkbox value
            consolidate_baseline_value, # consolidate-baseline-checkbox value
            None,                       # merged-dataframe-store (clear existing)
            preview_content,            # data-preview-area (show success message)
            query_metadata              # current-query-metadata-store
        ]

    except Exception as e:
        logging.error(f"Error applying imported parameters: {e}")
        error_content = dbc.Alert([
            html.I(className="bi bi-exclamation-triangle me-2"),
            f"Error applying imported parameters: {str(e)}"
        ], color="danger")

        return [dash.no_update] * 9 + [error_content, dash.no_update]


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
