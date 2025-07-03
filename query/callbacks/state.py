"""
State management callbacks for the Query interface.

This module contains callbacks responsible for:
- State store updates
- Value restoration from stored state
- Session persistence
- Export parameter management
- UI state control
"""

import logging
import base64
from datetime import datetime
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html, no_update

from config_manager import get_config
from utils import (
    MergeKeys,
    export_query_parameters_to_toml,
    import_query_parameters_from_toml,
    validate_imported_query_parameters
)


# STORE UPDATE CALLBACKS
# These callbacks update state stores when user interactions occur

def update_study_site_store(study_site_values):
    """Update study site store when dropdown values change."""
    return study_site_values if study_site_values else []


def update_session_selection_store(session_values):
    """Update session selection store when dropdown values change."""
    return session_values if session_values else []


# STATE RESTORATION CALLBACKS
# These callbacks restore UI component values from persistent storage

def restore_table_multiselect_value(available_tables_data, stored_value):
    """Restore table multiselect value from persistent storage."""
    if stored_value is not None:
        return stored_value
    return []


def restore_enwiden_checkbox_value(merge_keys_dict, stored_value):
    """Restore enwiden checkbox value from persistent storage."""
    if stored_value is not None:
        return stored_value
    return False


def restore_study_site_dropdown_value(demo_cols, stored_value):
    """Restore study site dropdown value from persistent storage."""
    if stored_value is not None and len(stored_value) > 0:
        return stored_value
    return dash.no_update


def restore_session_dropdown_value(session_values, stored_value):
    """Restore session dropdown value from persistent storage."""
    if stored_value is not None and len(stored_value) > 0:
        return stored_value
    return dash.no_update


# STATE PERSISTENCE CALLBACKS
# These callbacks save state across page navigation

def save_all_filter_states(age_value, table_value, enwiden_value):
    """Save all filter states for persistence across page navigation."""
    return age_value, table_value, enwiden_value


# UI STATE CALLBACKS
# These callbacks control UI component visibility and behavior

def update_enwiden_checkbox_visibility(merge_keys_dict):
    """Control 'Enwiden Data' checkbox visibility based on data type."""
    if merge_keys_dict:
        mk = MergeKeys.from_dict(merge_keys_dict)
        if mk.is_longitudinal:
            return {'display': 'block', 'marginTop': '10px'}  # Show
    return {'display': 'none'}  # Hide


# EXPORT PARAMETER CALLBACKS
# These callbacks handle export parameter modal and query parameter export/import

def toggle_export_modal(export_clicks, cancel_clicks, confirm_clicks, is_open,
                       age_range, substudies, sessions, phenotypic_filters,
                       selected_tables, selected_columns, enwiden_longitudinal, consolidate_baseline):
    """Toggle export modal and generate export summary."""
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
        else:
            summary_content.append(html.Li("Age Range: All ages"))

        if substudies:
            summary_content.append(html.Li(f"Study Sites: {', '.join(substudies)}"))
        else:
            summary_content.append(html.Li("Study Sites: All sites"))

        if sessions:
            summary_content.append(html.Li(f"Sessions: {', '.join(map(str, sessions))}"))
        else:
            summary_content.append(html.Li("Sessions: All available"))

        # Phenotypic filters summary
        summary_content.append(html.H6("Phenotypic Filters:", style={'marginTop': '15px'}))
        if phenotypic_filters and phenotypic_filters.get('filters'):
            enabled_filters = [f for f in phenotypic_filters['filters'] if f.get('enabled')]
            if enabled_filters:
                for pf in enabled_filters:
                    filter_text = f"{pf.get('table', 'unknown')}.{pf.get('column', 'unknown')}"
                    if pf.get('filter_type') == 'numeric':
                        filter_text += f" ({pf.get('min_val')} - {pf.get('max_val')})"
                    elif pf.get('filter_type') == 'categorical':
                        values = pf.get('selected_values', [])
                        if values:
                            filter_text += f" ({', '.join(map(str, values[:3]))}{('...' if len(values) > 3 else '')})"
                    summary_content.append(html.Li(filter_text))
            else:
                summary_content.append(html.Li("No enabled phenotypic filters"))
        else:
            summary_content.append(html.Li("No phenotypic filters defined"))

        # Tables and columns summary
        summary_content.append(html.H6("Selected Data:", style={'marginTop': '15px'}))
        if selected_tables:
            summary_content.append(html.Li(f"Tables: {', '.join(selected_tables)}"))
        else:
            summary_content.append(html.Li("Tables: Demographics only"))

        if selected_columns:
            total_cols = sum(len(cols) for cols in selected_columns.values())
            summary_content.append(html.Li(f"Custom columns selected: {total_cols} columns"))
        else:
            summary_content.append(html.Li("Columns: All available columns"))

        # Export options summary
        summary_content.append(html.H6("Export Options:", style={'marginTop': '15px'}))
        if enwiden_longitudinal:
            summary_content.append(html.Li("✓ Enwiden longitudinal data"))
        if consolidate_baseline:
            summary_content.append(html.Li("✓ Consolidate baseline sessions"))
        if not enwiden_longitudinal and not consolidate_baseline:
            summary_content.append(html.Li("Standard export format"))

        return True, suggested_filename, html.Div(summary_content)

    elif button_id in ['cancel-export-button', 'confirm-export-button']:
        return False, dash.no_update, dash.no_update

    return is_open, dash.no_update, dash.no_update


def export_query_parameters(confirm_clicks, filename, notes,
                           age_range, substudies, sessions, phenotypic_filters,
                           selected_tables, selected_columns, enwiden_longitudinal, consolidate_baseline):
    """Export query parameters to TOML file."""
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


# IMPORT PARAMETER CALLBACKS
# These callbacks handle query parameter import functionality

def toggle_import_modal(import_clicks, cancel_clicks, confirm_clicks, is_open):
    """Toggle import modal for query parameters."""
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'import-query-button':
        return True
    elif button_id in ['cancel-import-button', 'confirm-import-button']:
        return False

    return is_open


def handle_file_upload(contents, filename, available_tables, demographics_columns, behavioral_columns):
    """Handle query parameter file upload and validation."""
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
            imported_data, available_tables, demographics_columns, 
            behavioral_columns, config
        )

        # Generate preview content
        preview_content = []
        
        # File info
        preview_content.append(html.H6("File Information:"))
        if 'metadata' in imported_data:
            metadata = imported_data['metadata']
            if 'export_timestamp' in metadata:
                preview_content.append(html.Li(f"Created: {metadata['export_timestamp']}"))
            elif 'creation_date' in metadata:
                preview_content.append(html.Li(f"Created: {metadata['creation_date']}"))
            if 'user_notes' in metadata:
                notes = metadata['user_notes']
                if notes:
                    preview_content.append(html.Li(f"Notes: {notes}"))

        # Parameters preview
        preview_content.append(html.H6("Parameters to Import:", style={'marginTop': '15px'}))
        
        if 'cohort_filters' in imported_data:
            cohort_filters = imported_data['cohort_filters']
            if cohort_filters.get('age_range'):
                age_range = cohort_filters['age_range']
                preview_content.append(html.Li(f"Age Range: {age_range[0]} - {age_range[1]}"))
            if cohort_filters.get('substudies'):
                preview_content.append(html.Li(f"Study Sites: {', '.join(cohort_filters['substudies'])}"))
            if cohort_filters.get('sessions'):
                preview_content.append(html.Li(f"Sessions: {', '.join(map(str, cohort_filters['sessions']))}"))

        if 'phenotypic_filters' in imported_data and imported_data['phenotypic_filters']:
            pf_count = len(imported_data['phenotypic_filters'])
            preview_content.append(html.Li(f"Phenotypic Filters: {pf_count} filters"))

        if 'export_selection' in imported_data:
            export_selection = imported_data['export_selection']
            if export_selection.get('selected_tables'):
                preview_content.append(html.Li(f"Tables: {', '.join(export_selection['selected_tables'])}"))
            if export_selection.get('enwiden_longitudinal'):
                preview_content.append(html.Li("✓ Enwiden longitudinal data"))
            if export_selection.get('consolidate_baseline'):
                preview_content.append(html.Li("✓ Consolidate baseline sessions"))

        # Generate validation results display
        validation_content = []
        has_errors = bool(validation_errors)

        if validation_errors:
            validation_content.append(dbc.Alert([
                html.H6("Validation Errors:", className="alert-heading"),
                html.Ul([html.Li(error) for error in validation_errors])
            ], color="danger"))
        else:
            validation_content.append(dbc.Alert([
                html.I(className="bi bi-check-circle me-2"),
                "All parameters validated successfully!"
            ], color="success"))

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

        if any(invalid_params.values() if isinstance(invalid_params, dict) else [invalid_params]):
            validation_content.append(html.H6("Will be skipped (invalid):", className="text-danger mt-3"))
            skip_items = []
            if isinstance(invalid_params, dict):
                if invalid_params.get('cohort_filters'):
                    skip_items.append(f"Cohort filters: {len(invalid_params['cohort_filters'])} items")
                if invalid_params.get('phenotypic_filters'):
                    skip_items.append(f"Phenotypic filters: {len(invalid_params['phenotypic_filters'])} items")
                if invalid_params.get('export_selection', {}).get('selected_tables'):
                    skip_items.append(f"Export tables: {len(invalid_params['export_selection']['selected_tables'])} items")
            
            if skip_items:
                validation_content.append(html.Ul([html.Li(item) for item in skip_items]))

        # Determine if import should be allowed
        import_disabled = has_errors
        
        return [
            dbc.Alert(f"✓ Successfully parsed {filename}", color="success"),
            html.Div(preview_content),  # import-preview-content
            {'display': 'block'},  # import-preview-content style
            html.Div(validation_content),  # import-validation-results
            {'display': 'block'},  # import-validation-results style
            import_disabled,  # confirm-import-button disabled
            file_content,  # imported-file-content-store
            validation_results  # import-validation-results-store
        ]

    except Exception as e:
        logging.error(f"Error processing uploaded file: {e}")
        error_content = dbc.Alert([
            html.H6("File Processing Error:", className="alert-heading"),
            html.P(str(e))
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


def apply_imported_parameters(confirm_clicks, validation_results, file_content):
    """Apply imported query parameters to the UI components."""
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


def register_callbacks(app):
    """Register all state management callbacks with the Dash app."""
    from dash import Input, Output, State
    
    # Store Update Callbacks
    app.callback(
        Output('study-site-store', 'data'),
        Input('study-site-dropdown', 'value'),
        prevent_initial_call=True
    )(update_study_site_store)
    
    app.callback(
        Output('session-selection-store', 'data'),
        Input('session-dropdown', 'value'),
        prevent_initial_call=True
    )(update_session_selection_store)
    
    # State Restoration Callbacks
    app.callback(
        Output('table-multiselect', 'value', allow_duplicate=True),
        Input('available-tables-store', 'data'),
        State('table-multiselect-state-store', 'data'),
        prevent_initial_call=True
    )(restore_table_multiselect_value)
    
    app.callback(
        Output('enwiden-data-checkbox', 'value', allow_duplicate=True),
        Input('merge-keys-store', 'data'),
        State('enwiden-data-checkbox-state-store', 'data'),
        prevent_initial_call=True
    )(restore_enwiden_checkbox_value)
    
    app.callback(
        Output('study-site-dropdown', 'value', allow_duplicate=True),
        Input('demographics-columns-store', 'data'),
        State('study-site-store', 'data'),
        prevent_initial_call=True
    )(restore_study_site_dropdown_value)
    
    app.callback(
        Output('session-dropdown', 'value', allow_duplicate=True),
        Input('session-values-store', 'data'),
        State('session-selection-store', 'data'),
        prevent_initial_call=True
    )(restore_session_dropdown_value)
    
    # State Persistence Callback
    app.callback(
        [Output('age-slider-state-store', 'data'),
         Output('table-multiselect-state-store', 'data'),
         Output('enwiden-data-checkbox-state-store', 'data')],
        [Input('age-slider', 'value'),
         Input('table-multiselect', 'value'),
         Input('enwiden-data-checkbox', 'value')]
    )(save_all_filter_states)
    
    # UI State Callbacks
    app.callback(
        Output('enwiden-checkbox-wrapper', 'style'),
        Input('merge-keys-store', 'data')
    )(update_enwiden_checkbox_visibility)
    
    # Export Parameter Callbacks
    app.callback(
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
    )(toggle_export_modal)
    
    app.callback(
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
    )(export_query_parameters)
    
    # Import Parameter Callbacks
    app.callback(
        Output('import-query-modal', 'is_open'),
        [Input('import-query-button', 'n_clicks'),
         Input('cancel-import-button', 'n_clicks'),
         Input('confirm-import-button', 'n_clicks')],
        State('import-query-modal', 'is_open'),
        prevent_initial_call=True
    )(toggle_import_modal)
    
    app.callback(
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
    )(handle_file_upload)
    
    app.callback(
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
    )(apply_imported_parameters)