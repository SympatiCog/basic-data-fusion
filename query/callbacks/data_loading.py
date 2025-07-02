"""
Data loading and status callbacks for the Query interface.

This module contains callbacks responsible for:
- Loading initial data information
- Updating data status sections
- Managing table multiselect options
- Column selection area updates
"""

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html, no_update

from config_manager import get_config
from utils import MergeKeys, get_table_info


@callback(
    Output('query-data-status-section', 'children'),
    [Input('available-tables-store', 'data'),
     Input('merge-keys-store', 'data')]
)
def update_data_status_section(available_tables, merge_keys_dict):
    """Show data status and import link if needed"""
    if not available_tables:
        # No data available - show import prompt
        return dbc.Row([
            dbc.Col([
                dbc.Alert([
                    html.I(className="bi bi-info-circle me-2"),
                    html.Strong("No data found. "),
                    html.A("Import CSV files", href="/import", className="alert-link"),
                    " to get started with data analysis."
                ], color="info")
            ], width=12)
        ], className="mb-4")
    else:
        # Data available - show quick summary
        num_tables = len(available_tables)
        return dbc.Row([
            dbc.Col([
                dbc.Alert([
                    html.I(className="bi bi-check-circle me-2"),
                    f"Data loaded: {num_tables} behavioral tables available. ",
                    html.A("Import more data", href="/import", className="alert-link"),
                    " if needed."
                ], color="info")
            ], width=12)
        ], className="mb-4")


@callback(
    [Output('available-tables-store', 'data'),
     Output('demographics-columns-store', 'data'),
     Output('behavioral-columns-store', 'data'),
     Output('column-dtypes-store', 'data'),
     Output('column-ranges-store', 'data'),
     Output('merge-keys-store', 'data'),
     Output('session-values-store', 'data'),
     Output('all-messages-store', 'data')], # To display errors from get_table_info
    [Input('query-data-status-section', 'id')], # Trigger on page load
    [State('user-session-id', 'data')] # User context for StateManager
)
def load_initial_data_info(_, user_session_id): # Trigger on page load
    """Load initial data information from get_table_info and populate stores."""
    # Re-fetch config if it could have changed (e.g., if settings were editable in another part of the app)
    # For now, assume config loaded at app start is sufficient, or re-initialize.
    current_config = get_config()  # Get fresh config instance

    (behavioral_tables, demographics_cols, behavioral_cols_by_table,
     col_dtypes, col_ranges, merge_keys_dict,
     actions_taken, session_vals, is_empty, messages) = get_table_info(current_config)

    # Note: Message display and merge strategy display are now handled by dedicated callbacks
    # This callback focuses on loading and storing the raw data from get_table_info

    # StateManager disabled to prevent state conflicts
    # state_manager = get_state_manager()
    # if user_session_id:
    #     # Import helper function from session_manager
    #     from session_manager import ensure_session_context
    #     context_changed = ensure_session_context(user_session_id)
    #
    #     # Store critical stores in StateManager (hybrid approach)
    #     try:
    #         state_manager.set_store_data('merge-keys-store', merge_keys_dict)
    #         state_manager.set_store_data('available-tables-store', behavioral_tables)
    #         state_manager.set_store_data('demographics-columns-store', demographics_cols)
    #         logging.info(f"Stored critical data in StateManager for user {user_session_id[:8]}...")
    #     except Exception as e:
    #         logging.error(f"Failed to store data in StateManager: {e}")

    return (behavioral_tables, demographics_cols, behavioral_cols_by_table,
            col_dtypes, col_ranges, merge_keys_dict, session_vals,
            messages) # Store raw messages from get_table_info for potential detailed display


@callback(
    Output('table-multiselect', 'options'),
    Input('available-tables-store', 'data')
)
def update_table_multiselect_options(available_tables_data):
    """Update table multiselect dropdown options based on available tables."""
    if not available_tables_data:
        return []
    # available_tables_data is a list of table names (strings)
    return [{'label': table, 'value': table} for table in available_tables_data]


@callback(
    Output('column-selection-area', 'children'),
    Input('table-multiselect', 'value'), # List of selected table names
    State('demographics-columns-store', 'data'),
    State('behavioral-columns-store', 'data'),
    State('merge-keys-store', 'data'),
    State('selected-columns-per-table-store', 'data') # To pre-populate selections
)
def update_column_selection_area(selected_tables, demo_cols, behavioral_cols, merge_keys_dict, stored_selections):
    """Update column selection area based on selected tables."""
    if not selected_tables:
        return dbc.Alert("Select tables above to choose columns for export.", color="info")

    if not demo_cols: demo_cols = []
    if not behavioral_cols: behavioral_cols = {}
    if not stored_selections: stored_selections = {}

    config = get_config()  # Get fresh config
    merge_keys = MergeKeys.from_dict(merge_keys_dict) if merge_keys_dict else MergeKeys(primary_id="unknown")
    id_cols_to_exclude = {merge_keys.primary_id, merge_keys.session_id, merge_keys.composite_id}
    demographics_table_name = config.get_demographics_table_name()

    cards = []
    for table_name in selected_tables:
        options = []
        actual_cols_for_table = []
        is_demographics_table = (table_name == demographics_table_name)

        if is_demographics_table:
            actual_cols_for_table = demo_cols
        elif table_name in behavioral_cols:
            actual_cols_for_table = behavioral_cols[table_name]

        for col in actual_cols_for_table:
            if col not in id_cols_to_exclude: # Exclude ID columns from selection
                options.append({'label': col, 'value': col})

        # Get previously selected columns for this table, if any
        current_selection_for_table = stored_selections.get(table_name, [])

        card_body_content = [
            dcc.Dropdown(
                id={'type': 'column-select-dropdown', 'table': table_name},
                options=options,
                value=current_selection_for_table, # Pre-populate with stored selections
                multi=True,
                placeholder=f"Select columns from {table_name}..."
            )
        ]
        # If it's the demographics table, add a note that ID columns are auto-included
        if is_demographics_table:
            card_body_content.insert(0, html.P(f"All demographics columns (including {merge_keys.get_merge_column()}) will be included by default. You can select additional ones if needed, or deselect to only include IDs/merge keys.", className="small text-muted"))

        cards.append(dbc.Card([
            dbc.CardHeader(f"Columns for: {table_name}"),
            dbc.CardBody(card_body_content)
        ], className="mb-3"))

    return cards


@callback(
    Output('selected-columns-per-table-store', 'data'),
    Input({'type': 'column-select-dropdown', 'table': dash.ALL}, 'value'), # Values from all column dropdowns
    State({'type': 'column-select-dropdown', 'table': dash.ALL}, 'id'), # IDs of all column dropdowns
    State('selected-columns-per-table-store', 'data') # Current stored data
    # Note: Removed table-multiselect cleanup logic temporarily to fix startup issues
)
def update_selected_columns_store(all_column_values, all_column_ids, current_stored_data):
    """Update the selected columns store based on dropdown selections."""
    ctx = dash.callback_context

    # Make a copy to modify, or initialize if None
    updated_selections = current_stored_data.copy() if current_stored_data else {}

    # Note: Table cleanup logic temporarily disabled due to component reference issues
    # TODO: Re-add cleanup when layout migration is complete

    # Only update if callback was actually triggered by user interaction
    # This prevents overwriting stored data on initial page load
    if ctx.triggered and all_column_ids and all_column_values:
        for i, component_id_dict in enumerate(all_column_ids):
            table_name = component_id_dict['table']
            selected_cols_for_table = all_column_values[i]

            if selected_cols_for_table is not None: # An empty selection is an empty list, None means no interaction yet
                updated_selections[table_name] = selected_cols_for_table

    return updated_selections


def register_callbacks(app):
    """Register all data loading callbacks with the Dash app."""
    # All callbacks are already registered with @callback decorator
    # This function is called from the main callback registration system
    pass