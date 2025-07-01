"""
Filter management callbacks for the Query interface.

This module contains callbacks responsible for:
- Age slider updates
- Dynamic demographic filters
- Phenotypic filter management
- Live participant count updates
"""

import logging
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html, no_update, MATCH

from analysis.demographics import has_multisite_data
from config_manager import get_config
from utils import (
    MergeKeys,
    get_study_site_values,
    get_unique_column_values,
    is_numeric_dtype,
    get_db_connection,
)


@callback(
    [Output('age-slider', 'min'),
     Output('age-slider', 'max'),
     Output('age-slider', 'value'),
     Output('age-slider', 'marks'),
     Output('age-slider', 'disabled'),
     Output('age-slider-info', 'children')],
    [Input('demographics-columns-store', 'data'),
     Input('column-ranges-store', 'data')],
    [State('age-slider-state-store', 'data')]
)
def update_age_slider(demo_cols, col_ranges, stored_age_value):
    """Update age slider properties based on demographic data."""
    config = get_config()  # Get fresh config
    if not demo_cols or config.AGE_COLUMN not in demo_cols or not col_ranges:
        return 0, 100, [0, 100], {}, True, f"Age filter disabled: '{config.AGE_COLUMN}' column not found in demographics or ranges not available."

    # Look up age column range using simple column name
    age_col_key = config.AGE_COLUMN

    if age_col_key in col_ranges:
        min_age, max_age = col_ranges[age_col_key]
        min_age = int(min_age)
        max_age = int(max_age)

        default_min, default_max = config.DEFAULT_AGE_SELECTION

        # Use stored value if available and valid, otherwise use default
        if stored_age_value is not None and len(stored_age_value) == 2:
            stored_min, stored_max = stored_age_value
            if min_age <= stored_min <= max_age and min_age <= stored_max <= max_age:
                value = stored_age_value
            else:
                value = [max(min_age, default_min), min(max_age, default_max)]
        else:
            value = [max(min_age, default_min), min(max_age, default_max)]

        marks = {i: str(i) for i in range(min_age, max_age + 1, 10)}
        if min_age not in marks:
            marks[min_age] = str(min_age)
        if max_age not in marks:
            marks[max_age] = str(max_age)

        return min_age, max_age, value, marks, False, f"Age range: {min_age}-{max_age}"
    else:
        # Fallback if age column is in demo_cols but no range found (should ideally not happen if get_table_info is robust)
        return 0, 100, [config.DEFAULT_AGE_SELECTION[0], config.DEFAULT_AGE_SELECTION[1]], {}, True, f"Age filter disabled: Range for '{config.AGE_COLUMN}' column not found."


@callback(
    Output('dynamic-demo-filters-placeholder', 'children'),
    [Input('demographics-columns-store', 'data'),
     Input('session-values-store', 'data'),
     Input('merge-keys-store', 'data'),
     Input('study-site-store', 'data'),
     Input('session-selection-store', 'data')]
)
def update_dynamic_demographic_filters(demo_cols, session_values, merge_keys_dict,
                                     input_rockland_values, input_session_values):
    """Update dynamic demographic filters based on available data."""
    config = get_config()  # Get fresh config
    if not demo_cols:
        return html.P("Demographic information not yet available to populate dynamic filters.")

    children = []

    # Multisite/Multistudy Filters
    if has_multisite_data(demo_cols, config.STUDY_SITE_COLUMN):
        children.append(html.H5("Study Site Selection", style={'marginTop': '15px'}))

        # Get actual study site values from data
        study_site_values = get_study_site_values(config)
        if not study_site_values:
            # Fallback to Rockland defaults if no values found
            study_site_values = config.ROCKLAND_BASE_STUDIES

        # Use input values if available, otherwise use default (all sites selected)
        selected_sites = input_rockland_values if input_rockland_values else study_site_values

        children.append(
            dcc.Dropdown(
                id='study-site-dropdown',
                options=[{'label': s, 'value': s} for s in study_site_values],
                value=selected_sites,
                multi=True,
                placeholder="Select Study Sites..."
            )
        )

    # Session Filters
    if merge_keys_dict:
        mk = MergeKeys.from_dict(merge_keys_dict)
        if mk.is_longitudinal and mk.session_id and session_values:
            children.append(html.H5("Session/Visit Selection", style={'marginTop': '15px'}))
            # Use input values if available, otherwise default to all available sessions
            session_value = input_session_values if input_session_values else session_values
            children.append(
                dcc.Dropdown(
                    id='session-dropdown',
                    options=[{'label': s, 'value': s} for s in session_values],
                    value=session_value,
                    multi=True,
                    placeholder=f"Select {mk.session_id} values..."
                )
            )

    if not children:
        return html.P("No dataset-specific demographic filters applicable.", style={'fontStyle': 'italic'})

    return children


# Live Participant Count Callback (optimized with cached DB connection)
@callback(
    Output('live-participant-count', 'children'),
    [Input('age-slider', 'value'),
     Input('study-site-store', 'data'), # For Rockland substudies
     Input('session-selection-store', 'data'), # For session filtering
     Input('phenotypic-filters-store', 'data'), # For phenotypic filters
     # Data stores needed for query generation
     Input('merge-keys-store', 'data'),
     Input('available-tables-store', 'data')],
    prevent_initial_call=False
)
def update_live_participant_count(
    age_range,
    rockland_substudy_values, # Rockland substudies from store
    session_values, # Session values from store
    phenotypic_filters_state, # Phenotypic filters state
    merge_keys_dict, available_tables,
):
    """Update live participant count based on current filter settings."""
    config = get_config()  # Get fresh config

    # Early return if no data loaded yet
    if not merge_keys_dict or not available_tables:
        return html.P("Loading data...", style={'color': 'gray'})

    try:
        merge_keys = MergeKeys.from_dict(merge_keys_dict)

        # Build demographic filters using the structure expected by secure query functions
        demographic_filters = {}

        # Age filter - use 'age_range' key as expected by secure query
        if age_range and len(age_range) == 2:
            demographic_filters['age_range'] = age_range

        # Study site filter - use 'substudies' key as expected by secure query
        if rockland_substudy_values:
            demographic_filters['substudies'] = rockland_substudy_values

        # Session filter - use 'sessions' key as expected by secure query
        if session_values and merge_keys.is_longitudinal:
            demographic_filters['sessions'] = session_values

        # Convert phenotypic filters to behavioral format
        from query.helpers.data_formatters import convert_phenotypic_to_behavioral_filters
        behavioral_filters = convert_phenotypic_to_behavioral_filters(phenotypic_filters_state)

        # Generate count query using existing query infrastructure  
        from query.query_secure import generate_base_query_logic_secure, generate_count_query_secure

        try:
            # First generate the base query logic
            base_query_logic, base_params = generate_base_query_logic_secure(
                config_params={
                    'data_dir': config.DATA_DIR,
                    'demographics_file': config.DEMOGRAPHICS_FILE,
                    'age_column': config.AGE_COLUMN,
                    'study_site_column': config.STUDY_SITE_COLUMN
                },
                merge_keys=merge_keys,
                demographic_filters=demographic_filters,
                behavioral_filters=behavioral_filters,
                tables_to_join=available_tables or []
            )
            
            # Then generate the count query from the base query
            count_query, count_params = generate_count_query_secure(
                base_query_logic=base_query_logic,
                params=base_params,
                merge_keys=merge_keys
            )

            # Execute count query using shared connection (safer for file handles)
            try:
                conn = get_db_connection()
                result = conn.execute(count_query, count_params).fetchone()
                count = result[0] if result else 0
            except Exception as db_error:
                logging.error(f"Database query failed: {db_error}")
                return html.Div([
                    html.I(className="bi bi-exclamation-triangle me-2", style={'color': 'red'}),
                    html.Span("Error executing count query", style={'color': 'red'})
                ])

            if count == 0:
                return html.Div([
                    html.I(className="bi bi-exclamation-triangle me-2", style={'color': 'orange'}),
                    html.Span(f"0 participants match current filters", style={'color': 'orange'})
                ])
            else:
                return html.Div([
                    html.I(className="bi bi-people-fill me-2", style={'color': 'green'}),
                    html.Span(f"{count:,} participants match current filters", style={'color': 'green', 'fontWeight': 'bold'})
                ])

        except Exception as query_error:
            logging.error(f"Error executing count query: {query_error}")
            return html.Div([
                html.I(className="bi bi-exclamation-triangle me-2", style={'color': 'red'}),
                html.Span("Error calculating participant count", style={'color': 'red'})
            ])

    except Exception as e:
        logging.error(f"Error in live participant count: {e}")
        return html.Div([
            html.I(className="bi bi-exclamation-triangle me-2", style={'color': 'red'}),
            html.Span("Error updating participant count", style={'color': 'red'})
        ])


# Note: The following large callbacks still need to be extracted from pages/query.py:
# - manage_phenotypic_filters() - Complex state management for phenotypic filters
# - render_phenotypic_filters() - UI rendering for phenotypic filters
# - update_phenotypic_session_notice() - Session notice updates
# These will be completed in the next iteration of Phase 2


def register_callbacks(app):
    """Register all filter management callbacks with the Dash app."""
    # All callbacks are already registered with @callback decorator
    # This function is called from the main callback registration system
    pass