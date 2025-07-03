"""
UI building helper functions for the Query interface.

This module contains functions that generate dynamic UI components
based on data and user selections.
"""

import dash_bootstrap_components as dbc
from dash import html


def build_file_information_section(filename, import_timestamp, metadata):
    """Build the file information section for query details modal."""
    content = []
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
    return content


def build_user_notes_section(metadata):
    """Build the user notes section for query details modal."""
    content = []
    if metadata.get('user_notes'):
        content.append(html.H5("Notes", className="mb-3 mt-4"))
        content.append(dbc.Card([
            dbc.CardBody([
                html.P(metadata['user_notes'], className="mb-0", style={'color': 'black'})
            ], style={'background-color': 'white'})
        ], className="mb-3", style={'background-color': 'white'}))
    return content


def build_cohort_filters_section(cohort_filters):
    """Build the cohort filters section for query details modal."""
    content = []
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
    return content


def build_phenotypic_filters_section(phenotypic_filters):
    """Build the phenotypic filters section for query details modal."""
    content = []
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
    return content


def build_export_selection_section(export_selection):
    """Build the export selection section for query details modal."""
    content = []
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


def build_query_details_content(query_metadata):
    """Build complete query details modal content from query metadata."""
    filename = query_metadata.get('filename', 'Unknown')
    metadata = query_metadata.get('metadata', {})
    import_timestamp = query_metadata.get('import_timestamp', 'Unknown')
    toml_content = query_metadata.get('full_toml_content', '')

    # Parse the TOML content to display formatted details
    from query.query_parameters import import_query_parameters_from_toml
    imported_data, _ = import_query_parameters_from_toml(toml_content)

    content = []

    # File Information
    content.extend(build_file_information_section(filename, import_timestamp, metadata))

    # User Notes
    content.extend(build_user_notes_section(metadata))

    # Query Parameters
    content.append(html.H5("Query Parameters", className="mb-3 mt-4"))

    # Cohort Filters
    cohort_filters = imported_data.get('cohort_filters', {})
    content.extend(build_cohort_filters_section(cohort_filters))

    # Phenotypic Filters
    phenotypic_filters = imported_data.get('phenotypic_filters', [])
    content.extend(build_phenotypic_filters_section(phenotypic_filters))

    # Export Selection
    export_selection = imported_data.get('export_selection', {})
    content.extend(build_export_selection_section(export_selection))

    return content


# Legacy placeholder functions (to be implemented in future phases if needed)
def build_phenotypic_filter_card(filter_data):
    """Build a phenotypic filter card based on filter data."""
    # To be implemented in future phases if needed
    pass


def build_demographic_filter_section(demo_data):
    """Build demographic filter section based on available data."""
    # To be implemented in future phases if needed
    pass


def build_column_selection_ui(table_data):
    """Build column selection UI based on selected tables."""
    # To be implemented in future phases if needed
    pass


def build_data_preview_table(data):
    """Build data preview table for display."""
    # To be implemented in future phases if needed
    pass