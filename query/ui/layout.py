"""
Main layout definition for the Query page.

This module contains the complete layout structure for the data query interface,
assembled from reusable components and following consistent styling.
"""

import dash_bootstrap_components as dbc
from dash import html

from .components import (
    create_data_status_section,
    create_logo_section,
    create_live_participant_count_section,
    create_query_management_section,
    create_demographic_filters_card,
    create_phenotypic_filters_card,
    create_data_export_card,
    create_query_results_card,
    create_export_parameters_modal,
    create_import_parameters_modal,
    create_query_details_modal
)


# Main page layout
layout = dbc.Container([
    # Data status and import link section
    create_data_status_section(),
    
    # Data Overview and Logo Section
    dbc.Row([
        dbc.Col([
            html.H3("Data Overview"),
            html.Div(id='merge-strategy-info'),
        ], width=6), # Left column for merge strategy info
        dbc.Col([
            create_logo_section()
        ], width=5) # Right column for logo
    ]),
    
    html.Br(), ## Standard spacing between sections
    
    # Live Participant Count and Query Management Section
    dbc.Row([
        dbc.Col([
            create_live_participant_count_section()
        ], width=6),
        dbc.Col([
            create_query_management_section()
        ], width=6)
    ]),
    
    # Define Cohort Filters Section Header
    dbc.Row([
        dbc.Col([
            html.H3("Define Cohort Filters"),
        ], width=12)
    ]),
    
    # Demographic and Phenotypic Filters Section
    dbc.Row([
        dbc.Col([
            create_demographic_filters_card()
        ], md=6), # Left column for demographic filters
        dbc.Col([
            create_phenotypic_filters_card()
        ], md=6) # Right column for phenotypic filters
    ]),
    
    # Data Exports Section
    dbc.Row([
        dbc.Col([
            html.H3("Data Exports"),
            create_data_export_card()
        ], md=12) # Full width for data export selection
    ]),
    
    # Query Results Section
    dbc.Row([
        dbc.Col([
            html.H3("Query Results"),
            create_query_results_card()
        ], width=12)
    ]),

    # Modals
    create_export_parameters_modal(),
    create_import_parameters_modal(),
    create_query_details_modal(),
    
], fluid=True)


# === PHASE 4 COMPLETE ===
# State management has been consolidated into a single store: 'consolidated-query-state-store'
# Individual stores remain for backward compatibility during the transition period
# See app.py lines 102-115 for the consolidated state store implementation