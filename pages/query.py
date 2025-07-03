# NOTE: This file is now a minimal compatibility layer
# 
# All query page functionality has been moved to the modular system:
# - Page registration: handled in app.py
# - UI layout: query/ui/layout.py
# - Callbacks: query/callbacks/ modules
# - Components: query/ui/components.py
# - Helpers: query/helpers/ modules
#
# This file exists only for backward compatibility and can be removed
# once the integration is verified to work correctly.

# Legacy imports kept for compatibility
import logging
from datetime import datetime
import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, State, callback, dash_table, dcc, html, no_update, MATCH
from analysis.demographics import has_multisite_data
from config_manager import get_config
from utils import (
    MergeKeys,
    _file_access_lock,
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
from query.callbacks import register_all_callbacks

# Compatibility note: All functionality has been moved to modular system
print("WARNING: pages/query.py is deprecated. All functionality moved to modular query system.")