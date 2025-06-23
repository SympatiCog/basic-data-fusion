import dash
from dash import dcc, Input, Output
import dash_bootstrap_components as dbc
import argparse
import webbrowser
import threading
import time

app = dash.Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.SLATE], suppress_callback_exceptions=True)

app.layout = dbc.Container([
    # Global location component for handling redirects
    dcc.Location(id='global-location', refresh=False),
    # Dedicated store for empty state check to avoid callback loops
    dcc.Store(id='empty-state-store', storage_type='session'),
    dbc.NavbarSimple(
        id='main-navbar',
        children=[
            dbc.NavItem(dbc.NavLink("Query Data", href="/")),
            dbc.NavItem(dbc.NavLink("Import Data", href="/import")),
            dbc.NavItem(dbc.NavLink("Profile Data", href="/profiling")),
            dbc.NavItem(dbc.NavLink("Plot Data", href="/plotting")),
            dbc.NavItem(dbc.NavLink("Settings", href="/settings")),
        ],
        brand="Basic Data Fusion",
        brand_href="/",
        color="dark",
        dark=True,
        className="mb-2",
    ),
    dash.page_container,
    # Shared stores that need to be accessible across pages
    dcc.Store(id='merged-dataframe-store', storage_type='session'),
    dcc.Store(id='app-config-store', storage_type='local'),
    # Persistent query page state stores
    dcc.Store(id='available-tables-store', storage_type='session'),
    dcc.Store(id='demographics-columns-store', storage_type='session'),
    dcc.Store(id='behavioral-columns-store', storage_type='session'),
    dcc.Store(id='column-dtypes-store', storage_type='session'),
    dcc.Store(id='column-ranges-store', storage_type='session'),
    dcc.Store(id='merge-keys-store', storage_type='session'),
    dcc.Store(id='session-values-store', storage_type='session'),
    dcc.Store(id='all-messages-store', storage_type='session'),
    dcc.Store(id='rockland-substudy-store', storage_type='session', data=[]),
    dcc.Store(id='session-selection-store', storage_type='session', data=[]),
    dcc.Store(id='phenotypic-filters-store', storage_type='session', data={'filters': [], 'next_id': 1}),
    dcc.Store(id='selected-columns-per-table-store', storage_type='session'),
    # Filter state stores (using local storage for persistence)
    dcc.Store(id='age-slider-state-store', storage_type='local'),
    dcc.Store(id='table-multiselect-state-store', storage_type='local'),
    dcc.Store(id='enwiden-data-checkbox-state-store', storage_type='local')
], fluid=True)

# Check for empty state only once on app startup
@app.callback(
    Output('empty-state-store', 'data'),
    [Input('global-location', 'id')],  # Trigger only once on component creation
    prevent_initial_call=False
)
def check_empty_state_on_startup(_):
    """Check for empty state on app startup"""
    from config_manager import get_config
    from utils import get_table_info
    
    try:
        config = get_config()
        (behavioral_tables, demographics_cols, behavioral_cols_by_table,
         col_dtypes, col_ranges, merge_keys_dict,
         actions_taken, session_vals, is_empty, messages) = get_table_info(config)
        
        if is_empty or not behavioral_tables:
            return {'redirect_needed': True}
        else:
            return {'redirect_needed': False}
    except Exception:
        return {'redirect_needed': True}

# Callback to conditionally show Setup navigation item only when data is empty
@app.callback(
    Output('main-navbar', 'children'),
    [Input('empty-state-store', 'data')]
)
def update_navbar(empty_state_data):
    """Add Setup navigation item only when data is empty"""
    base_nav_items = [
        dbc.NavItem(dbc.NavLink("Query Data", href="/")),
        dbc.NavItem(dbc.NavLink("Import Data", href="/import")),
        dbc.NavItem(dbc.NavLink("Profile Data", href="/profiling")),
        dbc.NavItem(dbc.NavLink("Plot Data", href="/plotting")),
        dbc.NavItem(dbc.NavLink("Settings", href="/settings")),
    ]
    
    # Add Setup link only if data is empty
    if empty_state_data and empty_state_data.get('redirect_needed', False):
        base_nav_items.insert(-1, dbc.NavItem(dbc.NavLink("Setup", href="/onboarding")))
    
    return base_nav_items

# Clientside callback to handle redirects
app.clientside_callback(
    """
    function(empty_state_data) {
        // Redirect to onboarding if data is empty and user is on root page
        if (empty_state_data && 
            empty_state_data.redirect_needed && 
            window.location.pathname === '/') {
            
            window.location.pathname = '/onboarding';
        }
        
        // Redirect away from onboarding if data exists and user tries to access it directly
        if (empty_state_data && 
            !empty_state_data.redirect_needed && 
            window.location.pathname === '/onboarding') {
            
            window.location.pathname = '/';
        }
        
        return window.dash_clientside.no_update;
    }
    """,
    Output('global-location', 'href'),
    [Input('empty-state-store', 'data')]
)

def open_browser(url, delay=1.5):
    """Open browser after a delay"""
    def _open():
        time.sleep(delay)
        try:
            webbrowser.open(url)
        except Exception as e:
            print(f"Could not open browser automatically: {e}")
    
    threading.Thread(target=_open, daemon=True).start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Basic Data Fusion - Laboratory Data Browser')
    parser.add_argument('--no-browser', action='store_true', 
                       help='Do not automatically open browser')
    args = parser.parse_args()
    
    port = 8050
    url = f"http://127.0.0.1:{port}"
    
    if not args.no_browser:
        open_browser(url)
    
    # Try disabling reloader which might cause multiple processes/browser opens
    app.run(debug=True, port=port, use_reloader=False)
