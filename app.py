import argparse
import threading
import time
import webbrowser

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, dcc, html, no_update

# Import StateManager for session management
from config_manager import get_state_manager_config
from session_manager import get_or_create_session
from state_manager import get_state_manager

app = dash.Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.SLATE], suppress_callback_exceptions=True)

app.layout = dbc.Container([
    # Global location component for handling redirects
    dcc.Location(id='global-location', refresh=False),
    # User session management for StateManager
    dcc.Store(id='user-session-id', storage_type='session'),
    # Dedicated store for empty state check to avoid callback loops
    dcc.Store(id='empty-state-store', storage_type='session'),
    dbc.Navbar(
        id='main-navbar',
        children=[
            dbc.Container([
                dbc.Row([
                    # Thumbs up logo on the left
                    dbc.Col(
                        html.A(
                            html.Img(
                                src="/assets/thumbsup.png",
                                height="75px",
                                style={"cursor": "pointer"}
                            ),
                            href="/",
                            id="thumbs-up-logo"
                        ),
                        width="auto",
                        className="d-flex align-items-center"
                    ),
                    # Brand name
                    dbc.Col(
                        dbc.NavbarBrand("Basic Data Fusion", href="/", className="ms-2"),
                        width="auto",
                        className="d-flex align-items-center"
                    ),
                    # Navigation items on the right
                    dbc.Col(
                        dbc.Nav([
                            dbc.NavItem(dbc.NavLink("Query Data", href="/")),
                            dbc.NavItem(dbc.NavLink("Import Data", href="/import")),
                            dbc.NavItem(dbc.NavLink("Profile Data", href="/profiling")),
                            dbc.NavItem(dbc.NavLink("Plot Data", href="/plotting")),
                            dbc.NavItem(dbc.NavLink("Settings", href="/settings")),
                        ], className="ms-auto", navbar=True),
                        className="d-flex justify-content-end"
                    )
                ], className="w-100 align-items-center")
            ], fluid=True)
        ],
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
    dcc.Store(id='study-site-store', storage_type='local', data=[]),
    dcc.Store(id='session-selection-store', storage_type='local', data=[]),
    dcc.Store(id='phenotypic-filters-store', storage_type='local', data={'filters': [], 'next_id': 1}),
    dcc.Store(id='selected-columns-per-table-store', storage_type='session'),
    # Filter state stores (using local storage for persistence)
    dcc.Store(id='age-slider-state-store', storage_type='local'),
    dcc.Store(id='table-multiselect-state-store', storage_type='local'),
    dcc.Store(id='enwiden-data-checkbox-state-store', storage_type='local'),
    # Plotting page state stores (using local storage for persistence across navigation)
    dcc.Store(id='plot-type-state-store', storage_type='local'),
    dcc.Store(id='plot-config-state-store', storage_type='local'),
    dcc.Store(id='plot-analysis-options-store', storage_type='local'),
    dcc.Store(id='selected-plot-points-store', storage_type='session'),
    # Profiling page state stores
    dcc.Store(id='profiling-options-state-store', storage_type='local'),
    # Query parameter import/export stores
    dcc.Store(id='import-validation-results-store', storage_type='session'),
    dcc.Store(id='imported-file-content-store', storage_type='session'),
    dcc.Store(id='query-export-modal-state', storage_type='session'),
    dcc.Store(id='query-import-modal-state', storage_type='session'),
    dcc.Store(id='current-query-metadata-store', storage_type='local')
], fluid=True)

# Session clearing disabled to prevent conflicts
# app.clientside_callback(
#     """
#     function() {
#         // Clear any existing session storage for user-session-id to prevent multiple sessions
#         // This helps with the multiple session issue during development
#         if (window.sessionStorage) {
#             const keys = Object.keys(window.sessionStorage);
#             keys.forEach(key => {
#                 if (key.includes('user-session-id')) {
#                     console.log('Clearing old session:', key);
#                     window.sessionStorage.removeItem(key);
#                 }
#             });
#         }
#         return window.dash_clientside.no_update;
#     }
#     """,
#     Output('global-location', 'refresh'),  # Dummy output
#     Input('global-location', 'id'),
#     prevent_initial_call=False
# )

# Initialize StateManager with configuration
try:
    state_manager_config = get_state_manager_config()
    state_manager = get_state_manager(state_manager_config)
    print(f"StateManager initialized with {state_manager_config.backend_type} backend")
except Exception as e:
    print(f"Warning: StateManager initialization failed: {e}")
    # Fallback to default client backend
    state_manager = get_state_manager()

# Initialize user session ONCE on app startup
@app.callback(
    Output('user-session-id', 'data'),
    [Input('global-location', 'id')],  # Trigger only once on component creation
    [State('user-session-id', 'data')], # Check existing session
    prevent_initial_call=False
)
def initialize_user_session(_, existing_session_id):
    """Initialize user session ID for StateManager isolation - ONCE per user session"""
    session_id, is_new = get_or_create_session(existing_session_id)

    if existing_session_id and not is_new:
        return no_update  # Don't change the existing session ID

    return session_id

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

    return [
        dbc.Container([
            dbc.Row([
                # Thumbs up logo on the left
                dbc.Col(
                    html.A(
                        html.Img(
                            src="/assets/thumbsup.png",
                            height="75px",
                            style={"cursor": "pointer"}
                        ),
                        href="/",
                        id="thumbs-up-logo"
                    ),
                    width="auto",
                    className="d-flex align-items-center"
                ),
                # Brand name
                dbc.Col(
                    dbc.NavbarBrand("Basic Data Fusion", href="/", className="ms-2"),
                    width="auto",
                    className="d-flex align-items-center"
                ),
                # Navigation items on the right
                dbc.Col(
                    dbc.Nav(base_nav_items, className="ms-auto", navbar=True),
                    className="d-flex justify-content-end"
                )
            ], className="w-100 align-items-center")
        ], fluid=True)
    ]

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

    # Enable reloader for auto-refresh when code changes
    app.run(debug=True, port=port, use_reloader=False)
