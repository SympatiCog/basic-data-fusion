import dash
from dash import dcc
import dash_bootstrap_components as dbc

app = dash.Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.SLATE], suppress_callback_exceptions=True)

app.layout = dbc.Container([
    dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Query Data", href="/")),
            dbc.NavItem(dbc.NavLink("Profile Data", href="/profiling")),
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
    dcc.Store(id='sex-dropdown-state-store', storage_type='local'),
    dcc.Store(id='table-multiselect-state-store', storage_type='local'),
    dcc.Store(id='enwiden-data-checkbox-state-store', storage_type='local')
], fluid=True)

if __name__ == '__main__':
    app.run(debug=True)
