"""
Callback functions for the Query module.

This package contains all Dash callback functions organized by functionality:
- data_loading: Data status and table information callbacks
- filters: Demographic and phenotypic filter callbacks
- export: Data generation and export callbacks
- state: State management and persistence callbacks
"""

def register_all_callbacks(app):
    """
    Register all query page callbacks with the Dash app.
    
    Args:
        app: The Dash application instance
    """
    from . import data_loading, filters, export, state
    
    data_loading.register_callbacks(app)
    filters.register_callbacks(app)
    export.register_callbacks(app)
    state.register_callbacks(app)

__all__ = ['register_all_callbacks']