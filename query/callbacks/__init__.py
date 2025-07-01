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
    # Import all callback modules to ensure they are registered
    # The @callback decorators will automatically register them with Dash
    from . import data_loading, filters, export, state
    
    # Note: The actual registration happens when the modules are imported
    # because each callback uses the @callback decorator
    data_loading.register_callbacks(app)
    filters.register_callbacks(app)
    export.register_callbacks(app)
    state.register_callbacks(app)
    
    print("Phase 2: Modular query callbacks registered successfully")

# Auto-register callbacks when this module is imported
# This ensures callbacks are available when pages/query.py imports this module
try:
    import dash
    # Get the current Dash app instance
    app = dash.get_app()
    if app:
        register_all_callbacks(app)
except Exception as e:
    # This is expected during initial import before app is created
    pass

__all__ = ['register_all_callbacks']