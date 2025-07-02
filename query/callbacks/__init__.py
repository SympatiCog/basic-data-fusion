"""
Callback functions for the Query module.

This package contains all Dash callback functions organized by functionality:
- data_loading: Data status and table information callbacks
- filters: Demographic and phenotypic filter callbacks
- export: Data generation and export callbacks
- state: State management and persistence callbacks
"""

# Track registered callbacks to prevent duplicates
_registered_callbacks = set()

def register_all_callbacks(app):
    """
    Register all query page callbacks with the Dash app.
    
    Args:
        app: The Dash application instance
    """
    if not app:
        raise ValueError("Valid Dash app instance required for callback registration")
    
    # Check if callbacks are already registered for this app
    app_id = id(app)
    if app_id in _registered_callbacks:
        print("Modular query callbacks already registered for this app instance")
        return
    
    # Import callback modules and register their callbacks
    from . import data_loading, filters, export, state
    
    try:
        # Register callbacks from each module
        data_loading.register_callbacks(app)
        filters.register_callbacks(app)
        export.register_callbacks(app)
        state.register_callbacks(app)
        
        # Mark as registered
        _registered_callbacks.add(app_id)
        print("Modular query callbacks registered successfully")
        
    except Exception as e:
        print(f"Error registering modular callbacks: {e}")
        raise

__all__ = ['register_all_callbacks']