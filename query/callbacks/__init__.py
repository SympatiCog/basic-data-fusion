"""
Callback functions for the Query module.

This package contains all Dash callback functions organized by functionality:
- data_loading: Data status and table information callbacks
- filters: Demographic and phenotypic filter callbacks
- export: Data generation and export callbacks
- state: State management and persistence callbacks
"""

import time
import logging
from typing import Dict, List, Optional

# Track registered callbacks to prevent duplicates
_registered_callbacks = set()
_registration_stats = {}

# Define expected callback modules and their minimum callback counts
CALLBACK_MODULES = {
    'data_loading': {'min_callbacks': 3, 'description': 'Data loading and status'},
    'filters': {'min_callbacks': 4, 'description': 'Filter management'},
    'export': {'min_callbacks': 2, 'description': 'Data export and generation'},
    'state': {'min_callbacks': 15, 'description': 'State management and persistence'}
}

def register_all_callbacks(app, verbose: bool = True) -> Dict[str, any]:
    """
    Register all query page callbacks with the Dash app.
    
    Args:
        app: The Dash application instance
        verbose: Whether to print detailed registration information
        
    Returns:
        Dict containing registration statistics and status
    """
    if not app:
        raise ValueError("Valid Dash app instance required for callback registration")
    
    # Check if callbacks are already registered for this app
    app_id = id(app)
    if app_id in _registered_callbacks:
        if verbose:
            print("Modular query callbacks already registered for this app instance")
        return _registration_stats.get(app_id, {})
    
    # Initialize registration tracking
    start_time = time.time()
    registration_results = {
        'app_id': app_id,
        'start_time': start_time,
        'modules': {},
        'total_callbacks': 0,
        'success': False,
        'errors': []
    }
    
    # Import callback modules
    modules = {}
    try:
        from . import data_loading, filters, export, state
        modules = {
            'data_loading': data_loading,
            'filters': filters,
            'export': export,
            'state': state
        }
    except ImportError as e:
        error_msg = f"Failed to import callback modules: {e}"
        registration_results['errors'].append(error_msg)
        if verbose:
            print(f"Error: {error_msg}")
        raise
    
    # Register callbacks from each module
    successful_modules = []
    failed_modules = []
    
    for module_name, module in modules.items():
        module_start = time.time()
        module_info = CALLBACK_MODULES.get(module_name, {})
        
        try:
            # Count callbacks before registration
            # Dash stores callbacks in different places depending on version
            callbacks_before = 0
            if hasattr(app, '_callback_map'):
                callbacks_before = len(app._callback_map)
            elif hasattr(app, 'callback_map'):
                callbacks_before = len(app.callback_map)
            
            # Register module callbacks
            if hasattr(module, 'register_callbacks'):
                module.register_callbacks(app)
            else:
                raise AttributeError(f"Module {module_name} missing register_callbacks function")
            
            # Count callbacks after registration
            callbacks_after = 0
            if hasattr(app, '_callback_map'):
                callbacks_after = len(app._callback_map)
            elif hasattr(app, 'callback_map'):
                callbacks_after = len(app.callback_map)
            
            callbacks_registered = callbacks_after - callbacks_before
            
            # If we can't count callbacks directly, estimate based on module
            if callbacks_registered == 0:
                estimated_count = module_info.get('min_callbacks', 1)
                callbacks_registered = f"~{estimated_count} (estimated)"
            
            # Validate minimum callback count (only for numeric counts)
            min_expected = module_info.get('min_callbacks', 1)
            if isinstance(callbacks_registered, int) and callbacks_registered < min_expected:
                logging.warning(
                    f"Module {module_name} registered {callbacks_registered} callbacks, "
                    f"expected at least {min_expected}"
                )
            
            # Record module registration success
            module_duration = time.time() - module_start
            registration_results['modules'][module_name] = {
                'success': True,
                'callbacks_registered': callbacks_registered,
                'duration_ms': round(module_duration * 1000, 2),
                'description': module_info.get('description', 'Unknown')
            }
            # Add to total only if numeric
            if isinstance(callbacks_registered, int):
                registration_results['total_callbacks'] += callbacks_registered
            successful_modules.append(module_name)
            
            if verbose:
                print(f"✓ {module_name}: {callbacks_registered} callbacks registered "
                      f"({module_duration*1000:.1f}ms)")
                      
        except Exception as e:
            error_msg = f"Failed to register {module_name} callbacks: {e}"
            registration_results['modules'][module_name] = {
                'success': False,
                'error': str(e),
                'duration_ms': round((time.time() - module_start) * 1000, 2)
            }
            registration_results['errors'].append(error_msg)
            failed_modules.append(module_name)
            
            if verbose:
                print(f"✗ {module_name}: Registration failed - {e}")
    
    # Finalize registration
    total_duration = time.time() - start_time
    registration_results['duration_ms'] = round(total_duration * 1000, 2)
    registration_results['end_time'] = time.time()
    
    if failed_modules:
        # Partial or complete failure
        error_summary = f"Failed to register {len(failed_modules)} modules: {', '.join(failed_modules)}"
        registration_results['errors'].append(error_summary)
        
        if len(failed_modules) == len(modules):
            # Complete failure
            if verbose:
                print(f"Error: Complete callback registration failure")
            raise RuntimeError(f"All callback modules failed to register: {registration_results['errors']}")
        else:
            # Partial failure - log warning but continue
            logging.warning(f"Partial callback registration failure: {error_summary}")
            
    else:
        # Complete success
        registration_results['success'] = True
        _registered_callbacks.add(app_id)
        
        if verbose:
            print(f"Modular query callbacks registered successfully: "
                  f"{registration_results['total_callbacks']} callbacks in {total_duration*1000:.1f}ms")
    
    # Store results for future reference
    _registration_stats[app_id] = registration_results
    
    return registration_results

def get_registration_stats(app_id: Optional[int] = None) -> Dict:
    """
    Get callback registration statistics.
    
    Args:
        app_id: Specific app ID to get stats for, or None for all
        
    Returns:
        Registration statistics
    """
    if app_id is not None:
        return _registration_stats.get(app_id, {})
    return _registration_stats.copy()

def is_registered(app) -> bool:
    """
    Check if callbacks are registered for a specific app.
    
    Args:
        app: The Dash application instance
        
    Returns:
        True if callbacks are registered, False otherwise
    """
    return id(app) in _registered_callbacks

def unregister_callbacks(app) -> bool:
    """
    Mark callbacks as unregistered for a specific app.
    Note: This doesn't actually remove callbacks from Dash,
    just allows re-registration.
    
    Args:
        app: The Dash application instance
        
    Returns:
        True if app was registered, False otherwise
    """
    app_id = id(app)
    if app_id in _registered_callbacks:
        _registered_callbacks.remove(app_id)
        if app_id in _registration_stats:
            del _registration_stats[app_id]
        return True
    return False

__all__ = [
    'register_all_callbacks',
    'get_registration_stats', 
    'is_registered',
    'unregister_callbacks'
]