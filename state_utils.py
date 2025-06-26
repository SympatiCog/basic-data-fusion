"""
Utility functions and decorators for StateManager integration.
Provides helper functions to ease the transition from dcc.Store to StateManager.
"""

import functools
import logging
from typing import Any, Callable, Optional, Union, List, Dict
import dash
from dash import callback_context

from state_manager import get_state_manager, StateManagerConfig

logger = logging.getLogger(__name__)


def state_managed_callback(store_mappings: Dict[str, str] = None, 
                          user_context_input: str = 'user-session-id'):
    """
    Decorator to automatically handle StateManager integration in Dash callbacks.
    
    Args:
        store_mappings: Dictionary mapping callback parameter names to store IDs
        user_context_input: The input ID that contains the user session ID
    
    Example:
        @state_managed_callback(store_mappings={'merge_keys': 'merge-keys-store'})
        @callback(...)
        def my_callback(merge_keys, user_session_id):
            # merge_keys will be automatically loaded from StateManager
            return process_data(merge_keys)
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get the StateManager instance
            state_manager = get_state_manager()
            
            # Extract user session ID from callback context
            user_session_id = None
            if callback_context.triggered:
                # Try to find user session ID in the callback inputs/states
                for input_item in callback_context.inputs_list + callback_context.states_list:
                    if input_item.get('id') == user_context_input:
                        user_session_id = input_item.get('value')
                        break
            
            # Set user context if found
            if user_session_id:
                state_manager.set_user_context(user_session_id)
            
            # Process store mappings if provided
            if store_mappings:
                # Get function signature to map parameters
                import inspect
                sig = inspect.signature(func)
                param_names = list(sig.parameters.keys())
                
                # Replace store parameters with StateManager data
                new_args = list(args)
                for i, param_name in enumerate(param_names):
                    if param_name in store_mappings and i < len(new_args):
                        store_id = store_mappings[param_name]
                        store_data = state_manager.get_store_data(store_id)
                        
                        # If client-managed, use the original callback data
                        if store_data != "CLIENT_MANAGED":
                            new_args[i] = store_data
                
                args = tuple(new_args)
            
            # Call the original function
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def get_store_data_safe(store_id: str, default: Any = None, 
                       user_id: Optional[str] = None) -> Any:
    """
    Safely get store data with fallback to default value.
    
    Args:
        store_id: The store identifier
        default: Default value to return if store data is not found
        user_id: Optional user ID for isolation
    
    Returns:
        Store data or default value
    """
    try:
        state_manager = get_state_manager()
        data = state_manager.get_store_data(store_id, user_id)
        
        # Return default for client-managed stores or None data
        if data == "CLIENT_MANAGED" or data is None:
            return default
        
        return data
    except Exception as e:
        logger.error(f"Error getting store data for {store_id}: {e}")
        return default


def set_store_data_safe(store_id: str, data: Any, ttl: Optional[int] = None,
                       user_id: Optional[str] = None) -> Any:
    """
    Safely set store data with error handling.
    
    Args:
        store_id: The store identifier
        data: The data to store
        ttl: Time to live in seconds
        user_id: Optional user ID for isolation
    
    Returns:
        Appropriate return value for Dash callbacks
    """
    try:
        state_manager = get_state_manager()
        return state_manager.set_store_data(store_id, data, ttl, user_id)
    except Exception as e:
        logger.error(f"Error setting store data for {store_id}: {e}")
        return dash.no_update


def batch_get_stores(store_ids: List[str], user_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Get multiple store values in a single operation.
    
    Args:
        store_ids: List of store identifiers
        user_id: Optional user ID for isolation
    
    Returns:
        Dictionary mapping store IDs to their data
    """
    state_manager = get_state_manager()
    results = {}
    
    for store_id in store_ids:
        try:
            results[store_id] = state_manager.get_store_data(store_id, user_id)
        except Exception as e:
            logger.error(f"Error getting store data for {store_id}: {e}")
            results[store_id] = None
    
    return results


def batch_set_stores(store_data: Dict[str, Any], ttl: Optional[int] = None,
                    user_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Set multiple store values in a single operation.
    
    Args:
        store_data: Dictionary mapping store IDs to their data
        ttl: Time to live in seconds
        user_id: Optional user ID for isolation
    
    Returns:
        Dictionary mapping store IDs to their return values
    """
    state_manager = get_state_manager()
    results = {}
    
    for store_id, data in store_data.items():
        try:
            results[store_id] = state_manager.set_store_data(store_id, data, ttl, user_id)
        except Exception as e:
            logger.error(f"Error setting store data for {store_id}: {e}")
            results[store_id] = dash.no_update
    
    return results


def migrate_callback_to_state_manager(callback_func: Callable, 
                                     store_mappings: Dict[str, str],
                                     preserve_client_stores: bool = True) -> Callable:
    """
    Migrate an existing callback to use StateManager while preserving compatibility.
    
    Args:
        callback_func: The original callback function
        store_mappings: Dictionary mapping parameter names to store IDs
        preserve_client_stores: Whether to preserve client-managed stores
    
    Returns:
        Modified callback function
    """
    @functools.wraps(callback_func)
    def wrapper(*args, **kwargs):
        state_manager = get_state_manager()
        
        # Get function signature
        import inspect
        sig = inspect.signature(callback_func)
        param_names = list(sig.parameters.keys())
        
        # Process arguments
        new_args = list(args)
        for i, param_name in enumerate(param_names):
            if param_name in store_mappings and i < len(new_args):
                store_id = store_mappings[param_name]
                
                # Get data from StateManager
                store_data = state_manager.get_store_data(store_id)
                
                # Handle client-managed stores
                if store_data == "CLIENT_MANAGED":
                    if preserve_client_stores:
                        # Keep original callback data for client-managed stores
                        pass
                    else:
                        # Force server-side lookup
                        new_args[i] = None
                else:
                    # Use server-side data
                    new_args[i] = store_data
        
        # Call original function
        result = callback_func(*tuple(new_args), **kwargs)
        
        # Handle return values for stores
        if isinstance(result, (list, tuple)):
            # Multi-output callback
            processed_result = []
            for output_value in result:
                # Could add logic here to automatically store outputs
                processed_result.append(output_value)
            return type(result)(processed_result)
        else:
            # Single output
            return result
    
    return wrapper


def create_state_store_component(store_id: str, storage_type: str = 'session',
                                data: Any = None, clear_data: bool = False):
    """
    Create a dcc.Store component that's compatible with StateManager.
    
    Args:
        store_id: The store identifier
        storage_type: Type of storage ('session', 'local', 'memory')
        data: Initial data for the store
        clear_data: Whether to clear existing data
    
    Returns:
        dcc.Store component
    """
    from dash import dcc
    
    # Check if this store should be server-managed
    state_manager = get_state_manager()
    
    # For client backend, return normal dcc.Store
    if isinstance(state_manager.backend, type(state_manager.backend)) and \
       state_manager.backend.__class__.__name__ == 'ClientStateBackend':
        return dcc.Store(
            id=store_id,
            storage_type=storage_type,
            data=data,
            clear_data=clear_data
        )
    
    # For server backends, return store with minimal data
    return dcc.Store(
        id=store_id,
        storage_type='memory',  # Use memory for server-managed stores
        data=None,  # Data is managed server-side
        clear_data=clear_data
    )


def get_user_session_from_context() -> Optional[str]:
    """
    Extract user session ID from current callback context.
    
    Returns:
        User session ID if found, None otherwise
    """
    if not callback_context.triggered:
        return None
    
    # Look for user session ID in inputs and states
    all_inputs = callback_context.inputs_list + callback_context.states_list
    
    for input_item in all_inputs:
        if input_item.get('id') in ['user-session-id', 'session-id', 'user-id']:
            return input_item.get('value')
    
    return None


def validate_state_manager_config(config: StateManagerConfig) -> List[str]:
    """
    Validate StateManager configuration and return any issues.
    
    Args:
        config: StateManager configuration to validate
    
    Returns:
        List of validation error messages
    """
    issues = []
    
    # Validate backend type
    valid_backends = ['client', 'memory', 'redis', 'database']
    if config.backend_type not in valid_backends:
        issues.append(f"Invalid backend_type: {config.backend_type}. Must be one of {valid_backends}")
    
    # Validate TTL
    if config.default_ttl <= 0:
        issues.append("default_ttl must be positive")
    
    # Validate URLs for specific backends
    if config.backend_type == 'redis':
        if not config.redis_url.startswith(('redis://', 'rediss://')):
            issues.append("redis_url must start with 'redis://' or 'rediss://'")
    
    if config.backend_type == 'database':
        if not config.database_url:
            issues.append("database_url is required for database backend")
    
    # Validate size limits
    if config.max_value_size <= 0:
        issues.append("max_value_size must be positive")
    
    return issues


def performance_monitor(func: Callable) -> Callable:
    """
    Decorator to monitor StateManager operation performance.
    
    Args:
        func: Function to monitor
    
    Returns:
        Wrapped function with performance monitoring
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        import time
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            if execution_time > 1.0:  # Log slow operations
                logger.warning(f"Slow StateManager operation: {func.__name__} took {execution_time:.2f}s")
            
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"StateManager operation failed: {func.__name__} after {execution_time:.2f}s - {e}")
            raise
    
    return wrapper


class StateManagerMiddleware:
    """
    Middleware class for integrating StateManager with Dash applications.
    Provides automatic user context management and error handling.
    """
    
    def __init__(self, app, config: Optional[StateManagerConfig] = None):
        self.app = app
        self.state_manager = get_state_manager(config)
        self._setup_middleware()
    
    def _setup_middleware(self):
        """Setup middleware hooks"""
        # Could add Flask middleware here for automatic user context
        # This would require access to Flask request context
        pass
    
    def get_user_context(self) -> Optional[str]:
        """Get current user context"""
        return self.state_manager.user_context
    
    def set_user_context(self, user_id: str):
        """Set current user context"""
        self.state_manager.set_user_context(user_id)
    
    def clear_user_context(self):
        """Clear current user context"""
        self.state_manager.set_user_context(None)