"""
Session management utilities for StateManager integration.
Separate module to avoid circular imports with app.py.
"""

from state_manager import get_state_manager, generate_session_id

# Global session tracking to prevent multiple initialization
_app_session_initialized = False
_app_session_id = None

def ensure_session_context(user_session_id):
    """Helper function to ensure StateManager has correct user context"""
    state_manager = get_state_manager()
    if user_session_id and user_session_id != state_manager.user_context:
        state_manager.set_user_context(user_session_id)
        return True
    return False

def get_or_create_session(existing_session_id=None):
    """
    Get or create a session ID with singleton pattern.
    Used by the main app session initialization callback.
    """
    global _app_session_initialized, _app_session_id
    
    # If we already initialized globally, return the same session (ignore browser storage)
    if _app_session_initialized and _app_session_id:
        print(f"Using existing app session: {_app_session_id}")
        state_manager = get_state_manager()
        state_manager.set_user_context(_app_session_id)
        return _app_session_id, False  # (session_id, is_new)
    
    # If we have an existing session in browser storage, use it ONLY if we haven't initialized yet
    if existing_session_id and not _app_session_initialized:
        print(f"Using existing browser session: {existing_session_id}")
        state_manager = get_state_manager()
        state_manager.set_user_context(existing_session_id)
        _app_session_id = existing_session_id
        _app_session_initialized = True
        return existing_session_id, False  # (session_id, is_new)
    
    # Only generate a new session ID if we haven't initialized yet
    session_id = generate_session_id()
    
    # Set global tracking
    _app_session_initialized = True
    _app_session_id = session_id
    
    # Set user context in StateManager
    state_manager = get_state_manager()
    state_manager.set_user_context(session_id)
    
    print(f"Initialized NEW user session: {session_id}")
    return session_id, True  # (session_id, is_new)

def get_current_session():
    """Get the current session ID if available"""
    return _app_session_id

def reset_session():
    """Reset session state (useful for testing)"""
    global _app_session_initialized, _app_session_id
    _app_session_initialized = False
    _app_session_id = None