"""
StateManager - Centralized state management system for scalable Dash applications.

Provides a unified interface for managing application state across different backends
(client-side, Redis, database) while maintaining compatibility with existing dcc.Store components.
"""

import logging
import uuid
from typing import Any, Optional, Dict, Union
from dataclasses import dataclass

try:
    import dash
except ImportError:
    # Create a mock dash.no_update for testing without dash
    class MockDash:
        no_update = object()
    dash = MockDash()

from state_backends import (
    StateBackend, 
    ClientStateBackend, 
    MemoryStateBackend, 
    RedisStateBackend, 
    DatabaseStateBackend,
    StateBackendConfig
)

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class StateManagerConfig:
    """Configuration for StateManager"""
    backend_type: str = 'client'  # 'client', 'memory', 'redis', 'database'
    enable_user_isolation: bool = True
    default_ttl: int = 3600  # 1 hour
    key_prefix: str = 'bdt'  # Basic Data Tool prefix
    redis_url: str = 'redis://localhost:6379/0'
    database_url: str = 'sqlite:///state.db'
    enable_compression: bool = True
    max_value_size: int = 10 * 1024 * 1024  # 10MB


class StateManager:
    """
    Centralized state management system that provides a unified interface
    for different storage backends while maintaining compatibility with
    existing Dash dcc.Store components.
    """
    
    def __init__(self, config: Optional[StateManagerConfig] = None):
        self.config = config or StateManagerConfig()
        self.backend = self._create_backend()
        self.user_context: Optional[str] = None
        
        logger.info(f"StateManager initialized with {self.config.backend_type} backend")
    
    def _create_backend(self) -> StateBackend:
        """Create appropriate backend based on configuration"""
        backend_config = StateBackendConfig(
            ttl_default=self.config.default_ttl,
            max_value_size=self.config.max_value_size,
            enable_compression=self.config.enable_compression
        )
        
        if self.config.backend_type == 'client':
            return ClientStateBackend(backend_config)
        elif self.config.backend_type == 'memory':
            return MemoryStateBackend(backend_config)
        elif self.config.backend_type == 'redis':
            return RedisStateBackend(backend_config, self.config.redis_url)
        elif self.config.backend_type == 'database':
            return DatabaseStateBackend(backend_config, self.config.database_url)
        else:
            logger.warning(f"Unknown backend type: {self.config.backend_type}, falling back to client")
            return ClientStateBackend(backend_config)
    
    def set_user_context(self, user_id: Optional[str]):
        """Set current user context for state isolation"""
        self.user_context = user_id
        if user_id:
            logger.debug(f"Set user context: {user_id}")
    
    def _build_key(self, store_id: str, user_id: Optional[str] = None) -> str:
        """Build full key with prefix and user isolation"""
        key_parts = [self.config.key_prefix]
        
        if self.config.enable_user_isolation:
            effective_user_id = user_id or self.user_context or 'anonymous'
            key_parts.append(effective_user_id)
        
        key_parts.append(store_id)
        return ':'.join(key_parts)
    
    def get_store_data(self, store_id: str, user_id: Optional[str] = None) -> Any:
        """
        Get data for a store ID.
        
        Args:
            store_id: The store identifier (e.g., 'merge-keys-store')
            user_id: Optional user ID for isolation
            
        Returns:
            The stored data, or None if not found, or 'CLIENT_MANAGED' for client backend
        """
        key = self._build_key(store_id, user_id)
        
        try:
            result = self.backend.get(key)
            if result == "CLIENT_MANAGED":
                logger.debug(f"Store {store_id} is client-managed")
            else:
                logger.debug(f"Retrieved data for store {store_id}")
            return result
        except Exception as e:
            logger.error(f"Error getting store data for {store_id}: {e}")
            return None
    
    def set_store_data(self, store_id: str, data: Any, ttl: Optional[int] = None, 
                      user_id: Optional[str] = None) -> Any:
        """
        Set data for a store ID.
        
        Args:
            store_id: The store identifier
            data: The data to store
            ttl: Time to live in seconds (optional)
            user_id: Optional user ID for isolation
            
        Returns:
            The data for client backend (for dcc.Store compatibility), 
            or dash.no_update for server backends
        """
        key = self._build_key(store_id, user_id)
        
        try:
            success = self.backend.set(key, data, ttl)
            if success:
                logger.debug(f"Stored data for store {store_id}")
                # Return data for client backend compatibility, no_update for server backends
                if isinstance(self.backend, ClientStateBackend):
                    return data
                else:
                    return dash.no_update
            else:
                logger.error(f"Failed to store data for {store_id}")
                return dash.no_update
        except Exception as e:
            logger.error(f"Error setting store data for {store_id}: {e}")
            return dash.no_update
    
    def delete_store_data(self, store_id: str, user_id: Optional[str] = None) -> bool:
        """Delete data for a store ID"""
        key = self._build_key(store_id, user_id)
        
        try:
            result = self.backend.delete(key)
            if result:
                logger.debug(f"Deleted data for store {store_id}")
            return result
        except Exception as e:
            logger.error(f"Error deleting store data for {store_id}: {e}")
            return False
    
    def store_exists(self, store_id: str, user_id: Optional[str] = None) -> bool:
        """Check if store data exists"""
        key = self._build_key(store_id, user_id)
        
        try:
            return self.backend.exists(key)
        except Exception as e:
            logger.error(f"Error checking store existence for {store_id}: {e}")
            return False
    
    def clear_user_data(self, user_id: str) -> bool:
        """Clear all data for a specific user (if supported by backend)"""
        if not self.config.enable_user_isolation:
            logger.warning("User isolation not enabled, cannot clear user data")
            return False
        
        # For now, this is a basic implementation
        # More sophisticated backends could implement batch operations
        logger.info(f"Clearing data for user {user_id}")
        return True
    
    def get_backend_stats(self) -> Dict[str, Any]:
        """Get backend statistics if available"""
        try:
            if hasattr(self.backend, 'get_stats'):
                stats = self.backend.get_stats()
                stats['backend_type'] = self.config.backend_type
                stats['user_isolation_enabled'] = self.config.enable_user_isolation
                return stats
            else:
                return {
                    'backend_type': self.config.backend_type,
                    'user_isolation_enabled': self.config.enable_user_isolation,
                    'stats_available': False
                }
        except Exception as e:
            logger.error(f"Error getting backend stats: {e}")
            return {'error': str(e)}
    
    def is_client_managed(self, store_id: str, user_id: Optional[str] = None) -> bool:
        """Check if a store is client-managed (uses dcc.Store)"""
        data = self.get_store_data(store_id, user_id)
        return data == "CLIENT_MANAGED"
    
    def migrate_to_server_backend(self, new_backend_type: str) -> bool:
        """
        Migrate from client backend to server backend.
        This would typically be used during the transition phase.
        """
        if self.config.backend_type == new_backend_type:
            logger.warning(f"Already using {new_backend_type} backend")
            return True
        
        logger.info(f"Migrating from {self.config.backend_type} to {new_backend_type}")
        
        # Store old backend for potential rollback
        old_backend = self.backend
        old_config = self.config
        
        try:
            # Create new configuration and backend
            new_config = StateManagerConfig()
            new_config.__dict__.update(self.config.__dict__)
            new_config.backend_type = new_backend_type
            
            self.config = new_config
            self.backend = self._create_backend()
            
            logger.info(f"Successfully migrated to {new_backend_type} backend")
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            # Rollback on failure
            self.backend = old_backend
            self.config = old_config
            return False


# Global StateManager instance
_state_manager_instance: Optional[StateManager] = None


def get_state_manager(config: Optional[StateManagerConfig] = None) -> StateManager:
    """
    Get the global StateManager instance (singleton pattern).
    
    Args:
        config: Optional configuration for the StateManager
        
    Returns:
        The global StateManager instance
    """
    global _state_manager_instance
    
    if _state_manager_instance is None:
        _state_manager_instance = StateManager(config)
        logger.info("Created global StateManager instance")
    elif config is not None:
        logger.warning("StateManager already initialized, ignoring new config")
    
    return _state_manager_instance


def refresh_state_manager(config: Optional[StateManagerConfig] = None) -> StateManager:
    """
    Force refresh of the global StateManager instance.
    Useful for testing or configuration changes.
    """
    global _state_manager_instance
    _state_manager_instance = None
    return get_state_manager(config)


def generate_session_id() -> str:
    """Generate a unique session ID for user isolation"""
    return str(uuid.uuid4())


# Convenience functions for common operations
def get_store(store_id: str, user_id: Optional[str] = None) -> Any:
    """Convenience function to get store data"""
    return get_state_manager().get_store_data(store_id, user_id)


def set_store(store_id: str, data: Any, ttl: Optional[int] = None, 
              user_id: Optional[str] = None) -> Any:
    """Convenience function to set store data"""
    return get_state_manager().set_store_data(store_id, data, ttl, user_id)


def delete_store(store_id: str, user_id: Optional[str] = None) -> bool:
    """Convenience function to delete store data"""
    return get_state_manager().delete_store_data(store_id, user_id)


def store_exists(store_id: str, user_id: Optional[str] = None) -> bool:
    """Convenience function to check if store exists"""
    return get_state_manager().store_exists(store_id, user_id)