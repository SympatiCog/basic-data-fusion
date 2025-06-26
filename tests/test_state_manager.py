"""
Tests for StateManager and state backends.
"""

import pytest
import tempfile
import os
import time
import sys
from unittest.mock import patch, MagicMock

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from state_manager import (
    StateManager, 
    StateManagerConfig, 
    get_state_manager, 
    refresh_state_manager,
    generate_session_id
)
from state_backends import (
    ClientStateBackend,
    MemoryStateBackend, 
    StateBackendConfig
)


class TestStateBackends:
    """Test individual state backends"""
    
    def test_client_state_backend(self):
        """Test ClientStateBackend behavior"""
        backend = ClientStateBackend()
        
        # Client backend should return special signals
        assert backend.get('test-key') == "CLIENT_MANAGED"
        assert backend.set('test-key', {'data': 'value'}) is True
        assert backend.delete('test-key') is True
        assert backend.exists('test-key') is True
        assert backend.clear() is True
    
    def test_memory_state_backend_basic_operations(self):
        """Test MemoryStateBackend basic operations"""
        backend = MemoryStateBackend()
        
        # Test set and get
        test_data = {'key': 'value', 'number': 42}
        assert backend.set('test-key', test_data) is True
        assert backend.get('test-key') == test_data
        
        # Test exists
        assert backend.exists('test-key') is True
        assert backend.exists('nonexistent-key') is False
        
        # Test delete
        assert backend.delete('test-key') is True
        assert backend.get('test-key') is None
        assert backend.exists('test-key') is False
    
    def test_memory_state_backend_ttl(self):
        """Test MemoryStateBackend TTL functionality"""
        backend = MemoryStateBackend()
        
        # Set with short TTL
        backend.set('ttl-key', 'ttl-value', ttl=1)
        assert backend.get('ttl-key') == 'ttl-value'
        
        # Wait for expiration
        time.sleep(1.1)
        assert backend.get('ttl-key') is None
        assert backend.exists('ttl-key') is False
    
    def test_memory_state_backend_stats(self):
        """Test MemoryStateBackend statistics"""
        backend = MemoryStateBackend()
        
        # Add some data
        backend.set('key1', 'value1')
        backend.set('key2', {'complex': 'data'})
        
        stats = backend.get_stats()
        assert stats['backend_type'] == 'memory'
        assert stats['total_keys'] == 2
        assert stats['total_size_bytes'] > 0
    
    def test_memory_state_backend_validation(self):
        """Test MemoryStateBackend validation"""
        config = StateBackendConfig(max_value_size=10)  # Very small for testing
        backend = MemoryStateBackend(config)
        
        # Test value too large
        large_value = 'x' * 1000
        assert backend.set('large-key', large_value) is False
        
        # Test invalid key
        assert backend.get('') is None


class TestStateManager:
    """Test StateManager functionality"""
    
    def test_state_manager_initialization(self):
        """Test StateManager initialization with different configs"""
        # Default config (client backend)
        sm = StateManager()
        assert sm.config.backend_type == 'client'
        assert isinstance(sm.backend, ClientStateBackend)
        
        # Custom config
        config = StateManagerConfig(backend_type='memory', default_ttl=7200)
        sm = StateManager(config)
        assert sm.config.backend_type == 'memory'
        assert sm.config.default_ttl == 7200
        assert isinstance(sm.backend, MemoryStateBackend)
    
    def test_state_manager_user_context(self):
        """Test user context functionality"""
        sm = StateManager(StateManagerConfig(backend_type='memory'))
        
        # Set user context
        user_id = 'test-user-123'
        sm.set_user_context(user_id)
        assert sm.user_context == user_id
        
        # Test key building with user context
        key = sm._build_key('test-store')
        assert user_id in key
        assert 'bdt' in key  # prefix
        assert 'test-store' in key
    
    def test_state_manager_store_operations_memory(self):
        """Test store operations with memory backend"""
        sm = StateManager(StateManagerConfig(backend_type='memory'))
        
        test_data = {'filters': [1, 2, 3], 'count': 42}
        store_id = 'phenotypic-filters-store'
        
        # Test set and get
        result = sm.set_store_data(store_id, test_data)
        # Memory backend should return no_update (not client-managed)
        try:
            import dash
            assert result == dash.no_update
        except ImportError:
            # If dash is not available, just check that result is not the data
            assert result != test_data
        
        retrieved = sm.get_store_data(store_id)
        assert retrieved == test_data
        
        # Test exists
        assert sm.store_exists(store_id) is True
        assert sm.store_exists('nonexistent-store') is False
        
        # Test delete
        assert sm.delete_store_data(store_id) is True
        assert sm.get_store_data(store_id) is None
    
    def test_state_manager_store_operations_client(self):
        """Test store operations with client backend"""
        sm = StateManager(StateManagerConfig(backend_type='client'))
        
        test_data = {'merge_keys': 'ursi'}
        store_id = 'merge-keys-store'
        
        # Test set with client backend
        result = sm.set_store_data(store_id, test_data)
        assert result == test_data  # Client backend returns data
        
        # Test get with client backend
        retrieved = sm.get_store_data(store_id)
        assert retrieved == "CLIENT_MANAGED"  # Special signal
        
        # Test client-managed check
        assert sm.is_client_managed(store_id) is True
    
    def test_state_manager_user_isolation(self):
        """Test user isolation in state management"""
        sm = StateManager(StateManagerConfig(
            backend_type='memory',
            enable_user_isolation=True
        ))
        
        store_id = 'user-specific-store'
        user1_data = {'user': 'user1', 'data': [1, 2, 3]}
        user2_data = {'user': 'user2', 'data': [4, 5, 6]}
        
        # Set data for different users
        sm.set_store_data(store_id, user1_data, user_id='user1')
        sm.set_store_data(store_id, user2_data, user_id='user2')
        
        # Get data for each user
        assert sm.get_store_data(store_id, user_id='user1') == user1_data
        assert sm.get_store_data(store_id, user_id='user2') == user2_data
        
        # Users should not see each other's data
        assert sm.get_store_data(store_id, user_id='user1') != user2_data
        assert sm.get_store_data(store_id, user_id='user2') != user1_data
    
    def test_state_manager_ttl(self):
        """Test TTL functionality"""
        sm = StateManager(StateManagerConfig(backend_type='memory'))
        
        store_id = 'ttl-test-store'
        test_data = {'expires': 'soon'}
        
        # Set with short TTL
        sm.set_store_data(store_id, test_data, ttl=1)
        assert sm.get_store_data(store_id) == test_data
        
        # Wait for expiration
        time.sleep(1.1)
        assert sm.get_store_data(store_id) is None
    
    def test_state_manager_backend_stats(self):
        """Test backend statistics"""
        sm = StateManager(StateManagerConfig(backend_type='memory'))
        
        # Add some data
        sm.set_store_data('store1', {'data': 'value1'})
        sm.set_store_data('store2', {'data': 'value2'})
        
        stats = sm.get_backend_stats()
        assert stats['backend_type'] == 'memory'
        assert stats['user_isolation_enabled'] is True
        assert 'total_keys' in stats
    
    def test_state_manager_migration(self):
        """Test backend migration functionality"""
        # Start with client backend
        config = StateManagerConfig(backend_type='client')
        sm = StateManager(config)
        assert sm.config.backend_type == 'client'
        
        # Migrate to memory backend
        result = sm.migrate_to_server_backend('memory')
        assert result is True
        assert sm.config.backend_type == 'memory'
        assert isinstance(sm.backend, MemoryStateBackend)
        
        # Test that migration works
        sm.set_store_data('migration-test', {'migrated': True})
        assert sm.get_store_data('migration-test') == {'migrated': True}


class TestGlobalStateManager:
    """Test global StateManager functions"""
    
    def test_singleton_pattern(self):
        """Test that get_state_manager returns the same instance"""
        # Clear any existing instance
        refresh_state_manager()
        
        sm1 = get_state_manager()
        sm2 = get_state_manager()
        assert sm1 is sm2  # Should be the same instance
    
    def test_refresh_state_manager(self):
        """Test refreshing the global instance"""
        sm1 = get_state_manager()
        sm2 = refresh_state_manager()
        assert sm1 is not sm2  # Should be different instances
    
    def test_convenience_functions(self):
        """Test convenience functions"""
        from state_manager import get_store, set_store, delete_store, store_exists
        
        # Use memory backend for testing
        refresh_state_manager(StateManagerConfig(backend_type='memory'))
        
        store_id = 'convenience-test'
        test_data = {'convenience': True}
        
        # Test convenience functions
        set_store(store_id, test_data)
        assert get_store(store_id) == test_data
        assert store_exists(store_id) is True
        assert delete_store(store_id) is True
        assert get_store(store_id) is None
    
    def test_generate_session_id(self):
        """Test session ID generation"""
        session_id1 = generate_session_id()
        session_id2 = generate_session_id()
        
        # Should be different UUIDs
        assert session_id1 != session_id2
        assert len(session_id1) == 36  # UUID length
        assert '-' in session_id1  # UUID format


class TestErrorHandling:
    """Test error handling and edge cases"""
    
    def test_invalid_backend_type(self):
        """Test handling of invalid backend type"""
        config = StateManagerConfig(backend_type='invalid')
        sm = StateManager(config)
        
        # Should fallback to client backend
        assert isinstance(sm.backend, ClientStateBackend)
    
    def test_error_recovery(self):
        """Test error recovery in operations"""
        sm = StateManager(StateManagerConfig(backend_type='memory'))
        
        # Test with None data
        assert sm.set_store_data('test', None) != dash.no_update
        assert sm.get_store_data('test') is None
        
        # Test with empty store ID
        assert sm.get_store_data('') is None
    
    @patch('state_backends.logger')
    def test_logging(self, mock_logger):
        """Test that operations are properly logged"""
        sm = StateManager(StateManagerConfig(backend_type='memory'))
        
        # Perform operations that should be logged
        sm.set_store_data('log-test', {'logged': True})
        sm.get_store_data('log-test')
        
        # Verify logging calls were made
        assert mock_logger.debug.called


@pytest.fixture
def temp_db_file():
    """Fixture for temporary database file"""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


class TestDatabaseBackend:
    """Test DatabaseStateBackend (requires SQLAlchemy)"""
    
    @pytest.mark.skipif(
        not pytest.importorskip("sqlalchemy", reason="SQLAlchemy not available"),
        reason="SQLAlchemy required for DatabaseStateBackend"
    )
    def test_database_backend_basic(self, temp_db_file):
        """Test DatabaseStateBackend basic operations"""
        from state_backends import DatabaseStateBackend
        
        db_url = f"sqlite:///{temp_db_file}"
        backend = DatabaseStateBackend(db_url=db_url)
        
        # Test basic operations
        test_data = {'db_test': True, 'value': 123}
        assert backend.set('db-key', test_data) is True
        assert backend.get('db-key') == test_data
        assert backend.exists('db-key') is True
        assert backend.delete('db-key') is True
        assert backend.get('db-key') is None


class TestRedisBackend:
    """Test RedisStateBackend (requires redis)"""
    
    @pytest.mark.skipif(
        not pytest.importorskip("redis", reason="redis-py not available"),
        reason="redis-py required for RedisStateBackend"
    )
    @patch('redis.from_url')
    def test_redis_backend_basic(self, mock_redis):
        """Test RedisStateBackend basic operations"""
        from state_backends import RedisStateBackend
        
        # Mock Redis client
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis.return_value = mock_client
        
        backend = RedisStateBackend()
        
        # Test that Redis client was configured
        mock_redis.assert_called_once()
        mock_client.ping.assert_called_once()
        
        # Test set operation (mocked)
        mock_client.setex.return_value = True
        assert backend.set('redis-key', {'test': 'data'}, ttl=300) is True
        
        # Test get operation (mocked)
        mock_client.get.return_value = '{"test": "data"}'
        result = backend.get('redis-key')
        assert result == {'test': 'data'}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])