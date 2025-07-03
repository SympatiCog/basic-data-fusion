"""
Tests for the enhanced callback registration system.

These tests ensure that the modular callback registration system works correctly,
including error handling, statistics tracking, and proper module loading.
"""

import pytest
import dash
from unittest.mock import Mock, patch, MagicMock
import time

from query.callbacks import (
    register_all_callbacks,
    get_registration_stats,
    is_registered,
    unregister_callbacks,
    CALLBACK_MODULES
)


class TestCallbackRegistration:
    """Test the callback registration system."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.app = dash.Dash(__name__)
        # Clear any existing registration state
        unregister_callbacks(self.app)
    
    def test_register_all_callbacks_success(self):
        """Test successful callback registration."""
        stats = register_all_callbacks(self.app, verbose=False)
        
        # Verify basic statistics
        assert stats['success'] is True
        assert stats['total_callbacks'] >= 0
        assert stats['duration_ms'] > 0
        assert len(stats['modules']) == 4  # data_loading, filters, export, state
        
        # Verify all modules registered successfully
        for module_name in CALLBACK_MODULES.keys():
            assert module_name in stats['modules']
            assert stats['modules'][module_name]['success'] is True
            assert 'callbacks_registered' in stats['modules'][module_name]
            assert stats['modules'][module_name]['duration_ms'] >= 0
        
        # Verify app is marked as registered
        assert is_registered(self.app)
    
    def test_duplicate_registration_prevention(self):
        """Test that duplicate registrations are prevented."""
        # First registration
        stats1 = register_all_callbacks(self.app, verbose=False)
        assert stats1['success'] is True
        
        # Second registration should return cached stats
        stats2 = register_all_callbacks(self.app, verbose=False)
        assert stats2 == stats1  # Should return the same stats
    
    def test_invalid_app_parameter(self):
        """Test error handling for invalid app parameter."""
        with pytest.raises(ValueError, match="Valid Dash app instance required"):
            register_all_callbacks(None)
    
    @patch('query.callbacks.data_loading.register_callbacks')
    @patch('query.callbacks.filters.register_callbacks')
    @patch('query.callbacks.export.register_callbacks')
    @patch('query.callbacks.state.register_callbacks')
    def test_complete_module_failure(self, mock_state, mock_export, mock_filters, mock_data):
        """Test handling of complete module registration failures."""
        # Make all modules fail
        mock_data.side_effect = Exception("Data loading failed")
        mock_filters.side_effect = Exception("Filters failed")
        mock_export.side_effect = Exception("Export failed")
        mock_state.side_effect = Exception("State failed")
        
        with pytest.raises(RuntimeError):
            register_all_callbacks(self.app, verbose=False)
    
    @patch('query.callbacks.data_loading.register_callbacks')
    def test_partial_module_failure(self, mock_register):
        """Test handling of partial module registration failures."""
        # Make one module fail
        mock_register.side_effect = Exception("Registration failed")
        
        # Should not raise an exception for partial failure
        stats = register_all_callbacks(self.app, verbose=False)
        
        # Should have recorded the failure
        assert len(stats['errors']) > 0
        assert stats['modules']['data_loading']['success'] is False
        assert 'error' in stats['modules']['data_loading']
    
    @patch('query.callbacks.filters.register_callbacks')
    def test_partial_failure_recovery_no_duplicates(self, mock_filters):
        """Test that partial failures don't cause duplicate registrations on retry."""
        # First call: Make filters module fail
        mock_filters.side_effect = Exception("Filters failed")
        
        stats1 = register_all_callbacks(self.app, verbose=False)
        
        # Should have partial success but app not marked as fully registered
        assert not stats1['success']
        assert not is_registered(self.app)
        assert stats1['modules']['filters']['success'] is False
        assert stats1['modules']['data_loading']['success'] is True  # others should succeed
        
        # Second call: All modules should work, but successful ones should be skipped
        mock_filters.side_effect = None  # Reset the mock to allow success
        mock_filters.return_value = None
        
        stats2 = register_all_callbacks(self.app, verbose=False)
        
        # Should now have complete success
        assert stats2['success'] is True
        assert is_registered(self.app)
        
        # Previously successful modules should be skipped
        assert stats2['modules']['data_loading'].get('skipped') is True
        assert stats2['modules']['export'].get('skipped') is True
        assert stats2['modules']['state'].get('skipped') is True
        
        # Only the previously failed module should be newly registered
        assert stats2['modules']['filters'].get('skipped') is not True
        assert stats2['modules']['filters']['success'] is True
    
    def test_get_registration_stats(self):
        """Test getting registration statistics."""
        # Register callbacks first
        register_all_callbacks(self.app, verbose=False)
        app_id = id(self.app)
        
        # Test getting stats for specific app
        stats = get_registration_stats(app_id)
        assert stats is not None
        assert stats['app_id'] == app_id
        
        # Test getting all stats
        all_stats = get_registration_stats()
        assert app_id in all_stats
        
        # Test getting stats for non-existent app
        non_existent_stats = get_registration_stats(99999)
        assert non_existent_stats == {}
    
    def test_is_registered(self):
        """Test checking registration status."""
        # Initially not registered
        assert is_registered(self.app) is False
        
        # After registration
        register_all_callbacks(self.app, verbose=False)
        assert is_registered(self.app) is True
    
    def test_unregister_callbacks(self):
        """Test unregistering callbacks."""
        # Register first
        register_all_callbacks(self.app, verbose=False)
        assert is_registered(self.app) is True
        
        # Unregister
        result = unregister_callbacks(self.app)
        assert result is True
        assert is_registered(self.app) is False
        
        # Unregister already unregistered app
        result = unregister_callbacks(self.app)
        assert result is False
    
    def test_verbose_output(self, capsys):
        """Test verbose output during registration."""
        register_all_callbacks(self.app, verbose=True)
        captured = capsys.readouterr()
        
        # Should contain module registration messages
        assert "callbacks registered" in captured.out
        assert "Modular query callbacks registered successfully" in captured.out
    
    def test_performance_monitoring(self):
        """Test that performance metrics are collected."""
        stats = register_all_callbacks(self.app, verbose=False)
        
        # Should have timing information
        assert 'duration_ms' in stats
        assert stats['duration_ms'] > 0
        
        # Each module should have timing
        for module_stats in stats['modules'].values():
            assert 'duration_ms' in module_stats
            assert module_stats['duration_ms'] >= 0
    
    def test_callback_module_configuration(self):
        """Test that callback module configuration is correct."""
        # Verify all expected modules are configured
        expected_modules = {'data_loading', 'filters', 'export', 'state'}
        assert set(CALLBACK_MODULES.keys()) == expected_modules
        
        # Verify each module has required configuration
        for module_name, config in CALLBACK_MODULES.items():
            assert 'min_callbacks' in config
            assert 'description' in config
            assert isinstance(config['min_callbacks'], int)
            assert config['min_callbacks'] > 0
            assert isinstance(config['description'], str)
            assert len(config['description']) > 0


class TestCallbackRegistrationIntegration:
    """Integration tests for callback registration with real modules."""
    
    def test_real_module_registration(self):
        """Test registration with real callback modules."""
        app = dash.Dash(__name__)
        
        # Should not raise any exceptions
        stats = register_all_callbacks(app, verbose=False)
        
        # Verify successful registration
        assert stats['success'] is True
        assert stats['total_callbacks'] > 0
        
        # Verify all modules loaded
        assert len(stats['modules']) == 4
        for module_stats in stats['modules'].values():
            assert module_stats['success'] is True
    
    def test_callback_counting_accuracy(self):
        """Test that callback counting is reasonably accurate."""
        app = dash.Dash(__name__)
        stats = register_all_callbacks(app, verbose=False)
        
        # Should have registered a reasonable number of callbacks
        # Based on our current modules, we expect at least 20+ callbacks total
        assert stats['total_callbacks'] >= 20
        
        # Each module should register some callbacks
        for module_name, module_stats in stats['modules'].items():
            callbacks_registered = module_stats['callbacks_registered']
            if isinstance(callbacks_registered, int):
                assert callbacks_registered > 0
            else:
                # Should be an estimated count string
                assert 'estimated' in str(callbacks_registered)


if __name__ == '__main__':
    # Run tests if script is executed directly
    pytest.main([__file__, '-v'])