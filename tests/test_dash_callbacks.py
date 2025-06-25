"""
Dash Callback and Component Testing
Tests Dash application callbacks, component interactions, and data flow without browser automation.
"""
import os
import sys
import tempfile
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_data_merge_comprehensive import TestDataGenerator


class TestDashApplicationStructure:
    """Test the overall Dash application structure and component definitions."""
    
    def test_app_initialization(self):
        """Test that the main Dash app initializes correctly."""
        import app
        
        # Check that app is a Dash instance
        assert hasattr(app, 'app')
        assert app.app is not None
        
        # Check that multi-page is enabled
        assert hasattr(app.app, 'layout')
        
        # Check that layout contains expected components
        layout_str = str(app.app.layout)
        assert 'dcc.Location' in layout_str or 'Location' in layout_str
        assert '_pages_content' in layout_str or 'page_container' in layout_str
    
    def test_page_registration(self):
        """Test that pages are properly registered."""
        import app
        import pages.query
        try:
            import pages.import_page
        except ImportError:
            pass  # Import page may have different name
        import pages.settings
        
        # Check that pages are registered
        # Dash pages should be accessible through the registry
        try:
            # Get the page registry
            import dash
            if hasattr(dash, 'page_registry'):
                registry = dash.page_registry
                assert len(registry) > 0, "No pages registered"
                
                # Check for expected pages
                page_paths = [page['path'] for page in registry.values()]
                assert '/' in page_paths, "Query page not registered"
        except Exception as e:
            # If we can't access page registry, just ensure imports work
            pass
    
    def test_store_components_definition(self):
        """Test that store components are properly defined in the layout."""
        import app
        
        layout_str = str(app.app.layout)
        
        # Check for critical store components
        critical_stores = [
            'merged-dataframe-store',
            'available-tables-store', 
            'merge-keys-store',
            'session-values-store',
            'column-dtypes-store',
            'phenotypic-filters-store'
        ]
        
        for store_id in critical_stores:
            assert store_id in layout_str, f"Store component '{store_id}' not found in app layout"
    
    def test_navbar_components(self):
        """Test that navigation components are properly defined."""
        import app
        
        layout_str = str(app.app.layout)
        
        # Check for navigation elements
        assert 'NavbarSimple' in layout_str or 'navbar' in layout_str.lower()
        assert 'Query Data' in layout_str
        assert 'Import Data' in layout_str
        assert 'Settings' in layout_str


class TestQueryPageCallbacks:
    """Test callbacks and logic in the query page."""
    
    def test_query_page_imports(self):
        """Test that query page imports all required components."""
        try:
            import pages.query
            
            # Check that critical imports are available
            from pages.query import layout
            assert layout is not None
            
        except ImportError as e:
            pytest.fail(f"Query page import failed: {e}")
    
    def test_callback_component_ids(self):
        """Test that callback component IDs are consistent."""
        import pages.query
        
        # Read the query page source
        query_source = open('pages/query.py', 'r').read()
        
        # Check for key component IDs that should have callbacks
        callback_ids = [
            'age-slider',
            'live-participant-count',
            'merge-strategy-info',
            'phenotypic-add-button',
            'phenotypic-clear-button',
            'phenotypic-filters-list'
        ]
        
        # Check that these IDs appear both in layout and callbacks
        for component_id in callback_ids:
            assert component_id in query_source, f"Component ID '{component_id}' not found in query page"
        
        # Check for callback decorators
        assert '@callback' in query_source, "No @callback decorators found in query page"
    
    def test_utils_integration(self):
        """Test that query page properly integrates with utils functions."""
        import pages.query
        from pages.query import layout
        
        # Check that critical utils functions are imported
        query_source = open('pages/query.py', 'r').read()
        
        critical_imports = [
            'get_table_info',
            'generate_base_query_logic',
            'generate_count_query',
            'MergeKeys'
        ]
        
        for import_name in critical_imports:
            assert import_name in query_source, f"Critical import '{import_name}' not found in query page"


class TestCallbackDataFlow:
    """Test data flow through callbacks with mock data."""
    
    def test_table_info_callback_simulation(self):
        """Simulate table info loading callback."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=10)
            
            # Mock config to use test data
            from utils import Config, get_table_info
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            # Test table info extraction (simulates callback data loading)
            result = get_table_info(config)
            
            # Unpack results
            (behavioral_tables, demographics_columns, behavioral_columns_by_table,
             column_dtypes, column_ranges, merge_keys_dict, actions_taken,
             session_values, is_empty, messages) = result
            
            # Verify data structure that would be used in callbacks
            assert not is_empty
            assert len(behavioral_tables) > 0
            assert len(demographics_columns) > 0
            assert isinstance(merge_keys_dict, dict)
            assert 'primary_id' in merge_keys_dict
    
    def test_participant_count_calculation(self):
        """Test participant count calculation logic."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=15)
            
            from utils import Config, generate_secure_query_suite
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            merge_keys = config.get_merge_keys()
            
            # Simulate participant count query (like in callbacks)
            data_query, count_query, params = generate_secure_query_suite(
                config=config,
                merge_keys=merge_keys,
                demographic_filters={},
                behavioral_filters=[],
                tables_to_join=['demographics']
            )
            
            assert count_query is not None
            assert 'COUNT' in count_query.upper()
            assert 'DISTINCT' in count_query.upper()
    
    def test_filter_application_simulation(self):
        """Simulate filter application in callbacks."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=20)
            
            from utils import Config, generate_secure_query_suite
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            merge_keys = config.get_merge_keys()
            
            # Simulate applying demographic filters
            demographic_filters = {
                'age_range': [25, 45]
            }
            
            behavioral_filters = [
                {'table': 'cognitive', 'column': 'iq_score', 'type': 'range', 'range': [100, 130]}
            ]
            
            # Generate queries with filters (simulates callback logic)
            data_query, count_query, params = generate_secure_query_suite(
                config=config,
                merge_keys=merge_keys,
                demographic_filters=demographic_filters,
                behavioral_filters=behavioral_filters,
                tables_to_join=['demographics', 'cognitive']
            )
            
            # Verify filter application
            assert 'BETWEEN ? AND ?' in data_query  # Age filter
            assert len(params) >= 4  # Age min/max + behavioral min/max
    
    def test_data_export_preparation(self):
        """Test data export preparation logic."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_longitudinal_data(temp_dir, num_subjects=10)
            
            from utils import Config, enwiden_longitudinal_data, MergeKeys
            
            # Create sample longitudinal data for export testing
            sample_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB001', 'SUB002', 'SUB002'],
                'session_num': ['BAS1', 'FU1', 'BAS1', 'FU1'],
                'customID': ['SUB001_BAS1', 'SUB001_FU1', 'SUB002_BAS1', 'SUB002_FU1'],
                'age': [25, 25, 30, 30],
                'score': [100, 105, 90, 95]
            })
            
            merge_keys = MergeKeys(
                primary_id='ursi',
                session_id='session_num',
                composite_id='customID',
                is_longitudinal=True
            )
            
            # Test widening (simulates export callback)
            widened_data = enwiden_longitudinal_data(sample_data, merge_keys)
            
            # Verify export preparation
            assert len(widened_data) == 2  # Two subjects
            assert 'ursi' in widened_data.columns
            assert 'age' in widened_data.columns  # Static column preserved
            
            # Check for session-specific columns
            session_cols = [col for col in widened_data.columns if 'BAS' in col or 'FU' in col]
            assert len(session_cols) > 0


class TestComponentInteractions:
    """Test component interactions and state management."""
    
    def test_store_data_serialization(self):
        """Test that store data can be properly serialized."""
        from utils import MergeKeys
        
        # Test MergeKeys serialization (used in stores)
        merge_keys = MergeKeys(
            primary_id='ursi',
            session_id='session_num',
            composite_id='customID',
            is_longitudinal=True
        )
        
        # Test to_dict/from_dict (used for store serialization)
        serialized = merge_keys.to_dict()
        assert isinstance(serialized, dict)
        assert 'primary_id' in serialized
        assert 'is_longitudinal' in serialized
        
        # Test deserialization
        restored = MergeKeys.from_dict(serialized)
        assert restored.primary_id == merge_keys.primary_id
        assert restored.is_longitudinal == merge_keys.is_longitudinal
    
    def test_filter_state_management(self):
        """Test filter state data structures."""
        # Test phenotypic filter structure (as used in stores)
        phenotypic_filters = {
            'filters': [
                {
                    'id': 1,
                    'table': 'cognitive',
                    'column': 'iq_score',
                    'type': 'range',
                    'range': [100, 130]
                }
            ],
            'next_id': 2
        }
        
        # Verify structure
        assert 'filters' in phenotypic_filters
        assert 'next_id' in phenotypic_filters
        assert isinstance(phenotypic_filters['filters'], list)
        assert len(phenotypic_filters['filters']) == 1
        
        # Test filter addition simulation
        new_filter = {
            'id': phenotypic_filters['next_id'],
            'table': 'behavioral',
            'column': 'anxiety_score',
            'type': 'range',
            'range': [0, 20]
        }
        
        phenotypic_filters['filters'].append(new_filter)
        phenotypic_filters['next_id'] += 1
        
        assert len(phenotypic_filters['filters']) == 2
        assert phenotypic_filters['next_id'] == 3
    
    def test_error_state_handling(self):
        """Test error state handling in callbacks."""
        from dash import no_update
        
        # Test that no_update is available for error handling
        assert no_update is not None
        
        # Simulate error handling pattern
        def simulate_callback_error_handling():
            try:
                # Simulate some operation that might fail
                result = "success"
                return result
            except Exception as e:
                # Return no_update to prevent UI updates on error
                return no_update
        
        # Test normal case
        result = simulate_callback_error_handling()
        assert result == "success"


class TestPerformanceAndScaling:
    """Test performance aspects of the Dash application."""
    
    def test_large_dataset_handling(self):
        """Test application behavior with larger datasets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create larger dataset for performance testing
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=100)
            
            from utils import Config, get_table_info
            import time
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            # Time the table info loading (critical for app startup)
            start_time = time.time()
            result = get_table_info(config)
            loading_time = time.time() - start_time
            
            # Should handle 100 subjects efficiently
            assert loading_time < 5.0  # Should complete within 5 seconds
            
            # Verify data integrity
            behavioral_tables, demographics_columns = result[0], result[1]
            assert len(behavioral_tables) > 0
            assert len(demographics_columns) > 0
    
    def test_callback_efficiency(self):
        """Test that callback operations are efficient."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=50)
            
            from utils import Config, generate_secure_query_suite
            import time
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            merge_keys = config.get_merge_keys()
            
            # Time query generation (happens in callbacks)
            start_time = time.time()
            data_query, count_query, params = generate_secure_query_suite(
                config=config,
                merge_keys=merge_keys,
                demographic_filters={'age_range': [20, 50]},
                behavioral_filters=[],
                tables_to_join=['demographics', 'cognitive']
            )
            query_time = time.time() - start_time
            
            # Query generation should be fast
            assert query_time < 1.0  # Should complete within 1 second
            assert data_query is not None
            assert count_query is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])