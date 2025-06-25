"""
Integration tests for secure query generation functions.
Tests the complete secure query generation pipeline.
"""
import os
import sys
import tempfile
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    Config, 
    generate_base_query_logic_secure,
    generate_data_query_secure,
    generate_secure_query_suite
)
from tests.test_data_merge_comprehensive import TestDataGenerator


class TestSecureQueryIntegration:
    """Test secure query generation with real data."""
    
    def test_secure_query_with_safe_inputs(self):
        """Test secure query generation with safe inputs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=10)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            merge_keys = config.get_merge_keys()
            
            # Test secure query generation
            data_query, count_query, params = generate_secure_query_suite(
                config=config,
                merge_keys=merge_keys,
                demographic_filters={'age_range': [25, 65]},
                behavioral_filters=[
                    {'table': 'cognitive', 'column': 'iq_score', 'type': 'range', 'range': [90, 120]}
                ],
                tables_to_join=['demographics', 'cognitive'],
                selected_columns={'cognitive': ['iq_score', 'memory_score']}
            )
            
            # Verify secure queries are generated
            assert data_query is not None
            assert count_query is not None
            assert params is not None
            
            # Verify queries use parameterized form
            assert '?' in data_query
            assert '?' in count_query
            assert len(params) > 0
            
            # Verify no direct SQL injection patterns
            assert 'DROP' not in data_query.upper()
            assert 'DELETE' not in data_query.upper()
            assert '--' not in data_query
            
            # Verify proper column quoting
            assert '"age"' in data_query
            assert '"iq_score"' in data_query
    
    def test_secure_query_blocks_sql_injection(self):
        """Test that secure query generation blocks SQL injection attempts."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=5)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            merge_keys = config.get_merge_keys()
            
            # Test with malicious inputs
            malicious_filters = [
                {'table': "'; DROP TABLE users; --", 'column': 'score', 'type': 'range', 'range': [0, 100]},
                {'table': 'cognitive', 'column': "'; SELECT password FROM users; --", 'type': 'range', 'range': [0, 100]}
            ]
            
            malicious_tables = ['demographics', "'; DROP DATABASE; --"]
            
            malicious_columns = {
                "'; DROP TABLE data; --": ['malicious_col'],
                'cognitive': ["'; UNION SELECT password FROM users; --"]
            }
            
            # Generate queries with malicious inputs
            data_query, count_query, params = generate_secure_query_suite(
                config=config,
                merge_keys=merge_keys,
                demographic_filters={'age_range': [25, 65]},
                behavioral_filters=malicious_filters,
                tables_to_join=malicious_tables,
                selected_columns=malicious_columns
            )
            
            # Verify malicious inputs are sanitized
            assert data_query is not None
            assert 'DROP' not in data_query.upper()
            assert 'DELETE' not in data_query.upper()
            assert 'UNION' not in data_query.upper()
            assert 'SELECT password' not in data_query
            assert '--' not in data_query
            
            # Verify only safe tables and columns are included
            query_upper = data_query.upper()
            assert 'COGNITIVE' in query_upper  # Safe table should be included
            
            # Verify that sanitized column names don't contain SQL injection patterns
            # (they may contain sanitized versions of keywords, but not functional SQL)
            assert 'SELECT PASSWORD FROM USERS' not in query_upper  # Original malicious pattern removed
            assert 'DROP TABLE' not in query_upper  # Original malicious pattern removed
            assert 'UNION SELECT' not in query_upper  # Original malicious pattern removed
    
    def test_secure_query_validates_table_whitelist(self):
        """Test that secure queries only allow tables that exist in data directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=5)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            merge_keys = config.get_merge_keys()
            
            # Try to query non-existent tables
            data_query, count_query, params = generate_secure_query_suite(
                config=config,
                merge_keys=merge_keys,
                demographic_filters={},
                behavioral_filters=[],
                tables_to_join=['demographics', 'nonexistent_table', 'another_fake_table'],
                selected_columns={'nonexistent_table': ['fake_column']}
            )
            
            # Verify only existing tables are included
            assert 'demographics' in data_query
            assert 'nonexistent_table' not in data_query
            assert 'another_fake_table' not in data_query
            assert 'fake_column' not in data_query
    
    def test_secure_query_handles_empty_inputs(self):
        """Test secure query generation with empty/None inputs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=5)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            merge_keys = config.get_merge_keys()
            
            # Test with minimal inputs
            data_query, count_query, params = generate_secure_query_suite(
                config=config,
                merge_keys=merge_keys,
                demographic_filters={},
                behavioral_filters=[],
                tables_to_join=[],
                selected_columns=None
            )
            
            # Should still generate valid queries
            assert data_query is not None
            assert count_query is not None
            assert isinstance(params, list)
            
            # Should include demographics table by default
            assert 'demo' in data_query
    
    def test_secure_query_parameterization(self):
        """Test that all user inputs are properly parameterized."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=5)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            merge_keys = config.get_merge_keys()
            
            # Test with various filter types
            data_query, count_query, params = generate_secure_query_suite(
                config=config,
                merge_keys=merge_keys,
                demographic_filters={'age_range': [20, 60]},
                behavioral_filters=[
                    {'table': 'cognitive', 'column': 'iq_score', 'type': 'range', 'range': [90, 120]}
                ],
                tables_to_join=['demographics', 'cognitive']
            )
            
            # Count placeholders in query
            placeholder_count = data_query.count('?')
            
            # Should have parameters for age range (2) and behavioral filter range (2)
            assert placeholder_count == len(params)
            assert len(params) >= 4  # At least age min/max and behavioral min/max
            
            # Verify parameter values are correct types
            for param in params:
                assert isinstance(param, (int, float, str))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])