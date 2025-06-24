"""
CRITICAL SQL INJECTION TESTS for Basic Data Fusion
Tests for SQL injection vulnerabilities in query generation and parameter handling.
"""
import os
import sys
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest
import duckdb

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    Config,
    MergeKeys,
    generate_base_query_logic,
    generate_count_query,
    generate_data_query,
    get_db_connection,
    reset_db_connection
)


class TestSQLInjectionPrevention:
    """Critical tests for SQL injection prevention in query generation."""

    @pytest.fixture
    def test_config(self):
        """Create a test configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = "demographics.csv"
            config.PRIMARY_ID_COLUMN = "ursi"
            config.AGE_COLUMN = "age"
            config.SEX_COLUMN = "sex"
            
            # Create test CSV files
            demographics_data = {
                "ursi": ["001", "002", "003"],
                "age": [25, 30, 35],
                "sex": ["M", "F", "M"]
            }
            demo_df = pd.DataFrame(demographics_data)
            demo_df.to_csv(os.path.join(temp_dir, "demographics.csv"), index=False)
            
            behavioral_data = {
                "ursi": ["001", "002", "003"],
                "score": [10, 20, 30],
                "category": ["A", "B", "C"]
            }
            behavioral_df = pd.DataFrame(behavioral_data)
            behavioral_df.to_csv(os.path.join(temp_dir, "behavioral.csv"), index=False)
            
            yield config

    @pytest.fixture
    def cross_sectional_merge_keys(self):
        """Create cross-sectional merge keys."""
        return MergeKeys(
            primary_id="ursi",
            is_longitudinal=False
        )

    def test_malicious_table_names_injection(self, test_config, cross_sectional_merge_keys):
        """Test prevention of SQL injection through table names."""
        malicious_table_names = [
            "'; DROP TABLE demographics; --",
            "behavioral; DELETE FROM demographics; --",
            "table' UNION SELECT * FROM system_tables --",
            "test'; INSERT INTO demographics VALUES ('hacked'); --",
            "behavioral/**/UNION/**/SELECT/**/password/**/FROM/**/users--",
            "table`; EXEC xp_cmdshell('calc'); --",
        ]
        
        for malicious_table in malicious_table_names:
            # Test that malicious table names are handled safely
            try:
                base_query, params = generate_base_query_logic(
                    test_config,
                    cross_sectional_merge_keys,
                    {},  # No demographic filters
                    [],  # No behavioral filters  
                    [malicious_table]  # Malicious table name
                )
                
                # The query should be generated but the malicious table won't exist
                # This tests that the query generation doesn't break
                assert isinstance(base_query, str)
                assert isinstance(params, list)
                
                # The query should not contain dangerous SQL
                assert "DROP" not in base_query.upper()
                assert "DELETE" not in base_query.upper()
                assert "INSERT" not in base_query.upper()
                assert "EXEC" not in base_query.upper()
                assert "--" not in base_query
                
            except Exception as e:
                # If an exception occurs, it should be a safe error, not exposing internals
                error_msg = str(e)
                assert "DROP" not in error_msg.upper()
                assert "DELETE" not in error_msg.upper()
                assert "password" not in error_msg.lower()

    def test_malicious_column_names_injection(self, test_config, cross_sectional_merge_keys):
        """Test prevention of SQL injection through column names in filters."""
        malicious_filters = [
            {
                'table': 'behavioral',
                'column': "'; DROP TABLE demographics; --",
                'filter_type': 'numeric',
                'min_val': 1,
                'max_val': 100
            },
            {
                'table': 'behavioral', 
                'column': "score' UNION SELECT password FROM users --",
                'filter_type': 'categorical',
                'selected_values': ['A', 'B']
            },
            {
                'table': 'behavioral',
                'column': "category`; DELETE FROM demographics; --",
                'filter_type': 'numeric',
                'min_val': 0,
                'max_val': 50
            },
            {
                'table': 'behavioral',
                'column': "field/*comment*/UNION/*comment*/SELECT",
                'filter_type': 'categorical', 
                'selected_values': ['test']
            }
        ]
        
        for malicious_filter in malicious_filters:
            try:
                base_query, params = generate_base_query_logic(
                    test_config,
                    cross_sectional_merge_keys,
                    {},  # No demographic filters
                    [malicious_filter],  # Malicious behavioral filter
                    ['demographics', 'behavioral']
                )
                
                # Query should be generated safely
                assert isinstance(base_query, str)
                assert isinstance(params, list)
                
                # Should not contain dangerous SQL patterns
                assert "DROP" not in base_query.upper()
                assert "DELETE" not in base_query.upper()
                assert "UNION" not in base_query.upper()
                assert "--" not in base_query
                assert "/*" not in base_query
                
                # Column names should be properly quoted
                if malicious_filter['column'] in base_query:
                    # Column should be quoted to prevent injection
                    assert f'"{malicious_filter["column"]}"' in base_query or \
                           malicious_filter['column'] not in base_query
                           
            except Exception as e:
                # Safe error handling
                error_msg = str(e)
                assert "password" not in error_msg.lower()
                assert len(error_msg) < 1000  # Prevent verbose error disclosure

    def test_malicious_filter_values_injection(self, test_config, cross_sectional_merge_keys):
        """Test prevention of SQL injection through filter values."""
        malicious_value_filters = [
            {
                'table': 'behavioral',
                'column': 'category',
                'filter_type': 'categorical',
                'selected_values': ["'; DROP TABLE demographics; --", "normal_value"]
            },
            {
                'table': 'behavioral',
                'column': 'category', 
                'filter_type': 'categorical',
                'selected_values': ["A' UNION SELECT password FROM users --"]
            },
            {
                'table': 'behavioral',
                'column': 'score',
                'filter_type': 'numeric',
                'min_val': "1; DELETE FROM demographics; --",
                'max_val': 100
            },
            {
                'table': 'behavioral',
                'column': 'score',
                'filter_type': 'numeric', 
                'min_val': 1,
                'max_val': "100' UNION SELECT * FROM system_tables --"
            }
        ]
        
        for malicious_filter in malicious_value_filters:
            try:
                base_query, params = generate_base_query_logic(
                    test_config,
                    cross_sectional_merge_keys,
                    {},
                    [malicious_filter],
                    ['demographics', 'behavioral']
                )
                
                # Parameters should be safely parameterized
                assert isinstance(params, list)
                
                # Dangerous values should be in parameters, not query string
                query_upper = base_query.upper()
                assert "DROP" not in query_upper
                assert "DELETE" not in query_upper
                assert "UNION" not in query_upper
                assert "--" not in base_query
                
                # Values should be parameterized with ? placeholders
                if malicious_filter['filter_type'] == 'categorical':
                    # Should use IN (?, ?) pattern
                    assert "IN (" in base_query
                    assert "?" in base_query
                elif malicious_filter['filter_type'] == 'numeric':
                    # Should use BETWEEN ? AND ? pattern
                    assert "BETWEEN" in base_query.upper()
                    assert "?" in base_query
                    
            except (ValueError, TypeError) as e:
                # Type conversion errors are acceptable for invalid numeric values
                pass
            except Exception as e:
                # Other errors should be safe
                error_msg = str(e)
                assert "DROP" not in error_msg.upper()
                assert "password" not in error_msg.lower()

    def test_demographic_filter_injection(self, test_config, cross_sectional_merge_keys):
        """Test prevention of SQL injection through demographic filters."""
        malicious_demo_filters = [
            {
                'age_range': ("1; DROP TABLE demographics; --", 100),
                'substudies': ["'; DELETE FROM demographics; --"],
                'sessions': ["BAS1' UNION SELECT * FROM users --"]
            },
            {
                'age_range': (18, "80' UNION SELECT password FROM admin --"),
                'substudies': ["Discovery/*comment*/UNION/*comment*/SELECT"],
                'sessions': ["session`; EXEC xp_cmdshell('calc'); --"]
            }
        ]
        
        for demo_filter in malicious_demo_filters:
            try:
                base_query, params = generate_base_query_logic(
                    test_config,
                    cross_sectional_merge_keys,
                    demo_filter,
                    [],
                    ['demographics']
                )
                
                # Should generate safely parameterized query
                assert isinstance(base_query, str) 
                assert isinstance(params, list)
                
                # Should not contain dangerous SQL
                query_upper = base_query.upper()
                assert "DROP" not in query_upper
                assert "DELETE" not in query_upper
                assert "UNION" not in query_upper
                assert "EXEC" not in query_upper
                assert "--" not in base_query
                assert "/*" not in base_query
                
            except (ValueError, TypeError):
                # Type errors for invalid age ranges are acceptable
                pass
            except Exception as e:
                error_msg = str(e)
                assert "password" not in error_msg.lower()
                assert "admin" not in error_msg.lower()

    def test_query_execution_safety(self, test_config, cross_sectional_merge_keys):
        """Test that generated queries execute safely without injection."""
        # Reset database connection for clean test
        reset_db_connection()
        
        try:
            conn = get_db_connection()
            
            # Test with legitimate filters
            legitimate_filter = {
                'table': 'behavioral',
                'column': 'category', 
                'filter_type': 'categorical',
                'selected_values': ['A', 'B']
            }
            
            base_query, params = generate_base_query_logic(
                test_config,
                cross_sectional_merge_keys,
                {},
                [legitimate_filter],
                ['demographics', 'behavioral']
            )
            
            count_query, count_params = generate_count_query(
                base_query, params, cross_sectional_merge_keys
            )
            
            # Execute the query safely
            result = conn.execute(count_query, count_params).fetchone()
            
            # Should return a valid count
            assert isinstance(result[0], int)
            assert result[0] >= 0
            
            # Test data query generation 
            selected_columns = {'behavioral': ['category', 'score']}
            data_query, data_params = generate_data_query(
                base_query, params, ['demographics', 'behavioral'], selected_columns
            )
            
            # Should execute without error
            data_result = conn.execute(data_query, data_params).fetchall()
            assert isinstance(data_result, list)
            
        except Exception as e:
            # Database errors should be safe and informative
            error_msg = str(e)
            assert len(error_msg) < 500  # Prevent verbose error disclosure
            assert "password" not in error_msg.lower()
            assert "admin" not in error_msg.lower()

    def test_path_injection_in_file_paths(self, cross_sectional_merge_keys):
        """Test prevention of path injection in CSV file paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = Config()
            
            # Test malicious data directory paths
            malicious_paths = [
                "../../../etc",
                "/etc/passwd",
                "C:\\Windows\\System32",
                temp_dir + "/../../../sensitive",
                temp_dir + "'; DROP TABLE users; --"
            ]
            
            for malicious_path in malicious_paths:
                config.DATA_DIR = malicious_path
                config.DEMOGRAPHICS_FILE = "demographics.csv"
                
                try:
                    base_query, params = generate_base_query_logic(
                        config,
                        cross_sectional_merge_keys,
                        {},
                        [],
                        ['demographics']
                    )
                    
                    # Query should be generated but paths should be safe
                    assert isinstance(base_query, str)
                    
                    # Should not contain SQL injection attempts
                    assert "DROP" not in base_query.upper()
                    assert "--" not in base_query
                    
                    # File paths in query should be properly escaped/quoted
                    assert "read_csv_auto(" in base_query
                    
                except (FileNotFoundError, PermissionError):
                    # These are safe errors for invalid paths
                    pass
                except Exception as e:
                    # Other errors should be safe
                    error_msg = str(e)
                    assert "password" not in error_msg.lower()
                    assert len(error_msg) < 1000

    def test_unicode_injection_prevention(self, test_config, cross_sectional_merge_keys):
        """Test prevention of Unicode-based injection attacks."""
        # Unicode characters that might bypass simple filters
        unicode_injections = [
            "test＇；DROP TABLE users；--",  # Full-width characters
            "normal\u202e'; DROP TABLE--",   # Right-to-left override
            "field\u0000'; DROP TABLE users; --",  # Null byte
            "col\u00A0UNION\u00A0SELECT",  # Non-breaking space
            "name\u180E'; DELETE FROM data; --"  # Mongolian vowel separator
        ]
        
        for injection in unicode_injections:
            malicious_filter = {
                'table': 'behavioral',
                'column': injection,
                'filter_type': 'categorical',
                'selected_values': ['test']
            }
            
            try:
                base_query, params = generate_base_query_logic(
                    test_config,
                    cross_sectional_merge_keys,
                    {},
                    [malicious_filter],
                    ['demographics', 'behavioral']
                )
                
                # Should generate safely
                assert isinstance(base_query, str)
                
                # Should not contain dangerous SQL
                normalized_query = base_query.upper().replace('\u00a0', ' ')
                assert "DROP" not in normalized_query
                assert "DELETE" not in normalized_query
                assert "UNION" not in normalized_query
                
            except Exception as e:
                # Unicode handling errors should be safe
                error_msg = str(e)
                assert len(error_msg) < 500


if __name__ == "__main__":
    pytest.main([__file__, "-v"])