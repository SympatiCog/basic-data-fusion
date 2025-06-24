"""
Integration tests for end-to-end workflows and DuckDB operations.
"""
import os
import shutil
import sys
import tempfile
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    Config,
    FlexibleMergeStrategy,
    MergeKeys,
    generate_base_query_logic,
    generate_count_query,
    generate_data_query,
    get_db_connection,
    get_table_info,
)


class TestDuckDBIntegration:
    """Test DuckDB database operations and SQL execution."""

    def test_get_db_connection(self):
        """Test DuckDB connection establishment."""
        conn = get_db_connection()

        assert conn is not None
        assert isinstance(conn, duckdb.DuckDBPyConnection)

        # Test that connection is functional
        result = conn.execute("SELECT 1 as test_value").fetchone()
        assert result[0] == 1

        # Test connection caching (should return same instance)
        conn2 = get_db_connection()
        assert conn is conn2

    def test_sql_query_execution_with_real_data(self):
        """Test executing generated SQL queries with real data."""
        # Create temporary CSV files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create demographics.csv
            demo_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB002', 'SUB003'],
                'age': [25, 32, 28],
                'sex': [1.0, 2.0, 1.0]
            })
            demo_path = os.path.join(temp_dir, 'demographics.csv')
            demo_data.to_csv(demo_path, index=False)

            # Create cognitive.csv
            cog_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB002', 'SUB003'],
                'working_memory': [105, 98, 112],
                'attention_score': [78, 82, 85]
            })
            cog_path = os.path.join(temp_dir, 'cognitive.csv')
            cog_data.to_csv(cog_path, index=False)

            # Generate SQL query
            merge_keys = MergeKeys(primary_id='ursi', is_longitudinal=False)
            demographic_filters = {
                'age_range': (20, 35),
                'sex': ['Female', 'Male'],
                'sessions': None,
                'studies': None,
                'substudies': None
            }
            behavioral_filters = [
                {
                    'table': 'cognitive',
                    'column': 'working_memory',
                    'min_val': 100,
                    'max_val': 120
                }
            ]

            # Generate base query with actual file paths
            base_query, params = generate_base_query_logic(
                demographic_filters=demographic_filters,
                behavioral_filters=behavioral_filters,
                tables_to_join=['cognitive'],
                merge_keys=merge_keys
            )

            # Replace placeholder paths with actual paths
            actual_query = base_query.replace('data/demographics.csv', demo_path)
            actual_query = actual_query.replace('data/cognitive.csv', cog_path)

            # Execute query
            conn = get_db_connection()
            try:
                result = conn.execute(f"SELECT COUNT(*) {actual_query}", params).fetchone()
                assert result[0] >= 0  # Should return valid count

                # Test data query
                selected_columns = {
                    'demo': ['ursi', 'age', 'sex'],  # Demographics table is aliased as 'demo'
                    'cognitive': ['working_memory', 'attention_score']
                }
                data_query, data_params = generate_data_query(
                    base_query_logic=actual_query,
                    params=params,
                    selected_tables=['demo', 'cognitive'],  # Use 'demo' instead of 'demographics'
                    selected_columns=selected_columns
                )

                if data_query:
                    data_result = conn.execute(data_query, data_params).fetchdf()
                    assert isinstance(data_result, pd.DataFrame)
                    assert len(data_result) >= 0

            except Exception as e:
                # Print debug info if query fails
                print(f"Query failed: {e}")
                print(f"Query: {actual_query}")
                print(f"Params: {params}")
                raise

    def test_longitudinal_sql_execution(self):
        """Test SQL execution with longitudinal data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create longitudinal data
            demo_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB001', 'SUB002', 'SUB002'],
                'session_num': ['BAS1', 'BAS2', 'BAS1', 'BAS2'],
                'customID': ['SUB001_BAS1', 'SUB001_BAS2', 'SUB002_BAS1', 'SUB002_BAS2'],
                'age': [25, 25, 32, 32],
                'sex': [1.0, 1.0, 2.0, 2.0]
            })
            demo_path = os.path.join(temp_dir, 'demographics.csv')
            demo_data.to_csv(demo_path, index=False)

            merge_keys = MergeKeys(
                primary_id='ursi',
                session_id='session_num',
                composite_id='customID',
                is_longitudinal=True
            )

            demographic_filters = {
                'age_range': None,
                'sex': None,
                'sessions': ['BAS1'],
                'studies': None,
                'substudies': None
            }

            base_query, params = generate_base_query_logic(
                demographic_filters=demographic_filters,
                behavioral_filters=[],
                tables_to_join=[],
                merge_keys=merge_keys
            )

            actual_query = base_query.replace('data/demographics.csv', demo_path)

            conn = get_db_connection()
            result = conn.execute(f"SELECT COUNT(*) {actual_query}", params).fetchone()
            assert result[0] >= 0


class TestEndToEndWorkflow:
    """Test complete end-to-end data processing workflows."""

    @pytest.fixture
    def cross_sectional_data_dir(self):
        """Create a temporary directory with cross-sectional test data."""
        temp_dir = tempfile.mkdtemp()

        # Create demographics.csv
        demo_data = pd.DataFrame({
            'ursi': ['SUB001', 'SUB002', 'SUB003', 'SUB004'],
            'age': [25, 32, 28, 45],
            'sex': [1.0, 2.0, 1.0, 2.0],
            'height': [165.5, 178.0, 162.3, 185.2],
            'weight': [60.2, 75.8, 55.9, 82.1]
        })
        demo_data.to_csv(os.path.join(temp_dir, 'demographics.csv'), index=False)

        # Create cognitive.csv
        cog_data = pd.DataFrame({
            'ursi': ['SUB001', 'SUB002', 'SUB003', 'SUB004'],
            'working_memory': [105, 98, 112, 89],
            'processing_speed': [45, 52, 48, 41],
            'attention_score': [78, 82, 85, 72]
        })
        cog_data.to_csv(os.path.join(temp_dir, 'cognitive.csv'), index=False)

        # Create flanker.csv
        flanker_data = pd.DataFrame({
            'ursi': ['SUB001', 'SUB002', 'SUB003'],
            'rt_congruent': [500, 520, 490],
            'rt_incongruent': [550, 580, 540],
            'accuracy': [0.95, 0.92, 0.97]
        })
        flanker_data.to_csv(os.path.join(temp_dir, 'flanker.csv'), index=False)

        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def longitudinal_data_dir(self):
        """Create a temporary directory with longitudinal test data."""
        temp_dir = tempfile.mkdtemp()

        # Create demographics.csv
        demo_data = pd.DataFrame({
            'ursi': ['SUB001', 'SUB001', 'SUB002', 'SUB002', 'SUB003', 'SUB003'],
            'session_num': ['BAS1', 'BAS2', 'BAS1', 'BAS2', 'BAS1', 'BAS2'],
            'customID': ['SUB001_BAS1', 'SUB001_BAS2', 'SUB002_BAS1', 'SUB002_BAS2', 'SUB003_BAS1', 'SUB003_BAS2'],
            'age': [25, 25, 32, 32, 28, 28],
            'sex': [1.0, 1.0, 2.0, 2.0, 1.0, 1.0]
        })
        demo_data.to_csv(os.path.join(temp_dir, 'demographics.csv'), index=False)

        # Create cognitive.csv
        cog_data = pd.DataFrame({
            'ursi': ['SUB001', 'SUB001', 'SUB002', 'SUB002', 'SUB003', 'SUB003'],
            'session_num': ['BAS1', 'BAS2', 'BAS1', 'BAS2', 'BAS1', 'BAS2'],
            'customID': ['SUB001_BAS1', 'SUB001_BAS2', 'SUB002_BAS1', 'SUB002_BAS2', 'SUB003_BAS1', 'SUB003_BAS2'],
            'working_memory': [105, 108, 98, 102, 112, 115],
            'processing_speed': [45, 47, 52, 54, 48, 50]
        })
        cog_data.to_csv(os.path.join(temp_dir, 'cognitive.csv'), index=False)

        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_cross_sectional_workflow(self, cross_sectional_data_dir):
        """Test complete cross-sectional data processing workflow."""
        # Step 1: Configure for cross-sectional data directory
        original_data_dir = Config.DATA_DIR
        Config.DATA_DIR = cross_sectional_data_dir
        Config.refresh_merge_detection()  # Force refresh to pick up new data directory

        try:
            # Step 2: Get table info (main data discovery function)
            conn = get_db_connection()
            behavioral_tables, demographics_columns, behavioral_columns, column_dtypes, column_ranges, merge_keys, actions_taken, session_values, is_empty_state = get_table_info(conn, cross_sectional_data_dir)

            # Verify table detection
            assert 'cognitive' in behavioral_tables
            assert 'flanker' in behavioral_tables
            assert 'demographics' not in behavioral_tables  # Demographics is handled separately
            assert len(demographics_columns) > 0  # Demographics should be detected

            # Verify merge keys detection
            assert merge_keys.primary_id == 'ursi'
            assert not merge_keys.is_longitudinal

            # Step 3: Test query generation and execution
            demographic_filters = {
                'age_range': (20, 40),
                'sex': ['Female', 'Male'],
                'sessions': None,
                'studies': None,
                'substudies': None
            }

            behavioral_filters = [
                {
                    'table': 'cognitive',
                    'column': 'working_memory',
                    'min_val': 100,
                    'max_val': 120
                }
            ]

            # Generate and execute count query
            base_query, params = generate_base_query_logic(
                demographic_filters=demographic_filters,
                behavioral_filters=behavioral_filters,
                tables_to_join=['cognitive'],
                merge_keys=merge_keys
            )

            count_query, count_params = generate_count_query(
                base_query_logic=base_query,
                params=params,
                merge_keys=merge_keys
            )

            conn = get_db_connection()
            if count_query:
                count_result = conn.execute(count_query, count_params).fetchone()
                assert count_result[0] >= 0

            # Step 4: Test data export query
            selected_columns = {
                'demo': ['ursi', 'age', 'sex'],  # Demographics table is aliased as 'demo'
                'cognitive': ['working_memory', 'processing_speed']
            }

            data_query, data_params = generate_data_query(
                base_query_logic=base_query,
                params=params,
                selected_tables=['demo', 'cognitive'],  # Use 'demo' instead of 'demographics'
                selected_columns=selected_columns
            )

            if data_query:
                data_result = conn.execute(data_query, data_params).fetchdf()
                assert isinstance(data_result, pd.DataFrame)

                # Verify expected columns are present
                # Note: demo.* includes all demographics columns, selected columns get table prefixes
                assert len(data_result.columns) > 0
                # Check that we have some expected columns (may be prefixed)
                column_names = ' '.join(data_result.columns)
                assert 'ursi' in column_names
                assert 'age' in column_names

        finally:
            Config.DATA_DIR = original_data_dir

    def test_longitudinal_workflow(self, longitudinal_data_dir):
        """Test complete longitudinal data processing workflow."""
        original_data_dir = Config.DATA_DIR
        Config.DATA_DIR = longitudinal_data_dir
        Config.refresh_merge_detection()  # Force refresh to pick up new data directory

        try:
            # Step 1: Get table info and verify longitudinal detection
            conn = get_db_connection()
            behavioral_tables, demographics_columns, behavioral_columns, column_dtypes, column_ranges, merge_keys, actions_taken, session_values, is_empty_state = get_table_info(conn, longitudinal_data_dir)
            assert merge_keys.is_longitudinal
            assert merge_keys.primary_id == 'ursi'
            assert merge_keys.session_id == 'session_num'
            assert merge_keys.composite_id == 'customID'

            # Step 2: Test session filtering
            demographic_filters = {
                'age_range': None,
                'sex': None,
                'sessions': ['BAS1'],  # Filter to only BAS1 session
                'studies': None,
                'substudies': None
            }

            base_query, params = generate_base_query_logic(
                demographic_filters=demographic_filters,
                behavioral_filters=[],
                tables_to_join=['cognitive'],
                merge_keys=merge_keys
            )

            # Step 3: Execute query and verify session filtering works
            conn = get_db_connection()
            count_query, count_params = generate_count_query(
                base_query_logic=base_query,
                params=params,
                merge_keys=merge_keys
            )

            if count_query:
                count_result = conn.execute(count_query, count_params).fetchone()
                # Should have 3 participants with BAS1 data
                assert count_result[0] == 3

        finally:
            Config.DATA_DIR = original_data_dir

    def test_file_upload_to_query_workflow(self):
        """Test workflow from file upload to query execution."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Step 1: Simulate file upload by creating CSV files
            demo_data = pd.DataFrame({
                'participant_id': ['P001', 'P002'],
                'age': [25, 30],
                'sex': [1.0, 2.0]
            })
            demo_path = os.path.join(temp_dir, 'demographics.csv')
            demo_data.to_csv(demo_path, index=False)

            # Step 2: Configure for custom column names
            original_data_dir = Config.DATA_DIR
            original_primary_id = Config.PRIMARY_ID_COLUMN
            Config.DATA_DIR = temp_dir
            Config.PRIMARY_ID_COLUMN = 'participant_id'
            Config.refresh_merge_detection()  # Force refresh to pick up new configuration

            try:
                # Step 3: Test data discovery with custom configuration
                conn = get_db_connection()
                behavioral_tables, demographics_columns, behavioral_columns, column_dtypes, column_ranges, merge_keys, actions_taken, session_values, is_empty_state = get_table_info(conn, temp_dir)
                assert merge_keys.primary_id == 'participant_id'

                # Step 4: Test query execution with custom merge keys
                base_query, params = generate_base_query_logic(
                    demographic_filters={'age_range': None, 'sex': None, 'sessions': None, 'studies': None, 'substudies': None},
                    behavioral_filters=[],
                    tables_to_join=[],
                    merge_keys=merge_keys
                )

                conn = get_db_connection()
                count_query, count_params = generate_count_query(
                    base_query_logic=base_query,
                    params=params,
                    merge_keys=merge_keys
                )

                if count_query:
                    count_result = conn.execute(count_query, count_params).fetchone()
                    assert count_result[0] == 2  # Should find 2 participants

            finally:
                Config.DATA_DIR = original_data_dir
                Config.PRIMARY_ID_COLUMN = original_primary_id


class TestCLIConfiguration:
    """Test CLI argument parsing and configuration integration."""

    def test_config_parse_cli_args(self):
        """Test CLI argument parsing updates configuration correctly."""
        # Save original values
        original_primary_id = Config.PRIMARY_ID_COLUMN
        original_session_col = Config.SESSION_COLUMN
        original_data_dir = Config.DATA_DIR

        try:
            # Mock sys.argv for CLI parsing
            test_args = [
                'main.py',
                '--',
                '--primary-id-column', 'subject_id',
                '--session-column', 'timepoint',
                '--data-dir', '/test/data'
            ]

            with patch('sys.argv', test_args):
                Config.parse_cli_args()

                # Verify configuration was updated
                assert Config.PRIMARY_ID_COLUMN == 'subject_id'
                assert Config.SESSION_COLUMN == 'timepoint'
                assert Config.DATA_DIR == '/test/data'

        finally:
            # Restore original values
            Config.PRIMARY_ID_COLUMN = original_primary_id
            Config.SESSION_COLUMN = original_session_col
            Config.DATA_DIR = original_data_dir

    def test_merge_strategy_with_custom_config(self):
        """Test that merge strategy respects custom configuration."""
        # Create test CSV with custom column names
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("subject_id,timepoint,study_phase,age,sex\n")
            f.write("S001,T1,baseline,25,Female\n")
            f.write("S001,T2,followup,25,Female\n")
            temp_path = f.name

        try:
            # Test with custom column configuration
            strategy = FlexibleMergeStrategy(
                primary_id_column='subject_id',
                session_column='timepoint',
                composite_id_column='study_phase'
            )

            merge_keys = strategy.detect_structure(temp_path)

            assert merge_keys.primary_id == 'subject_id'
            assert merge_keys.session_id == 'timepoint'
            assert merge_keys.composite_id == 'study_phase'
            assert merge_keys.is_longitudinal

        finally:
            os.unlink(temp_path)


class TestErrorHandlingIntegration:
    """Test error handling in integration scenarios."""

    def test_missing_demographics_file(self):
        """Test behavior when demographics.csv is missing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create only cognitive.csv, no demographics.csv
            cog_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB002'],
                'working_memory': [105, 98]
            })
            cog_data.to_csv(os.path.join(temp_dir, 'cognitive.csv'), index=False)

            original_data_dir = Config.DATA_DIR
            Config.DATA_DIR = temp_dir

            try:
                # Should handle missing demographics gracefully
                conn = get_db_connection()
                behavioral_tables, demographics_columns, behavioral_columns, column_dtypes, column_ranges, merge_keys, actions_taken, session_values, is_empty_state = get_table_info(conn, temp_dir)

                # Should still return some structure, even if limited
                assert isinstance(behavioral_tables, list)

            finally:
                Config.DATA_DIR = original_data_dir

    def test_invalid_csv_structure(self):
        """Test handling of CSV files with invalid structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create malformed CSV
            with open(os.path.join(temp_dir, 'demographics.csv'), 'w') as f:
                f.write("invalid,csv,structure\n")
                f.write("missing,columns\n")  # Wrong number of columns
                f.write("more,missing,data,extra\n")  # Different number of columns

            original_data_dir = Config.DATA_DIR
            Config.DATA_DIR = temp_dir

            try:
                # Should handle malformed CSV gracefully
                conn = get_db_connection()
                behavioral_tables, demographics_columns, behavioral_columns, column_dtypes, column_ranges, merge_keys, actions_taken, session_values, is_empty_state = get_table_info(conn, temp_dir)
                assert isinstance(behavioral_tables, list)

            except Exception as e:
                # If it raises an exception, it should be informative
                assert isinstance(e, (pd.errors.ParserError, ValueError))

            finally:
                Config.DATA_DIR = original_data_dir

    def test_empty_data_directory(self):
        """Test behavior with empty data directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_data_dir = Config.DATA_DIR
            Config.DATA_DIR = temp_dir

            try:
                conn = get_db_connection()
                behavioral_tables, demographics_columns, behavioral_columns, column_dtypes, column_ranges, merge_keys, actions_taken, session_values, is_empty_state = get_table_info(conn, temp_dir)

                # Should return empty but valid structure
                assert isinstance(behavioral_tables, list)
                assert len(behavioral_tables) == 0

            finally:
                Config.DATA_DIR = original_data_dir
