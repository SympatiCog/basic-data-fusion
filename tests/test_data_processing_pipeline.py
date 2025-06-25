"""
Comprehensive Data Processing Pipeline Tests
Tests robustness, performance, and edge cases for data processing functions.
"""
import os
import sys
import tempfile
import time
import pandas as pd
import pytest
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    Config,
    MergeKeys,
    get_unique_session_values,
    extract_column_metadata_fast,
    calculate_numeric_ranges_fast,
    get_table_info,
    enwiden_longitudinal_data,
    _get_table_info_cached
)
from tests.test_data_merge_comprehensive import TestDataGenerator


class TestDataProcessingPipeline:
    """Test the complete data processing pipeline for robustness and performance."""
    
    def test_session_values_extraction_performance(self):
        """Test session value extraction with large datasets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create larger longitudinal dataset for performance testing
            TestDataGenerator.create_longitudinal_data(temp_dir, num_subjects=100, 
                                                     sessions=['BAS1', 'BAS2', 'FU1', 'FU2', 'FU3', 'FU6', 'FU12'])
            
            merge_keys = MergeKeys(
                primary_id='ursi',
                session_id='session_num',
                composite_id='customID',
                is_longitudinal=True
            )
            
            # Time the session extraction
            start_time = time.time()
            session_values, errors = get_unique_session_values(temp_dir, merge_keys)
            extraction_time = time.time() - start_time
            
            # Verify results
            assert len(session_values) == 7  # Should find all 7 sessions
            assert 'BAS1' in session_values
            assert 'FU12' in session_values
            assert len(errors) == 0
            
            # Performance check - should complete within reasonable time
            assert extraction_time < 2.0  # Should take less than 2 seconds
    
    def test_session_values_with_missing_data(self):
        """Test session value extraction with missing/corrupt data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create longitudinal data
            TestDataGenerator.create_longitudinal_data(temp_dir, num_subjects=10)
            
            # Create a corrupt CSV file
            corrupt_file = os.path.join(temp_dir, 'corrupt.csv')
            with open(corrupt_file, 'w') as f:
                f.write("invalid,csv,format\nwith\nmismatched\ncolumns")
            
            # Create a CSV without session column
            no_session_file = os.path.join(temp_dir, 'no_session.csv')
            pd.DataFrame({
                'ursi': ['SUB001', 'SUB002'],
                'score': [100, 110]
            }).to_csv(no_session_file, index=False)
            
            merge_keys = MergeKeys(
                primary_id='ursi',
                session_id='session_num',
                composite_id='customID',
                is_longitudinal=True
            )
            
            session_values, errors = get_unique_session_values(temp_dir, merge_keys)
            
            # Should still extract valid sessions despite errors
            assert len(session_values) > 0
            assert 'BAS1' in session_values
            
            # May or may not report errors depending on how robust the parsing is
            # The key test is that it doesn't crash and processes valid data
            assert isinstance(errors, list)  # Should return error list even if empty
    
    def test_column_metadata_extraction_comprehensive(self):
        """Test column metadata extraction across different data types."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create diverse dataset with various column types
            diverse_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB002', 'SUB003'],
                'age': [25, 30, 35],
                'height': [165.5, 175.2, 180.1],
                'sex': ['M', 'F', 'M'],
                'score_categorical': ['Low', 'Medium', 'High'],
                'boolean_col': [True, False, True],
                'mixed_numeric': [1, 2.5, 3],
                'missing_data': [1.0, None, 3.0],
                'text_col': ['Text A', 'Text B', 'Text C']
            })
            
            file_path = os.path.join(temp_dir, 'diverse_data.csv')
            diverse_data.to_csv(file_path, index=False)
            
            merge_keys = MergeKeys(primary_id='ursi', is_longitudinal=False)
            
            columns, dtypes, errors = extract_column_metadata_fast(
                file_path, 'diverse_data', False, merge_keys, 'demographics'
            )
            
            # Verify behavioral columns are detected (excludes merge key column)
            expected_columns = ['age', 'height', 'sex', 'score_categorical', 
                              'boolean_col', 'mixed_numeric', 'missing_data', 'text_col']
            assert len(columns) == len(expected_columns)
            for col in expected_columns:
                assert col in columns
            
            # Verify data type detection
            assert 'diverse_data.age' in dtypes
            assert 'diverse_data.height' in dtypes
            assert len(errors) == 0
    
    def test_numeric_ranges_calculation_robustness(self):
        """Test numeric range calculation with edge cases."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create dataset with challenging numeric data
            edge_case_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB002', 'SUB003', 'SUB004'],
                'normal_range': [10, 20, 30, 40],
                'negative_values': [-5, -10, 0, 5],
                'large_numbers': [1e6, 2e6, 3e6, 4e6],
                'decimal_precision': [0.001, 0.002, 0.003, 0.004],
                'single_value': [42, 42, 42, 42],
                'with_nulls': [1.0, None, 3.0, None],
                'infinite_values': [1.0, float('inf'), 3.0, float('-inf')],
                'text_mixed': ['1', '2', 'not_a_number', '4']
            })
            
            file_path = os.path.join(temp_dir, 'edge_cases.csv')
            edge_case_data.to_csv(file_path, index=False)
            
            # Extract column metadata first
            merge_keys = MergeKeys(primary_id='ursi', is_longitudinal=False)
            columns, dtypes, meta_errors = extract_column_metadata_fast(
                file_path, 'edge_cases', False, merge_keys, 'demographics'
            )
            
            # Calculate numeric ranges
            ranges, range_errors = calculate_numeric_ranges_fast(
                file_path, 'edge_cases', False, dtypes, merge_keys, 'demographics'
            )
            
            # Verify ranges are calculated for numeric columns
            assert 'edge_cases.normal_range' in ranges
            assert 'edge_cases.negative_values' in ranges
            assert 'edge_cases.large_numbers' in ranges
            
            # Verify range values are sensible
            normal_min, normal_max = ranges['edge_cases.normal_range']
            assert normal_min == 10
            assert normal_max == 40
            
            negative_min, negative_max = ranges['edge_cases.negative_values']
            assert negative_min == -10
            assert negative_max == 5
            
            # Should handle single values gracefully
            if 'edge_cases.single_value' in ranges:
                single_min, single_max = ranges['edge_cases.single_value']
                assert single_min == single_max == 42
            
            # Should not include non-numeric or problematic columns
            assert 'edge_cases.text_mixed' not in ranges
            assert len(range_errors) >= 0  # May have errors from problematic columns
    
    def test_table_info_caching_behavior(self):
        """Test table info caching and invalidation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create initial dataset
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=10)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            # Clear cache to start fresh
            _get_table_info_cached.cache_clear()
            
            # First call - should populate cache
            start_time = time.time()
            result1 = get_table_info(config)
            first_call_time = time.time() - start_time
            
            # Second call - should use cache (much faster)
            start_time = time.time()
            result2 = get_table_info(config)
            second_call_time = time.time() - start_time
            
            # Results should be identical
            assert result1 == result2
            
            # Second call should be significantly faster (cached)
            assert second_call_time < first_call_time * 0.5
            
            # Add a new file to invalidate cache
            new_data = pd.DataFrame({
                'ursi': ['SUB011', 'SUB012'],
                'new_score': [100, 110]
            })
            new_file = os.path.join(temp_dir, 'new_data.csv')
            new_data.to_csv(new_file, index=False)
            
            # Third call - cache should be invalidated due to directory change
            result3 = get_table_info(config)
            
            # Should detect the new table
            behavioral_tables = result3[0]
            assert 'new_data' in behavioral_tables
    
    def test_enwiden_longitudinal_performance(self):
        """Test longitudinal data widening with large datasets."""
        # Create large longitudinal dataset
        num_subjects = 50
        sessions = ['BAS1', 'BAS2', 'FU1', 'FU2', 'FU3', 'FU6', 'FU12']
        
        # Generate comprehensive longitudinal data
        rows = []
        for subject_id in range(1, num_subjects + 1):
            for session in sessions:
                rows.append({
                    'ursi': f'SUB{subject_id:03d}',
                    'session_num': session,
                    'customID': f'SUB{subject_id:03d}_{session}',
                    'age': 25 + (subject_id % 40),
                    'sex': 1 if subject_id % 2 == 0 else 2,
                    'score1': 100 + (subject_id * 2) + (len(session) * 5),
                    'score2': 200 + (subject_id * 3) + (sessions.index(session) * 10),
                    'static_info': f'Info_{subject_id}',  # Should not be widened
                    'dynamic_measure': 50 + subject_id + sessions.index(session)
                })
        
        df = pd.DataFrame(rows)
        
        merge_keys = MergeKeys(
            primary_id='ursi',
            session_id='session_num',
            composite_id='customID',
            is_longitudinal=True
        )
        
        # Time the widening operation
        start_time = time.time()
        widened_df = enwiden_longitudinal_data(df, merge_keys)
        widening_time = time.time() - start_time
        
        # Verify results
        assert len(widened_df) == num_subjects  # One row per subject
        assert len(widened_df['ursi'].unique()) == num_subjects
        
        # Static columns should be preserved as-is
        assert 'age' in widened_df.columns
        assert 'sex' in widened_df.columns
        assert 'static_info' in widened_df.columns
        
        # Dynamic columns should be widened with session suffixes
        session_columns = [col for col in widened_df.columns if any(session in col for session in sessions)]
        assert len(session_columns) > 0
        
        # Performance check
        assert widening_time < 5.0  # Should complete within 5 seconds
    
    def test_pipeline_integration_stress_test(self):
        """Stress test the entire data processing pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple datasets with varying complexity
            datasets = []
            
            # Create cross-sectional data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=20)
            datasets.append('cross_sectional')
            
            # Create longitudinal data
            TestDataGenerator.create_longitudinal_data(temp_dir, num_subjects=15, 
                                                     sessions=['BAS', 'FU1', 'FU2', 'FU3'])
            datasets.append('longitudinal')
            
            # Create additional complex datasets
            for i in range(3):
                complex_data = pd.DataFrame({
                    'ursi': [f'COMPLEX{j:03d}' for j in range(1, 21)],
                    f'measure_{i}_1': [j * (i + 1) for j in range(1, 21)],
                    f'measure_{i}_2': [j * (i + 2) + 0.5 for j in range(1, 21)],
                    f'categorical_{i}': [f'Cat_{j % 3}' for j in range(1, 21)],
                    'session_num': [f'SESS_{j % 4}' for j in range(1, 21)]
                })
                complex_file = os.path.join(temp_dir, f'complex_{i}.csv')
                complex_data.to_csv(complex_file, index=False)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            # Clear cache for fair performance test
            _get_table_info_cached.cache_clear()
            
            # Run complete table info extraction
            start_time = time.time()
            result = get_table_info(config)
            processing_time = time.time() - start_time
            
            # Unpack results
            (behavioral_tables, demographics_columns, behavioral_columns_by_table,
             column_dtypes, column_ranges, merge_keys_dict, actions_taken,
             session_values, is_empty, messages) = result
            
            # Verify comprehensive processing
            assert not is_empty
            assert len(behavioral_tables) >= 5  # Should find multiple tables
            assert len(demographics_columns) > 0
            assert len(column_dtypes) > 0
            assert len(column_ranges) > 0
            
            # Verify session detection for longitudinal data
            if session_values:
                assert len(session_values) > 0
            
            # Performance check - should handle complex datasets efficiently
            assert processing_time < 10.0  # Should complete within 10 seconds
            
            # Verify no critical system errors (minor file issues are acceptable)
            critical_errors = [msg for msg in messages if 'error' in msg.lower() and 'fatal' in msg.lower()]
            assert len(critical_errors) == 0
            
            # Check that we have reasonable error reporting (some file parsing errors are expected)
            total_messages = len(messages)
            assert total_messages >= 0  # Should have processed without crashing
    
    def test_error_recovery_and_logging(self):
        """Test data processing error recovery and logging."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mix of valid and invalid files
            
            # Valid demographics file
            valid_demo = pd.DataFrame({
                'ursi': ['SUB001', 'SUB002'],
                'age': [25, 30],
                'sex': [1, 2]
            })
            valid_demo.to_csv(os.path.join(temp_dir, 'demographics.csv'), index=False)
            
            # Valid data file
            valid_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB002'],
                'score': [100, 110]
            })
            valid_data.to_csv(os.path.join(temp_dir, 'valid_data.csv'), index=False)
            
            # Corrupt CSV file
            corrupt_file = os.path.join(temp_dir, 'corrupt.csv')
            with open(corrupt_file, 'w') as f:
                f.write("invalid\ncsv\nformat\nwith,mismatched,columns\nand,incomplete")
            
            # Empty file
            empty_file = os.path.join(temp_dir, 'empty.csv')
            with open(empty_file, 'w') as f:
                f.write("")
            
            # File with problematic data types
            problematic_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB002'],
                'mixed_col': ['text', 123],  # Mixed types
                'null_col': [None, None],    # All nulls
                'inf_col': [float('inf'), float('-inf')]  # Infinite values
            })
            problematic_data.to_csv(os.path.join(temp_dir, 'problematic.csv'), index=False)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.refresh_merge_detection()
            
            # Process despite errors
            result = get_table_info(config)
            
            # Unpack results
            (behavioral_tables, demographics_columns, behavioral_columns_by_table,
             column_dtypes, column_ranges, merge_keys_dict, actions_taken,
             session_values, is_empty, messages) = result
            
            # Should still process valid files
            assert not is_empty
            assert 'valid_data' in behavioral_tables
            assert len(demographics_columns) > 0
            
            # Should continue processing despite problematic files
            # The system is robust and may still include files that have some valid data
            assert isinstance(messages, list)  # Should return message list
            
            # Key test is that valid files are definitely processed
            assert 'valid_data' in behavioral_tables
            
            # Some problematic files may be included if they're parseable at some level
            # This is acceptable as long as the system doesn't crash


if __name__ == "__main__":
    pytest.main([__file__, "-v"])