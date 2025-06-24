"""
Comprehensive Data Merge Logic Testing for Basic Data Fusion

This module provides thorough testing of the FlexibleMergeStrategy and data structure
detection logic. It serves as both validation and baseline establishment for testing
secure query generation functions.
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Tuple
import logging

import pandas as pd
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    Config,
    FlexibleMergeStrategy,
    MergeKeys,
    get_table_info,
    generate_base_query_logic,
    generate_count_query,
    generate_data_query,
    get_db_connection,
    reset_db_connection
)


class TestDataGenerator:
    """Helper class to generate test datasets for various merge scenarios."""
    
    @staticmethod
    def create_cross_sectional_data(data_dir: str, num_subjects: int = 100) -> Dict[str, str]:
        """Create cross-sectional test data with multiple tables."""
        
        # Demographics table
        demographics_data = {
            'ursi': [f'SUB{i:03d}' for i in range(1, num_subjects + 1)],
            'age': [20 + (i % 50) for i in range(num_subjects)],
            'sex': [1 if i % 2 == 0 else 2 for i in range(num_subjects)],
            'site': ['Site_A' if i % 3 == 0 else 'Site_B' if i % 3 == 1 else 'Site_C' 
                    for i in range(num_subjects)]
        }
        
        demographics_df = pd.DataFrame(demographics_data)
        demographics_path = os.path.join(data_dir, 'demographics.csv')
        demographics_df.to_csv(demographics_path, index=False)
        
        # Cognitive assessment table
        cognitive_data = {
            'ursi': [f'SUB{i:03d}' for i in range(1, num_subjects + 1)],
            'iq_score': [90 + (i % 40) for i in range(num_subjects)],
            'memory_score': [15 + (i % 25) for i in range(num_subjects)],
            'attention_score': [8 + (i % 12) for i in range(num_subjects)]
        }
        
        cognitive_df = pd.DataFrame(cognitive_data)
        cognitive_path = os.path.join(data_dir, 'cognitive.csv')
        cognitive_df.to_csv(cognitive_path, index=False)
        
        # Behavioral assessment table (with some missing subjects)
        behavioral_subjects = num_subjects - 10  # Simulate some missing data
        behavioral_data = {
            'ursi': [f'SUB{i:03d}' for i in range(1, behavioral_subjects + 1)],
            'anxiety_score': [5 + (i % 15) for i in range(behavioral_subjects)],
            'depression_score': [3 + (i % 12) for i in range(behavioral_subjects)]
        }
        
        behavioral_df = pd.DataFrame(behavioral_data)
        behavioral_path = os.path.join(data_dir, 'behavioral.csv')
        behavioral_df.to_csv(behavioral_path, index=False)
        
        return {
            'demographics': demographics_path,
            'cognitive': cognitive_path,
            'behavioral': behavioral_path
        }
    
    @staticmethod
    def create_longitudinal_data(data_dir: str, num_subjects: int = 50, 
                               sessions: List[str] = None) -> Dict[str, str]:
        """Create longitudinal test data with multiple sessions."""
        
        if sessions is None:
            sessions = ['BAS1', 'BAS2', 'FU1', 'FU2']
        
        # Demographics table with session data
        demo_rows = []
        for subject_id in range(1, num_subjects + 1):
            for session in sessions:
                # Skip some sessions randomly to simulate missing data
                if subject_id % 7 == 0 and session == 'FU2':
                    continue  # Simulate dropout
                    
                demo_rows.append({
                    'ursi': f'SUB{subject_id:03d}',
                    'session_num': session,
                    'age': 20 + subject_id % 50,
                    'sex': 1 if subject_id % 2 == 0 else 2,
                    'visit_date': f'2023-{(subject_id % 12) + 1:02d}-01'
                })
        
        demographics_df = pd.DataFrame(demo_rows)
        demographics_path = os.path.join(data_dir, 'demographics.csv')
        demographics_df.to_csv(demographics_path, index=False)
        
        # Cognitive data with sessions (some sessions missing for some subjects)
        cognitive_rows = []
        for subject_id in range(1, num_subjects + 1):
            for session in sessions:
                # Simulate some missing cognitive sessions
                if subject_id % 5 == 0 and session == 'FU1':
                    continue
                if subject_id % 7 == 0 and session == 'FU2':
                    continue
                    
                cognitive_rows.append({
                    'ursi': f'SUB{subject_id:03d}',
                    'session_num': session,
                    'iq_score': 90 + (subject_id % 40) + (len(session) * 2),  # Slight session effect
                    'memory_score': 15 + (subject_id % 25) + (sessions.index(session)),
                    'processing_speed': 8 + (subject_id % 12)
                })
        
        cognitive_df = pd.DataFrame(cognitive_rows)
        cognitive_path = os.path.join(data_dir, 'cognitive.csv')
        cognitive_df.to_csv(cognitive_path, index=False)
        
        # Behavioral data (only baseline sessions)
        behavioral_rows = []
        for subject_id in range(1, num_subjects + 1):
            for session in ['BAS1', 'BAS2']:  # Only baseline sessions
                if session in sessions:
                    behavioral_rows.append({
                        'ursi': f'SUB{subject_id:03d}',
                        'session_num': session,
                        'anxiety_score': 5 + (subject_id % 15),
                        'depression_score': 3 + (subject_id % 12)
                    })
        
        behavioral_df = pd.DataFrame(behavioral_rows)
        behavioral_path = os.path.join(data_dir, 'behavioral.csv')
        behavioral_df.to_csv(behavioral_path, index=False)
        
        return {
            'demographics': demographics_path,
            'cognitive': cognitive_path,
            'behavioral': behavioral_path
        }
    
    @staticmethod
    def create_mixed_structure_data(data_dir: str) -> Dict[str, str]:
        """Create data with mixed structural patterns (some longitudinal, some cross-sectional)."""
        
        # Demographics with sessions (longitudinal)
        demo_data = {
            'ursi': ['SUB001', 'SUB001', 'SUB002', 'SUB002', 'SUB003'],
            'session_num': ['BAS1', 'BAS2', 'BAS1', 'BAS2', 'BAS1'],
            'age': [25, 25, 30, 30, 35],
            'sex': [1, 1, 2, 2, 1]
        }
        
        demographics_df = pd.DataFrame(demo_data)
        demographics_path = os.path.join(data_dir, 'demographics.csv')
        demographics_df.to_csv(demographics_path, index=False)
        
        # Cognitive with sessions (longitudinal)
        cognitive_data = {
            'ursi': ['SUB001', 'SUB001', 'SUB002', 'SUB003'],
            'session_num': ['BAS1', 'BAS2', 'BAS1', 'BAS1'],
            'iq_score': [110, 112, 105, 98]
        }
        
        cognitive_df = pd.DataFrame(cognitive_data)
        cognitive_path = os.path.join(data_dir, 'cognitive.csv')
        cognitive_df.to_csv(cognitive_path, index=False)
        
        # Behavioral without sessions (cross-sectional style)
        behavioral_data = {
            'ursi': ['SUB001', 'SUB002', 'SUB003'],
            'anxiety_score': [8, 12, 6]
        }
        
        behavioral_df = pd.DataFrame(behavioral_data)
        behavioral_path = os.path.join(data_dir, 'behavioral.csv')
        behavioral_df.to_csv(behavioral_path, index=False)
        
        return {
            'demographics': demographics_path,
            'cognitive': cognitive_path,
            'behavioral': behavioral_path
        }
    
    @staticmethod
    def create_edge_case_data(data_dir: str, case_type: str) -> Dict[str, str]:
        """Create edge case datasets for specific testing scenarios."""
        
        if case_type == 'missing_primary_id':
            # Demographics missing primary ID column
            demo_data = {
                'subject_identifier': ['SUB001', 'SUB002'],  # Different column name
                'age': [25, 30]
            }
            
        elif case_type == 'duplicate_composite_ids':
            # Data with duplicate composite IDs (should be unique)
            demo_data = {
                'ursi': ['SUB001', 'SUB001', 'SUB002'],
                'session_num': ['BAS1', 'BAS1', 'BAS1'],  # Duplicate ursi+session
                'age': [25, 25, 30]
            }
            
        elif case_type == 'inconsistent_sessions':
            # Inconsistent session naming
            demo_data = {
                'ursi': ['SUB001', 'SUB001', 'SUB002'],
                'session_num': ['baseline', 'BAS1', 'visit_1'],  # Inconsistent naming
                'age': [25, 25, 30]
            }
            
        elif case_type == 'empty_demographics':
            # Empty demographics file
            demo_data = {'ursi': [], 'age': []}
            
        else:
            raise ValueError(f"Unknown edge case type: {case_type}")
        
        demographics_df = pd.DataFrame(demo_data)
        demographics_path = os.path.join(data_dir, 'demographics.csv')
        demographics_df.to_csv(demographics_path, index=False)
        
        return {'demographics': demographics_path}


class TestMergeKeysLogic:
    """Test MergeKeys dataclass logic and methods."""
    
    def test_cross_sectional_merge_column(self):
        """Test merge column selection for cross-sectional data."""
        merge_keys = MergeKeys(
            primary_id='ursi',
            is_longitudinal=False
        )
        
        assert merge_keys.get_merge_column() == 'ursi'
        assert not merge_keys.is_longitudinal
        assert merge_keys.session_id is None
        assert merge_keys.composite_id is None
    
    def test_longitudinal_merge_column_with_composite(self):
        """Test merge column selection for longitudinal data with composite ID."""
        merge_keys = MergeKeys(
            primary_id='ursi',
            session_id='session_num',
            composite_id='customID',
            is_longitudinal=True
        )
        
        assert merge_keys.get_merge_column() == 'customID'
        assert merge_keys.is_longitudinal
    
    def test_longitudinal_merge_column_without_composite(self):
        """Test merge column selection for longitudinal data without composite ID."""
        merge_keys = MergeKeys(
            primary_id='ursi',
            session_id='session_num',
            is_longitudinal=True
        )
        
        assert merge_keys.get_merge_column() == 'ursi'
        assert merge_keys.is_longitudinal
    
    def test_merge_keys_serialization(self):
        """Test MergeKeys to_dict and from_dict methods."""
        original = MergeKeys(
            primary_id='ursi',
            session_id='session_num',
            composite_id='customID',
            is_longitudinal=True
        )
        
        # Test serialization
        data_dict = original.to_dict()
        expected_keys = {'primary_id', 'session_id', 'composite_id', 'is_longitudinal'}
        assert set(data_dict.keys()) == expected_keys
        
        # Test deserialization
        restored = MergeKeys.from_dict(data_dict)
        assert restored.primary_id == original.primary_id
        assert restored.session_id == original.session_id
        assert restored.composite_id == original.composite_id
        assert restored.is_longitudinal == original.is_longitudinal


class TestFlexibleMergeStrategyDetection:
    """Test structure detection logic in FlexibleMergeStrategy."""
    
    def test_cross_sectional_detection(self):
        """Test detection of cross-sectional data structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create cross-sectional test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=10)
            
            strategy = FlexibleMergeStrategy()
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            
            merge_keys = strategy.detect_structure(demographics_path)
            
            assert merge_keys.primary_id == 'ursi'
            assert not merge_keys.is_longitudinal
            assert merge_keys.session_id is None
            assert merge_keys.get_merge_column() == 'ursi'
    
    def test_longitudinal_detection(self):
        """Test detection of longitudinal data structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create longitudinal test data
            TestDataGenerator.create_longitudinal_data(temp_dir, num_subjects=10)
            
            strategy = FlexibleMergeStrategy()
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            
            merge_keys = strategy.detect_structure(demographics_path)
            
            assert merge_keys.primary_id == 'ursi'
            assert merge_keys.is_longitudinal
            assert merge_keys.session_id == 'session_num'
            assert merge_keys.composite_id == 'customID'
            assert merge_keys.get_merge_column() == 'customID'
    
    def test_custom_column_names_detection(self):
        """Test detection with custom column names."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data with custom column names
            demo_data = {
                'subject_id': ['P001', 'P002'],
                'visit': ['V1', 'V2'],
                'age': [25, 25]
            }
            
            demographics_df = pd.DataFrame(demo_data)
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            demographics_df.to_csv(demographics_path, index=False)
            
            # Use custom column names
            strategy = FlexibleMergeStrategy(
                primary_id_column='subject_id',
                session_column='visit',
                composite_id_column='participant_visit'
            )
            
            merge_keys = strategy.detect_structure(demographics_path)
            
            assert merge_keys.primary_id == 'subject_id'
            assert merge_keys.is_longitudinal
            assert merge_keys.session_id == 'visit'
            assert merge_keys.composite_id == 'participant_visit'
    
    def test_fallback_id_detection(self):
        """Test fallback ID detection when standard columns missing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data without standard ID columns
            demo_data = {
                'participant_identifier': ['P001', 'P002'],
                'age': [25, 30]
            }
            
            demographics_df = pd.DataFrame(demo_data)
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            demographics_df.to_csv(demographics_path, index=False)
            
            strategy = FlexibleMergeStrategy()
            merge_keys = strategy.detect_structure(demographics_path)
            
            # Should detect 'participant_identifier' as it contains 'id'
            assert merge_keys.primary_id == 'participant_identifier'
            assert not merge_keys.is_longitudinal
    
    def test_missing_file_error(self):
        """Test error handling when demographics file is missing."""
        strategy = FlexibleMergeStrategy()
        
        with pytest.raises(FileNotFoundError):
            strategy.detect_structure('/nonexistent/path/demographics.csv')
    
    def test_no_suitable_id_column_fallback(self):
        """Test fallback behavior when no suitable ID column can be found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data without any ID-like columns
            demo_data = {
                'age': [25, 30],
                'height': [165, 175]
            }
            
            demographics_df = pd.DataFrame(demo_data)
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            demographics_df.to_csv(demographics_path, index=False)
            
            strategy = FlexibleMergeStrategy()
            
            # Should return fallback instead of raising error
            merge_keys = strategy.detect_structure(demographics_path)
            assert merge_keys.primary_id == 'customID'  # Fallback value
            assert not merge_keys.is_longitudinal
    
    def test_composite_id_column_detection(self):
        """Test detection when only composite ID column exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data with only composite ID
            demo_data = {
                'customID': ['SUB001_BAS1', 'SUB002_BAS1'],
                'age': [25, 30]
            }
            
            demographics_df = pd.DataFrame(demo_data)
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            demographics_df.to_csv(demographics_path, index=False)
            
            strategy = FlexibleMergeStrategy()
            merge_keys = strategy.detect_structure(demographics_path)
            
            assert merge_keys.primary_id == 'customID'
            assert not merge_keys.is_longitudinal  # Without session_num, treated as cross-sectional


class TestDataPreparation:
    """Test dataset preparation and composite ID generation."""
    
    def test_longitudinal_composite_id_creation(self):
        """Test creation of composite IDs for longitudinal data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create longitudinal data without composite IDs
            TestDataGenerator.create_longitudinal_data(temp_dir, num_subjects=5)
            
            strategy = FlexibleMergeStrategy()
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            merge_keys = strategy.detect_structure(demographics_path)
            
            # Prepare datasets (should add composite IDs)
            success, actions = strategy.prepare_datasets(temp_dir, merge_keys)
            
            assert success
            assert len(actions) > 0  # Should have taken some actions
            
            # Verify composite IDs were added
            demo_df = pd.read_csv(demographics_path)
            assert 'customID' in demo_df.columns
            
            # Check composite ID format
            expected_id = f"{demo_df.iloc[0]['ursi']}_{demo_df.iloc[0]['session_num']}"
            assert demo_df.iloc[0]['customID'] == expected_id
    
    def test_cross_sectional_id_handling(self):
        """Test ID column handling for cross-sectional data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create cross-sectional data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=5)
            
            strategy = FlexibleMergeStrategy()
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            merge_keys = strategy.detect_structure(demographics_path)
            
            # Prepare datasets
            success, actions = strategy.prepare_datasets(temp_dir, merge_keys)
            
            assert success
            # Should require minimal or no actions for cross-sectional data with proper IDs
    
    def test_missing_primary_id_creation(self):
        """Test creation of primary ID when missing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data without primary ID column
            demo_data = {
                'alternative_id': ['P001', 'P002'],
                'age': [25, 30]
            }
            
            demographics_df = pd.DataFrame(demo_data)
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            demographics_df.to_csv(demographics_path, index=False)
            
            # Create merge keys expecting 'ursi' column
            merge_keys = MergeKeys(primary_id='ursi', is_longitudinal=False)
            
            strategy = FlexibleMergeStrategy()
            success, actions = strategy.prepare_datasets(temp_dir, merge_keys)
            
            assert success
            assert len(actions) > 0
            
            # Verify primary ID was added
            updated_df = pd.read_csv(demographics_path)
            assert 'ursi' in updated_df.columns
    
    def test_composite_id_consistency_validation(self):
        """Test validation of existing composite ID consistency."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data with inconsistent composite IDs
            demo_data = {
                'ursi': ['SUB001', 'SUB002'],
                'session_num': ['BAS1', 'BAS1'],
                'customID': ['SUB001_BAS1', 'WRONG_ID'],  # Second ID is wrong
                'age': [25, 30]
            }
            
            demographics_df = pd.DataFrame(demo_data)
            demographics_path = os.path.join(temp_dir, 'demographics.csv')
            demographics_df.to_csv(demographics_path, index=False)
            
            merge_keys = MergeKeys(
                primary_id='ursi',
                session_id='session_num',
                composite_id='customID',
                is_longitudinal=True
            )
            
            strategy = FlexibleMergeStrategy()
            success, actions = strategy.prepare_datasets(temp_dir, merge_keys)
            
            assert success
            assert any('Fixed inconsistent' in action for action in actions)
            
            # Verify composite ID was corrected
            updated_df = pd.read_csv(demographics_path)
            assert updated_df.iloc[1]['customID'] == 'SUB002_BAS1'


class TestQueryGenerationWithMergeLogic:
    """Test query generation using different merge scenarios."""
    
    def test_cross_sectional_query_generation(self):
        """Test query generation for cross-sectional data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=10)
            
            # Set up config
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            
            # Clear any cached merge keys to ensure fresh detection
            config.refresh_merge_detection()
            
            # Get merge keys
            merge_keys = config.get_merge_keys()
            assert not merge_keys.is_longitudinal
            
            # Test basic query generation
            base_query, params = generate_base_query_logic(
                config,
                merge_keys,
                {},  # No demographic filters
                [],  # No behavioral filters
                ['demographics', 'cognitive']
            )
            
            assert 'FROM read_csv_auto(' in base_query
            assert 'LEFT JOIN' in base_query
            assert 'AS demo' in base_query
            assert 'AS cognitive' in base_query
            assert f'demo."{merge_keys.get_merge_column()}"' in base_query
            
            # Test count query
            count_query, count_params = generate_count_query(base_query, params, merge_keys)
            assert 'COUNT(DISTINCT' in count_query
            assert count_params == params
    
    def test_longitudinal_query_generation(self):
        """Test query generation for longitudinal data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_longitudinal_data(temp_dir, num_subjects=10)
            
            # Set up config
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            
            # Get merge keys and prepare datasets
            merge_keys = config.get_merge_keys()
            assert merge_keys.is_longitudinal
            
            strategy = config.get_merge_strategy()
            success, actions = strategy.prepare_datasets(temp_dir, merge_keys)
            assert success
            
            # Re-detect after preparation
            merge_keys = config.get_merge_keys()
            
            # Test query generation with session filter
            demographic_filters = {'sessions': ['BAS1', 'BAS2']}
            
            base_query, params = generate_base_query_logic(
                config,
                merge_keys,
                demographic_filters,
                [],
                ['demographics', 'cognitive']
            )
            
            assert 'FROM read_csv_auto(' in base_query
            assert 'LEFT JOIN' in base_query
            assert 'WHERE' in base_query
            assert 'IN (' in base_query  # Session filter
            assert len(params) >= 2  # At least the session parameters
            
            # Verify session parameters
            assert 'BAS1' in params
            assert 'BAS2' in params
    
    def test_behavioral_filter_query_generation(self):
        """Test query generation with behavioral filters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=20)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            
            merge_keys = config.get_merge_keys()
            
            # Test numeric behavioral filter
            behavioral_filters = [
                {
                    'table': 'cognitive',
                    'column': 'iq_score',
                    'filter_type': 'numeric',
                    'min_val': 100,
                    'max_val': 130
                }
            ]
            
            base_query, params = generate_base_query_logic(
                config,
                merge_keys,
                {},
                behavioral_filters,
                ['demographics', 'cognitive']
            )
            
            assert 'WHERE' in base_query
            assert 'BETWEEN' in base_query
            assert 'cognitive."iq_score"' in base_query
            assert 100 in params
            assert 130 in params
            
            # Test categorical behavioral filter
            categorical_filters = [
                {
                    'table': 'behavioral',
                    'column': 'anxiety_score',
                    'filter_type': 'categorical',
                    'selected_values': [5, 6, 7]
                }
            ]
            
            base_query, params = generate_base_query_logic(
                config,
                merge_keys,
                {},
                categorical_filters,
                ['demographics', 'behavioral']
            )
            
            assert 'WHERE' in base_query
            assert 'IN (' in base_query
            assert 'behavioral."anxiety_score"' in base_query
            assert 5 in params and 6 in params and 7 in params
    
    def test_data_query_column_selection(self):
        """Test data query generation with column selection."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=10)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            
            merge_keys = config.get_merge_keys()
            
            base_query, params = generate_base_query_logic(
                config, merge_keys, {}, [], ['demographics', 'cognitive']
            )
            
            # Test column selection
            selected_columns = {
                'cognitive': ['iq_score', 'memory_score']
            }
            
            data_query, data_params = generate_data_query(
                base_query,
                params,
                ['demographics', 'cognitive'],
                selected_columns
            )
            
            assert 'SELECT demo.*' in data_query
            assert 'cognitive."iq_score"' in data_query
            assert 'cognitive."memory_score"' in data_query
            assert 'cognitive."attention_score"' not in data_query  # Not selected
            assert data_params == params


class TestTableInfoIntegration:
    """Test get_table_info integration with merge logic."""
    
    def test_cross_sectional_table_info(self):
        """Test table info extraction for cross-sectional data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=15)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            
            # Clear any cached merge keys and table info to ensure fresh detection
            config.refresh_merge_detection()
            
            # Clear the LRU cache for get_table_info to ensure fresh results
            from utils import _get_table_info_cached
            _get_table_info_cached.cache_clear()
            
            (behavioral_tables, demographics_columns, behavioral_columns_by_table,
             column_dtypes, column_ranges, merge_keys_dict, actions_taken,
             session_values, is_empty, messages) = get_table_info(config)
            
            # Verify basic structure
            assert not is_empty
            assert 'cognitive' in behavioral_tables
            assert 'behavioral' in behavioral_tables
            assert 'ursi' in demographics_columns
            assert 'age' in demographics_columns
            
            # Verify merge keys
            merge_keys = MergeKeys.from_dict(merge_keys_dict)
            assert not merge_keys.is_longitudinal
            assert merge_keys.primary_id == 'ursi'
            
            # Verify column metadata
            assert 'cognitive.iq_score' in column_dtypes
            assert 'cognitive.memory_score' in column_dtypes
            
            # Verify no session values for cross-sectional
            assert len(session_values) == 0
    
    def test_longitudinal_table_info(self):
        """Test table info extraction for longitudinal data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            sessions = ['BAS1', 'BAS2', 'FU1']
            TestDataGenerator.create_longitudinal_data(temp_dir, num_subjects=20, sessions=sessions)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            
            (behavioral_tables, demographics_columns, behavioral_columns_by_table,
             column_dtypes, column_ranges, merge_keys_dict, actions_taken,
             session_values, is_empty, messages) = get_table_info(config)
            
            # Verify longitudinal structure
            assert not is_empty
            merge_keys = MergeKeys.from_dict(merge_keys_dict)
            assert merge_keys.is_longitudinal
            assert merge_keys.session_id == 'session_num'
            
            # Verify session detection
            assert set(session_values) == set(sessions)
            
            # Verify actions taken (composite ID creation)
            assert len(actions_taken) > 0
            assert any('customID' in action for action in actions_taken)
    
    def test_table_info_with_missing_data(self):
        """Test table info handling with missing/incomplete data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create minimal data with some missing files
            demo_data = pd.DataFrame({
                'ursi': ['SUB001', 'SUB002'],
                'age': [25, 30]
            })
            demo_data.to_csv(os.path.join(temp_dir, 'demographics.csv'), index=False)
            
            # Create cognitive data for only one subject
            cognitive_data = pd.DataFrame({
                'ursi': ['SUB001'],
                'iq_score': [110]
            })
            cognitive_data.to_csv(os.path.join(temp_dir, 'cognitive.csv'), index=False)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            
            (behavioral_tables, demographics_columns, behavioral_columns_by_table,
             column_dtypes, column_ranges, merge_keys_dict, actions_taken,
             session_values, is_empty, messages) = get_table_info(config)
            
            assert not is_empty
            assert 'cognitive' in behavioral_tables
            assert 'cognitive.iq_score' in column_dtypes
            
            # Should handle missing data gracefully
            if messages:
                for msg in messages:
                    assert 'Error' not in msg or 'graceful' in msg.lower()


class TestRealWorldScenarios:
    """Test realistic data scenarios that might occur in research settings."""
    
    def test_participant_dropout_scenario(self):
        """Test handling of participant dropout in longitudinal studies."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create data with realistic dropout patterns
            sessions = ['BAS1', 'BAS2', 'FU1', 'FU2', 'FU3']
            
            # Manually create data with dropout
            demo_rows = []
            for subj_id in range(1, 21):  # 20 participants
                for i, session in enumerate(sessions):
                    # Simulate increasing dropout
                    dropout_prob = i * 0.15  # 0%, 15%, 30%, 45%, 60% dropout
                    if subj_id <= (20 * (1 - dropout_prob)):
                        demo_rows.append({
                            'ursi': f'SUB{subj_id:03d}',
                            'session_num': session,
                            'age': 20 + subj_id,
                            'visit_completed': 1
                        })
            
            demographics_df = pd.DataFrame(demo_rows)
            demographics_df.to_csv(os.path.join(temp_dir, 'demographics.csv'), index=False)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            
            merge_keys = config.get_merge_keys()
            assert merge_keys.is_longitudinal
            
            # Test query with session filtering
            demographic_filters = {'sessions': ['BAS1', 'FU3']}
            
            base_query, params = generate_base_query_logic(
                config, merge_keys, demographic_filters, [], ['demographics']
            )
            
            # Should handle missing sessions gracefully
            assert 'WHERE' in base_query
            assert 'BAS1' in params
            assert 'FU3' in params
    
    def test_multisite_data_scenario(self):
        """Test handling of multi-site research data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multi-site data
            sites = ['Site_A', 'Site_B', 'Site_C']
            demo_rows = []
            
            for site_idx, site in enumerate(sites):
                for subj_id in range(1 + site_idx * 10, 11 + site_idx * 10):
                    demo_rows.append({
                        'ursi': f'{site}_{subj_id:03d}',
                        'age': 20 + subj_id,
                        'site': site,
                        'all_studies': f'Study_{site}'
                    })
            
            demographics_df = pd.DataFrame(demo_rows)
            demographics_df.to_csv(os.path.join(temp_dir, 'demographics.csv'), index=False)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            config.STUDY_SITE_COLUMN = 'all_studies'
            
            merge_keys = config.get_merge_keys()
            
            # Test site-specific filtering
            demographic_filters = {'substudies': ['Study_Site_A', 'Study_Site_B']}
            
            base_query, params = generate_base_query_logic(
                config, merge_keys, demographic_filters, [], ['demographics']
            )
            
            assert 'WHERE' in base_query
            assert 'LIKE' in base_query
            assert any('Study_Site_A' in str(p) for p in params)
    
    def test_large_dataset_performance(self):
        """Test performance with larger datasets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create larger dataset
            num_subjects = 1000
            TestDataGenerator.create_cross_sectional_data(temp_dir, num_subjects=num_subjects)
            
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = 'demographics.csv'
            
            import time
            start_time = time.time()
            
            # Test table info extraction performance
            (behavioral_tables, demographics_columns, behavioral_columns_by_table,
             column_dtypes, column_ranges, merge_keys_dict, actions_taken,
             session_values, is_empty, messages) = get_table_info(config)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Should complete within reasonable time (adjust threshold as needed)
            assert processing_time < 10.0  # 10 seconds max
            assert not is_empty
            assert len(demographics_columns) > 0
            
            # Test query generation performance
            merge_keys = MergeKeys.from_dict(merge_keys_dict)
            
            start_time = time.time()
            base_query, params = generate_base_query_logic(
                config, merge_keys, {}, [], ['demographics', 'cognitive', 'behavioral']
            )
            end_time = time.time()
            
            query_time = end_time - start_time
            assert query_time < 1.0  # Query generation should be very fast


if __name__ == "__main__":
    # Run specific test classes for development
    pytest.main([__file__ + "::TestMergeKeysLogic", "-v"])
    pytest.main([__file__ + "::TestFlexibleMergeStrategyDetection", "-v"])