"""
Merge strategy classes for handling cross-sectional and longitudinal data.

This module provides flexible merge strategies that automatically detect
whether data is cross-sectional or longitudinal and adapt merging accordingly.
"""

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd

from core.exceptions import ValidationError

# Exception aliases for this module
DataProcessingError = ValidationError
MergeStrategyError = ValidationError


@dataclass
class MergeKeys:
    """Encapsulates the merge keys for a dataset."""
    primary_id: str  # e.g., 'ursi', 'subject_id'
    session_id: Optional[str] = None  # e.g., 'session_num'
    composite_id: Optional[str] = None  # e.g., 'customID' (derived)
    is_longitudinal: bool = False

    def get_merge_column(self) -> str:
        """Returns the appropriate column for merge operations."""
        if self.is_longitudinal:
            return self.composite_id if self.composite_id else self.primary_id
        return self.primary_id

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            'primary_id': self.primary_id,
            'session_id': self.session_id,
            'composite_id': self.composite_id,
            'is_longitudinal': self.is_longitudinal
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'MergeKeys':
        """Create from dictionary for deserialization."""
        return cls(
            primary_id=data['primary_id'],
            session_id=data.get('session_id'),
            composite_id=data.get('composite_id'),
            is_longitudinal=data.get('is_longitudinal', False)
        )


class MergeStrategy(ABC):
    """Abstract base class for merge strategies."""

    @abstractmethod
    def detect_structure(self, demographics_path: str) -> MergeKeys:
        """Detect the merge structure from demographics file."""
        pass

    @abstractmethod
    def prepare_datasets(self, data_dir: str, merge_keys: MergeKeys) -> Tuple[bool, List[str]]:
        """Prepare datasets with appropriate merge keys. Returns success status and list of actions."""
        pass


class FlexibleMergeStrategy(MergeStrategy):
    """Flexible merge strategy that adapts to cross-sectional or longitudinal data."""

    def __init__(self, primary_id_column: str = 'ursi', session_column: str = 'session_num', composite_id_column: str = 'customID'):
        self.primary_id_column = primary_id_column
        self.session_column = session_column
        self.composite_id_column = composite_id_column

    def detect_structure(self, demographics_path: str) -> MergeKeys:
        """Detect whether data is cross-sectional or longitudinal."""
        try:
            if not os.path.exists(demographics_path):
                raise FileNotFoundError(f"Demographics file not found: {demographics_path}")

            df_headers = pd.read_csv(demographics_path, nrows=0, low_memory=False)
            columns = df_headers.columns.tolist()

            has_primary_id = self.primary_id_column in columns
            has_session_id = self.session_column and self.session_column in columns
            has_composite_id = self.composite_id_column in columns

            if has_primary_id and has_session_id:
                return MergeKeys(
                    primary_id=self.primary_id_column,
                    session_id=self.session_column,
                    composite_id=self.composite_id_column,
                    is_longitudinal=True
                )
            elif has_primary_id:
                return MergeKeys(primary_id=self.primary_id_column, is_longitudinal=False)
            elif has_composite_id:
                return MergeKeys(primary_id=self.composite_id_column, is_longitudinal=False)
            else:
                id_candidates = [col for col in columns if 'id' in col.lower() or 'ursi' in col.lower()]
                if id_candidates:
                    return MergeKeys(primary_id=id_candidates[0], is_longitudinal=False)
                else:
                    raise MergeStrategyError(f"No suitable ID column found in {demographics_path}")
        except (FileNotFoundError, pd.errors.EmptyDataError) as e:
            logging.error(f"Error detecting merge structure (file/data error): {e}")
            raise MergeStrategyError(f"Error detecting merge structure: {e}")
        except Exception as e:
            logging.error(f"Error detecting merge structure: {e}")
            # Fallback to a default if detection fails critically
            return MergeKeys(primary_id='customID', is_longitudinal=False)

    def prepare_datasets(self, data_dir: str, merge_keys: MergeKeys) -> Tuple[bool, List[str]]:
        """Prepare datasets with appropriate ID columns. Returns success and actions."""
        actions_taken = []

        try:
            csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
            for csv_file in csv_files:
                file_path = os.path.join(data_dir, csv_file)
                if merge_keys.is_longitudinal:
                    action = self._add_composite_id_if_needed(file_path, merge_keys)
                else:
                    action = self._ensure_primary_id_column(file_path, merge_keys)
                if action:
                    actions_taken.append(action)
            return True, actions_taken
        except Exception as e:
            logging.error(f"Error preparing datasets: {e}")
            actions_taken.append(f"Error preparing datasets: {e}")
            return False, actions_taken

    def _add_composite_id_if_needed(self, file_path: str, merge_keys: MergeKeys) -> Optional[str]:
        """Add composite ID column to a file if it doesn't exist or validate existing one."""
        filename = os.path.basename(file_path)
        try:
            df = pd.read_csv(file_path, low_memory=False)
            if not (merge_keys.primary_id in df.columns and merge_keys.session_id in df.columns):
                return None  # Not applicable for this file

            expected_composite_id_col_name = merge_keys.composite_id if merge_keys.composite_id else "customID"

            # Ensure primary_id and session_id columns are treated as strings for concatenation
            primary_series = df[merge_keys.primary_id].astype(str)
            session_series = df[merge_keys.session_id].astype(str)
            expected_composite_values = primary_series + '_' + session_series

            if expected_composite_id_col_name in df.columns:
                current_composite_values = df[expected_composite_id_col_name].astype(str)
                if not current_composite_values.equals(expected_composite_values):
                    df[expected_composite_id_col_name] = expected_composite_values
                    df.to_csv(file_path, index=False)
                    return f"🔧 Fixed inconsistent {expected_composite_id_col_name} in {filename}"
                return None  # Already consistent
            else:
                df[expected_composite_id_col_name] = expected_composite_values
                df.to_csv(file_path, index=False)
                return f"✅ Added {expected_composite_id_col_name} to {filename}"
        except Exception as e:
            return f"⚠️ Could not process {filename} for composite ID: {str(e)}"

    def _ensure_primary_id_column(self, file_path: str, merge_keys: MergeKeys) -> Optional[str]:
        """Ensure primary ID column exists for cross-sectional data, creating it if needed."""
        filename = os.path.basename(file_path)
        try:
            df = pd.read_csv(file_path, low_memory=False)
            expected_primary_id = merge_keys.primary_id

            if expected_primary_id in df.columns:
                return None  # Column already exists

            # Look for alternative ID columns
            id_candidates = [col for col in df.columns if 'id' in col.lower() or 'ursi' in col.lower() or 'subject' in col.lower()]

            if id_candidates:
                # Use the first candidate and rename it
                source_col = id_candidates[0]
                df[expected_primary_id] = df[source_col]
                df.to_csv(file_path, index=False)
                return f"🔧 Added {expected_primary_id} column (mapped from {source_col}) in {filename}"
            else:
                # Create a simple index-based ID
                df[expected_primary_id] = range(1, len(df) + 1)
                df.to_csv(file_path, index=False)
                return f"🔧 Created {expected_primary_id} column (auto-generated) in {filename}"

        except Exception as e:
            return f"⚠️ Could not process {filename} for primary ID: {str(e)}"


def create_merge_strategy(primary_id_column: str = 'ursi', 
                         session_column: str = 'session_num', 
                         composite_id_column: str = 'customID') -> FlexibleMergeStrategy:
    """Factory function to create a merge strategy with specified column names."""
    return FlexibleMergeStrategy(
        primary_id_column=primary_id_column,
        session_column=session_column,
        composite_id_column=composite_id_column
    )