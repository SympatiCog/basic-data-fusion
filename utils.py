import hashlib
import logging
import os
import re
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from threading import Lock
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd
import toml

# Import security utilities
from security_utils import sanitize_sql_identifier, validate_table_name

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Connection Pool ---
_db_connection = None
_db_lock = threading.Lock()

# Thread-safe cache replacements - temp fix for heap corruption
_column_values_cache = {}
_column_values_cache_lock = Lock()
_table_info_cache = {}
_table_info_cache_lock = Lock()

# File access coordination - temp fix for concurrent pandas/DuckDB file access
_file_access_lock = Lock()

def get_db_connection():
    """
    Returns a cached DuckDB connection for improved performance.
    Thread-safe singleton pattern with file access coordination.
    """
    global _db_connection
    if _db_connection is None:
        with _db_lock:
            if _db_connection is None:
                # temp fix: coordinate with file access to prevent concurrent pandas/DuckDB operations
                with _file_access_lock:
                    _db_connection = duckdb.connect(database=':memory:', read_only=False)
                    logging.info("Created new DuckDB connection")
    return _db_connection

def reset_db_connection():
    """Reset the database connection (useful for testing or error recovery)."""
    global _db_connection
    with _db_lock:
        if _db_connection:
            _db_connection.close()
        _db_connection = None
        logging.info("Reset DuckDB connection")

# --- Path Utilities ---
def shorten_path(path_str: str, max_length: int = 60) -> str:
    """
    Shorten a file system path for display purposes.
    
    Strategies:
    1. Replace home directory with ~
    2. If still too long, use middle truncation with ...
    
    Args:
        path_str: The path string to shorten
        max_length: Maximum length for the shortened path
        
    Returns:
        Shortened path string suitable for display
    """
    if not path_str:
        return path_str
        
    # Convert to Path object for easier manipulation
    path = Path(path_str).expanduser().resolve()
    
    # Replace home directory with ~
    try:
        home = Path.home()
        if path.is_relative_to(home):
            relative_path = path.relative_to(home)
            shortened = "~/" + str(relative_path)
        else:
            shortened = str(path)
    except (ValueError, OSError):
        # Fallback if relative_to fails or home directory issues
        shortened = str(path)
    
    # If still too long, apply middle truncation
    if len(shortened) > max_length:
        # Keep first and last parts, truncate middle
        if "/" in shortened:
            parts = shortened.split("/")
            if len(parts) > 3:
                # Keep first directory, last directory, and filename
                first_part = parts[0]
                last_parts = parts[-2:]  # Last directory and filename
                truncated = f"{first_part}/.../{'/'.join(last_parts)}"
                
                # If still too long, just show last parts
                if len(truncated) > max_length:
                    truncated = f".../{'/'.join(last_parts)}"
                
                shortened = truncated
            else:
                # If only a few parts, just truncate the middle
                if len(shortened) > max_length:
                    keep_length = (max_length - 3) // 2
                    shortened = shortened[:keep_length] + "..." + shortened[-keep_length:]
        else:
            # Single name without slashes, just truncate
            if len(shortened) > max_length:
                keep_length = (max_length - 3) // 2
                shortened = shortened[:keep_length] + "..." + shortened[-keep_length:]
    
    return shortened

# --- Merge Strategy Classes ---
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

            df_headers = pd.read_csv(demographics_path, nrows=0, low_memory=False)  # temp fix
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
                    raise ValueError(f"No suitable ID column found in {demographics_path}")
        except (FileNotFoundError, pd.errors.EmptyDataError) as e:
            logging.error(f"Error detecting merge structure (file/data error): {e}")
            raise
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
            df = pd.read_csv(file_path, low_memory=False)  # temp fix
            if not (merge_keys.primary_id in df.columns and merge_keys.session_id in df.columns):
                return None # Not applicable for this file

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
                return None # Already consistent
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
            df = pd.read_csv(file_path, low_memory=False)  # temp fix
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

# --- Configuration ---
@dataclass
class Config:
    CONFIG_FILE_PATH: str = "config.toml"

    # File and directory settings
    DATA_DIR: str = 'data'
    DEMOGRAPHICS_FILE: str = 'demographics.csv'
    # PARTICIPANT_ID_COLUMN is now dynamically determined by MergeKeys
    PRIMARY_ID_COLUMN: str = 'ursi'
    SESSION_COLUMN: str = 'session_num'
    COMPOSITE_ID_COLUMN: str = 'customID'

    # Column name settings
    AGE_COLUMN: str = 'age'
    SEX_COLUMN: str = 'sex'
    STUDY_SITE_COLUMN: Optional[str] = None  # For multisite/multistudy detection

    _merge_strategy: Optional[FlexibleMergeStrategy] = field(init=False, default=None)
    _merge_keys: Optional[MergeKeys] = field(init=False, default=None)

    # UI defaults - these might be moved or handled differently in Dash
    DEFAULT_AGE_RANGE: Tuple[int, int] = (0, 120)
    DEFAULT_AGE_SELECTION: Tuple[int, int] = (18, 80)
    DEFAULT_FILTER_RANGE: Tuple[int, int] = (0, 100)
    MAX_DISPLAY_ROWS: int = 50 # For Dash tables, pagination is better
    CACHE_TTL_SECONDS: int = 600 # For Dash, use Flask-Caching or similar

    # StateManager settings
    STATE_BACKEND: str = 'client'  # 'client', 'memory', 'redis', 'database'
    STATE_TTL_DEFAULT: int = 3600  # 1 hour default TTL for state data
    STATE_ENABLE_USER_ISOLATION: bool = True
    STATE_REDIS_URL: str = 'redis://localhost:6379/0'
    STATE_DATABASE_URL: str = 'sqlite:///state.db'

    # Rockland Study Configuration (using 'all_studies' column)
    ROCKLAND_BASE_STUDIES: List[str] = field(default_factory=lambda: ['Discovery', 'Longitudinal_Adult', 'Longitudinal_Child', 'Neurofeedback'])
    DEFAULT_ROCKLAND_STUDIES: List[str] = field(default_factory=lambda: ['Discovery', 'Longitudinal_Adult', 'Longitudinal_Child', 'Neurofeedback'])
    # Note: Sessions are automatically inferred from session_num column via get_unique_session_values()
    ROCKLAND_SAMPLE1_COLUMNS: List[str] = field(default_factory=lambda: ['rockland-sample1'])
    ROCKLAND_SAMPLE1_LABELS: Dict[str, str] = field(default_factory=lambda: {'rockland-sample1': 'Rockland Sample 1'})
    DEFAULT_ROCKLAND_SAMPLE1_SELECTION: List[str] = field(default_factory=lambda: ['rockland-sample1'])


    def __post_init__(self):
        self.load_config() # Load config when an instance is created

    def save_config(self):
        config_data = {
            'data_dir': self.DATA_DIR,
            'demographics_file': self.DEMOGRAPHICS_FILE,
            'primary_id_column': self.PRIMARY_ID_COLUMN,
            'session_column': self.SESSION_COLUMN,
            'composite_id_column': self.COMPOSITE_ID_COLUMN,
            'age_column': self.AGE_COLUMN,
            'sex_column': self.SEX_COLUMN,
            'study_site_column': self.STUDY_SITE_COLUMN,
            'default_age_min': self.DEFAULT_AGE_SELECTION[0],
            'default_age_max': self.DEFAULT_AGE_SELECTION[1],
            'max_display_rows': self.MAX_DISPLAY_ROWS
        }
        try:
            with open(self.CONFIG_FILE_PATH, 'w') as f:
                toml.dump(config_data, f)
            logging.info(f"Configuration saved to {self.CONFIG_FILE_PATH}")
        except Exception as e:
            logging.error(f"Error saving configuration: {e}")
            raise # Re-raise for Dash to handle or display

    def load_config(self):
        try:
            with open(self.CONFIG_FILE_PATH) as f:
                config_data = toml.load(f)

            self.DATA_DIR = config_data.get('data_dir', self.DATA_DIR)
            self.DEMOGRAPHICS_FILE = config_data.get('demographics_file', self.DEMOGRAPHICS_FILE)
            self.PRIMARY_ID_COLUMN = config_data.get('primary_id_column', self.PRIMARY_ID_COLUMN)
            self.SESSION_COLUMN = config_data.get('session_column', self.SESSION_COLUMN)
            self.COMPOSITE_ID_COLUMN = config_data.get('composite_id_column', self.COMPOSITE_ID_COLUMN)
            self.AGE_COLUMN = config_data.get('age_column', self.AGE_COLUMN)
            self.SEX_COLUMN = config_data.get('sex_column', self.SEX_COLUMN)
            self.STUDY_SITE_COLUMN = config_data.get('study_site_column', self.STUDY_SITE_COLUMN)

            default_age_min = config_data.get('default_age_min', self.DEFAULT_AGE_SELECTION[0])
            default_age_max = config_data.get('default_age_max', self.DEFAULT_AGE_SELECTION[1])
            self.DEFAULT_AGE_SELECTION = (default_age_min, default_age_max)
            self.MAX_DISPLAY_ROWS = config_data.get('max_display_rows', self.MAX_DISPLAY_ROWS)

            logging.info(f"Configuration loaded from {self.CONFIG_FILE_PATH}")
            self.refresh_merge_detection() # Apply loaded settings to merge strategy

        except FileNotFoundError:
            logging.info(f"{self.CONFIG_FILE_PATH} not found. Creating with default values.")
            self.save_config() # Create with defaults if not found
        except toml.TomlDecodeError as e:
            logging.error(f"Error decoding {self.CONFIG_FILE_PATH}: {e}. Using default configuration.")
            # Potentially re-save defaults or raise error
        except Exception as e:
            logging.error(f"Error loading configuration: {e}. Using default configuration.")
            # Potentially re-save defaults or raise error

    def get_demographics_table_name(self) -> str:
        return Path(self.DEMOGRAPHICS_FILE).stem

    def get_merge_strategy(self) -> FlexibleMergeStrategy:
        if self._merge_strategy is None:
            self._merge_strategy = FlexibleMergeStrategy(
                primary_id_column=self.PRIMARY_ID_COLUMN,
                session_column=self.SESSION_COLUMN,
                composite_id_column=self.COMPOSITE_ID_COLUMN
            )
        return self._merge_strategy

    def get_merge_keys(self) -> MergeKeys:
        if self._merge_keys is None:
            demographics_path = os.path.join(self.DATA_DIR, self.DEMOGRAPHICS_FILE)
            try:
                # Ensure data directory exists before trying to detect structure
                Path(self.DATA_DIR).mkdir(parents=True, exist_ok=True)
                if not os.path.exists(demographics_path):
                    logging.warning(f"Demographics file {demographics_path} not found. Using cross-sectional defaults.")
                    # If demographics file is missing, default to cross-sectional
                    self._merge_keys = MergeKeys(
                        primary_id=self.PRIMARY_ID_COLUMN,
                        session_id=None,
                        composite_id=None,
                        is_longitudinal=False
                    )
                else:
                    self._merge_keys = self.get_merge_strategy().detect_structure(demographics_path)
            except Exception as e:
                logging.error(f"Failed to detect merge keys from {demographics_path}: {e}. Using cross-sectional defaults.")
                # If detection fails, default to cross-sectional to be safe
                self._merge_keys = MergeKeys(
                    primary_id=self.PRIMARY_ID_COLUMN,
                    session_id=None,
                    composite_id=None,
                    is_longitudinal=False
                )
        return self._merge_keys

    def refresh_merge_detection(self) -> None:
        self._merge_keys = None
        self._merge_strategy = None
        # Re-initialize strategy with current config values
        self._merge_strategy = FlexibleMergeStrategy(
            primary_id_column=self.PRIMARY_ID_COLUMN,
            session_column=self.SESSION_COLUMN,
            composite_id_column=self.COMPOSITE_ID_COLUMN
        )
        # Trigger re-detection of merge keys
        self.get_merge_keys()


# --- File Handling Helper Functions ---
def secure_filename(filename: str) -> str:
    """
    Enhanced secure filename function that prevents path traversal and injection attacks.
    """
    # Get basename only, preventing path traversal
    filename = os.path.basename(filename)

    # Remove null bytes and control characters
    filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)

    # Replace whitespace with underscores
    filename = re.sub(r'\s+', '_', filename)

    # Remove path traversal patterns completely
    filename = re.sub(r'\.\.+', '', filename)  # Remove any sequence of dots

    # Remove all non-alphanumeric except safe characters
    filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)

    # Consolidate underscores
    filename = re.sub(r'_+', '_', filename)

    # Strip leading/trailing underscores and dots
    filename = filename.strip('_.')

    # Ensure not empty
    if not filename:
        filename = "safe_file"

    # Ensure reasonable length
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        if ext:
            filename = name[:250] + ext
        else:
            filename = filename[:255]

    return filename

def sanitize_column_names(columns: List[str]) -> Tuple[List[str], Dict[str, str]]:
    """
    Enhanced sanitization of column names to prevent SQL injection and ensure safety.
    
    Args:
        columns: List of original column names
        
    Returns:
        Tuple of (sanitized_column_names, mapping_dict)
        mapping_dict maps original names to sanitized names
    """
    sanitized_columns = []
    column_mapping = {}

    # SQL keywords that should be prefixed to make them safe
    sql_keywords = {
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
        'UNION', 'WHERE', 'FROM', 'JOIN', 'HAVING', 'GROUP', 'ORDER', 'BY',
        'EXEC', 'EXECUTE', 'SCRIPT', 'TRUNCATE', 'MERGE', 'GRANT', 'REVOKE'
    }

    for original_col in columns:
        # Start with string conversion of the original column
        sanitized = str(original_col)

        # Remove null bytes, control characters, and dangerous SQL characters
        sanitized = re.sub(r'[\x00-\x1f\x7f\'"`\;\\]', '', sanitized)

        # Remove SQL comment patterns
        sanitized = re.sub(r'--.*$', '', sanitized)  # Remove -- comments
        sanitized = re.sub(r'/\*.*?\*/', '', sanitized)  # Remove /* */ comments

        # Replace whitespace and problematic characters with underscores
        sanitized = re.sub(r'[\s\-\(\)\[\]\{\}\@\#\$\%\^\&\*\+\=\|\?\<\>\,\.\:\/\\]+', '_', sanitized)

        # Remove any remaining non-alphanumeric characters except underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '', sanitized)

        # Check for and modify SQL keywords (preserve original case)
        words = sanitized.split('_')
        safe_words = []
        for word in words:
            if word.upper() in sql_keywords:
                # Prefix SQL keywords to make them safe, preserving original case
                safe_words.append(f"FIELD_{word}")
            else:
                safe_words.append(word)
        sanitized = '_'.join(safe_words)

        # Consolidate multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)

        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')

        # Ensure column name is not empty
        if not sanitized:
            sanitized = f"col_{len(sanitized_columns)}"

        # Ensure column name doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"

        # Ensure uniqueness
        original_sanitized = sanitized
        counter = 1
        while sanitized in sanitized_columns:
            sanitized = f"{original_sanitized}_{counter}"
            counter += 1

        sanitized_columns.append(sanitized)
        column_mapping[original_col] = sanitized

    return sanitized_columns, column_mapping

def validate_csv_file(file_content: bytes, filename: str, required_columns: Optional[List[str]] = None) -> Tuple[List[str], Optional[pd.DataFrame]]:
    """
    Validate uploaded CSV file content.
    Args:
        file_content: Bytes of the file content.
        filename: Original name of the file.
        required_columns: List of column names that must be present.
    Returns:
        Tuple of (list of error messages, DataFrame or None)
    """
    errors = []
    df = None
    try:
        # File size check (e.g., 50MB limit)
        if len(file_content) > 50 * 1024 * 1024:
            errors.append(f"File '{filename}' too large (maximum 50MB)")

        # File extension check (already handled by dcc.Upload, but good for direct calls)
        if not filename.lower().endswith('.csv'):
            errors.append(f"File '{filename}' must be a CSV (.csv extension)")

        if not errors: # Proceed only if basic checks pass
            # Try to read the CSV from bytes
            from io import BytesIO
            df = pd.read_csv(BytesIO(file_content), low_memory=False)  # temp fix

            if len(df) == 0:
                errors.append(f"File '{filename}' is empty (no data rows)")
            if required_columns:
                missing_cols = set(required_columns) - set(df.columns)
                if missing_cols:
                    errors.append(f"File '{filename}' missing required columns: {', '.join(missing_cols)}")
            if len(df.columns) == 0:
                errors.append(f"File '{filename}' has no columns")
            elif len(df.columns) > 1000: # Arbitrary limit
                errors.append(f"File '{filename}' has too many columns (maximum 1000)")
            if len(df.columns) != len(set(df.columns)):
                duplicates = [col for col in df.columns if list(df.columns).count(col) > 1]
                errors.append(f"File '{filename}' has duplicate column names: {', '.join(set(duplicates))}")

    except pd.errors.EmptyDataError:
        errors.append(f"File '{filename}' is empty or contains no valid CSV data")
    except pd.errors.ParserError as e:
        errors.append(f"Invalid CSV format in '{filename}': {str(e)}")
    except UnicodeDecodeError:
        errors.append(f"File '{filename}' encoding not supported (please use UTF-8)")
    except Exception as e:
        errors.append(f"Error reading file '{filename}': {str(e)}")

    return errors, df if not errors and df is not None else None


@dataclass
class DuplicateFileInfo:
    """Information about a duplicate file conflict."""
    original_filename: str
    safe_filename: str
    existing_path: str
    content: bytes

@dataclass
class FileActionChoice:
    """User's choice for handling a duplicate file."""
    action: str  # 'replace', 'rename', or 'cancel'
    new_filename: Optional[str] = None  # Used when action is 'rename'

def check_for_duplicate_files(file_contents: List[bytes], filenames: List[str], data_dir: str) -> Tuple[List[DuplicateFileInfo], List[int]]:
    """
    Check for duplicate files without saving them.
    Returns:
        Tuple of (list of duplicate file info, list of indices of non-duplicate files)
    """
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    duplicates = []
    non_duplicate_indices = []

    for i, (content, filename) in enumerate(zip(file_contents, filenames)):
        safe_filename = secure_filename(filename)
        file_path = Path(data_dir) / safe_filename

        if file_path.exists():
            duplicates.append(DuplicateFileInfo(
                original_filename=filename,
                safe_filename=safe_filename,
                existing_path=str(file_path),
                content=content
            ))
        else:
            non_duplicate_indices.append(i)

    return duplicates, non_duplicate_indices

def save_uploaded_files_to_data_dir(
    file_contents: List[bytes],
    filenames: List[str],
    data_dir: str,
    duplicate_actions: Optional[Dict[str, FileActionChoice]] = None
) -> Tuple[List[str], List[str]]:
    """
    Save uploaded file contents to the data directory with column name sanitization.
    Args:
        file_contents: List of file contents as bytes.
        filenames: List of original filenames.
        data_dir: Target directory.
        duplicate_actions: Dict mapping original filenames to user's action choices for duplicates.
    Returns:
        Tuple of (list of success messages, list of error messages)
    """
    success_messages = []
    error_messages = []
    Path(data_dir).mkdir(parents=True, exist_ok=True)

    for content, filename in zip(file_contents, filenames):
        safe_filename = secure_filename(filename)
        file_path = Path(data_dir) / safe_filename

        # Handle filename conflicts based on user choice
        if file_path.exists() and duplicate_actions and filename in duplicate_actions:
            action_choice = duplicate_actions[filename]

            if action_choice.action == 'cancel':
                # Skip this file
                continue
            elif action_choice.action == 'replace':
                # Use the existing file path (will overwrite)
                success_messages.append(f"🔄 Replaced existing file '{safe_filename}'")
            elif action_choice.action == 'rename' and action_choice.new_filename:
                # Use the new filename provided by user
                new_safe_filename = secure_filename(action_choice.new_filename)
                file_path = Path(data_dir) / new_safe_filename

                # Check if the new name also conflicts
                if file_path.exists():
                    error_messages.append(f"❌ New filename '{new_safe_filename}' also already exists")
                    continue

                success_messages.append(f"📝 Saved '{filename}' as '{new_safe_filename}'")
        elif file_path.exists():
            # Fallback to old behavior (auto-rename) if no user choice provided
            counter = 1
            original_file_path = file_path
            while file_path.exists():
                base_name, ext = os.path.splitext(original_file_path.name)
                file_path = Path(data_dir) / f"{base_name}_{counter}{ext}"
                counter += 1
            success_messages.append(f"File '{filename}' already exists, saved as '{file_path.name}'")

        try:
            # Read CSV content and sanitize column names
            from io import BytesIO
            df = pd.read_csv(BytesIO(content), low_memory=False)  # temp fix

            # Sanitize column names
            original_columns = df.columns.tolist()
            sanitized_columns, column_mapping = sanitize_column_names(original_columns)

            # Check if any columns were renamed
            renamed_columns = {orig: sanitized for orig, sanitized in column_mapping.items()
                             if orig != sanitized}

            # Apply sanitized column names
            df.columns = sanitized_columns

            # Save the CSV with sanitized column names
            df.to_csv(file_path, index=False)

            # Create success message
            size_msg = f"({len(content):,} bytes)"
            if not any(msg.startswith("🔄") or msg.startswith("📝") for msg in success_messages[-3:]):
                success_messages.append(f"✅ Saved '{filename}' as '{file_path.name}' {size_msg}")

            # Report column renames if any occurred
            if renamed_columns:
                rename_count = len(renamed_columns)
                success_messages.append(f"🔧 Sanitized {rename_count} column name(s) in '{file_path.name}'")
                # Optionally show details for first few renames to avoid overwhelming output
                if rename_count <= 5:
                    for orig, sanitized in list(renamed_columns.items())[:5]:
                        success_messages.append(f"   '{orig}' → '{sanitized}'")
                elif rename_count > 5:
                    # Show first 3 examples
                    for orig, sanitized in list(renamed_columns.items())[:3]:
                        success_messages.append(f"   '{orig}' → '{sanitized}'")
                    success_messages.append(f"   ... and {rename_count - 3} more")

        except Exception as e:
            error_messages.append(f"❌ Failed to save '{filename}': {str(e)}")

    return success_messages, error_messages

# --- Data Analysis Helper Functions ---

def scan_csv_files(data_dir: str) -> Tuple[List[str], List[str]]:
    """Scans directory for CSV files. Returns (list of filenames, list of error messages)."""
    errors = []
    files_found = []
    try:
        Path(data_dir).mkdir(parents=True, exist_ok=True) # Ensure dir exists
        files = os.listdir(data_dir)
        files_found = [f for f in files if f.endswith('.csv')]
    except FileNotFoundError:
        errors.append(f"Error: The data directory was not found at '{data_dir}'.")
    except PermissionError:
        errors.append(f"Error: Permission denied accessing directory '{data_dir}'.")
    except OSError as e:
        errors.append(f"Error accessing directory '{data_dir}': {e}")
    return files_found, errors

def get_table_alias(table_name: str, demo_table_name: str) -> str:
    return 'demo' if table_name == demo_table_name else table_name

def is_numeric_column(dtype_str: str) -> bool:
    return 'int' in dtype_str or 'float' in dtype_str


def has_multisite_data(demographics_columns: List[str], study_site_column: Optional[str] = None) -> bool:
    """
    Checks if the demographics data contains multisite/multistudy information.
    
    Args:
        demographics_columns: List of column names in the demographics file
        study_site_column: User-configured column name for study/site data
    
    Returns:
        True if multisite data is detected, False otherwise
    """
    if study_site_column and study_site_column in demographics_columns:
        return True
    # Backward compatibility: check for legacy 'all_studies' column
    return 'all_studies' in demographics_columns


def detect_rockland_format(demographics_columns: List[str]) -> bool:
    """
    Legacy function for backward compatibility.
    Use has_multisite_data() instead.
    """
    return has_multisite_data(demographics_columns, 'all_studies')


def get_unique_session_values(data_dir: str, merge_keys: MergeKeys) -> Tuple[List[str], List[str]]:
    """Extract unique session values. Returns (session_values, error_messages)."""
    if not merge_keys.is_longitudinal or not merge_keys.session_id:
        return [], []

    unique_sessions = set()
    errors = []
    try:
        csv_files, scan_errors = scan_csv_files(data_dir)
        errors.extend(scan_errors)

        for csv_file in csv_files:
            file_path = os.path.join(data_dir, csv_file)
            try:
                df_sample = pd.read_csv(file_path, nrows=0, low_memory=False)  # temp fix - Read only headers
                if merge_keys.session_id in df_sample.columns:
                    # If session_id is present, read that column
                    df_session_col = pd.read_csv(file_path, usecols=[merge_keys.session_id], low_memory=False)  # temp fix
                    sessions = df_session_col[merge_keys.session_id].dropna().astype(str).unique()
                    unique_sessions.update(sessions)
            except Exception as e:
                errors.append(f"Could not read session values from {csv_file}: {e}")
                continue
    except Exception as e:
        errors.append(f"Error scanning for session values: {e}")
        return [], errors
    return sorted(list(unique_sessions)), errors

# @lru_cache(maxsize=100)  # temp fix - replaced with thread-safe cache
def _get_unique_column_values_cached(file_path: str, file_mtime: float, column_name: str) -> Tuple[List[Any], Optional[str]]:
    """
    Thread-safe cached version of get_unique_column_values.
    Cache is invalidated when file modification time changes.
    """
    cache_key = f"{file_path}:{file_mtime}:{column_name}"
    
    # Check cache first
    with _column_values_cache_lock:
        if cache_key in _column_values_cache:
            return _column_values_cache[cache_key]
    
    # Not in cache, compute value
    if not os.path.exists(file_path):
        return [], f"Error: File not found at {file_path}"

    try:
        # temp fix: coordinate file access with DuckDB operations
        with _file_access_lock:
            df = pd.read_csv(file_path, usecols=[column_name], low_memory=False)  # temp fix
        # Drop NA values, get uniques, convert to list, and sort
        # Convert to string to handle mixed types before sorting, then convert back if possible or keep as string
        unique_values = sorted(list(df[column_name].dropna().astype(str).unique()))
        # Attempt to convert back to numeric if all are numeric, otherwise keep as string
        try:
            # Check if all can be converted to float (includes integers)
            numeric_values = [float(val) for val in unique_values]
            # If original dtype was int-like (no decimals in string representation after float conversion)
            if all(float(val) == int(float(val)) for val in unique_values):
                 unique_values = sorted([int(val) for val in numeric_values]) # Convert to int
            else:
                 unique_values = sorted(numeric_values) # Keep as float
        except ValueError:
            # Not all values are numeric, keep as sorted strings
            pass

        result = (unique_values, None)
    except FileNotFoundError:
        result = ([], f"Error: File not found at {file_path}")
    except ValueError as ve: # Happens if column_name is not in the CSV
        result = ([], f"Error: Column '{column_name}' not found in '{os.path.basename(file_path)}' or file is empty. Details: {ve}")
    except Exception as e:
        result = ([], f"Error reading or processing file '{os.path.basename(file_path)}': {e}")
    
    # Store in cache and return
    with _column_values_cache_lock:
        # Limit cache size to prevent memory growth
        if len(_column_values_cache) >= 100:
            # Remove oldest entry (simple FIFO)
            oldest_key = next(iter(_column_values_cache))
            del _column_values_cache[oldest_key]
        _column_values_cache[cache_key] = result
    
    return result

def get_unique_column_values(data_dir: str, table_name: str, column_name: str, demo_table_name: str, demographics_file_name: str) -> Tuple[List[Any], Optional[str]]:
    """
    Public interface for get_unique_column_values with caching.
    Extracts unique, sorted, non-null values from a specified column in a CSV file.
    Cache is invalidated when file modification time changes.
    Args:
        data_dir: The directory where data files are stored.
        table_name: The name of the table (CSV file name without .csv extension).
        column_name: The name of the column from which to extract unique values.
        demo_table_name: The configured name for the demographics table.
        demographics_file_name: The configured file name for the demographics CSV.
    Returns:
        A tuple containing a list of unique values and an optional error message string.
    """
    actual_file_name = demographics_file_name if table_name == demo_table_name else f"{table_name}.csv"
    file_path = os.path.join(data_dir, actual_file_name)

    # Get file modification time for cache invalidation
    try:
        file_mtime = os.path.getmtime(file_path) if os.path.exists(file_path) else 0.0
    except Exception:
        file_mtime = 0.0

    return _get_unique_column_values_cached(file_path, file_mtime, column_name)

def validate_csv_structure(file_path: str, filename: str, merge_keys: MergeKeys) -> List[str]:
    """Validates basic CSV structure. Returns list of error messages."""
    errors = []
    try:
        df_headers = pd.read_csv(file_path, nrows=0, low_memory=False)  # temp fix
        columns = df_headers.columns.tolist()

        if not columns:
            errors.append(f"Warning: '{filename}' has no columns.")
            return errors

        if not merge_keys.is_longitudinal:
            if merge_keys.primary_id not in columns:
                errors.append(f"Warning: '{filename}' missing required column '{merge_keys.primary_id}'.")
        else:
            has_composite = merge_keys.composite_id and merge_keys.composite_id in columns
            has_primary = merge_keys.primary_id in columns
            if not has_composite and not has_primary:
                errors.append(f"Warning: '{filename}' missing '{merge_keys.primary_id}' or '{merge_keys.composite_id}'.")
    except Exception as e:
        errors.append(f"Error validating '{filename}': {e}")
    return errors


def extract_column_metadata_fast(file_path: str, table_name: str, is_demo_table: bool, merge_keys: MergeKeys, demo_table_name: str) -> Tuple[List[str], Dict[str, str], List[str]]:
    """Extracts columns and dtypes. Returns (columns_list, column_dtypes_dict, error_messages)."""
    errors = []
    columns = []
    column_dtypes = {}
    try:
        df_name = get_table_alias(table_name if not is_demo_table else demo_table_name, demo_table_name)
        # temp fix: coordinate file access with DuckDB operations
        with _file_access_lock:
            df_sample = pd.read_csv(file_path, nrows=100, low_memory=False)  # temp fix - Sample for metadata

        id_columns_to_exclude = {merge_keys.primary_id}
        if merge_keys.session_id: id_columns_to_exclude.add(merge_keys.session_id)
        if merge_keys.composite_id and merge_keys.composite_id in df_sample.columns:
            id_columns_to_exclude.add(merge_keys.composite_id)

        columns = [col for col in df_sample.columns if col not in id_columns_to_exclude]
        for col in df_sample.columns:
            if col in id_columns_to_exclude: continue
            column_dtypes[f"{df_name}.{col}"] = str(df_sample[col].dtype)
    except Exception as e:
        errors.append(f"Error extracting metadata from {Path(file_path).name}: {e}")
    return columns, column_dtypes, errors

def calculate_numeric_ranges_fast(file_path: str, table_name: str, is_demo_table: bool, column_dtypes: Dict[str, str], merge_keys: MergeKeys, demo_table_name: str) -> Tuple[Dict[str, Tuple[float, float]], List[str]]:
    """Calculates min/max for numeric columns. Returns (ranges_dict, error_messages)."""
    errors = []
    column_ranges = {}
    try:
        df_name = get_table_alias(table_name if not is_demo_table else demo_table_name, demo_table_name)

        id_columns_to_exclude = {merge_keys.primary_id}
        if merge_keys.session_id: id_columns_to_exclude.add(merge_keys.session_id)
        if merge_keys.composite_id: id_columns_to_exclude.add(merge_keys.composite_id)

        numeric_cols = []
        for col_key, dtype_str in column_dtypes.items():
            if col_key.startswith(f"{df_name}.") and is_numeric_column(dtype_str):
                col_name = col_key.split('.', 1)[1]
                if col_name not in id_columns_to_exclude:
                    numeric_cols.append(col_name)

        if not numeric_cols: return {}, []

        chunk_iter = pd.read_csv(file_path, chunksize=1000, usecols=numeric_cols, low_memory=False)  # temp fix
        min_vals = {col: float('inf') for col in numeric_cols}
        max_vals = {col: float('-inf') for col in numeric_cols}

        for chunk in chunk_iter:
            for col in numeric_cols:
                if col in chunk.columns:
                    numeric_series = pd.to_numeric(chunk[col], errors='coerce')
                    col_min, col_max = numeric_series.min(), numeric_series.max()
                    if pd.notna(col_min): min_vals[col] = min(min_vals[col], col_min)
                    if pd.notna(col_max): max_vals[col] = max(max_vals[col], col_max)

        for col in numeric_cols:
            if min_vals[col] != float('inf') and max_vals[col] != float('-inf'):
                column_ranges[f"{df_name}.{col}"] = (float(min_vals[col]), float(max_vals[col]))
    except Exception as e:
        errors.append(f"Error calculating numeric ranges for {Path(file_path).name}: {e}")
    return column_ranges, errors


def _get_directory_mtime(directory: str) -> float:
    """Get the latest modification time in a directory."""
    try:
        if not os.path.exists(directory):
            return 0.0

        latest_mtime = os.path.getmtime(directory)
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    file_mtime = os.path.getmtime(file_path)
                    latest_mtime = max(latest_mtime, file_mtime)
        return latest_mtime
    except Exception:
        return time.time()  # Return current time if error

def _get_config_hash(config: Config) -> str:
    """Generate a hash of the configuration for cache invalidation."""
    config_str = f"{config.DATA_DIR}|{config.DEMOGRAPHICS_FILE}|{config.PRIMARY_ID_COLUMN}|{config.SESSION_COLUMN}|{config.COMPOSITE_ID_COLUMN}|{config.AGE_COLUMN}"
    return hashlib.md5(config_str.encode()).hexdigest()

# @lru_cache(maxsize=4)  # temp fix - replaced with thread-safe cache
def _get_table_info_cached(config_hash: str, dir_mtime: float, data_dir: str, demographics_file: str,
                          primary_id: str, session_col: str, composite_id: str, age_col: str) -> Tuple[
    List[str], List[str], Dict[str, List[str]], Dict[str, str],
    Dict[str, Tuple[float, float]], Dict, List[str], List[str], bool, List[str]
]:
    """
    Thread-safe cached version of get_table_info with config parameters as arguments.
    Cache is invalidated when config or directory modification time changes.
    """
    cache_key = f"{config_hash}:{dir_mtime}"
    
    # Check cache first
    with _table_info_cache_lock:
        if cache_key in _table_info_cache:
            return _table_info_cache[cache_key]
    
    # Avoid creating a new Config instance to prevent unnecessary config file reads
    # Instead, pass parameters directly to the optimized implementation
    result = _get_table_info_impl_optimized(data_dir, demographics_file, primary_id, session_col, composite_id, age_col)
    
    # Store in cache and return
    with _table_info_cache_lock:
        # Limit cache size to prevent memory growth
        if len(_table_info_cache) >= 4:
            # Remove oldest entry (simple FIFO)
            oldest_key = next(iter(_table_info_cache))
            del _table_info_cache[oldest_key]
        _table_info_cache[cache_key] = result
    
    return result

def _get_table_info_impl(config: Config) -> Tuple[
    List[str], List[str], Dict[str, List[str]], Dict[str, str],
    Dict[str, Tuple[float, float]], Dict, List[str], List[str], bool, List[str]
]:
    """
    Internal implementation of get_table_info.
    Scans data directory for CSVs and returns info.
    Returns: behavioral_tables, demographics_columns, behavioral_columns_by_table,
             column_dtypes, column_ranges, merge_keys_dict, actions_taken,
             session_values, is_empty_state, all_messages (errors/warnings)
    """
    all_messages = []

    # Ensure data directory exists, create if not
    try:
        Path(config.DATA_DIR).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        all_messages.append(f"Error creating data directory {config.DATA_DIR}: {e}")
        # Return empty state if data directory cannot be accessed/created
        return [], [], {}, {}, {}, {}, [], [], True, all_messages

    merge_keys = config.get_merge_keys() # This now handles missing demo file by returning defaults

    actions_taken = []
    if merge_keys.is_longitudinal:
        success, prep_actions = config.get_merge_strategy().prepare_datasets(config.DATA_DIR, merge_keys)
        actions_taken.extend(prep_actions)
        if not success:
            all_messages.append("Failed to prepare longitudinal datasets.")

    behavioral_tables: List[str] = []
    demographics_columns: List[str] = []
    behavioral_columns_by_table: Dict[str, List[str]] = {}
    column_dtypes: Dict[str, str] = {}
    column_ranges: Dict[str, Tuple[float, float]] = {}

    all_csv_files, scan_errors = scan_csv_files(config.DATA_DIR)
    all_messages.extend(scan_errors)

    is_empty_state = not all_csv_files
    if is_empty_state:
        all_messages.append("No CSV files found in the data directory.")
        # Return default merge_keys if empty, as get_merge_keys() would have provided them
        return [], [], {}, {}, {}, merge_keys.to_dict(), [], [], True, all_messages

    demo_table_name = config.get_demographics_table_name()

    for f_name in all_csv_files:
        table_name = Path(f_name).stem
        is_demo_table = (f_name == config.DEMOGRAPHICS_FILE)
        if not is_demo_table:
            behavioral_tables.append(table_name)

        table_path = os.path.join(config.DATA_DIR, f_name)

        val_errors = validate_csv_structure(table_path, f_name, merge_keys)
        if val_errors:
            all_messages.extend(val_errors)
            continue # Skip processing this file if structure is invalid

        try:
            cols, dtypes, meta_errors = extract_column_metadata_fast(table_path, table_name, is_demo_table, merge_keys, demo_table_name)
            all_messages.extend(meta_errors)
            column_dtypes.update(dtypes)

            if is_demo_table:
                # For demographics, get all columns directly from a sample read for full list
                df_sample_demo = pd.read_csv(table_path, nrows=0, low_memory=False)  # temp fix
                demographics_columns = df_sample_demo.columns.tolist()
                # Basic validation for essential demo columns (can be expanded)
                if config.AGE_COLUMN not in demographics_columns:
                    all_messages.append(f"Info: '{config.AGE_COLUMN}' column not found in {f_name}. Age filtering will be affected.")
            else:
                behavioral_columns_by_table[table_name] = cols

            ranges, range_errors = calculate_numeric_ranges_fast(table_path, table_name, is_demo_table, dtypes, merge_keys, demo_table_name)
            all_messages.extend(range_errors)
            column_ranges.update(ranges)

        except Exception as e: # Catch-all for other processing errors for this file
            all_messages.append(f"Unexpected error processing file {f_name}: {e}")
            continue

    session_values, sess_errors = get_unique_session_values(config.DATA_DIR, merge_keys)
    all_messages.extend(sess_errors)

    return (behavioral_tables, demographics_columns, behavioral_columns_by_table,
            column_dtypes, column_ranges, merge_keys.to_dict(), actions_taken,
            session_values, False, all_messages)

def _get_table_info_impl_optimized(data_dir: str, demographics_file: str, primary_id: str, 
                                 session_col: str, composite_id: str, age_col: str) -> Tuple[
    List[str], List[str], Dict[str, List[str]], Dict[str, str],
    Dict[str, Tuple[float, float]], Dict, List[str], List[str], bool, List[str]
]:
    """
    Optimized implementation that doesn't create a Config instance.
    This avoids reading the config.toml file during cached operations.
    """
    all_messages = []

    # Ensure data directory exists, create if not
    try:
        Path(data_dir).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        all_messages.append(f"Error creating data directory {data_dir}: {e}")
        return [], [], {}, {}, {}, {}, [], [], True, all_messages

    # Create merge keys directly without full Config instance
    try:
        demographics_path = os.path.join(data_dir, demographics_file)
        merge_keys = _detect_merge_structure_direct(demographics_path, primary_id, session_col, composite_id)
    except Exception as e:
        all_messages.append(f"Error detecting merge structure: {e}")
        # Fallback to cross-sectional with provided primary_id
        merge_keys = MergeKeys(primary_id=primary_id, is_longitudinal=False)

    actions_taken = []
    if merge_keys.is_longitudinal:
        # Create minimal merge strategy without full Config
        merge_strategy = FlexibleMergeStrategy()
        merge_strategy.primary_id_column = primary_id
        merge_strategy.session_column = session_col
        merge_strategy.composite_id_column = composite_id
        
        success, prep_actions = merge_strategy.prepare_datasets(data_dir, merge_keys)
        actions_taken.extend(prep_actions)
        if not success:
            all_messages.append("Failed to prepare longitudinal datasets.")

    behavioral_tables: List[str] = []
    demographics_columns: List[str] = []
    behavioral_columns_by_table: Dict[str, List[str]] = {}
    column_dtypes: Dict[str, str] = {}
    column_ranges: Dict[str, Tuple[float, float]] = {}

    all_csv_files, scan_errors = scan_csv_files(data_dir)
    all_messages.extend(scan_errors)

    is_empty_state = not all_csv_files
    if is_empty_state:
        all_messages.append("No CSV files found in the data directory.")
        return [], [], {}, {}, {}, merge_keys.to_dict(), [], [], True, all_messages

    demo_table_name = Path(demographics_file).stem

    for f_name in all_csv_files:
        table_name = Path(f_name).stem
        is_demo_table = (f_name == demographics_file)
        if not is_demo_table:
            behavioral_tables.append(table_name)

        table_path = os.path.join(data_dir, f_name)

        val_errors = validate_csv_structure(table_path, f_name, merge_keys)
        if val_errors:
            all_messages.extend(val_errors)
            continue

        try:
            cols, dtypes, meta_errors = extract_column_metadata_fast(table_path, table_name, is_demo_table, merge_keys, demo_table_name)
            all_messages.extend(meta_errors)
            column_dtypes.update(dtypes)

            if is_demo_table:
                df_sample_demo = pd.read_csv(table_path, nrows=0, low_memory=False)
                demographics_columns = df_sample_demo.columns.tolist()
                if age_col not in demographics_columns:
                    all_messages.append(f"Info: '{age_col}' column not found in {f_name}. Age filtering will be affected.")
            else:
                behavioral_columns_by_table[table_name] = cols

            ranges, range_errors = calculate_numeric_ranges_fast(table_path, table_name, is_demo_table, dtypes, merge_keys, demo_table_name)
            all_messages.extend(range_errors)
            column_ranges.update(ranges)

        except Exception as e:
            all_messages.append(f"Unexpected error processing file {f_name}: {e}")
            continue

    session_values, sess_errors = get_unique_session_values(data_dir, merge_keys)
    all_messages.extend(sess_errors)

    return (behavioral_tables, demographics_columns, behavioral_columns_by_table,
            column_dtypes, column_ranges, merge_keys.to_dict(), actions_taken,
            session_values, False, all_messages)

def _detect_merge_structure_direct(demographics_path: str, primary_id: str, session_col: str, composite_id: str) -> MergeKeys:
    """
    Direct merge structure detection without creating a Config instance.
    """
    try:
        if not os.path.exists(demographics_path):
            return MergeKeys(primary_id=primary_id, is_longitudinal=False)
            
        with _file_access_lock:
            df_sample = pd.read_csv(demographics_path, nrows=5, low_memory=False)
        columns = set(df_sample.columns)
        
        has_primary_id = primary_id in columns
        has_session_id = session_col in columns
        has_composite_id = composite_id in columns

        if has_primary_id and has_session_id:
            return MergeKeys(primary_id=primary_id, session_id=session_col, 
                           composite_id=composite_id, is_longitudinal=True)
        elif has_primary_id:
            return MergeKeys(primary_id=primary_id, is_longitudinal=False)
        elif has_composite_id:
            return MergeKeys(primary_id=composite_id, is_longitudinal=False)
        else:
            id_candidates = [col for col in columns if 'id' in col.lower() or 'ursi' in col.lower()]
            if id_candidates:
                return MergeKeys(primary_id=id_candidates[0], is_longitudinal=False)
            else:
                raise ValueError(f"No suitable ID column found in {demographics_path}")
    except Exception as e:
        logging.error(f"Error detecting merge structure: {e}")
        return MergeKeys(primary_id=primary_id, is_longitudinal=False)

def get_table_info(config: Config) -> Tuple[
    List[str], List[str], Dict[str, List[str]], Dict[str, str],
    Dict[str, Tuple[float, float]], Dict, List[str], List[str], bool, List[str]
]:
    """
    Public interface for get_table_info with caching.
    Cache is invalidated when configuration or data directory changes.
    """
    config_hash = _get_config_hash(config)
    dir_mtime = _get_directory_mtime(config.DATA_DIR)

    return _get_table_info_cached(
        config_hash, dir_mtime, config.DATA_DIR, config.DEMOGRAPHICS_FILE,
        config.PRIMARY_ID_COLUMN, config.SESSION_COLUMN,
        config.COMPOSITE_ID_COLUMN, config.AGE_COLUMN
    )

# Example of how Config might be instantiated and used globally if needed
# config_instance = Config()
# config_instance.load_config() # Load or create config.toml
# Then pass config_instance to functions needing it, or access its members.
# For Dash, it's often better to create and manage config within callbacks or app setup.


# --- Query Generation Logic ---

def generate_base_query_logic_secure(
    config: Config,
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str]
) -> Tuple[str, List[Any]]:
    """
    SECURE version: Generates the common FROM, JOIN, and WHERE clauses for all queries.
    Uses parameterized queries and input validation to prevent SQL injection.
    """
    # Use instance-specific values from the passed config object
    demographics_table_name = config.get_demographics_table_name()

    if not tables_to_join:
        tables_to_join = [demographics_table_name]

    # Get allowed tables by scanning the data directory safely
    allowed_tables = set()
    try:
        csv_files = [f[:-4] for f in os.listdir(config.DATA_DIR) if f.endswith('.csv')]
        allowed_tables = set(csv_files)
    except Exception as e:
        logging.warning(f"Could not scan data directory for allowed tables: {e}")
        allowed_tables = {demographics_table_name}

    # Validate and sanitize table names
    safe_tables_to_join = []
    for table in tables_to_join:
        safe_table = validate_table_name(table, allowed_tables)
        if safe_table:
            safe_tables_to_join.append(safe_table)
        else:
            logging.warning(f"Invalid or unauthorized table name rejected: {table}")

    if not safe_tables_to_join:
        safe_tables_to_join = [demographics_table_name]

    # Safely construct base table path
    base_table_path = os.path.join(config.DATA_DIR, config.DEMOGRAPHICS_FILE).replace('\\', '/')
    from_join_clause = f"FROM read_csv_auto('{base_table_path}') AS demo"

    # Collect all tables that need to be joined (including from behavioral filters)
    all_join_tables: set[str] = set(safe_tables_to_join)
    for bf in behavioral_filters:
        if bf.get('table'):
            safe_table = validate_table_name(bf['table'], allowed_tables)
            if safe_table:
                all_join_tables.add(safe_table)
            else:
                logging.warning(f"Invalid table name in behavioral filter rejected: {bf.get('table')}")

    # Sanitize merge column name
    safe_merge_column = sanitize_sql_identifier(merge_keys.get_merge_column())

    # Build secure JOIN clauses
    for table in all_join_tables:
        if table == demographics_table_name:
            continue

        # Double-validate table name
        safe_table = sanitize_sql_identifier(table)
        table_path = os.path.join(config.DATA_DIR, f"{safe_table}.csv").replace('\\', '/')

        from_join_clause += f"""
        LEFT JOIN read_csv_auto('{table_path}') AS {safe_table}
        ON demo."{safe_merge_column}" = {safe_table}."{safe_merge_column}" """

    where_clauses: List[str] = []
    params: List[Any] = []  # Use list for ordered parameters instead of dict

    # 1. Demographic Filters - Read available columns securely
    demographics_path = os.path.join(config.DATA_DIR, config.DEMOGRAPHICS_FILE)
    available_demo_columns = []
    try:
        df_headers = pd.read_csv(demographics_path, nrows=0, low_memory=False)  # temp fix
        available_demo_columns = df_headers.columns.tolist()
    except Exception as e:
        logging.warning(f"Could not read demographics file headers from {demographics_path}: {e}")
        available_demo_columns = []

    # Age filtering with sanitized column name
    if demographic_filters.get('age_range'):
        safe_age_column = sanitize_sql_identifier(config.AGE_COLUMN)
        if config.AGE_COLUMN in available_demo_columns:
            where_clauses.append(f"demo.\"{safe_age_column}\" BETWEEN ? AND ?")
            params.extend([demographic_filters['age_range'][0], demographic_filters['age_range'][1]])
        else:
            logging.warning(f"Age filtering requested but '{config.AGE_COLUMN}' column not found in demographics file")

    # Multisite/Multistudy Filters with sanitized column names
    if demographic_filters.get('substudies'):
        study_site_column = config.STUDY_SITE_COLUMN if config.STUDY_SITE_COLUMN else 'all_studies'
        safe_study_column = sanitize_sql_identifier(study_site_column)

        if study_site_column in available_demo_columns:
            substudy_conditions = []
            for substudy in demographic_filters['substudies']:
                # Sanitize substudy value and use parameterized query
                substudy_conditions.append(f"demo.\"{safe_study_column}\" LIKE ?")
                params.append(f'%{substudy}%')
            if substudy_conditions:
                where_clauses.append(f"({' OR '.join(substudy_conditions)})")
        else:
            logging.info(f"Skipping substudy filters: '{study_site_column}' column not found in demographics file")

    # Session filtering with sanitized column names
    if demographic_filters.get('sessions') and merge_keys.session_id:
        safe_session_column = sanitize_sql_identifier(merge_keys.session_id)
        session_conditions = []
        for session in demographic_filters['sessions']:
            session_conditions.append(f"demo.\"{safe_session_column}\" = ?")
            params.append(session)
        if session_conditions:
            where_clauses.append(f"({' OR '.join(session_conditions)})")

    # 2. Behavioral Filters with validation
    for bf in behavioral_filters:
        safe_table = validate_table_name(bf.get('table', ''), allowed_tables)
        safe_column = sanitize_sql_identifier(bf.get('column', ''))

        if not safe_table or not safe_column:
            logging.warning(f"Invalid behavioral filter rejected: table={bf.get('table')}, column={bf.get('column')}")
            continue

        filter_type = bf.get('type', 'range')
        if filter_type == 'range' and 'range' in bf:
            where_clauses.append(f"{safe_table}.\"{safe_column}\" BETWEEN ? AND ?")
            params.extend([bf['range'][0], bf['range'][1]])
        elif filter_type == 'categorical' and 'selected_values' in bf:
            if bf['selected_values']:
                placeholders = ', '.join(['?' for _ in bf['selected_values']])
                where_clauses.append(f"{safe_table}.\"{safe_column}\" IN ({placeholders})")
                params.extend(bf['selected_values'])

    # Combine clauses
    where_clause = ""
    if where_clauses:
        where_clause = f"WHERE {' AND '.join(where_clauses)}"

    full_query = f"{from_join_clause}\n{where_clause}"

    return full_query, params


def generate_base_query_logic(
    config: Config,
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str]
) -> Tuple[str, List[Any]]:
    """
    LEGACY version: Generates the common FROM, JOIN, and WHERE clauses for all queries.
    WARNING: This function contains SQL injection vulnerabilities and should be replaced
    with generate_base_query_logic_secure().
    """
    # Use instance-specific values from the passed config object
    demographics_table_name = config.get_demographics_table_name()

    if not tables_to_join:
        tables_to_join = [demographics_table_name]

    # If session filtering is active, ensure we have behavioral tables.
    if demographic_filters.get('sessions'):
        behavioral_tables_present = any(table != demographics_table_name for table in tables_to_join)
        if not behavioral_tables_present:
            # Try to add a behavioral table if only demographics is present and session filter is active
            # This logic might need refinement based on actual available tables stored elsewhere
            # For now, this is a simplified version.
            # In a Dash app, available tables would come from a store.
            # We'll assume 'scan_csv_files' can be used for this, though it's an FS call.
            # Ideally, table list comes from a more direct source like available_tables_store.

            # This part is tricky as utils.py shouldn't ideally depend on app state.
            # For now, we will assume 'tables_to_join' is comprehensive enough or
            # this logic is handled before calling this function.
            # If not, this function might not behave as expected if only demo table is passed
            # and session filters are applied.
            pass # Placeholder for now, as direct FS scan here is not ideal.

    base_table_path = os.path.join(config.DATA_DIR, config.DEMOGRAPHICS_FILE).replace('\\', '/')
    from_join_clause = f"FROM read_csv_auto('{base_table_path}') AS demo"

    all_join_tables: set[str] = set(tables_to_join)
    for bf in behavioral_filters:
        if bf.get('table'):
            all_join_tables.add(bf['table'])

    for table in all_join_tables:
        if table == demographics_table_name:
            continue
        table_path = os.path.join(config.DATA_DIR, f"{table}.csv").replace('\\', '/')
        # Ensure merge_column is correctly determined using the MergeKeys object
        merge_column = merge_keys.get_merge_column()
        from_join_clause += f"""
        LEFT JOIN read_csv_auto('{table_path}') AS {table}
        ON demo."{merge_column}" = {table}."{merge_column}" """
        # Quoted merge_column in case it contains special characters or spaces, though ideally it shouldn't.

    where_clauses: List[str] = []
    params: Dict[str, Any] = {}

    # 1. Demographic Filters
    # Check what columns are available in demographics file
    demographics_path = os.path.join(config.DATA_DIR, config.DEMOGRAPHICS_FILE)
    available_demo_columns = []
    try:
        df_headers = pd.read_csv(demographics_path, nrows=0, low_memory=False)  # temp fix
        available_demo_columns = df_headers.columns.tolist()
    except Exception as e:
        logging.warning(f"Could not read demographics file headers from {demographics_path}: {e}")
        available_demo_columns = []  # Fallback to empty list

    # Age filtering (only if age column exists)
    if demographic_filters.get('age_range') and config.AGE_COLUMN in available_demo_columns:
        where_clauses.append(f"demo.{config.AGE_COLUMN} BETWEEN ? AND ?")
        params['age_min'] = demographic_filters['age_range'][0]
        params['age_max'] = demographic_filters['age_range'][1]
    elif demographic_filters.get('age_range') and config.AGE_COLUMN not in available_demo_columns:
        logging.warning(f"Age filtering requested but '{config.AGE_COLUMN}' column not found in demographics file")


    # Multisite/Multistudy Filters (uses configured study/site column or fallback to 'all_studies')
    if demographic_filters.get('substudies'):
        study_site_column = config.STUDY_SITE_COLUMN if config.STUDY_SITE_COLUMN else 'all_studies'
        if study_site_column in available_demo_columns:
            substudy_conditions = []
            for substudy in demographic_filters['substudies']:
                substudy_conditions.append(f"demo.\"{study_site_column}\" LIKE ?")
                params[f'substudy_{substudy}'] = f'%{substudy}%'
            if substudy_conditions:
                where_clauses.append(f"({' OR '.join(substudy_conditions)})")
        else:
            logging.info(f"Skipping substudy filters: '{study_site_column}' column not found in demographics file")

    # Session Filters
    if demographic_filters.get('sessions') and merge_keys.session_id:
        session_conditions = []
        session_values = demographic_filters['sessions']
        session_placeholders = ', '.join(['?' for _ in session_values])

        # Iterate through tables that are known to potentially have session info
        # This should ideally be based on metadata (e.g., if table has session_id column)
        for table_alias_for_session in all_join_tables:
            # We need to know if this table *has* the session_id column.
            # This is a simplification; ideally, we'd check column_dtypes or similar metadata.
            # For now, assume any table *might* have it if it's longitudinal.

            # Use proper table alias: 'demo' for demographics table, table name for others
            table_alias = 'demo' if table_alias_for_session == demographics_table_name else table_alias_for_session
            session_conditions.append(f"{table_alias}.\"{merge_keys.session_id}\" IN ({session_placeholders})")

        if session_conditions:
            # This creates a complex OR condition if multiple tables have session_id.
            # It might be intended that session filter applies to *any* table having that session.
            where_clauses.append(f"({' OR '.join(session_conditions)})")
            # Add session parameters for each condition - each condition needs its own set of parameters
            for _ in session_conditions:
                for session_val in session_values:
                    params[f'session_{len(params)}'] = session_val

    # 2. Behavioral Filters
    # Convert params dict to a list for query execution
    params_list: List[Any] = list(params.values())

    for i, b_filter in enumerate(behavioral_filters):
        if b_filter.get('table') and b_filter.get('column'):
            df_name = get_table_alias(b_filter['table'], demographics_table_name)
            col_name = f'"{b_filter["column"]}"'  # Quote column name

            if b_filter.get('filter_type') == 'numeric' and b_filter.get('min_val') is not None and b_filter.get('max_val') is not None:
                where_clauses.append(f"{df_name}.{col_name} BETWEEN ? AND ?")
                params_list.append(b_filter['min_val'])
                params_list.append(b_filter['max_val'])
            elif b_filter.get('filter_type') == 'categorical' and b_filter.get('selected_values'):
                selected_cat_values = b_filter['selected_values']
                if selected_cat_values:  # Ensure list is not empty
                    placeholders = ', '.join(['?' for _ in selected_cat_values])
                    where_clauses.append(f"{df_name}.{col_name} IN ({placeholders})")
                    params_list.extend(selected_cat_values)

    where_clause_str = ""
    if where_clauses:
        where_clause_str = "\nWHERE " + " AND ".join(where_clauses)

    return f"{from_join_clause}{where_clause_str}", params_list


def generate_data_query_secure(
    base_query_logic: str,
    params: List[Any],
    selected_tables: List[str],
    selected_columns: Dict[str, List[str]],
    allowed_tables: set[str],
    # config: Config, # Not strictly needed if demo table name is handled by base_query
    # merge_keys: MergeKeys # Not strictly needed here if demo.* is always selected
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """SECURE version: Generates the full SQL query to fetch data with input validation."""
    if not base_query_logic:
        return None, None

    # Always select all columns from the demographics table (aliased as 'demo')
    select_clause = "SELECT demo.*"

    # Add selected columns from other tables with validation
    for table, columns in selected_columns.items():
        # Validate table name
        safe_table = validate_table_name(table, allowed_tables)
        if not safe_table:
            logging.warning(f"Invalid table name rejected in data query: {table}")
            continue

        # Ensure table was intended to be joined
        if safe_table in selected_tables and columns:
            for col in columns:
                # Sanitize column name
                safe_col = sanitize_sql_identifier(col)
                if safe_col:
                    # Columns from non-demographic tables are selected as table_name."column_name"
                    select_clause += f', {safe_table}."{safe_col}"'
                else:
                    logging.warning(f"Invalid column name rejected in data query: {col}")

    return f"{select_clause} {base_query_logic}", params


def generate_secure_query_suite(
    config: Config,
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str],
    selected_columns: Dict[str, List[str]] = None
) -> Tuple[str, str, List[Any]]:
    """
    Secure all-in-one query generation function.
    
    Returns:
        Tuple of (data_query, count_query, params)
    """
    # Get allowed tables from data directory
    allowed_tables = set()
    try:
        csv_files = [f[:-4] for f in os.listdir(config.DATA_DIR) if f.endswith('.csv')]
        allowed_tables = set(csv_files)
    except Exception as e:
        logging.warning(f"Could not scan data directory for allowed tables: {e}")
        allowed_tables = {config.get_demographics_table_name()}

    # Generate secure base query
    base_query, params = generate_base_query_logic_secure(
        config=config,
        merge_keys=merge_keys,
        demographic_filters=demographic_filters,
        behavioral_filters=behavioral_filters,
        tables_to_join=tables_to_join
    )

    # Generate secure count query
    count_query, count_params = generate_count_query(base_query, params, merge_keys)

    # Generate secure data query
    if selected_columns is None:
        selected_columns = {}

    data_query, data_params = generate_data_query_secure(
        base_query_logic=base_query,
        params=params,
        selected_tables=tables_to_join,
        selected_columns=selected_columns,
        allowed_tables=allowed_tables
    )

    return data_query, count_query, params


def generate_data_query(
    base_query_logic: str,
    params: List[Any],
    selected_tables: List[str],
    selected_columns: Dict[str, List[str]],
    # config: Config, # Not strictly needed if demo table name is handled by base_query
    # merge_keys: MergeKeys # Not strictly needed here if demo.* is always selected
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """LEGACY version: Generates the full SQL query to fetch data.
    WARNING: Contains SQL injection vulnerabilities. Use generate_data_query_secure() instead."""
    if not base_query_logic:
        return None, None

    # Always select all columns from the demographics table (aliased as 'demo')
    select_clause = "SELECT demo.*"

    # Add selected columns from other tables
    for table, columns in selected_columns.items():
        # We assume 'table' here is the actual table name (not alias 'demo')
        if table in selected_tables and columns: # Ensure table was intended to be joined
            for col in columns:
                # Columns from non-demographic tables are selected as table_name."column_name"
                select_clause += f', {table}."{col}"'

    return f"{select_clause} {base_query_logic}", params


def generate_count_query(
    base_query_logic: str,
    params: List[Any],
    merge_keys: MergeKeys
    # config: Config # Not needed if demo table alias is fixed in base_query
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """Generates a query to count distinct participants."""
    if not base_query_logic:
        return None, None

    # Use the merge column from the 'demo' aliased demographics table
    merge_column = merge_keys.get_merge_column()
    select_clause = f'SELECT COUNT(DISTINCT demo."{merge_column}")'

    return f"{select_clause} {base_query_logic}", params

def consolidate_baseline_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolidates multiple baseline columns (BAS1, BAS2, BAS3) into single BAS columns.
    
    For columns like 'variable_BAS1', 'variable_BAS2', 'variable_BAS3',
    creates 'variable_BAS' taking the value from the highest numbered BAS session.
    
    Args:
        df: DataFrame with potentially multiple BAS columns
        
    Returns:
        DataFrame with consolidated baseline columns
    """
    # Find all columns that end with _BAS1, _BAS2, or _BAS3
    bas_pattern = r'^(.+)_BAS([123])$'
    bas_columns = {}

    for col in df.columns:
        match = re.match(bas_pattern, col)
        if match:
            variable_name = match.group(1)  # e.g., 'ant_01'
            bas_number = int(match.group(2))  # e.g., 1, 2, or 3

            if variable_name not in bas_columns:
                bas_columns[variable_name] = {}
            bas_columns[variable_name][bas_number] = col

    # Only proceed if we have variables with multiple BAS sessions
    variables_to_consolidate = {
        var: sessions for var, sessions in bas_columns.items()
        if len(sessions) > 1  # Only consolidate if multiple BAS sessions exist
    }

    if not variables_to_consolidate:
        logging.info("No multiple baseline sessions found for consolidation.")
        return df

    logging.info(f"Consolidating baseline columns for {len(variables_to_consolidate)} variables.")

    # Create a copy of the dataframe to work with
    result_df = df.copy()

    for variable_name, sessions in variables_to_consolidate.items():
        # Create the consolidated column name
        consolidated_col = f"{variable_name}_BAS"

        # Get the session numbers in ascending order (BAS1, BAS2, BAS3)
        # We'll process them in ascending order so higher numbers overwrite lower ones
        session_numbers = sorted(sessions.keys())

        # Start with NaN values
        consolidated_values = pd.Series([None] * len(result_df), dtype='object')

        # Process in ascending order so higher BAS numbers overwrite lower ones
        for session_num in session_numbers:
            source_col = sessions[session_num]
            # Update consolidated values where source has non-null values
            # Higher numbered sessions will overwrite values from lower numbered sessions
            mask = result_df[source_col].notna()
            consolidated_values.loc[mask] = result_df.loc[mask, source_col]

        # Add the consolidated column
        result_df[consolidated_col] = consolidated_values

        # Remove the original BAS1, BAS2, BAS3 columns
        for session_num in sessions:
            result_df = result_df.drop(columns=[sessions[session_num]])

        logging.info(f"Consolidated {variable_name}: {list(sessions.values())} → {consolidated_col}")

    return result_df


def enwiden_longitudinal_data(
    df: pd.DataFrame,
    merge_keys: MergeKeys,
    consolidate_baseline: bool = True
    # selected_columns_per_table: Dict[str, List[str]] # This might not be strictly needed if df already has the right columns
) -> pd.DataFrame:
    """
    Pivots longitudinal data so each subject has one row with session-specific columns.
    Transforms columns like 'age' into 'age_BAS1', 'age_BAS2', etc.
    
    For data with multiple baseline sessions (BAS1, BAS2, BAS3), consolidates them
    into single BAS columns taking the value from the highest numbered session.
    """
    if not merge_keys.is_longitudinal or not merge_keys.session_id or merge_keys.session_id not in df.columns:
        logging.info("Data is not longitudinal or session_id is missing; enwidening not applied.")
        return df

    if merge_keys.primary_id not in df.columns:
        logging.error(f"Primary ID column '{merge_keys.primary_id}' not found in DataFrame for enwidening.")
        raise ValueError(f"Primary ID column '{merge_keys.primary_id}' not found for enwidening.")

    # Identify columns to pivot (exclude ID columns)
    id_columns_to_exclude = {merge_keys.primary_id}
    if merge_keys.composite_id and merge_keys.composite_id in df.columns:
        id_columns_to_exclude.add(merge_keys.composite_id)
    # session_id is used for pivoting, so it's implicitly handled, but good to be explicit.
    id_columns_to_exclude.add(merge_keys.session_id)


    # Columns that should be pivoted are all columns NOT in id_columns_to_exclude
    pivot_columns = [col for col in df.columns if col not in id_columns_to_exclude]

    if not pivot_columns:
        logging.info("No columns found to pivot for enwidening.")
        return df.drop_duplicates(subset=[merge_keys.primary_id])


    # Determine static columns (those that don't vary by session for any given primary_id)
    static_columns = []
    # Temporarily set index to primary_id and session_id to check for static nature
    # This requires primary_id and session_id to be present

    # Check if session_id and primary_id are in df columns
    if merge_keys.session_id not in df.columns or merge_keys.primary_id not in df.columns:
        logging.error("session_id or primary_id missing, cannot determine static columns accurately.")
        # Proceed by assuming all pivot_columns are dynamic, or handle as error
        # For now, treat all as dynamic if this happens.
        pass # dynamic_columns will be all pivot_columns
    else:
        try:
            # Check for sufficient data to perform groupby meaningfully
            if not df.empty and len(df) > 1:
                 # Count unique values per primary_id for each pivot_column, after dropping NaNs within groups
                for col in pivot_columns:
                    if col in df.columns:
                        # Group by primary_id and check if the column has only one unique value (or all NaNs) per subject
                        is_static = df.groupby(merge_keys.primary_id)[col].nunique(dropna=True).max(skipna=True) <= 1
                        if is_static:
                            static_columns.append(col)
            else: # Not enough data to determine, assume all dynamic
                pass

        except Exception as e:
            logging.warning(f"Could not accurately determine static columns due to: {e}. Assuming all pivot columns are dynamic.")
            static_columns = [] # Fallback: treat all as dynamic


    dynamic_columns = [col for col in pivot_columns if col not in static_columns]

    # Start with base (primary_id and static columns)
    # Ensure we handle cases where static_columns might be empty
    if static_columns:
        # Take the first non-NA value for static columns per primary_id
        static_df_grouped = df.groupby(merge_keys.primary_id)[static_columns].first()
        base_df = static_df_grouped.reset_index()
    else:
        # If no static columns, base_df is just unique primary_ids
        base_df = df[[merge_keys.primary_id]].drop_duplicates().reset_index(drop=True)

    if not dynamic_columns:
        logging.info("No dynamic columns to pivot. Returning base data with static columns.")
        return base_df

    # Pivot dynamic columns
    try:
        pivoted_df = df.pivot_table(
            index=merge_keys.primary_id,
            columns=merge_keys.session_id,
            values=dynamic_columns,
            aggfunc='first' # Use 'first' to take the first available value if multiple exist for same subject-session
        )
    except Exception as e:
        logging.error(f"Error during pivot_table: {e}")
        # This can happen if, for example, there are duplicate entries for a subject-session combination
        # that pandas cannot resolve into the pivot structure with 'first'.
        # Attempt to resolve by dropping duplicates before pivot
        logging.info("Attempting to resolve pivot error by dropping duplicates based on primary_id, session_id, and dynamic_columns.")
        df_deduplicated = df.drop_duplicates(subset=[merge_keys.primary_id, merge_keys.session_id] + dynamic_columns)
        try:
            pivoted_df = df_deduplicated.pivot_table(
                index=merge_keys.primary_id,
                columns=merge_keys.session_id,
                values=dynamic_columns,
                aggfunc='first'
            )
        except Exception as e_after_dedup:
            logging.error(f"Error during pivot_table even after deduplication: {e_after_dedup}")
            raise ValueError(f"Failed to pivot data: {e_after_dedup}")


    # Flatten MultiIndex columns: from (value, session) to value_session
    pivoted_df.columns = [f"{val_col}_{ses_col}" for val_col, ses_col in pivoted_df.columns]
    pivoted_df = pivoted_df.reset_index()

    # Merge static data with pivoted dynamic data
    if base_df.empty: # Should not happen if df was not empty
        final_df = pivoted_df
    else:
        final_df = pd.merge(base_df, pivoted_df, on=merge_keys.primary_id, how='left')

    # Apply baseline consolidation if multiple BAS sessions exist (optional)
    if consolidate_baseline:
        final_df = consolidate_baseline_columns(final_df)

    return final_df


def generate_export_filename(
    selected_tables: List[str],
    demographics_table_name: str,
    is_enwidened: bool = False
) -> str:
    """
    Generate a smart filename for CSV export based on selected tables and options.
    
    Format: [table1name]_[table2name]_...[enwidened]_yymmdd_hhmmss.csv
    
    Args:
        selected_tables: List of table names included in the export
        demographics_table_name: Name of the demographics table
        is_enwidened: Whether the data was enwidened
        
    Returns:
        Generated filename string
    """
    # Remove demographics table from the list since it's always included
    behavioral_tables = [table for table in selected_tables if table != demographics_table_name]

    # Start with demographics table
    filename_parts = [demographics_table_name]

    # Add behavioral tables in sorted order for consistency
    if behavioral_tables:
        filename_parts.extend(sorted(behavioral_tables))

    # Add enwidened indicator if applicable
    if is_enwidened:
        filename_parts.append("enwidened")

    # Add timestamp
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    filename_parts.append(timestamp)

    # Join with underscores and add .csv extension
    filename = "_".join(filename_parts) + ".csv"

    # Ensure filename is safe (remove any problematic characters)
    filename = secure_filename(filename)

    return filename


# --- Filtering Summary Report Generation ---

@dataclass
class FilterTracker:
    """Tracks filter application sequence and sample size impact for reporting."""
    initial_count: int = 0
    initial_demographics: Dict[str, Any] = field(default_factory=dict)
    filter_steps: List[Dict[str, Any]] = field(default_factory=list)

    def set_initial_state(self, count: int, demographics: Dict[str, Any]):
        """Set the initial unfiltered state."""
        self.initial_count = count
        self.initial_demographics = demographics.copy()

    def add_filter_step(self, filter_name: str, filter_parameter: str,
                       resulting_count: int, resulting_demographics: Dict[str, Any]):
        """Add a filter step to the tracking sequence."""
        step = {
            'filter_name': filter_name,
            'filter_parameter': filter_parameter,
            'resulting_count': resulting_count,
            'resulting_demographics': resulting_demographics.copy()
        }
        self.filter_steps.append(step)


def calculate_demographics_breakdown(config: Config, merge_keys: MergeKeys,
                                   base_query_logic: str, params: List[Any],
                                   preserve_original_sessions: bool = False,
                                   original_sessions: List[str] = None) -> Dict[str, Any]:
    """Calculate demographic breakdown (age range, sex counts, substudy/site, sessions) for a dataset."""
    try:
        con = get_db_connection()
        # temp fix: coordinate file access with pandas operations
        with _file_access_lock:
            # Get age range if age column exists
            age_range = None
            if config.AGE_COLUMN:
                age_query = f"SELECT MIN(demo.{config.AGE_COLUMN}) as min_age, MAX(demo.{config.AGE_COLUMN}) as max_age {base_query_logic}"
                age_result = con.execute(age_query, params).fetchone()
                if age_result and age_result[0] is not None and age_result[1] is not None:
                    age_range = f"{int(age_result[0])}-{int(age_result[1])}"

            # Get sex breakdown if sex column exists
            sex_breakdown = {"Male": 0, "Female": 0, "Other": 0}
            if config.SEX_COLUMN:
                sex_query = f"SELECT demo.{config.SEX_COLUMN}, COUNT(DISTINCT demo.\"{merge_keys.get_merge_column()}\") as count {base_query_logic} GROUP BY demo.{config.SEX_COLUMN}"
                sex_results = con.execute(sex_query, params).fetchall()

                for sex_value, count in sex_results:
                    if sex_value is None or str(sex_value).lower() in ['', 'nan', 'null']:
                        sex_breakdown["Other"] += count
                    elif str(sex_value).lower() in ['m', 'male', '1']:
                        sex_breakdown["Male"] += count
                    elif str(sex_value).lower() in ['f', 'female', '2']:
                        sex_breakdown["Female"] += count
                    else:
                        sex_breakdown["Other"] += count

            # Get substudy/site breakdown if study site column exists
            substudy_sites = []
            if config.STUDY_SITE_COLUMN:
                substudy_query = f"SELECT DISTINCT demo.{config.STUDY_SITE_COLUMN} {base_query_logic} ORDER BY demo.{config.STUDY_SITE_COLUMN}"
                substudy_results = con.execute(substudy_query, params).fetchall()
                substudy_sites = [str(result[0]) for result in substudy_results if result[0] is not None]

            # Get session breakdown if session column exists
            sessions = []
            if config.SESSION_COLUMN:
                if preserve_original_sessions and original_sessions:
                    # For behavioral filters, preserve the session list from previous steps
                    # Only show sessions that still have participants in the current filtered data
                    session_query = f"SELECT DISTINCT demo.{config.SESSION_COLUMN} {base_query_logic} ORDER BY demo.{config.SESSION_COLUMN}"
                    session_results = con.execute(session_query, params).fetchall()
                    current_sessions = set(str(result[0]) for result in session_results if result[0] is not None)

                    # Keep only original sessions that still have data
                    sessions = [session for session in original_sessions if session in current_sessions]
                else:
                    # Normal session detection
                    session_query = f"SELECT DISTINCT demo.{config.SESSION_COLUMN} {base_query_logic} ORDER BY demo.{config.SESSION_COLUMN}"
                    session_results = con.execute(session_query, params).fetchall()
                    sessions = [str(result[0]) for result in session_results if result[0] is not None]

            return {
                'age_range': age_range,
                'sex_breakdown': sex_breakdown,
                'substudy_sites': substudy_sites,
                'sessions': sessions
            }
    except Exception as e:
        logging.error(f"Error calculating demographics breakdown: {e}")
        return {
            'age_range': None,
            'sex_breakdown': {"Male": 0, "Female": 0, "Other": 0},
            'substudy_sites': [],
            'sessions': []
        }


def generate_filtering_report(config: Config, merge_keys: MergeKeys,
                            demographic_filters: Dict[str, Any],
                            behavioral_filters: List[Dict[str, Any]],
                            tables_to_join: List[str]) -> pd.DataFrame:
    """Generate a filtering steps report showing sequential filter application."""

    tracker = FilterTracker()

    try:
        # Step 1: Get initial unfiltered state
        demographics_table_name = config.get_demographics_table_name()
        base_tables = [demographics_table_name]

        # Add tables from behavioral filters to ensure proper joins
        for bf in behavioral_filters:
            if bf.get('table') and bf['table'] not in base_tables:
                base_tables.append(bf['table'])

        initial_base_query, initial_params = generate_base_query_logic(
            config, merge_keys, {}, [], base_tables
        )
        initial_count_query, initial_count_params = generate_count_query(
            initial_base_query, initial_params, merge_keys
        )

        con = get_db_connection()
        # temp fix: coordinate file access with pandas operations
        with _file_access_lock:
            initial_count = con.execute(initial_count_query, initial_count_params).fetchone()[0]
        initial_demographics = calculate_demographics_breakdown(
            config, merge_keys, initial_base_query, initial_params
        )

        tracker.set_initial_state(initial_count, initial_demographics)

        # Step 2: Apply filters in enhanced sequence and track impact
        current_demo_filters = {}
        current_behavioral_filters = []

        # Get original substudy/site and session lists for comparison
        original_substudies = initial_demographics.get('substudy_sites', [])
        original_sessions = initial_demographics.get('sessions', [])

        # Apply substudy/site filter first (if user reduced the list)
        if demographic_filters.get('substudies') and original_substudies:
            user_substudies = demographic_filters['substudies']
            # Check if user actually reduced the list
            if set(user_substudies) != set(original_substudies):
                current_demo_filters['substudies'] = user_substudies

                step_base_query, step_params = generate_base_query_logic(
                    config, merge_keys, current_demo_filters, current_behavioral_filters, base_tables
                )
                step_count_query, step_count_params = generate_count_query(
                    step_base_query, step_params, merge_keys
                )

                step_count = con.execute(step_count_query, step_count_params).fetchone()[0]
                step_demographics = calculate_demographics_breakdown(
                    config, merge_keys, step_base_query, step_params
                )

                # Format substudy list
                substudy_param = ';'.join(user_substudies)
                tracker.add_filter_step(
                    'substudy', substudy_param, step_count, step_demographics
                )

        # Apply session filter second (if user reduced the list)
        if demographic_filters.get('sessions') and original_sessions:
            user_sessions = demographic_filters['sessions']
            # Check if user actually reduced the list
            if set(user_sessions) != set(original_sessions):
                current_demo_filters['sessions'] = user_sessions

                step_base_query, step_params = generate_base_query_logic(
                    config, merge_keys, current_demo_filters, current_behavioral_filters, base_tables
                )
                step_count_query, step_count_params = generate_count_query(
                    step_base_query, step_params, merge_keys
                )

                step_count = con.execute(step_count_query, step_count_params).fetchone()[0]
                step_demographics = calculate_demographics_breakdown(
                    config, merge_keys, step_base_query, step_params
                )

                # Format session list
                session_param = ';'.join(user_sessions)
                tracker.add_filter_step(
                    'session', session_param, step_count, step_demographics
                )

        # Apply age filter third
        if demographic_filters.get('age_range'):
            current_demo_filters['age_range'] = demographic_filters['age_range']

            step_base_query, step_params = generate_base_query_logic(
                config, merge_keys, current_demo_filters, current_behavioral_filters, base_tables
            )
            step_count_query, step_count_params = generate_count_query(
                step_base_query, step_params, merge_keys
            )

            step_count = con.execute(step_count_query, step_count_params).fetchone()[0]
            step_demographics = calculate_demographics_breakdown(
                config, merge_keys, step_base_query, step_params
            )

            age_min, age_max = demographic_filters['age_range']
            tracker.add_filter_step(
                'age', f'{age_min}-{age_max}', step_count, step_demographics
            )

        # Apply behavioral filters
        for bf in behavioral_filters:
            current_behavioral_filters.append(bf)

            step_base_query, step_params = generate_base_query_logic(
                config, merge_keys, current_demo_filters, current_behavioral_filters, base_tables
            )
            step_count_query, step_count_params = generate_count_query(
                step_base_query, step_params, merge_keys
            )

            step_count = con.execute(step_count_query, step_count_params).fetchone()[0]

            # For behavioral filters, preserve the session list from the previous step
            # Get the last available session list to preserve
            if tracker.filter_steps:
                previous_sessions = tracker.filter_steps[-1]['resulting_demographics'].get('sessions', original_sessions)
            else:
                previous_sessions = original_sessions

            step_demographics = calculate_demographics_breakdown(
                config, merge_keys, step_base_query, step_params,
                preserve_original_sessions=True,
                original_sessions=previous_sessions
            )

            # Format filter parameter based on type
            if bf.get('filter_type') == 'numeric':
                param_str = f"{bf.get('min_val')}-{bf.get('max_val')}"
            elif bf.get('filter_type') == 'categorical':
                selected_vals = bf.get('selected_values', [])
                if len(selected_vals) <= 3:
                    param_str = ', '.join(str(v) for v in selected_vals)
                else:
                    param_str = f"{', '.join(str(v) for v in selected_vals[:3])}, +{len(selected_vals)-3} more"
            else:
                param_str = "unknown"

            filter_name = bf.get('column', 'unknown_column')
            tracker.add_filter_step(filter_name, param_str, step_count, step_demographics)

        # Step 3: Convert to DataFrame report with enhanced structure
        report_data = []

        # Helper function to format lists for display
        def format_list_for_display(items_list):
            if not items_list:
                return ''
            return ';'.join(items_list)

        # Add initial sample
        initial_sex = tracker.initial_demographics.get('sex_breakdown', {})
        initial_substudies = tracker.initial_demographics.get('substudy_sites', [])
        initial_sessions = tracker.initial_demographics.get('sessions', [])

        report_data.append({
            'Step': 'Original Sample',
            'Filter': '',
            'Parameter': '',
            'Substudy_Site': format_list_for_display(initial_substudies),
            'Sessions': format_list_for_display(initial_sessions),
            'Total_Participants': tracker.initial_count,
            'Age_Range': tracker.initial_demographics.get('age_range', ''),
            'Male': initial_sex.get('Male', 0),
            'Female': initial_sex.get('Female', 0),
            'Other': initial_sex.get('Other', 0)
        })

        # Add filter steps
        for i, step in enumerate(tracker.filter_steps, 1):
            sex = step['resulting_demographics'].get('sex_breakdown', {})
            step_substudies = step['resulting_demographics'].get('substudy_sites', [])
            step_sessions = step['resulting_demographics'].get('sessions', [])

            report_data.append({
                'Step': f'Filter {i}',
                'Filter': step['filter_name'],
                'Parameter': step['filter_parameter'],
                'Substudy_Site': format_list_for_display(step_substudies),
                'Sessions': format_list_for_display(step_sessions),
                'Total_Participants': step['resulting_count'],
                'Age_Range': step['resulting_demographics'].get('age_range', ''),
                'Male': sex.get('Male', 0),
                'Female': sex.get('Female', 0),
                'Other': sex.get('Other', 0)
            })

        return pd.DataFrame(report_data)

    except Exception as e:
        logging.error(f"Error generating filtering report: {e}")
        # Return empty report on error with updated columns
        return pd.DataFrame(columns=['Step', 'Filter', 'Parameter', 'Substudy_Site', 'Sessions',
                                   'Total_Participants', 'Age_Range', 'Male', 'Female', 'Other'])


def generate_final_data_summary(df: pd.DataFrame, merge_keys: MergeKeys) -> pd.DataFrame:
    """Generate descriptive statistics summary for the final filtered dataset."""

    if df.empty:
        return pd.DataFrame(columns=['variable_name', 'mean', 'median', 'stdev',
                                   'min', 'max', 'missing'])

    summary_data = []

    # Exclude ID columns from summary
    id_columns_to_exclude = {merge_keys.primary_id}
    if merge_keys.session_id:
        id_columns_to_exclude.add(merge_keys.session_id)
    if merge_keys.composite_id:
        id_columns_to_exclude.add(merge_keys.composite_id)

    for col in df.columns:
        if col in id_columns_to_exclude:
            continue

        try:
            series = df[col]
            missing_count = series.isna().sum()

            # Try to convert to numeric
            numeric_series = pd.to_numeric(series, errors='coerce')

            if not numeric_series.isna().all():
                # Numeric column
                summary_data.append({
                    'variable_name': col,
                    'mean': numeric_series.mean() if not numeric_series.isna().all() else float('nan'),
                    'median': numeric_series.median() if not numeric_series.isna().all() else float('nan'),
                    'stdev': numeric_series.std() if not numeric_series.isna().all() else float('nan'),
                    'min': numeric_series.min() if not numeric_series.isna().all() else float('nan'),
                    'max': numeric_series.max() if not numeric_series.isna().all() else float('nan'),
                    'missing': missing_count
                })
            else:
                # Categorical column - show value counts
                non_missing = series.dropna()
                if len(non_missing) > 0:
                    value_counts = non_missing.value_counts()
                    # Format as 'value1':count1, 'value2':count2, etc.
                    count_pairs = [f"'{val}':{count}" for val, count in value_counts.head(10).items()]
                    counts_str = ', '.join(count_pairs)
                    if len(value_counts) > 10:
                        counts_str += f', +{len(value_counts)-10} more'
                else:
                    counts_str = 'all_missing'

                summary_data.append({
                    'variable_name': col,
                    'mean': counts_str,  # Store categorical counts in mean field
                    'median': float('nan'),
                    'stdev': float('nan'),
                    'min': float('nan'),
                    'max': float('nan'),
                    'missing': missing_count
                })

        except Exception as e:
            logging.warning(f"Error processing column {col}: {e}")
            summary_data.append({
                'variable_name': col,
                'mean': float('nan'),
                'median': float('nan'),
                'stdev': float('nan'),
                'min': float('nan'),
                'max': float('nan'),
                'missing': len(df)  # All missing if error
            })

    return pd.DataFrame(summary_data)


# --- Query Parameter Export/Import Functions ---

def export_query_parameters_to_toml(
    age_range: Optional[List[int]] = None,
    substudies: Optional[List[str]] = None,
    sessions: Optional[List[str]] = None,
    phenotypic_filters: Optional[List[Dict[str, Any]]] = None,
    selected_tables: Optional[List[str]] = None,
    selected_columns: Optional[Dict[str, List[str]]] = None,
    enwiden_longitudinal: bool = False,
    user_notes: str = "",
    app_version: str = "1.0.0"
) -> str:
    """
    Export query parameters to TOML format string.
    
    Args:
        age_range: Age range [min, max]
        substudies: List of selected substudies/sites
        sessions: List of selected sessions/visits
        phenotypic_filters: List of phenotypic filter dictionaries
        selected_tables: List of selected table names
        selected_columns: Dict mapping table names to lists of column names
        enwiden_longitudinal: Whether to enwiden longitudinal data
        user_notes: User-provided notes/description
        app_version: Application version
        
    Returns:
        TOML format string
    """
    export_data = {
        "metadata": {
            "export_timestamp": datetime.now().isoformat(),
            "app_version": app_version,
            "user_notes": user_notes
        },
        "cohort_filters": {},
        "phenotypic_filters": [],
        "export_selection": {
            "selected_tables": selected_tables or [],
            "enwiden_longitudinal": enwiden_longitudinal,
            "selected_columns": selected_columns or {}
        }
    }

    # Add cohort filters
    if age_range:
        export_data["cohort_filters"]["age_range"] = age_range
    if substudies:
        export_data["cohort_filters"]["substudies"] = substudies
    if sessions:
        export_data["cohort_filters"]["sessions"] = sessions

    # Add phenotypic filters
    if phenotypic_filters:
        for pf in phenotypic_filters:
            if pf.get('enabled') and pf.get('table') and pf.get('column'):
                filter_data = {
                    "table": pf['table'],
                    "column": pf['column'],
                    "filter_type": pf.get('filter_type', 'unknown')
                }

                if pf['filter_type'] == 'numeric':
                    filter_data["min_val"] = pf.get('min_val')
                    filter_data["max_val"] = pf.get('max_val')
                elif pf['filter_type'] == 'categorical':
                    filter_data["selected_values"] = pf.get('selected_values', [])

                export_data["phenotypic_filters"].append(filter_data)

    return toml.dumps(export_data)


def import_query_parameters_from_toml(toml_string: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Import query parameters from TOML format string.
    
    Args:
        toml_string: TOML format string
        
    Returns:
        Tuple of (parsed_data, error_messages)
    """
    error_messages = []

    try:
        data = toml.loads(toml_string)
    except Exception as e:
        return {}, [f"Invalid TOML format: {str(e)}"]

    # Validate required sections
    required_sections = ["metadata", "cohort_filters", "phenotypic_filters", "export_selection"]
    for section in required_sections:
        if section not in data:
            error_messages.append(f"Missing required section: {section}")

    if error_messages:
        return {}, error_messages

    # Extract metadata
    metadata = data.get("metadata", {})
    if "export_timestamp" not in metadata:
        error_messages.append("Missing export_timestamp in metadata")
    if "app_version" not in metadata:
        error_messages.append("Missing app_version in metadata")

    # Validate cohort filters
    cohort_filters = data.get("cohort_filters", {})
    if "age_range" in cohort_filters:
        age_range = cohort_filters["age_range"]
        if not isinstance(age_range, list) or len(age_range) != 2:
            error_messages.append("age_range must be a list of exactly 2 numbers")
        elif not all(isinstance(x, (int, float)) for x in age_range):
            error_messages.append("age_range values must be numbers")

    # Validate phenotypic filters
    phenotypic_filters = data.get("phenotypic_filters", [])
    if not isinstance(phenotypic_filters, list):
        error_messages.append("phenotypic_filters must be a list")
    else:
        for i, pf in enumerate(phenotypic_filters):
            if not isinstance(pf, dict):
                error_messages.append(f"phenotypic_filters[{i}] must be a dictionary")
                continue

            required_pf_fields = ["table", "column", "filter_type"]
            for field in required_pf_fields:
                if field not in pf:
                    error_messages.append(f"phenotypic_filters[{i}] missing required field: {field}")

            filter_type = pf.get("filter_type")
            if filter_type == "numeric":
                if "min_val" not in pf or "max_val" not in pf:
                    error_messages.append(f"phenotypic_filters[{i}] numeric filter missing min_val or max_val")
            elif filter_type == "categorical":
                if "selected_values" not in pf or not isinstance(pf["selected_values"], list):
                    error_messages.append(f"phenotypic_filters[{i}] categorical filter missing or invalid selected_values")

    # Validate export selection
    export_selection = data.get("export_selection", {})
    if "selected_tables" not in export_selection:
        error_messages.append("export_selection missing selected_tables")
    elif not isinstance(export_selection["selected_tables"], list):
        error_messages.append("export_selection.selected_tables must be a list")

    if "selected_columns" not in export_selection:
        error_messages.append("export_selection missing selected_columns")
    elif not isinstance(export_selection["selected_columns"], dict):
        error_messages.append("export_selection.selected_columns must be a dictionary")

    if "enwiden_longitudinal" not in export_selection:
        export_selection["enwiden_longitudinal"] = False
    elif not isinstance(export_selection["enwiden_longitudinal"], bool):
        error_messages.append("export_selection.enwiden_longitudinal must be a boolean")

    return data, error_messages


def validate_imported_query_parameters(
    imported_data: Dict[str, Any],
    available_tables: List[str],
    demographics_columns: List[str],
    behavioral_columns: Dict[str, List[str]],
    config: 'Config'
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Validate imported query parameters against current dataset.
    
    Args:
        imported_data: Parsed TOML data
        available_tables: List of available table names
        demographics_columns: List of demographics columns
        behavioral_columns: Dict mapping table names to column lists
        config: Configuration object
        
    Returns:
        Tuple of (validation_results, error_messages)
    """
    validation_results = {
        "valid_parameters": {},
        "invalid_parameters": {},
        "warnings": []
    }
    error_messages = []

    demographics_table_name = config.get_demographics_table_name()
    all_available_tables = [demographics_table_name] + available_tables

    # Validate cohort filters
    cohort_filters = imported_data.get("cohort_filters", {})
    valid_cohort = {}
    invalid_cohort = {}

    # Age range validation
    if "age_range" in cohort_filters:
        age_range = cohort_filters["age_range"]
        if config.AGE_COLUMN in demographics_columns:
            valid_cohort["age_range"] = age_range
        else:
            invalid_cohort["age_range"] = age_range
            error_messages.append(f"Age column '{config.AGE_COLUMN}' not found in demographics")

    # Substudies validation
    if "substudies" in cohort_filters:
        substudies = cohort_filters["substudies"]
        if config.STUDY_SITE_COLUMN in demographics_columns:
            valid_cohort["substudies"] = substudies
        else:
            invalid_cohort["substudies"] = substudies
            error_messages.append(f"Study site column '{config.STUDY_SITE_COLUMN}' not found in demographics")

    # Sessions validation
    if "sessions" in cohort_filters:
        sessions = cohort_filters["sessions"]
        if config.SESSION_COLUMN in demographics_columns:
            valid_cohort["sessions"] = sessions
        else:
            invalid_cohort["sessions"] = sessions
            error_messages.append(f"Session column '{config.SESSION_COLUMN}' not found in demographics")

    validation_results["valid_parameters"]["cohort_filters"] = valid_cohort
    validation_results["invalid_parameters"]["cohort_filters"] = invalid_cohort

    # Validate phenotypic filters
    phenotypic_filters = imported_data.get("phenotypic_filters", [])
    valid_phenotypic = []
    invalid_phenotypic = []

    for i, pf in enumerate(phenotypic_filters):
        table_name = pf.get("table")
        column_name = pf.get("column")

        # Check if table exists
        if table_name not in all_available_tables:
            invalid_phenotypic.append({"index": i, "data": pf, "error": f"Table '{table_name}' not found"})
            error_messages.append(f"Phenotypic filter {i+1}: Table '{table_name}' not found")
            continue

        # Check if column exists in table
        if table_name == demographics_table_name:
            available_columns = demographics_columns
        else:
            available_columns = behavioral_columns.get(table_name, [])

        if column_name not in available_columns:
            invalid_phenotypic.append({"index": i, "data": pf, "error": f"Column '{column_name}' not found in table '{table_name}'"})
            error_messages.append(f"Phenotypic filter {i+1}: Column '{column_name}' not found in table '{table_name}'")
            continue

        # Valid filter
        valid_phenotypic.append(pf)

    validation_results["valid_parameters"]["phenotypic_filters"] = valid_phenotypic
    validation_results["invalid_parameters"]["phenotypic_filters"] = invalid_phenotypic

    # Validate export selection
    export_selection = imported_data.get("export_selection", {})
    valid_export = {}
    invalid_export = {}

    # Validate selected tables
    selected_tables = export_selection.get("selected_tables", [])
    valid_tables = []
    invalid_tables = []

    for table in selected_tables:
        if table in all_available_tables:
            valid_tables.append(table)
        else:
            invalid_tables.append(table)
            error_messages.append(f"Selected table '{table}' not found")

    valid_export["selected_tables"] = valid_tables
    invalid_export["selected_tables"] = invalid_tables

    # Validate selected columns
    selected_columns = export_selection.get("selected_columns", {})
    valid_columns = {}
    invalid_columns = {}

    for table_name, columns in selected_columns.items():
        if table_name not in all_available_tables:
            invalid_columns[table_name] = {"columns": columns, "error": f"Table '{table_name}' not found"}
            error_messages.append(f"Selected columns for table '{table_name}': table not found")
            continue

        # Get available columns for this table
        if table_name == demographics_table_name:
            available_columns = demographics_columns
        else:
            available_columns = behavioral_columns.get(table_name, [])

        valid_table_columns = []
        invalid_table_columns = []

        for column in columns:
            if column in available_columns:
                valid_table_columns.append(column)
            else:
                invalid_table_columns.append(column)
                error_messages.append(f"Column '{column}' not found in table '{table_name}'")

        if valid_table_columns:
            valid_columns[table_name] = valid_table_columns
        if invalid_table_columns:
            invalid_columns[table_name] = {"columns": invalid_table_columns, "error": "Columns not found"}

    valid_export["selected_columns"] = valid_columns
    invalid_export["selected_columns"] = invalid_columns

    # Enwiden longitudinal is always valid (boolean)
    valid_export["enwiden_longitudinal"] = export_selection.get("enwiden_longitudinal", False)

    validation_results["valid_parameters"]["export_selection"] = valid_export
    validation_results["invalid_parameters"]["export_selection"] = invalid_export

    return validation_results, error_messages
