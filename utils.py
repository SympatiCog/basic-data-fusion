import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, List, Tuple, Dict
import pandas as pd
import toml
import logging
from datetime import datetime

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

            df_headers = pd.read_csv(demographics_path, nrows=0)
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
            df = pd.read_csv(file_path)
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
            df = pd.read_csv(file_path)
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

    _merge_strategy: Optional[FlexibleMergeStrategy] = field(init=False, default=None)
    _merge_keys: Optional[MergeKeys] = field(init=False, default=None)

    # UI defaults - these might be moved or handled differently in Dash
    DEFAULT_AGE_RANGE: Tuple[int, int] = (0, 120)
    DEFAULT_AGE_SELECTION: Tuple[int, int] = (18, 80)
    DEFAULT_FILTER_RANGE: Tuple[int, int] = (0, 100)
    MAX_DISPLAY_ROWS: int = 50 # For Dash tables, pagination is better
    CACHE_TTL_SECONDS: int = 600 # For Dash, use Flask-Caching or similar


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
    filename = os.path.basename(filename)
    filename = re.sub(r'\s+', '_', filename)
    filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
    filename = re.sub(r'_+', '_', filename)
    filename = filename.strip('_')
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        filename = name[:250] + ext
    return filename

def sanitize_column_names(columns: List[str]) -> Tuple[List[str], Dict[str, str]]:
    """
    Sanitize column names to be safe for SQL queries and analytical tools.
    
    Args:
        columns: List of original column names
        
    Returns:
        Tuple of (sanitized_column_names, mapping_dict)
        mapping_dict maps original names to sanitized names
    """
    sanitized_columns = []
    column_mapping = {}
    
    for original_col in columns:
        # Replace whitespace and dashes with underscores
        sanitized = re.sub(r'[\s-]+', '_', original_col)
        
        # Remove problematic characters (parentheses, braces, periods, etc.)
        # Keep only alphanumeric characters and underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '', sanitized)
        
        # Consolidate multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        # Ensure column name is not empty
        if not sanitized:
            sanitized = f"col_{len(sanitized_columns)}"
        
        # Ensure column name doesn't start with a number (problematic for some tools)
        if sanitized and sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"
        
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
            df = pd.read_csv(BytesIO(file_content))

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
            df = pd.read_csv(BytesIO(content))
            
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


def detect_rockland_format(demographics_columns: List[str]) -> bool:
    return 'all_studies' in demographics_columns


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
                df_sample = pd.read_csv(file_path, nrows=0) # Read only headers
                if merge_keys.session_id in df_sample.columns:
                    # If session_id is present, read that column
                    df_session_col = pd.read_csv(file_path, usecols=[merge_keys.session_id])
                    sessions = df_session_col[merge_keys.session_id].dropna().astype(str).unique()
                    unique_sessions.update(sessions)
            except Exception as e:
                errors.append(f"Could not read session values from {csv_file}: {e}")
                continue
    except Exception as e:
        errors.append(f"Error scanning for session values: {e}")
        return [], errors
    return sorted(list(unique_sessions)), errors

def get_unique_column_values(data_dir: str, table_name: str, column_name: str, demo_table_name: str, demographics_file_name: str) -> Tuple[List[Any], Optional[str]]:
    """
    Extracts unique, sorted, non-null values from a specified column in a CSV file.
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

    if not os.path.exists(file_path):
        return [], f"Error: File not found at {file_path}"

    try:
        df = pd.read_csv(file_path, usecols=[column_name])
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

        return unique_values, None
    except FileNotFoundError:
        return [], f"Error: File not found at {file_path}"
    except ValueError as ve: # Happens if column_name is not in the CSV
        return [], f"Error: Column '{column_name}' not found in '{actual_file_name}' or file is empty. Details: {ve}"
    except Exception as e:
        return [], f"Error reading or processing file '{actual_file_name}': {e}"

def validate_csv_structure(file_path: str, filename: str, merge_keys: MergeKeys) -> List[str]:
    """Validates basic CSV structure. Returns list of error messages."""
    errors = []
    try:
        df_headers = pd.read_csv(file_path, nrows=0)
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
        df_sample = pd.read_csv(file_path, nrows=100) # Sample for metadata

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

        chunk_iter = pd.read_csv(file_path, chunksize=1000, usecols=numeric_cols)
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


def get_table_info(config: Config) -> Tuple[
    List[str], List[str], Dict[str, List[str]], Dict[str, str],
    Dict[str, Tuple[float, float]], Dict, List[str], List[str], bool, List[str]
]:
    """
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
                df_sample_demo = pd.read_csv(table_path, nrows=0)
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

# Example of how Config might be instantiated and used globally if needed
# config_instance = Config()
# config_instance.load_config() # Load or create config.toml
# Then pass config_instance to functions needing it, or access its members.
# For Dash, it's often better to create and manage config within callbacks or app setup.


# --- Query Generation Logic ---

def generate_base_query_logic(
    config: Config,
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str]
) -> Tuple[str, List[Any]]:
    """
    Generates the common FROM, JOIN, and WHERE clauses for all queries.
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
        df_headers = pd.read_csv(demographics_path, nrows=0)
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

    
    # Rockland Sample1 Substudy Filters (only if 'all_studies' column exists)
    if demographic_filters.get('substudies'):
        if 'all_studies' in available_demo_columns:
            substudy_conditions = []
            for substudy in demographic_filters['substudies']:
                substudy_conditions.append("demo.all_studies LIKE ?")
                params[f'substudy_{substudy}'] = f'%{substudy}%'
            if substudy_conditions:
                where_clauses.append(f"({' OR '.join(substudy_conditions)})")
        else:
            logging.info("Skipping substudy filters: 'all_studies' column not found in demographics file")

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


def generate_data_query(
    base_query_logic: str,
    params: List[Any],
    selected_tables: List[str],
    selected_columns: Dict[str, List[str]],
    # config: Config, # Not strictly needed if demo table name is handled by base_query
    # merge_keys: MergeKeys # Not strictly needed here if demo.* is always selected
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """Generates the full SQL query to fetch data."""
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

    # Apply baseline consolidation if multiple BAS sessions exist
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
