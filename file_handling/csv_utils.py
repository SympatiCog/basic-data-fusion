"""
CSV utilities for Basic Data Fusion.

This module provides utilities for CSV file validation, processing,
and metadata extraction.
"""

import os
from io import BytesIO
from typing import List, Optional, Tuple
from pathlib import Path

import pandas as pd

from core.exceptions import ValidationError, FileProcessingError

# Exception aliases for this module
FileValidationError = ValidationError
DataProcessingError = ValidationError
from .security import sanitize_column_names, detect_malicious_content, validate_file_size, check_file_extension


def validate_csv_file(
    file_content: bytes, 
    filename: str, 
    required_columns: Optional[List[str]] = None,
    max_size_mb: int = 50
) -> Tuple[List[str], Optional[pd.DataFrame]]:
    """
    Validate uploaded CSV file content.
    
    Args:
        file_content: Bytes of the file content
        filename: Original name of the file
        required_columns: List of column names that must be present
        max_size_mb: Maximum file size in megabytes
        
    Returns:
        Tuple of (list of error messages, DataFrame or None)
        
    Raises:
        FileValidationError: If validation fails critically
    """
    errors = []
    df = None
    
    try:
        # File size check
        if not validate_file_size(file_content, max_size_mb):
            errors.append(f"File '{filename}' too large (maximum {max_size_mb}MB)")

        # File extension check
        if not check_file_extension(filename, ['.csv']):
            errors.append(f"File '{filename}' must be a CSV (.csv extension)")
        
        # Security scan
        security_warnings = detect_malicious_content(file_content, filename)
        if security_warnings:
            errors.extend(security_warnings)

        if not errors:  # Proceed only if basic checks pass
            try:
                # Try to read the CSV from bytes
                df = pd.read_csv(BytesIO(file_content), low_memory=False)

                # Basic structure validation
                if len(df) == 0:
                    errors.append(f"File '{filename}' is empty (no data rows)")
                
                if len(df.columns) == 0:
                    errors.append(f"File '{filename}' has no columns")
                elif len(df.columns) > 1000:  # Arbitrary limit
                    errors.append(f"File '{filename}' has too many columns (maximum 1000)")
                
                # Check for duplicate column names
                if len(df.columns) != len(set(df.columns)):
                    duplicates = [col for col in df.columns if list(df.columns).count(col) > 1]
                    errors.append(f"File '{filename}' has duplicate column names: {', '.join(set(duplicates))}")
                
                # Check for required columns
                if required_columns:
                    missing_cols = set(required_columns) - set(df.columns)
                    if missing_cols:
                        errors.append(f"File '{filename}' missing required columns: {', '.join(missing_cols)}")
                
                # Additional data quality checks
                data_quality_errors = _validate_data_quality(df, filename)
                errors.extend(data_quality_errors)

            except pd.errors.EmptyDataError:
                errors.append(f"File '{filename}' is empty or contains no valid CSV data")
            except pd.errors.ParserError as e:
                errors.append(f"Invalid CSV format in '{filename}': {str(e)}")
            except UnicodeDecodeError:
                errors.append(f"File '{filename}' encoding not supported (please use UTF-8)")
            except Exception as e:
                errors.append(f"Error reading file '{filename}': {str(e)}")

        return errors, df if not errors and df is not None else None
    
    except Exception as e:
        error_msg = f"Critical error validating '{filename}': {e}"
        raise FileValidationError(error_msg, filename=filename, validation_errors=errors)


def _validate_data_quality(df: pd.DataFrame, filename: str) -> List[str]:
    """
    Perform additional data quality validation on CSV DataFrame.
    
    Args:
        df: DataFrame to validate
        filename: Name of the file for error messages
        
    Returns:
        List of data quality warnings
    """
    warnings = []
    
    try:
        # Check for completely empty columns
        empty_columns = [col for col in df.columns if df[col].isna().all()]
        if empty_columns:
            warnings.append(f"File '{filename}' has completely empty columns: {', '.join(empty_columns[:5])}")
        
        # Check for very high percentage of missing values
        for col in df.columns:
            missing_pct = df[col].isna().sum() / len(df) * 100
            if missing_pct > 95:  # More than 95% missing
                warnings.append(f"Column '{col}' in '{filename}' has {missing_pct:.1f}% missing values")
        
        # Check for suspicious column names that might indicate data issues
        suspicious_patterns = ['unnamed:', 'column', 'field']
        suspicious_columns = []
        for col in df.columns:
            col_lower = str(col).lower()
            if any(pattern in col_lower for pattern in suspicious_patterns):
                suspicious_columns.append(str(col))
        
        if suspicious_columns:
            warnings.append(f"File '{filename}' has potentially auto-generated column names: {', '.join(suspicious_columns[:3])}")
        
        # Check for very wide data (many columns might indicate transposed data)
        if len(df.columns) > 100 and len(df) < 10:
            warnings.append(f"File '{filename}' has many columns ({len(df.columns)}) but few rows ({len(df)}) - data might be transposed")
    
    except Exception as e:
        warnings.append(f"Error during data quality validation of '{filename}': {e}")
    
    return warnings


def process_csv_file(
    file_content: bytes, 
    filename: str, 
    sanitize_columns: bool = True
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """
    Process CSV file with validation and optional column sanitization.
    
    Args:
        file_content: Bytes of the file content
        filename: Original name of the file
        sanitize_columns: Whether to sanitize column names
        
    Returns:
        Tuple of (processed DataFrame, success messages, error messages)
        
    Raises:
        DataProcessingError: If processing fails
    """
    try:
        success_messages = []
        error_messages = []
        
        # Validate the CSV file
        errors, df = validate_csv_file(file_content, filename)
        
        if errors:
            error_messages.extend(errors)
            raise DataProcessingError(f"CSV validation failed for '{filename}'", 
                                    details={'errors': errors})
        
        if df is None:
            raise DataProcessingError(f"Failed to load CSV file '{filename}'")
        
        # Sanitize column names if requested
        if sanitize_columns:
            original_columns = df.columns.tolist()
            sanitized_columns, column_mapping = sanitize_column_names(original_columns)
            
            # Check if any columns were renamed
            renamed_columns = {orig: sanitized for orig, sanitized in column_mapping.items()
                             if orig != sanitized}
            
            # Apply sanitized column names
            df.columns = sanitized_columns
            
            # Report column renames if any occurred
            if renamed_columns:
                rename_count = len(renamed_columns)
                success_messages.append(f"🔧 Sanitized {rename_count} column name(s) in '{filename}'")
                
                # Show details for first few renames to avoid overwhelming output
                if rename_count <= 5:
                    for orig, sanitized in list(renamed_columns.items())[:5]:
                        success_messages.append(f"   '{orig}' → '{sanitized}'")
                elif rename_count > 5:
                    # Show first 3 examples
                    for orig, sanitized in list(renamed_columns.items())[:3]:
                        success_messages.append(f"   '{orig}' → '{sanitized}'")
                    success_messages.append(f"   ... and {rename_count - 3} more")
        
        return df, success_messages, error_messages
    
    except DataProcessingError:
        raise
    except Exception as e:
        error_msg = f"Error processing CSV file '{filename}': {e}"
        raise DataProcessingError(error_msg, details={'filename': filename})


def scan_csv_files(data_dir: str) -> Tuple[List[str], List[str]]:
    """
    Scan a directory for CSV files.
    
    Args:
        data_dir: Directory to scan for CSV files
        
    Returns:
        Tuple of (list of CSV filenames, list of error messages)
    """
    errors = []
    files_found = []
    
    try:
        # Ensure directory exists
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        
        # List files in directory
        files = os.listdir(data_dir)
        files_found = [f for f in files if f.endswith('.csv')]
        
    except FileNotFoundError:
        errors.append(f"Error: The data directory was not found at '{data_dir}'.")
    except PermissionError:
        errors.append(f"Error: Permission denied accessing directory '{data_dir}'.")
    except OSError as e:
        errors.append(f"Error accessing directory '{data_dir}': {e}")
    
    return files_found, errors


def get_csv_info(file_path: str) -> Tuple[dict, List[str]]:
    """
    Get basic information about a CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Tuple of (file info dict, list of error messages)
    """
    errors = []
    info = {}
    
    try:
        if not os.path.exists(file_path):
            errors.append(f"File not found: {file_path}")
            return info, errors
        
        # Get file stats
        stat = os.stat(file_path)
        info['file_size'] = stat.st_size
        info['modified_time'] = stat.st_mtime
        
        # Read CSV to get structure info
        try:
            # Read just the header and first few rows for efficiency
            df_sample = pd.read_csv(file_path, nrows=5, low_memory=False)
            
            info['num_columns'] = len(df_sample.columns)
            info['column_names'] = df_sample.columns.tolist()
            info['column_types'] = df_sample.dtypes.to_dict()
            
            # Get row count (more efficient than loading full file)
            with open(file_path, 'r', encoding='utf-8') as f:
                row_count = sum(1 for line in f) - 1  # Subtract header
            info['num_rows'] = max(0, row_count)
            
        except pd.errors.EmptyDataError:
            errors.append(f"File '{file_path}' is empty")
        except pd.errors.ParserError as e:
            errors.append(f"Invalid CSV format in '{file_path}': {e}")
        except UnicodeDecodeError:
            errors.append(f"Encoding issue in '{file_path}'")
        except Exception as e:
            errors.append(f"Error reading CSV '{file_path}': {e}")
    
    except Exception as e:
        errors.append(f"Error getting file info for '{file_path}': {e}")
    
    return info, errors


def validate_csv_structure(file_path: str, expected_columns: Optional[List[str]] = None) -> Tuple[bool, List[str]]:
    """
    Validate the structure of an existing CSV file.
    
    Args:
        file_path: Path to the CSV file
        expected_columns: List of expected column names (optional)
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    try:
        if not os.path.exists(file_path):
            errors.append(f"File not found: {file_path}")
            return False, errors
        
        # Read file header
        try:
            df_header = pd.read_csv(file_path, nrows=0, low_memory=False)
            
            # Check for empty file
            if len(df_header.columns) == 0:
                errors.append(f"File has no columns: {file_path}")
            
            # Check for expected columns
            if expected_columns:
                missing_columns = set(expected_columns) - set(df_header.columns)
                if missing_columns:
                    errors.append(f"Missing expected columns: {', '.join(missing_columns)}")
            
            # Check for duplicate column names
            if len(df_header.columns) != len(set(df_header.columns)):
                duplicates = [col for col in df_header.columns if list(df_header.columns).count(col) > 1]
                errors.append(f"Duplicate column names: {', '.join(set(duplicates))}")
        
        except pd.errors.EmptyDataError:
            errors.append(f"File is empty: {file_path}")
        except pd.errors.ParserError as e:
            errors.append(f"Invalid CSV format: {e}")
        except UnicodeDecodeError:
            errors.append(f"Encoding issue in file: {file_path}")
        except Exception as e:
            errors.append(f"Error reading file: {e}")
    
    except Exception as e:
        errors.append(f"Error validating CSV structure: {e}")
    
    return len(errors) == 0, errors