"""
Statistical analysis functions for Basic Data Fusion.

This module provides functions for statistical calculations,
data summaries, and analytical utilities.
"""

import logging
import os
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

from core.exceptions import ValidationError

# Exception alias for this module
DataProcessingError = ValidationError
from data_handling.merge_strategy import MergeKeys

# Threading lock for file access
_file_access_lock = Lock()


def get_unique_column_values(
    data_dir: str,
    table_name: str,
    column_name: str,
    demo_table_name: str,
    demographics_file_name: str,
    max_values: int = 1000
) -> Tuple[List[str], List[str]]:
    """
    Retrieve unique values from a column for filter options.
    
    Args:
        data_dir: Directory containing data files
        table_name: Name of the table
        column_name: Name of the column
        demo_table_name: Name of demographics table
        demographics_file_name: Name of demographics file
        max_values: Maximum number of unique values to return
        
    Returns:
        Tuple of (list of unique values, list of error messages)
    """
    errors = []
    unique_values = []
    
    try:
        import os
        from threading import Lock
        
        # Use threading lock for file access
        _file_access_lock = Lock()
        
        # Determine file path
        if table_name == demo_table_name:
            file_path = os.path.join(data_dir, demographics_file_name)
        else:
            file_path = os.path.join(data_dir, f"{table_name}.csv")
        
        if not os.path.exists(file_path):
            errors.append(f"File not found: {file_path}")
            return unique_values, errors
        
        # Read file with thread safety
        with _file_access_lock:
            try:
                # Read only the required column for efficiency
                df = pd.read_csv(file_path, usecols=[column_name], low_memory=False)
                
                if column_name not in df.columns:
                    errors.append(f"Column '{column_name}' not found in {table_name}")
                    return unique_values, errors
                
                # Get unique values, excluding NaN
                series = df[column_name].dropna()
                unique_vals = series.unique()
                
                # Limit number of values
                if len(unique_vals) > max_values:
                    unique_vals = unique_vals[:max_values]
                    errors.append(f"Column has > {max_values} unique values, showing first {max_values}")
                
                # Convert to strings and sort
                unique_values = sorted([str(val) for val in unique_vals])
                
            except pd.errors.EmptyDataError:
                errors.append(f"File {file_path} is empty")
            except pd.errors.ParserError as e:
                errors.append(f"Error parsing {file_path}: {e}")
            except UnicodeDecodeError:
                errors.append(f"Encoding error in {file_path}")
            except Exception as e:
                errors.append(f"Error reading column values from {file_path}: {e}")
    
    except Exception as e:
        errors.append(f"Error getting unique column values: {e}")
    
    return unique_values, errors


def calculate_column_statistics(
    df: pd.DataFrame,
    column_name: str,
    merge_keys: Optional[MergeKeys] = None
) -> Dict[str, Any]:
    """
    Calculate comprehensive statistics for a column.
    
    Args:
        df: DataFrame containing the data
        column_name: Name of the column to analyze
        merge_keys: Merge strategy information (optional)
        
    Returns:
        Dictionary with statistical summary
        
    Raises:
        DataProcessingError: If calculation fails
    """
    try:
        if column_name not in df.columns:
            raise DataProcessingError(f"Column '{column_name}' not found in DataFrame")
        
        series = df[column_name]
        stats = {
            'column_name': column_name,
            'total_count': len(series),
            'non_null_count': series.count(),
            'null_count': series.isna().sum(),
            'null_percentage': (series.isna().sum() / len(series) * 100) if len(series) > 0 else 0,
            'data_type': str(series.dtype),
            'unique_count': series.nunique()
        }
        
        # Numeric statistics
        if pd.api.types.is_numeric_dtype(series):
            numeric_series = pd.to_numeric(series, errors='coerce')
            valid_numeric = numeric_series.dropna()
            
            if len(valid_numeric) > 0:
                stats.update({
                    'mean': float(valid_numeric.mean()),
                    'median': float(valid_numeric.median()),
                    'std': float(valid_numeric.std()) if len(valid_numeric) > 1 else 0.0,
                    'min': float(valid_numeric.min()),
                    'max': float(valid_numeric.max()),
                    'q25': float(valid_numeric.quantile(0.25)),
                    'q75': float(valid_numeric.quantile(0.75)),
                    'skewness': float(valid_numeric.skew()) if len(valid_numeric) > 1 else 0.0,
                    'kurtosis': float(valid_numeric.kurtosis()) if len(valid_numeric) > 1 else 0.0
                })
                
                # Detect outliers using IQR method
                q1, q3 = valid_numeric.quantile([0.25, 0.75])
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                outliers = valid_numeric[(valid_numeric < lower_bound) | (valid_numeric > upper_bound)]
                
                stats['outlier_count'] = len(outliers)
                stats['outlier_percentage'] = (len(outliers) / len(valid_numeric) * 100) if len(valid_numeric) > 0 else 0
        
        # Categorical statistics
        else:
            value_counts = series.value_counts()
            stats.update({
                'most_common_value': str(value_counts.index[0]) if len(value_counts) > 0 else None,
                'most_common_count': int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                'least_common_value': str(value_counts.index[-1]) if len(value_counts) > 0 else None,
                'least_common_count': int(value_counts.iloc[-1]) if len(value_counts) > 0 else 0
            })
            
            # Add top categories (up to 10)
            if len(value_counts) > 0:
                top_categories = value_counts.head(10).to_dict()
                stats['top_categories'] = {str(k): int(v) for k, v in top_categories.items()}
        
        return stats
    
    except Exception as e:
        error_msg = f"Error calculating statistics for column '{column_name}': {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, details={'column_name': column_name})


def calculate_correlation_matrix(
    df: pd.DataFrame,
    merge_keys: Optional[MergeKeys] = None,
    method: str = 'pearson',
    min_valid_pairs: int = 10
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Calculate correlation matrix for numeric columns.
    
    Args:
        df: DataFrame containing the data
        merge_keys: Merge strategy information to exclude ID columns
        method: Correlation method ('pearson', 'spearman', 'kendall')
        min_valid_pairs: Minimum number of valid pairs required for correlation
        
    Returns:
        Tuple of (correlation matrix DataFrame, list of warnings)
        
    Raises:
        DataProcessingError: If calculation fails
    """
    warnings = []
    
    try:
        # Exclude ID columns
        exclude_columns = set()
        if merge_keys:
            exclude_columns.add(merge_keys.primary_id)
            if merge_keys.session_id:
                exclude_columns.add(merge_keys.session_id)
            if merge_keys.composite_id:
                exclude_columns.add(merge_keys.composite_id)
        
        # Select numeric columns
        numeric_columns = []
        for col in df.columns:
            if col not in exclude_columns and pd.api.types.is_numeric_dtype(df[col]):
                # Check if column has enough valid values
                valid_count = df[col].count()
                if valid_count >= min_valid_pairs:
                    numeric_columns.append(col)
                else:
                    warnings.append(f"Column '{col}' excluded: only {valid_count} valid values")
        
        if len(numeric_columns) < 2:
            warnings.append("Not enough numeric columns for correlation analysis")
            return pd.DataFrame(), warnings
        
        # Calculate correlation matrix
        numeric_df = df[numeric_columns]
        
        if method == 'pearson':
            corr_matrix = numeric_df.corr(method='pearson', min_periods=min_valid_pairs)
        elif method == 'spearman':
            corr_matrix = numeric_df.corr(method='spearman', min_periods=min_valid_pairs)
        elif method == 'kendall':
            corr_matrix = numeric_df.corr(method='kendall', min_periods=min_valid_pairs)
        else:
            raise DataProcessingError(f"Unsupported correlation method: {method}")
        
        # Check for any completely NaN columns/rows
        nan_columns = corr_matrix.columns[corr_matrix.isna().all()].tolist()
        if nan_columns:
            warnings.append(f"Columns with no valid correlations: {', '.join(nan_columns)}")
        
        return corr_matrix, warnings
    
    except Exception as e:
        error_msg = f"Error calculating correlation matrix: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, details={'method': method})


def identify_data_quality_issues(
    df: pd.DataFrame,
    merge_keys: Optional[MergeKeys] = None
) -> Dict[str, Any]:
    """
    Identify data quality issues in the dataset.
    
    Args:
        df: DataFrame to analyze
        merge_keys: Merge strategy information
        
    Returns:
        Dictionary with data quality assessment
        
    Raises:
        DataProcessingError: If analysis fails
    """
    try:
        issues = {
            'missing_data': {},
            'duplicates': {},
            'outliers': {},
            'inconsistencies': {},
            'summary': {
                'total_issues': 0,
                'severity_high': 0,
                'severity_medium': 0,
                'severity_low': 0
            }
        }
        
        # Check missing data
        missing_summary = []
        for col in df.columns:
            missing_count = df[col].isna().sum()
            missing_pct = (missing_count / len(df) * 100) if len(df) > 0 else 0
            
            if missing_pct > 0:
                severity = 'high' if missing_pct > 50 else 'medium' if missing_pct > 20 else 'low'
                missing_summary.append({
                    'column': col,
                    'missing_count': missing_count,
                    'missing_percentage': missing_pct,
                    'severity': severity
                })
        
        issues['missing_data']['columns'] = missing_summary
        issues['missing_data']['total_columns_affected'] = len(missing_summary)
        
        # Check for duplicate rows
        if merge_keys and merge_keys.primary_id in df.columns:
            # Check for duplicate participants
            if merge_keys.is_longitudinal:
                # For longitudinal data, check composite ID duplicates
                if merge_keys.composite_id and merge_keys.composite_id in df.columns:
                    duplicate_ids = df[merge_keys.composite_id].duplicated().sum()
                    issues['duplicates']['composite_id_duplicates'] = duplicate_ids
                    if duplicate_ids > 0:
                        issues['summary']['severity_medium'] += 1
            else:
                # For cross-sectional data, check primary ID duplicates
                duplicate_ids = df[merge_keys.primary_id].duplicated().sum()
                issues['duplicates']['primary_id_duplicates'] = duplicate_ids
                if duplicate_ids > 0:
                    issues['summary']['severity_high'] += 1
        
        # Check for completely duplicate rows
        duplicate_rows = df.duplicated().sum()
        issues['duplicates']['duplicate_rows'] = duplicate_rows
        if duplicate_rows > 0:
            issues['summary']['severity_medium'] += 1
        
        # Check for outliers in numeric columns
        outlier_summary = []
        exclude_columns = set()
        if merge_keys:
            exclude_columns.update({merge_keys.primary_id, merge_keys.session_id, merge_keys.composite_id})
        
        for col in df.columns:
            if col not in exclude_columns and pd.api.types.is_numeric_dtype(df[col]):
                numeric_series = pd.to_numeric(df[col], errors='coerce').dropna()
                
                if len(numeric_series) > 10:  # Need sufficient data for outlier detection
                    q1, q3 = numeric_series.quantile([0.25, 0.75])
                    iqr = q3 - q1
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    
                    outliers = numeric_series[(numeric_series < lower_bound) | (numeric_series > upper_bound)]
                    outlier_pct = (len(outliers) / len(numeric_series) * 100) if len(numeric_series) > 0 else 0
                    
                    if outlier_pct > 0:
                        severity = 'high' if outlier_pct > 10 else 'medium' if outlier_pct > 5 else 'low'
                        outlier_summary.append({
                            'column': col,
                            'outlier_count': len(outliers),
                            'outlier_percentage': outlier_pct,
                            'severity': severity
                        })
        
        issues['outliers']['columns'] = outlier_summary
        issues['outliers']['total_columns_affected'] = len(outlier_summary)
        
        # Check for data type inconsistencies
        inconsistency_summary = []
        for col in df.columns:
            series = df[col].dropna()
            if len(series) > 0:
                # Check if numeric column has non-numeric values
                if pd.api.types.is_object_dtype(series):
                    # Try to convert to numeric and see how many fail
                    numeric_conversion = pd.to_numeric(series, errors='coerce')
                    failed_conversions = numeric_conversion.isna().sum() - series.isna().sum()
                    
                    if failed_conversions > 0:
                        failed_pct = (failed_conversions / len(series) * 100)
                        if failed_pct < 90:  # If less than 90% fail, might be mostly numeric
                            inconsistency_summary.append({
                                'column': col,
                                'issue': 'Mixed numeric/text values',
                                'affected_count': failed_conversions,
                                'severity': 'medium'
                            })
        
        issues['inconsistencies']['columns'] = inconsistency_summary
        issues['inconsistencies']['total_columns_affected'] = len(inconsistency_summary)
        
        # Calculate total issues and severity counts
        total_issues = 0
        for category in ['missing_data', 'duplicates', 'outliers', 'inconsistencies']:
            if 'columns' in issues[category]:
                for item in issues[category]['columns']:
                    severity = item.get('severity', 'low')
                    issues['summary'][f'severity_{severity}'] += 1
                    total_issues += 1
        
        issues['summary']['total_issues'] = total_issues
        
        return issues
    
    except Exception as e:
        error_msg = f"Error identifying data quality issues: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, details={'dataframe_shape': df.shape})


def generate_data_profile(
    df: pd.DataFrame,
    merge_keys: Optional[MergeKeys] = None,
    sample_size: Optional[int] = None
) -> Dict[str, Any]:
    """
    Generate comprehensive data profile.
    
    Args:
        df: DataFrame to profile
        merge_keys: Merge strategy information
        sample_size: Optional sample size for large datasets
        
    Returns:
        Dictionary with comprehensive data profile
        
    Raises:
        DataProcessingError: If profiling fails
    """
    try:
        # Sample data if requested and dataset is large
        original_size = len(df)
        if sample_size and len(df) > sample_size:
            df_sample = df.sample(n=sample_size, random_state=42)
        else:
            df_sample = df
            sample_size = len(df)
        
        profile = {
            'overview': {
                'total_rows': original_size,
                'sample_rows': len(df_sample),
                'total_columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
            },
            'columns': {},
            'data_quality': {},
            'relationships': {}
        }
        
        # Profile each column
        for col in df_sample.columns:
            try:
                col_stats = calculate_column_statistics(df_sample, col, merge_keys)
                profile['columns'][col] = col_stats
            except Exception as e:
                logging.warning(f"Error profiling column {col}: {e}")
                profile['columns'][col] = {'error': str(e)}
        
        # Data quality assessment
        try:
            profile['data_quality'] = identify_data_quality_issues(df_sample, merge_keys)
        except Exception as e:
            logging.warning(f"Error in data quality assessment: {e}")
            profile['data_quality'] = {'error': str(e)}
        
        # Calculate correlations for numeric data
        try:
            corr_matrix, corr_warnings = calculate_correlation_matrix(df_sample, merge_keys)
            if not corr_matrix.empty:
                # Find high correlations (> 0.7 or < -0.7)
                high_corr_pairs = []
                for i in range(len(corr_matrix.columns)):
                    for j in range(i + 1, len(corr_matrix.columns)):
                        corr_val = corr_matrix.iloc[i, j]
                        if abs(corr_val) > 0.7 and not pd.isna(corr_val):
                            high_corr_pairs.append({
                                'column1': corr_matrix.columns[i],
                                'column2': corr_matrix.columns[j],
                                'correlation': float(corr_val)
                            })
                
                profile['relationships']['high_correlations'] = high_corr_pairs
                profile['relationships']['correlation_warnings'] = corr_warnings
        except Exception as e:
            logging.warning(f"Error calculating correlations: {e}")
            profile['relationships']['error'] = str(e)
        
        return profile
    
    except Exception as e:
        error_msg = f"Error generating data profile: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, details={'original_size': original_size})


def is_numeric_dtype(dtype_str: str) -> bool:
    """
    Check if a dtype string represents a numeric type.
    
    Args:
        dtype_str: String representation of the data type (e.g., 'float64', 'int32', 'object')
        
    Returns:
        True if the dtype represents a numeric type, False otherwise
    """
    if not dtype_str:
        return False

    dtype_lower = str(dtype_str).lower()
    return ('int' in dtype_lower or 'float' in dtype_lower) and 'object' not in dtype_lower


def is_numeric_column(data_dir: str, table_name: str, column_name: str, demo_table_name: str, demographics_file_name: str) -> bool:
    """
    Check if a column contains numeric data.
    
    Args:
        data_dir: Directory containing data files
        table_name: Name of the table
        column_name: Name of the column
        demo_table_name: Name of demographics table
        demographics_file_name: Name of demographics file
        
    Returns:
        True if column is numeric, False otherwise
    """
    try:
        # Determine file path
        if table_name == demo_table_name:
            file_path = os.path.join(data_dir, demographics_file_name)
        else:
            file_path = os.path.join(data_dir, f"{table_name}.csv")

        if not os.path.exists(file_path):
            return False

        # Read a small sample to check data type
        with _file_access_lock:
            try:
                df_sample = pd.read_csv(file_path, usecols=[column_name], nrows=100, low_memory=False)
                if column_name not in df_sample.columns:
                    return False

                # Check if column can be converted to numeric
                series = df_sample[column_name].dropna()
                if len(series) == 0:
                    return False

                # Try to convert to numeric
                pd.to_numeric(series, errors='raise')
                return True

            except (ValueError, TypeError, pd.errors.ParserError):
                return False
            except Exception:
                return False

    except Exception:
        return False