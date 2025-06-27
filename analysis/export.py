"""
Data export and transformation functions for Basic Data Fusion.

This module provides functions for data export, longitudinal data transformation,
and export format handling.
"""

import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.exceptions import ValidationError

# Exception alias for this module
DataProcessingError = ValidationError
from data_handling.merge_strategy import MergeKeys
from file_handling.security import secure_filename


def enwiden_longitudinal_data(
    df: pd.DataFrame, 
    merge_keys: MergeKeys, 
    consolidate_baseline: bool = False
) -> pd.DataFrame:
    """
    Pivot longitudinal data from long to wide format.
    
    Transforms columns like 'age' into 'age_BAS1', 'age_BAS2', etc.
    based on session values.
    
    Args:
        df: DataFrame with longitudinal data
        merge_keys: Merge strategy information
        consolidate_baseline: Whether to consolidate baseline sessions
        
    Returns:
        Wide-format DataFrame
        
    Raises:
        DataProcessingError: If transformation fails
    """
    try:
        if not merge_keys.is_longitudinal or not merge_keys.session_id:
            logging.warning("Data is not longitudinal or session column not found. Returning original DataFrame.")
            return df
        
        if merge_keys.session_id not in df.columns:
            raise DataProcessingError(f"Session column '{merge_keys.session_id}' not found in DataFrame")
        
        if merge_keys.primary_id not in df.columns:
            raise DataProcessingError(f"Primary ID column '{merge_keys.primary_id}' not found in DataFrame")
        
        # Get session values and sort them
        unique_sessions = sorted(df[merge_keys.session_id].dropna().unique())
        
        if len(unique_sessions) <= 1:
            logging.warning("Only one unique session found. Returning original DataFrame.")
            return df
        
        # Identify columns to transform (exclude ID columns)
        exclude_columns = {merge_keys.primary_id, merge_keys.session_id}
        if merge_keys.composite_id and merge_keys.composite_id in df.columns:
            exclude_columns.add(merge_keys.composite_id)
        
        # Separate static columns (same across all sessions) from dynamic columns
        static_columns = []
        dynamic_columns = []
        
        for col in df.columns:
            if col in exclude_columns:
                continue
            
            # Check if column has different values across sessions for same participant
            grouped = df.groupby(merge_keys.primary_id)[col].nunique()
            # If max unique values per participant is > 1, it's dynamic
            if grouped.max() > 1:
                dynamic_columns.append(col)
            else:
                static_columns.append(col)
        
        # Start with unique participants
        result_df = df[[merge_keys.primary_id]].drop_duplicates().reset_index(drop=True)
        
        # Add static columns (take first non-null value for each participant)
        for col in static_columns:
            static_values = df.groupby(merge_keys.primary_id)[col].first()
            result_df = result_df.merge(
                static_values.to_frame().reset_index(),
                on=merge_keys.primary_id,
                how='left'
            )
        
        # Transform dynamic columns
        for col in dynamic_columns:
            # Create pivot table for this column
            pivot_data = df.pivot_table(
                index=merge_keys.primary_id,
                columns=merge_keys.session_id,
                values=col,
                aggfunc='first'  # Take first value if duplicates
            ).reset_index()
            
            # Rename columns to include session suffix
            new_columns = {merge_keys.primary_id: merge_keys.primary_id}
            for session in unique_sessions:
                if session in pivot_data.columns:
                    # Convert session to string and create column name
                    session_str = str(session)
                    # Map session numbers to labels (BAS1, BAS2, etc.)
                    if session_str in ['1', '1.0']:
                        session_label = 'BAS1'
                    elif session_str in ['2', '2.0']:
                        session_label = 'BAS2'
                    elif session_str in ['3', '3.0']:
                        session_label = 'BAS3'
                    else:
                        session_label = f"SES{session_str}"
                    
                    new_columns[session] = f"{col}_{session_label}"
            
            pivot_data.rename(columns=new_columns, inplace=True)
            
            # Merge with result
            result_df = result_df.merge(pivot_data, on=merge_keys.primary_id, how='left')
        
        # Consolidate baseline columns if requested
        if consolidate_baseline:
            result_df = consolidate_baseline_columns(result_df)
        
        logging.info(f"Successfully enwidened data: {len(df)} rows -> {len(result_df)} rows, "
                    f"{len(df.columns)} columns -> {len(result_df.columns)} columns")
        
        return result_df
    
    except Exception as e:
        error_msg = f"Error enwidening longitudinal data: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, details={'original_shape': df.shape})


def consolidate_baseline_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolidate multiple baseline sessions (BAS1, BAS2, BAS3) into single columns.
    
    Takes the highest numbered session value where available.
    
    Args:
        df: DataFrame with baseline columns to consolidate
        
    Returns:
        DataFrame with consolidated baseline columns
        
    Raises:
        DataProcessingError: If consolidation fails
    """
    try:
        result_df = df.copy()
        
        # Find all baseline column patterns
        baseline_pattern = re.compile(r'^(.+)_(BAS[123])$')
        baseline_groups = {}
        
        for col in df.columns:
            match = baseline_pattern.match(col)
            if match:
                base_name = match.group(1)
                session = match.group(2)
                
                if base_name not in baseline_groups:
                    baseline_groups[base_name] = {}
                baseline_groups[base_name][session] = col
        
        # Consolidate each baseline group
        for base_name, sessions in baseline_groups.items():
            if len(sessions) > 1:  # Only consolidate if multiple sessions exist
                # Create consolidated column
                consolidated_col = f"{base_name}_baseline"
                
                # Priority order: BAS3 > BAS2 > BAS1
                priority_order = ['BAS3', 'BAS2', 'BAS1']
                
                # Start with all NaN
                result_df[consolidated_col] = pd.NA
                
                # Fill in values in reverse priority order (so higher priority overwrites)
                for session in reversed(priority_order):
                    if session in sessions:
                        col_name = sessions[session]
                        # Update where consolidated is still NaN and current column has value
                        mask = result_df[consolidated_col].isna() & result_df[col_name].notna()
                        result_df.loc[mask, consolidated_col] = result_df.loc[mask, col_name]
                
                # Remove original baseline columns
                for session_col in sessions.values():
                    result_df.drop(columns=[session_col], inplace=True)
        
        return result_df
    
    except Exception as e:
        error_msg = f"Error consolidating baseline columns: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg)


def generate_export_filename(
    selected_tables: List[str], 
    demographics_table_name: str, 
    is_enwidened: bool = False
) -> str:
    """
    Create smart filename for CSV exports.
    
    Args:
        selected_tables: List of selected table names
        demographics_table_name: Name of demographics table
        is_enwidened: Whether data is in wide format
        
    Returns:
        Secure filename string
    """
    try:
        # Filter out demographics table from the list for filename
        non_demo_tables = [t for t in selected_tables if t != demographics_table_name]
        
        # Create base filename
        if not non_demo_tables:
            base_name = "demographics_only"
        elif len(non_demo_tables) == 1:
            base_name = non_demo_tables[0]
        elif len(non_demo_tables) <= 3:
            base_name = "_".join(non_demo_tables)
        else:
            base_name = f"{non_demo_tables[0]}_and_{len(non_demo_tables)-1}_more"
        
        # Add format indicator
        if is_enwidened:
            base_name += "_wide"
        else:
            base_name += "_long"
        
        # Add timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{base_name}_{timestamp}.csv"
        
        # Ensure filename is secure
        return secure_filename(filename)
    
    except Exception as e:
        # Fallback to simple timestamp-based name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"data_export_{timestamp}.csv"


def validate_export_data(df: pd.DataFrame, merge_keys: MergeKeys) -> Tuple[bool, List[str]]:
    """
    Validate data before export.
    
    Args:
        df: DataFrame to validate
        merge_keys: Merge strategy information
        
    Returns:
        Tuple of (is_valid, list of warning messages)
    """
    warnings = []
    
    try:
        # Check if DataFrame is empty
        if df.empty:
            warnings.append("Export DataFrame is empty")
            return False, warnings
        
        # Check for required ID column
        if merge_keys.primary_id not in df.columns:
            warnings.append(f"Primary ID column '{merge_keys.primary_id}' missing from export data")
        
        # Check for completely empty columns
        empty_columns = [col for col in df.columns if df[col].isna().all()]
        if empty_columns:
            warnings.append(f"Export contains completely empty columns: {', '.join(empty_columns[:5])}")
        
        # Check for very sparse columns (>95% missing)
        sparse_columns = []
        for col in df.columns:
            missing_pct = df[col].isna().sum() / len(df) * 100
            if missing_pct > 95:
                sparse_columns.append(f"{col} ({missing_pct:.1f}% missing)")
        
        if sparse_columns:
            warnings.append(f"Export contains very sparse columns: {', '.join(sparse_columns[:3])}")
        
        # Check for duplicate participants (if this should be unique)
        if merge_keys.primary_id in df.columns:
            duplicates = df[merge_keys.primary_id].duplicated().sum()
            if duplicates > 0:
                warnings.append(f"Export contains {duplicates} duplicate participant(s)")
        
        # Check for very wide data
        if len(df.columns) > 1000:
            warnings.append(f"Export has many columns ({len(df.columns)}) - file may be large")
        
        return True, warnings
    
    except Exception as e:
        warnings.append(f"Error validating export data: {e}")
        return False, warnings


def prepare_export_data(
    df: pd.DataFrame, 
    merge_keys: MergeKeys, 
    enwiden: bool = False,
    consolidate_baseline: bool = False,
    remove_empty_columns: bool = True
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Prepare data for export with optional transformations.
    
    Args:
        df: Source DataFrame
        merge_keys: Merge strategy information
        enwiden: Whether to transform to wide format
        consolidate_baseline: Whether to consolidate baseline sessions
        remove_empty_columns: Whether to remove completely empty columns
        
    Returns:
        Tuple of (prepared DataFrame, list of processing messages)
    """
    messages = []
    result_df = df.copy()
    
    try:
        # Remove empty columns if requested
        if remove_empty_columns:
            empty_columns = [col for col in result_df.columns if result_df[col].isna().all()]
            if empty_columns:
                result_df.drop(columns=empty_columns, inplace=True)
                messages.append(f"Removed {len(empty_columns)} empty column(s)")
        
        # Apply longitudinal transformation if requested
        if enwiden and merge_keys.is_longitudinal:
            original_shape = result_df.shape
            result_df = enwiden_longitudinal_data(result_df, merge_keys, consolidate_baseline)
            messages.append(f"Transformed to wide format: {original_shape} -> {result_df.shape}")
        elif enwiden and not merge_keys.is_longitudinal:
            messages.append("Skipped wide format transformation (data is not longitudinal)")
        
        # Sort by primary ID for consistent output
        if merge_keys.primary_id in result_df.columns:
            result_df.sort_values(merge_keys.primary_id, inplace=True)
            result_df.reset_index(drop=True, inplace=True)
        
        # Final validation
        is_valid, warnings = validate_export_data(result_df, merge_keys)
        if warnings:
            messages.extend([f"Warning: {w}" for w in warnings])
        
        if not is_valid:
            raise DataProcessingError("Export data validation failed", details={'warnings': warnings})
        
        return result_df, messages
    
    except DataProcessingError:
        raise
    except Exception as e:
        error_msg = f"Error preparing export data: {e}"
        raise DataProcessingError(error_msg, details={'original_shape': df.shape})


def estimate_export_size(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Estimate the size and characteristics of export data.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with size estimates and data characteristics
    """
    try:
        # Basic dimensions
        info = {
            'rows': len(df),
            'columns': len(df.columns),
            'total_cells': len(df) * len(df.columns)
        }
        
        # Memory usage estimation
        memory_usage = df.memory_usage(deep=True).sum()
        info['memory_usage_mb'] = memory_usage / (1024 * 1024)
        
        # Estimate CSV file size (rough approximation)
        # Average characters per cell + delimiters + newlines
        avg_chars_per_cell = 10  # Conservative estimate
        estimated_csv_size = info['total_cells'] * avg_chars_per_cell
        info['estimated_csv_size_mb'] = estimated_csv_size / (1024 * 1024)
        
        # Data completeness
        total_values = info['total_cells']
        missing_values = df.isna().sum().sum()
        info['completeness_pct'] = ((total_values - missing_values) / total_values * 100) if total_values > 0 else 0
        
        # Column type breakdown
        dtypes = df.dtypes.value_counts().to_dict()
        info['column_types'] = {str(k): v for k, v in dtypes.items()}
        
        return info
    
    except Exception as e:
        return {
            'error': f"Could not estimate export size: {e}",
            'rows': len(df) if df is not None else 0,
            'columns': len(df.columns) if df is not None else 0
        }