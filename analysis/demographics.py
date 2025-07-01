"""
Demographics analysis functions for Basic Data Fusion.

This module provides functions for analyzing demographic data,
calculating breakdowns, and generating demographic summaries.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.database import get_database_manager
from core.exceptions import ValidationError, DatabaseError

# Exception alias for this module
DataProcessingError = ValidationError
from data_handling.merge_strategy import MergeKeys


def calculate_demographics_breakdown(
    config_params: Dict[str, Any],
    merge_keys: MergeKeys,
    base_query_logic: str,
    params: List[Any],
    preserve_original_sessions: bool = False,
    original_sessions: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Calculate demographic breakdown for filtered dataset.
    
    Args:
        config_params: Configuration parameters
        merge_keys: Merge strategy information
        base_query_logic: Base query string
        params: Query parameters
        preserve_original_sessions: Whether to preserve original session list
        original_sessions: Original list of sessions before filtering
        
    Returns:
        Dictionary containing demographic breakdown
        
    Raises:
        DataProcessingError: If demographic calculation fails
    """
    try:
        db_manager = get_database_manager()
        breakdown = {
            'age_range': None,
            'sex_breakdown': {},
            'substudy_sites': [],
            'available_sessions': [],
            'participant_count': 0,
            'error': None
        }
        
        # Get participant count
        if merge_keys.is_longitudinal and merge_keys.composite_id:
            count_column = merge_keys.composite_id
        else:
            count_column = merge_keys.primary_id
        
        count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {base_query_logic}"
        
        try:
            count_result = db_manager.execute_query_single(count_query, params)
            breakdown['participant_count'] = count_result[0] if count_result else 0
        except Exception as e:
            logging.error(f"Error getting participant count: {e}")
            breakdown['error'] = f"Could not calculate participant count: {e}"
            return breakdown
        
        if breakdown['participant_count'] == 0:
            breakdown['error'] = "No participants match the current filters"
            return breakdown
        
        # Calculate age range
        age_column = config_params.get('age_column', 'age')
        age_query = f"SELECT MIN(demo.{age_column}) as min_age, MAX(demo.{age_column}) as max_age {base_query_logic}"
        
        try:
            age_result = db_manager.execute_query_single(age_query, params)
            if age_result and age_result[0] is not None and age_result[1] is not None:
                breakdown['age_range'] = [float(age_result[0]), float(age_result[1])]
        except Exception as e:
            logging.warning(f"Error calculating age range: {e}")
        
        # Calculate sex breakdown
        sex_column = config_params.get('sex_column', 'sex')
        sex_query = f"SELECT demo.{sex_column}, COUNT(DISTINCT demo.{count_column}) as count {base_query_logic} GROUP BY demo.{sex_column}"
        
        try:
            sex_results = db_manager.execute_query(sex_query, params)
            for sex_value, count in sex_results:
                if sex_value is not None:
                    breakdown['sex_breakdown'][str(sex_value)] = int(count)
        except Exception as e:
            logging.warning(f"Error calculating sex breakdown: {e}")
        
        # Get available sessions (for longitudinal data)
        if merge_keys.is_longitudinal and merge_keys.session_id:
            if preserve_original_sessions and original_sessions:
                breakdown['available_sessions'] = original_sessions
            else:
                session_query = f"SELECT DISTINCT demo.{merge_keys.session_id} as session {base_query_logic} ORDER BY session"
                
                try:
                    session_results = db_manager.execute_query(session_query, params)
                    breakdown['available_sessions'] = [str(session[0]) for session in session_results if session[0] is not None]
                except Exception as e:
                    logging.warning(f"Error getting available sessions: {e}")
        
        # Detect multisite data
        study_site_column = config_params.get('study_site_column')
        if study_site_column:
            site_query = f"SELECT DISTINCT demo.{study_site_column} as site {base_query_logic} ORDER BY site"
            
            try:
                site_results = db_manager.execute_query(site_query, params)
                breakdown['substudy_sites'] = [str(site[0]) for site in site_results if site[0] is not None]
            except Exception as e:
                logging.warning(f"Error getting substudy sites: {e}")
        
        return breakdown
    
    except Exception as e:
        error_msg = f"Error calculating demographics breakdown: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, details={
            'merge_keys': merge_keys.to_dict(),
            'participant_count': breakdown.get('participant_count', 0)
        })


def generate_final_data_summary(df: pd.DataFrame, merge_keys: MergeKeys) -> pd.DataFrame:
    """
    Generate descriptive statistics summary for filtered dataset.
    
    Args:
        df: DataFrame to summarize
        merge_keys: Merge strategy information for excluding ID columns
        
    Returns:
        DataFrame with statistical summary
        
    Raises:
        DataProcessingError: If summary generation fails
    """
    try:
        if df.empty:
            return pd.DataFrame({
                'Column': ['No data'],
                'Type': ['N/A'],
                'Count': [0],
                'Missing': [0],
                'Summary': ['Dataset is empty']
            })
        
        # Exclude ID columns from summary
        exclude_columns = {merge_keys.primary_id}
        if merge_keys.session_id:
            exclude_columns.add(merge_keys.session_id)
        if merge_keys.composite_id:
            exclude_columns.add(merge_keys.composite_id)
        
        # Get columns to summarize
        columns_to_summarize = [col for col in df.columns if col not in exclude_columns]
        
        if not columns_to_summarize:
            return pd.DataFrame({
                'Column': ['Only ID columns'],
                'Type': ['N/A'],
                'Count': [len(df)],
                'Missing': [0],
                'Summary': ['No data columns to summarize']
            })
        
        summary_data = []
        
        for col in columns_to_summarize:
            try:
                series = df[col]
                non_null_count = series.count()
                missing_count = series.isna().sum()
                data_type = str(series.dtype)
                
                # Generate summary based on data type
                if pd.api.types.is_numeric_dtype(series):
                    if non_null_count > 0:
                        numeric_series = pd.to_numeric(series, errors='coerce')
                        summary = f"Mean: {numeric_series.mean():.2f}, "
                        summary += f"Median: {numeric_series.median():.2f}, "
                        summary += f"Std: {numeric_series.std():.2f}, "
                        summary += f"Range: [{numeric_series.min():.2f}, {numeric_series.max():.2f}]"
                    else:
                        summary = "All values missing"
                else:
                    # Categorical/string data
                    if non_null_count > 0:
                        unique_count = series.nunique()
                        if unique_count <= 10:
                            # Show value counts for small number of categories
                            value_counts = series.value_counts().head(5)
                            counts_str = ", ".join([f"{val}: {count}" for val, count in value_counts.items()])
                            summary = f"{unique_count} unique values - {counts_str}"
                        else:
                            # Just show unique count for many categories
                            top_value = series.mode().iloc[0] if not series.mode().empty else "N/A"
                            summary = f"{unique_count} unique values, most common: {top_value}"
                    else:
                        summary = "All values missing"
                
                summary_data.append({
                    'Column': col,
                    'Type': data_type,
                    'Count': non_null_count,
                    'Missing': missing_count,
                    'Missing %': f"{(missing_count / len(df) * 100):.1f}%",
                    'Summary': summary
                })
            
            except Exception as e:
                logging.warning(f"Error summarizing column {col}: {e}")
                summary_data.append({
                    'Column': col,
                    'Type': 'Error',
                    'Count': 0,
                    'Missing': len(df),
                    'Missing %': '100.0%',
                    'Summary': f"Error: {e}"
                })
        
        summary_df = pd.DataFrame(summary_data)
        
        # Add overall summary row
        overall_summary = {
            'Column': 'OVERALL',
            'Type': 'Summary',
            'Count': len(df),
            'Missing': 'N/A',
            'Missing %': 'N/A',
            'Summary': f"{len(df)} participants, {len(columns_to_summarize)} data columns"
        }
        
        summary_df = pd.concat([pd.DataFrame([overall_summary]), summary_df], ignore_index=True)
        
        return summary_df
    
    except Exception as e:
        error_msg = f"Error generating data summary: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, details={'dataframe_shape': df.shape})


def has_multisite_data(demographics_columns: List[str], study_site_column: Optional[str] = None) -> bool:
    """
    Detect if dataset contains multisite/multistudy data.
    
    Args:
        demographics_columns: List of column names in demographics table
        study_site_column: Configured study site column name
        
    Returns:
        True if multisite data is detected
    """
    try:
        # Check for configured study site column
        if study_site_column and study_site_column in demographics_columns:
            return True
        
        # Check for common multisite column patterns
        multisite_patterns = [
            'site', 'study_site', 'center', 'location', 'institution',
            'all_studies', 'study', 'cohort', 'batch'
        ]
        
        demographics_lower = [col.lower() for col in demographics_columns]
        
        for pattern in multisite_patterns:
            if any(pattern in col for col in demographics_lower):
                return True
        
        # Check for Rockland-specific multisite indicators
        if detect_rockland_format(demographics_columns):
            return True
        
        return False
    
    except Exception:
        return False


def detect_rockland_format(demographics_columns: List[str]) -> bool:
    """
    Detect Rockland-specific data format.
    
    Args:
        demographics_columns: List of column names in demographics table
        
    Returns:
        True if Rockland format is detected
    """
    try:
        # Rockland-specific column patterns
        rockland_patterns = [
            'rockland', 'all_studies', 'discovery', 'longitudinal_adult',
            'longitudinal_child', 'neurofeedback'
        ]
        
        demographics_lower = [col.lower() for col in demographics_columns]
        
        for pattern in rockland_patterns:
            if any(pattern in col for col in demographics_lower):
                return True
        
        return False
    
    except Exception:
        return False


def get_demographic_summary(
    config_params: Dict[str, Any],
    merge_keys: MergeKeys,
    data_dir: str
) -> Dict[str, Any]:
    """
    Get overall demographic summary from demographics file.
    
    Args:
        config_params: Configuration parameters
        merge_keys: Merge strategy information
        data_dir: Data directory path
        
    Returns:
        Dictionary with demographic summary
    """
    try:
        import os
        
        demographics_file = config_params.get('demographics_file', 'demographics.csv')
        demographics_path = os.path.join(data_dir, demographics_file)
        
        if not os.path.exists(demographics_path):
            return {
                'error': f"Demographics file not found: {demographics_path}",
                'total_participants': 0
            }
        
        # Read demographics file
        df = pd.read_csv(demographics_path, low_memory=False)
        
        summary = {
            'total_participants': len(df),
            'columns': list(df.columns),
            'has_multisite': has_multisite_data(df.columns.tolist(), config_params.get('study_site_column')),
            'is_longitudinal': merge_keys.is_longitudinal,
            'data_structure': 'longitudinal' if merge_keys.is_longitudinal else 'cross-sectional'
        }
        
        # Age summary
        age_column = config_params.get('age_column', 'age')
        if age_column in df.columns:
            age_series = pd.to_numeric(df[age_column], errors='coerce')
            summary['age_range'] = [float(age_series.min()), float(age_series.max())]
            summary['age_mean'] = float(age_series.mean())
        
        # Sex breakdown
        sex_column = config_params.get('sex_column', 'sex')
        if sex_column in df.columns:
            sex_counts = df[sex_column].value_counts().to_dict()
            summary['sex_breakdown'] = {str(k): int(v) for k, v in sex_counts.items()}
        
        # Session information (for longitudinal data)
        if merge_keys.is_longitudinal and merge_keys.session_id and merge_keys.session_id in df.columns:
            session_counts = df[merge_keys.session_id].value_counts().to_dict()
            summary['session_breakdown'] = {str(k): int(v) for k, v in session_counts.items()}
            summary['unique_sessions'] = sorted([str(s) for s in df[merge_keys.session_id].unique() if pd.notna(s)])
        
        return summary
    
    except Exception as e:
        return {
            'error': f"Error getting demographic summary: {e}",
            'total_participants': 0
        }


def validate_demographic_filters(
    demographic_filters: Dict[str, Any],
    config_params: Dict[str, Any],
    merge_keys: MergeKeys
) -> Tuple[bool, List[str]]:
    """
    Validate demographic filter parameters.
    
    Args:
        demographic_filters: Dictionary of demographic filters
        config_params: Configuration parameters
        merge_keys: Merge strategy information
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    try:
        # Validate age range
        age_range = demographic_filters.get('age_range')
        if age_range:
            if not isinstance(age_range, (list, tuple)) or len(age_range) != 2:
                errors.append("Age range must be a list/tuple of exactly 2 values")
            else:
                try:
                    age_min, age_max = float(age_range[0]), float(age_range[1])
                    if age_min >= age_max:
                        errors.append("Age range minimum must be less than maximum")
                    if age_min < 0 or age_max > 150:
                        errors.append("Age range values should be between 0 and 150")
                except (ValueError, TypeError):
                    errors.append("Age range values must be numeric")
        
        # Validate sessions (for longitudinal data)
        sessions = demographic_filters.get('sessions')
        if sessions:
            if not isinstance(sessions, list):
                errors.append("Sessions must be a list")
            elif merge_keys.is_longitudinal and not merge_keys.session_id:
                errors.append("Session filter specified but data is not longitudinal")
        
        # Validate substudies
        substudies = demographic_filters.get('substudies')
        if substudies and not isinstance(substudies, list):
            errors.append("Substudies must be a list")
        
        return len(errors) == 0, errors
    
    except Exception as e:
        errors.append(f"Error validating demographic filters: {e}")
        return False, errors


def has_multisite_data(data_dir: str, demographics_file_name: str) -> bool:
    """
    Check if the dataset has multisite/substudy structure.
    
    Args:
        data_dir: Directory containing data files
        demographics_file_name: Name of demographics file
        
    Returns:
        True if multisite data is detected, False otherwise
    """
    import os
    from threading import Lock
    
    # Use thread lock for file access
    _file_access_lock = Lock()
    
    try:
        file_path = os.path.join(data_dir, demographics_file_name)
        if not os.path.exists(file_path):
            return False

        with _file_access_lock:
            try:
                # Read just the column names first
                df_sample = pd.read_csv(file_path, nrows=0)
                columns = df_sample.columns.str.lower()

                # Look for common multisite indicators
                multisite_indicators = ['site', 'substudy', 'center', 'location', 'cohort']
                return any(indicator in col for col in columns for indicator in multisite_indicators)

            except Exception:
                return False

    except Exception:
        return False


def get_study_site_values(config) -> List[str]:
    """
    Get the actual study site values from the demographics data.
    Handles multiple formats:
    - Space-separated: "Discovery Longitudinal_Adult"
    - Comma-separated in braces: "{Discovery, Longitudinal_Adult}"
    - Single values: "Discovery" or "{Discovery}"
    
    Args:
        config: Configuration object
        
    Returns:
        List of unique study site values found in the data
    """
    import os
    import re
    
    try:
        if not config.STUDY_SITE_COLUMN:
            return []

        demographics_path = os.path.join(config.data.data_dir, config.data.demographics_file)
        if not os.path.exists(demographics_path):
            return []

        # Read the demographics file and extract unique study site values
        df = pd.read_csv(demographics_path)
        if config.STUDY_SITE_COLUMN not in df.columns:
            return []

        all_sites = set()
        unique_site_entries = df[config.STUDY_SITE_COLUMN].dropna().unique()

        for entry in unique_site_entries:
            entry_str = str(entry).strip()
            if not entry_str:
                continue

            # Remove outer curly braces and quotes
            cleaned = re.sub(r'^["\'{]*\{?|[}\'"]*$', '', entry_str)
            cleaned = re.sub(r'^\{|[}]*$', '', cleaned)
            cleaned = cleaned.strip()

            # Check if this is comma-separated or space-separated
            if ',' in cleaned:
                # Comma-separated format: "Discovery, Longitudinal_Adult"
                parts = [part.strip().strip('"\'') for part in cleaned.split(',')]
                all_sites.update(part for part in parts if part)
            elif ' ' in cleaned and not cleaned.endswith(' '):
                # Space-separated format: "Discovery Longitudinal_Adult"
                # But not just trailing spaces
                parts = [part.strip() for part in cleaned.split()]
                all_sites.update(part for part in parts if part)
            else:
                # Single value: "Discovery" or just trailing spaces
                clean_site = cleaned.strip().strip('"\'')
                if clean_site:
                    all_sites.add(clean_site)

        return sorted(all_sites)

    except Exception as e:
        logging.warning(f"Could not extract study site values: {e}")
        return []