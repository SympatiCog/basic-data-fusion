"""
Legacy SQL query generation for Basic Data Fusion.

This module contains the legacy query generation functions that have
SQL injection vulnerabilities. These are maintained for backward compatibility
but should be replaced with secure versions.

WARNING: These functions are deprecated and have security vulnerabilities.
Use query_secure.py functions instead.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from core.exceptions import QueryError
from data.merge_strategy import MergeKeys


def generate_base_query_logic(
    config_params: Dict[str, Any],
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str]
) -> Tuple[str, List[Any]]:
    """
    Legacy base query generation with SQL injection vulnerabilities.
    
    WARNING: This function has SQL injection vulnerabilities and should not be used.
    Use generate_base_query_logic_secure() instead.
    
    Args:
        config_params: Configuration parameters
        merge_keys: Merge strategy information
        demographic_filters: Age, substudy, session filters
        behavioral_filters: Phenotypic filters
        tables_to_join: Tables to include in joins
        
    Returns:
        Tuple of (SQL query string, parameters list)
        
    Deprecated:
        This function is deprecated due to SQL injection vulnerabilities.
    """
    logging.warning("Using deprecated generate_base_query_logic with SQL injection vulnerabilities")
    
    try:
        data_dir = config_params.get('data_dir', 'data')
        demographics_file = config_params.get('demographics_file', 'demographics.csv')
        demo_table_name = demographics_file.replace('.csv', '')
        
        # Build base table path (VULNERABLE: Direct string interpolation)
        base_table_path = os.path.join(data_dir, demographics_file).replace('\\', '/')
        from_join_clause = f"FROM read_csv_auto('{base_table_path}') AS demo"
        
        # Add other tables (VULNERABLE: No validation)
        for table in tables_to_join:
            if table != demo_table_name:
                table_file = f"{table}.csv"
                table_path = os.path.join(data_dir, table_file).replace('\\', '/')
                
                # VULNERABLE: Direct string interpolation without sanitization
                merge_column = merge_keys.get_merge_column()
                from_join_clause += f" LEFT JOIN read_csv_auto('{table_path}') AS {table} ON demo.{merge_column} = {table}.{merge_column}"
        
        # Build WHERE clause (VULNERABLE: String concatenation)
        where_conditions = []
        params = []
        
        # Add demographic filters
        if demographic_filters:
            age_range = demographic_filters.get('age_range')
            if age_range and len(age_range) == 2:
                age_column = config_params.get('age_column', 'age')
                # VULNERABLE: Direct column name insertion
                where_conditions.append(f"demo.{age_column} BETWEEN ? AND ?")
                params.extend([age_range[0], age_range[1]])
            
            sessions = demographic_filters.get('sessions')
            if sessions and merge_keys.is_longitudinal and merge_keys.session_id:
                # VULNERABLE: Direct column name insertion
                placeholders = ','.join(['?'] * len(sessions))
                where_conditions.append(f"demo.{merge_keys.session_id} IN ({placeholders})")
                params.extend(sessions)
        
        # Add behavioral filters (VULNERABLE: No input validation)
        for filter_def in behavioral_filters:
            table_name = filter_def.get('table')
            column_name = filter_def.get('column')
            filter_type = filter_def.get('type')
            value = filter_def.get('value')
            
            if not all([table_name, column_name, filter_type, value is not None]):
                continue
            
            # VULNERABLE: Direct table and column name insertion
            table_alias = 'demo' if table_name == demo_table_name else table_name
            
            if filter_type == 'range':
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    where_conditions.append(f"{table_alias}.{column_name} BETWEEN ? AND ?")
                    params.extend([value[0], value[1]])
            elif filter_type == 'categorical':
                if isinstance(value, (list, tuple)):
                    placeholders = ','.join(['?'] * len(value))
                    where_conditions.append(f"{table_alias}.{column_name} IN ({placeholders})")
                    params.extend(value)
        
        # Combine query parts
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
            full_query = f"{from_join_clause} {where_clause}"
        else:
            full_query = from_join_clause
        
        return full_query, params
    
    except Exception as e:
        error_msg = f"Error generating legacy base query: {e}"
        logging.error(error_msg)
        raise QueryError(error_msg, details={'tables_to_join': tables_to_join})


def generate_data_query(
    base_query_logic: str,
    params: List[Any],
    selected_tables: List[str],
    selected_columns: Dict[str, List[str]]
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """
    Legacy data query generation with SQL injection vulnerabilities.
    
    WARNING: This function has SQL injection vulnerabilities and should not be used.
    Use generate_data_query_secure() instead.
    
    Args:
        base_query_logic: Base FROM/JOIN/WHERE clause
        params: Query parameters
        selected_tables: Tables to include
        selected_columns: Columns to select per table
        
    Returns:
        Tuple of (complete SELECT query, parameters)
        
    Deprecated:
        This function is deprecated due to SQL injection vulnerabilities.
    """
    logging.warning("Using deprecated generate_data_query with SQL injection vulnerabilities")
    
    try:
        if not base_query_logic:
            return None, None
        
        select_clauses = []
        
        # Always include merge key (VULNERABLE: No sanitization)
        select_clauses.append("demo.ursi")
        
        # Add selected columns (VULNERABLE: No validation)
        for table_name, columns in selected_columns.items():
            table_alias = 'demo' if table_name == 'demographics' else table_name
            
            for column in columns:
                # VULNERABLE: Direct string interpolation
                select_clauses.append(f"{table_alias}.{column}")
        
        if not select_clauses:
            return None, None
        
        select_clause = "SELECT " + ", ".join(select_clauses)
        full_query = f"{select_clause} {base_query_logic}"
        
        return full_query, params
    
    except Exception as e:
        error_msg = f"Error generating legacy data query: {e}"
        logging.error(error_msg)
        raise QueryError(error_msg, query=base_query_logic)


def generate_count_query(
    base_query_logic: str,
    params: List[Any],
    merge_keys: MergeKeys
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """
    Generate count query for participant counting.
    
    This function is used by both secure and legacy systems.
    
    Args:
        base_query_logic: Base FROM/JOIN/WHERE clause
        params: Query parameters
        merge_keys: Merge strategy information
        
    Returns:
        Tuple of (COUNT query, parameters)
        
    Raises:
        QueryError: If query generation fails
    """
    try:
        if not base_query_logic:
            return None, None
        
        # Use appropriate count column based on merge strategy
        if merge_keys.is_longitudinal and merge_keys.composite_id:
            count_column = merge_keys.composite_id
        else:
            count_column = merge_keys.primary_id
        
        # Note: This could be vulnerable in legacy context but is used by both systems
        count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as participant_count {base_query_logic}"
        
        return count_query, params
    
    except Exception as e:
        error_msg = f"Error generating count query: {e}"
        logging.error(error_msg)
        raise QueryError(error_msg, query=base_query_logic)


def get_table_alias(table_name: str, demo_table_name: str) -> str:
    """
    Get table alias for SQL queries.
    
    Args:
        table_name: Name of the table
        demo_table_name: Name of the demographics table
        
    Returns:
        Table alias ('demo' for demographics, otherwise table name)
    """
    return 'demo' if table_name == demo_table_name else table_name