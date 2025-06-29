"""
Secure SQL query generation for Basic Data Fusion.

This module provides secure, parameterized query generation functions that
prevent SQL injection attacks and validate all inputs against whitelists.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from core.exceptions import QueryGenerationError, SecurityError

# Exception aliases for this module
QueryError = QueryGenerationError
SQLInjectionError = SecurityError
from data_handling.merge_strategy import MergeKeys
from security_utils import sanitize_sql_identifier, validate_table_name, validate_column_name


def generate_base_query_logic_secure(
    config_params: Dict[str, Any],
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str]
) -> Tuple[str, List[Any]]:
    """
    Generate secure base query logic with parameterized queries.
    
    Args:
        config_params: Configuration parameters
        merge_keys: Merge strategy information
        demographic_filters: Age, substudy, session filters
        behavioral_filters: Phenotypic filters
        tables_to_join: Tables to include in joins
        
    Returns:
        Tuple of (SQL query string, parameters list)
        
    Raises:
        QueryError: If query generation fails
        SecurityError: If security validation fails
    """
    try:
        data_dir = config_params.get('data_dir', 'data')
        demographics_file = config_params.get('demographics_file', 'demographics.csv')
        demo_table_name = demographics_file.replace('.csv', '')
        
        # Get allowed tables from directory scan
        import os
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        allowed_tables = {f.replace('.csv', '') for f in csv_files}
        
        # Validate requested tables
        validated_tables = []
        for table in tables_to_join:
            validated_table = validate_table_name(table, allowed_tables)
            if validated_table:
                validated_tables.append(validated_table)
            else:
                logging.warning(f"Invalid table name rejected: {table}")
        
        if demo_table_name not in validated_tables:
            validated_tables.insert(0, demo_table_name)
        
        # Build secure base table path
        base_table_path = os.path.join(data_dir, demographics_file).replace('\\', '/')
        
        # Start building query with demographics table
        from_clause = f"FROM read_csv_auto(?) AS demo"
        params = [base_table_path]
        
        # Add JOIN clauses for other tables
        join_clauses = []
        for table in validated_tables:
            if table != demo_table_name:
                table_file = f"{table}.csv"
                table_path = os.path.join(data_dir, table_file).replace('\\', '/')
                
                # Sanitize table alias
                safe_alias = sanitize_sql_identifier(table)
                
                # Build JOIN clause with merge key
                merge_column = merge_keys.get_merge_column()
                safe_merge_column = sanitize_sql_identifier(merge_column)
                
                join_clause = f"LEFT JOIN read_csv_auto(?) AS {safe_alias} ON demo.{safe_merge_column} = {safe_alias}.{safe_merge_column}"
                join_clauses.append(join_clause)
                params.append(table_path)
        
        # Combine FROM and JOIN clauses
        from_join_clause = from_clause + (" " + " ".join(join_clauses) if join_clauses else "")
        
        # Build WHERE clause with filters
        where_conditions = []
        
        # Add demographic filters
        if demographic_filters:
            # Age filter
            age_range = demographic_filters.get('age_range')
            if age_range and len(age_range) == 2:
                age_column = config_params.get('age_column', 'age')
                safe_age_column = sanitize_sql_identifier(age_column)
                where_conditions.append(f"demo.{safe_age_column} BETWEEN ? AND ?")
                params.extend([age_range[0], age_range[1]])
            
            # Session filter (for longitudinal data)
            sessions = demographic_filters.get('sessions')
            if sessions and merge_keys.is_longitudinal and merge_keys.session_id:
                safe_session_column = sanitize_sql_identifier(merge_keys.session_id)
                placeholders = ','.join(['?'] * len(sessions))
                where_conditions.append(f"demo.{safe_session_column} IN ({placeholders})")
                params.extend(sessions)
            
            # Study site/substudy filter
            substudies = demographic_filters.get('substudies')
            if substudies and config_params.get('study_site_column'):
                study_site_column = config_params.get('study_site_column')
                safe_study_site_column = sanitize_sql_identifier(study_site_column)
                # Use pattern matching for space-separated study site values
                # Instead of exact IN matching, use LIKE patterns for each substudy
                substudy_conditions = []
                for substudy in substudies:
                    # Match substudy as a word boundary in space-separated list
                    substudy_conditions.append(f"demo.{safe_study_site_column} LIKE ?")
                    params.append(f'%{substudy}%')
                
                if substudy_conditions:
                    # Join with OR since we want rows matching ANY of the selected substudies
                    where_conditions.append(f"({' OR '.join(substudy_conditions)})")
        
        # Add behavioral filters
        for filter_def in behavioral_filters:
            table_name = filter_def.get('table')
            column_name = filter_def.get('column')
            filter_type = filter_def.get('type')
            value = filter_def.get('value')
            
            if not all([table_name, column_name, filter_type, value is not None]):
                continue
            
            # Validate table and column names
            safe_table = validate_table_name(table_name, allowed_tables)
            if not safe_table:
                logging.warning(f"Invalid table name in behavioral filter: {table_name}")
                continue
            
            safe_column = sanitize_sql_identifier(column_name)
            table_alias = 'demo' if safe_table == demo_table_name else sanitize_sql_identifier(safe_table)
            
            if filter_type == 'range':
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    where_conditions.append(f"{table_alias}.{safe_column} BETWEEN ? AND ?")
                    params.extend([value[0], value[1]])
            elif filter_type == 'categorical':
                if isinstance(value, (list, tuple)):
                    placeholders = ','.join(['?'] * len(value))
                    where_conditions.append(f"{table_alias}.{safe_column} IN ({placeholders})")
                    params.extend(value)
        
        # Combine query parts
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
            full_query = f"{from_join_clause} {where_clause}"
        else:
            full_query = from_join_clause
        
        return full_query, params
    
    except Exception as e:
        error_msg = f"Error generating secure base query: {e}"
        logging.error(error_msg)
        raise QueryError(error_msg, details={'tables_to_join': tables_to_join})


def generate_data_query_secure(
    base_query_logic: str,
    params: List[Any],
    selected_tables: List[str],
    selected_columns: Dict[str, List[str]],
    allowed_tables: Set[str]
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """
    Generate secure data query with validated columns.
    Automatically includes all demographics columns when demographics table is selected.
    
    Args:
        base_query_logic: Base FROM/JOIN/WHERE clause
        params: Query parameters
        selected_tables: Tables to include
        selected_columns: Columns to select per table
        allowed_tables: Whitelist of allowed tables
        
    Returns:
        Tuple of (complete SELECT query, parameters)
        
    Raises:
        QueryError: If query generation fails
        SecurityError: If security validation fails
    """
    try:
        if not base_query_logic:
            return None, None
        
        select_clauses = []
        
        # Always include merge key columns
        select_clauses.append("demo.ursi")
        
        # If demographics table is in selected tables, automatically include all demographics columns
        demographics_in_selected = ('demographics' in selected_tables or 
                                   'demographics' in selected_columns or
                                   any('demo' in table for table in selected_tables))
        
        if demographics_in_selected:
            try:
                # Get all available demographics columns from table metadata
                from data_handling.metadata import get_table_info
                from config_manager import get_config
                
                config = get_config()
                table_info = get_table_info(config)
                
                if table_info and len(table_info) >= 2:
                    # table_info structure: (available_tables, demographics_columns, columns_by_table, column_dtypes, ...)
                    demographics_columns = table_info[1] if len(table_info) > 1 else []
                    
                    # Add all demographics columns except merge keys
                    for col_name in demographics_columns:
                        # Skip merge key columns as they're handled separately
                        if (col_name != config.data.primary_id_column and
                            col_name != config.data.session_column and
                            col_name != config.data.composite_id_column):
                            safe_column = sanitize_sql_identifier(col_name)
                            select_clauses.append(f"demo.{safe_column}")
                                
            except Exception as e:
                logging.warning(f"Could not auto-include demographics columns: {e}")
        
        # Add explicitly selected columns with validation
        for table_name, columns in selected_columns.items():
            # Validate table name
            safe_table = validate_table_name(table_name, allowed_tables)
            if not safe_table:
                logging.warning(f"Invalid table name rejected: {table_name}")
                continue
            
            table_alias = 'demo' if safe_table == 'demographics' else sanitize_sql_identifier(safe_table)
            
            for column in columns:
                safe_column = sanitize_sql_identifier(column)
                column_clause = f"{table_alias}.{safe_column}"
                # Avoid duplicates
                if column_clause not in select_clauses:
                    select_clauses.append(column_clause)
        
        if not select_clauses:
            return None, None
        
        select_clause = "SELECT " + ", ".join(select_clauses)
        full_query = f"{select_clause} {base_query_logic}"
        
        return full_query, params
    
    except Exception as e:
        error_msg = f"Error generating secure data query: {e}"
        logging.error(error_msg)
        raise QueryError(error_msg, query=base_query_logic)


def generate_count_query_secure(
    base_query_logic: str,
    params: List[Any],
    merge_keys: MergeKeys
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """
    Generate secure count query for participant counting.
    
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
            count_column = sanitize_sql_identifier(merge_keys.composite_id)
        else:
            count_column = sanitize_sql_identifier(merge_keys.primary_id)
        
        count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as participant_count {base_query_logic}"
        
        return count_query, params
    
    except Exception as e:
        error_msg = f"Error generating secure count query: {e}"
        logging.error(error_msg)
        raise QueryError(error_msg, query=base_query_logic)


def generate_secure_query_suite(
    config_params: Dict[str, Any],
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str],
    selected_columns: Optional[Dict[str, List[str]]] = None
) -> Tuple[str, str, List[Any]]:
    """
    Generate complete secure query suite (data + count queries).
    
    Args:
        config_params: Configuration parameters
        merge_keys: Merge strategy information
        demographic_filters: Age, substudy, session filters
        behavioral_filters: Phenotypic filters
        tables_to_join: Tables to include in joins
        selected_columns: Columns to select per table (optional)
        
    Returns:
        Tuple of (data_query, count_query, params)
        
    Raises:
        QueryError: If query generation fails
        SecurityError: If security validation fails
    """
    try:
        # Generate base query logic
        base_query, params = generate_base_query_logic_secure(
            config_params, merge_keys, demographic_filters, behavioral_filters, tables_to_join
        )
        
        # Get allowed tables
        data_dir = config_params.get('data_dir', 'data')
        import os
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        allowed_tables = {f.replace('.csv', '') for f in csv_files}
        
        # Generate count query
        count_query, count_params = generate_count_query_secure(base_query, params, merge_keys)
        
        # Generate data query if columns are specified
        data_query = None
        if selected_columns:
            data_query, data_params = generate_data_query_secure(
                base_query, params, tables_to_join, selected_columns, allowed_tables
            )
        
        return data_query or "", count_query or "", params
    
    except Exception as e:
        error_msg = f"Error generating secure query suite: {e}"
        logging.error(error_msg)
        raise QueryError(error_msg, details={
            'tables_to_join': tables_to_join,
            'selected_columns': selected_columns
        })


def validate_query_parameters(
    config_params: Dict[str, Any],
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str]
) -> List[str]:
    """
    Validate query parameters for security issues.
    
    Args:
        config_params: Configuration parameters
        demographic_filters: Demographic filters
        behavioral_filters: Behavioral filters
        tables_to_join: Tables to join
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    try:
        # Get allowed tables
        data_dir = config_params.get('data_dir', 'data')
        import os
        if not os.path.exists(data_dir):
            errors.append(f"Data directory does not exist: {data_dir}")
            return errors
        
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        allowed_tables = {f.replace('.csv', '') for f in csv_files}
        
        # Validate table names
        for table in tables_to_join:
            if not validate_table_name(table, allowed_tables):
                errors.append(f"Invalid or disallowed table name: {table}")
        
        # Validate behavioral filter parameters
        for filter_def in behavioral_filters:
            table_name = filter_def.get('table')
            column_name = filter_def.get('column')
            
            if table_name and not validate_table_name(table_name, allowed_tables):
                errors.append(f"Invalid table name in behavioral filter: {table_name}")
            
            if column_name:
                # Basic column name validation
                safe_column = sanitize_sql_identifier(column_name)
                if safe_column != column_name:
                    errors.append(f"Potentially unsafe column name: {column_name}")
        
        return errors
    
    except Exception as e:
        errors.append(f"Error validating query parameters: {e}")
        return errors