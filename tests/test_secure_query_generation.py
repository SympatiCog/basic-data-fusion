"""
Secure SQL query generation functions that prevent injection attacks.
"""
import os
import sys
import logging
from typing import Any, Dict, List, Optional, Tuple, Set

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import Config, MergeKeys, get_table_alias
from security_utils import (
    sanitize_sql_identifier,
    validate_table_name,
    validate_column_name,
    build_safe_table_alias,
    validate_numeric_value,
    validate_string_value,
    escape_file_path
)


def secure_generate_base_query_logic(
    config: Config,
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str],
    allowed_tables: Optional[Set[str]] = None,
    allowed_columns: Optional[Dict[str, Set[str]]] = None
) -> Tuple[str, List[Any]]:
    """
    SECURE version of generate_base_query_logic that prevents SQL injection.
    
    Args:
        config: Configuration object
        merge_keys: Merge key information
        demographic_filters: Demographic filter parameters
        behavioral_filters: Behavioral filter parameters
        tables_to_join: List of table names to join
        allowed_tables: Whitelist of allowed table names
        allowed_columns: Whitelist of allowed columns per table
        
    Returns:
        Tuple of (SQL query string, parameter list)
        
    Raises:
        ValueError: If invalid/malicious input is detected
    """
    # Get demographics table name safely
    demographics_table_name = config.get_demographics_table_name()
    
    # If no whitelist provided, create a basic one from known files
    if allowed_tables is None:
        allowed_tables = {demographics_table_name}
        try:
            # Scan for CSV files to build whitelist
            csv_files = [f[:-4] for f in os.listdir(config.DATA_DIR) 
                        if f.endswith('.csv')]
            allowed_tables.update(csv_files)
        except Exception:
            # If we can't scan, only allow demographics
            pass
    
    # Validate and sanitize tables to join
    safe_tables_to_join = []
    for table in tables_to_join:
        safe_table = validate_table_name(table, allowed_tables)
        if safe_table:
            safe_tables_to_join.append(safe_table)
        else:
            logging.warning(f"Rejecting invalid table name: {table}")
    
    if not safe_tables_to_join:
        safe_tables_to_join = [demographics_table_name]
    
    # Build secure file path for demographics table
    demo_file = sanitize_sql_identifier(config.DEMOGRAPHICS_FILE)
    if not demo_file.endswith('.csv'):
        demo_file += '.csv'
    
    base_table_path = os.path.join(config.DATA_DIR, demo_file).replace('\\', '/')
    base_table_path = escape_file_path(base_table_path)
    
    from_join_clause = f"FROM read_csv_auto('{base_table_path}') AS demo"
    
    # Collect all tables needed (including from behavioral filters)
    all_join_tables = set(safe_tables_to_join)
    for bf in behavioral_filters:
        if bf.get('table'):
            safe_table = validate_table_name(bf['table'], allowed_tables)
            if safe_table:
                all_join_tables.add(safe_table)
            else:
                logging.warning(f"Rejecting behavioral filter with invalid table: {bf.get('table')}")
    
    # Build JOIN clauses safely
    for table in all_join_tables:
        if table == demographics_table_name:
            continue
        
        # Build safe table path
        table_file = sanitize_sql_identifier(table)
        if not table_file.endswith('.csv'):
            table_file += '.csv'
        
        table_path = os.path.join(config.DATA_DIR, table_file).replace('\\', '/')
        table_path = escape_file_path(table_path)
        
        # Build safe table alias
        table_alias = build_safe_table_alias(table, demographics_table_name)
        
        # Safe merge column
        merge_column = sanitize_sql_identifier(merge_keys.get_merge_column())
        
        from_join_clause += f"""
        LEFT JOIN read_csv_auto('{table_path}') AS {table_alias}
        ON demo."{merge_column}" = {table_alias}."{merge_column}" """
    
    where_clauses: List[str] = []
    params: List[Any] = []
    
    # Secure demographic filters
    demographics_path = os.path.join(config.DATA_DIR, config.DEMOGRAPHICS_FILE)
    available_demo_columns = []
    try:
        df_headers = pd.read_csv(demographics_path, nrows=0)
        available_demo_columns = [sanitize_sql_identifier(col) for col in df_headers.columns.tolist()]
    except Exception as e:
        logging.warning(f"Could not read demographics headers: {e}")
        available_demo_columns = []
    
    # Age filtering with validation
    if demographic_filters.get('age_range'):
        age_col = sanitize_sql_identifier(config.AGE_COLUMN)
        if age_col in available_demo_columns:
            age_range = demographic_filters['age_range']
            if (len(age_range) == 2 and 
                validate_numeric_value(age_range[0]) and 
                validate_numeric_value(age_range[1])):
                where_clauses.append(f"demo.\"{age_col}\" BETWEEN ? AND ?")
                params.extend([float(age_range[0]), float(age_range[1])])
            else:
                logging.warning("Invalid age range values")
    
    # Substudy filtering with validation
    if demographic_filters.get('substudies'):
        study_col = sanitize_sql_identifier(config.STUDY_SITE_COLUMN or 'all_studies')
        if study_col in available_demo_columns:
            substudies = demographic_filters['substudies']
            valid_substudies = [s for s in substudies 
                             if isinstance(s, str) and validate_string_value(s, 100)]
            if valid_substudies:
                substudy_conditions = []
                for substudy in valid_substudies:
                    substudy_conditions.append(f"demo.\"{study_col}\" LIKE ?")
                    params.append(f'%{substudy}%')
                if substudy_conditions:
                    where_clauses.append(f"({' OR '.join(substudy_conditions)})")
    
    # Session filtering with validation
    if demographic_filters.get('sessions') and merge_keys.session_id:
        session_col = sanitize_sql_identifier(merge_keys.session_id)
        sessions = demographic_filters['sessions']
        valid_sessions = [s for s in sessions 
                         if isinstance(s, str) and validate_string_value(s, 50)]
        if valid_sessions:
            session_placeholders = ', '.join(['?' for _ in valid_sessions])
            # Only filter on demo table to be safe
            where_clauses.append(f"demo.\"{session_col}\" IN ({session_placeholders})")
            params.extend(valid_sessions)
    
    # Secure behavioral filters
    for b_filter in behavioral_filters:
        if not (b_filter.get('table') and b_filter.get('column')):
            continue
        
        # Validate table name
        safe_table = validate_table_name(b_filter['table'], allowed_tables)
        if not safe_table:
            logging.warning(f"Skipping filter with invalid table: {b_filter.get('table')}")
            continue
        
        # Build safe table alias
        table_alias = build_safe_table_alias(safe_table, demographics_table_name)
        
        # Validate column name
        safe_column = sanitize_sql_identifier(b_filter['column'])
        if not safe_column:
            logging.warning(f"Skipping filter with invalid column: {b_filter.get('column')}")
            continue
        
        # Handle different filter types safely
        filter_type = b_filter.get('filter_type')
        
        if filter_type == 'numeric':
            min_val = b_filter.get('min_val')
            max_val = b_filter.get('max_val')
            
            if (min_val is not None and max_val is not None and
                validate_numeric_value(min_val) and validate_numeric_value(max_val)):
                where_clauses.append(f"{table_alias}.\"{safe_column}\" BETWEEN ? AND ?")
                params.extend([float(min_val), float(max_val)])
            else:
                logging.warning(f"Invalid numeric filter values: {min_val}, {max_val}")
                
        elif filter_type == 'categorical':
            selected_values = b_filter.get('selected_values', [])
            if selected_values:
                # Validate all values
                valid_values = []
                for val in selected_values:
                    if validate_string_value(str(val), 200):
                        valid_values.append(str(val))
                
                if valid_values:
                    placeholders = ', '.join(['?' for _ in valid_values])
                    where_clauses.append(f"{table_alias}.\"{safe_column}\" IN ({placeholders})")
                    params.extend(valid_values)
    
    # Build final query
    where_clause_str = ""
    if where_clauses:
        where_clause_str = "\nWHERE " + " AND ".join(where_clauses)
    
    return f"{from_join_clause}{where_clause_str}", params


def secure_generate_data_query(
    base_query_logic: str,
    params: List[Any],
    selected_tables: List[str],
    selected_columns: Dict[str, List[str]],
    allowed_tables: Optional[Set[str]] = None,
    allowed_columns: Optional[Dict[str, Set[str]]] = None
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """
    Secure version of generate_data_query that prevents injection.
    """
    if not base_query_logic:
        return None, None
    
    # Always select all columns from demographics (safe)
    select_clause = "SELECT demo.*"
    
    # Validate and add columns from other tables
    for table, columns in selected_columns.items():
        # Validate table name
        if allowed_tables:
            safe_table = validate_table_name(table, allowed_tables)
        else:
            safe_table = sanitize_sql_identifier(table)
        
        if not safe_table or safe_table not in selected_tables:
            continue
        
        # Build safe table alias
        table_alias = build_safe_table_alias(safe_table, "demographics")
        
        for col in columns:
            # Validate column name
            if allowed_columns and table in allowed_columns:
                safe_col = validate_column_name(col, allowed_columns[table])
            else:
                safe_col = sanitize_sql_identifier(col)
            
            if safe_col:
                select_clause += f', {table_alias}."{safe_col}"'
    
    return f"{select_clause} {base_query_logic}", params


def secure_generate_count_query(
    base_query_logic: str,
    params: List[Any],
    merge_keys: MergeKeys
) -> Tuple[Optional[str], Optional[List[Any]]]:
    """
    Secure version of generate_count_query that prevents injection.
    """
    if not base_query_logic:
        return None, None
    
    # Use sanitized merge column
    merge_column = sanitize_sql_identifier(merge_keys.get_merge_column())
    select_clause = f'SELECT COUNT(DISTINCT demo."{merge_column}")'
    
    return f"{select_clause} {base_query_logic}", params


# Test the secure functions
if __name__ == "__main__":
    import tempfile
    import pytest
    
    def test_secure_query_generation():
        """Test that secure functions prevent injection."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test config
            config = Config()
            config.DATA_DIR = temp_dir
            config.DEMOGRAPHICS_FILE = "demographics.csv"
            config.PRIMARY_ID_COLUMN = "ursi"
            
            # Create test CSV files
            demo_data = pd.DataFrame({
                "ursi": ["001", "002"],
                "age": [25, 30]
            })
            demo_data.to_csv(os.path.join(temp_dir, "demographics.csv"), index=False)
            
            behavioral_data = pd.DataFrame({
                "ursi": ["001", "002"],
                "score": [10, 20]
            })
            behavioral_data.to_csv(os.path.join(temp_dir, "behavioral.csv"), index=False)
            
            merge_keys = MergeKeys(primary_id="ursi", is_longitudinal=False)
            
            # Test malicious table name
            malicious_tables = ["'; DROP TABLE users; --"]
            allowed_tables = {"demographics", "behavioral"}
            
            query, params = secure_generate_base_query_logic(
                config, merge_keys, {}, [], malicious_tables, allowed_tables
            )
            
            # Should not contain injection
            assert "DROP" not in query.upper()
            assert "--" not in query
            # Should only contain safe demographics table
            assert "demo" in query.lower()
            print("✓ Secure query generation prevents table name injection")
            
            # Test malicious column name
            malicious_filter = {
                'table': 'behavioral',
                'column': "'; DROP TABLE users; --",
                'filter_type': 'categorical',
                'selected_values': ['test']
            }
            
            query2, params2 = secure_generate_base_query_logic(
                config, merge_keys, {}, [malicious_filter], 
                ["demographics"], allowed_tables
            )
            
            print("Column test query:", repr(query2))
            
            # Should not contain injection
            assert "DROP" not in query2.upper()
            assert "--" not in query2
            print("✓ Secure query generation prevents column name injection")
            
    test_secure_query_generation()