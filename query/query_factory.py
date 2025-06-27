"""
Query factory for Basic Data Fusion.

This module provides a factory pattern for choosing between secure and legacy
query generation functions based on configuration or user preference.
"""

import logging
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.exceptions import QueryError, SecurityError
from data_handling.merge_strategy import MergeKeys

from .query_secure import (
    generate_base_query_logic_secure,
    generate_data_query_secure,
    generate_count_query_secure,
    generate_secure_query_suite,
    validate_query_parameters as validate_secure
)
from .query_builder import (
    generate_base_query_logic,
    generate_data_query,
    generate_count_query
)


class QueryMode(Enum):
    """Enumeration for query generation modes."""
    SECURE = "secure"
    LEGACY = "legacy"
    AUTO = "auto"


class QueryFactory:
    """
    Factory class for query generation functions.
    
    Provides a unified interface for both secure and legacy query generation
    with automatic mode selection and validation.
    """
    
    def __init__(self, mode: QueryMode = QueryMode.SECURE, strict_validation: bool = True):
        """
        Initialize the query factory.
        
        Args:
            mode: Query generation mode (secure/legacy/auto)
            strict_validation: Whether to enforce strict validation
        """
        self.mode = mode
        self.strict_validation = strict_validation
        
        if mode == QueryMode.LEGACY:
            logging.warning("QueryFactory initialized in LEGACY mode with security vulnerabilities")
    
    def get_base_query_logic(
        self,
        config_params: Dict[str, Any],
        merge_keys: MergeKeys,
        demographic_filters: Dict[str, Any],
        behavioral_filters: List[Dict[str, Any]],
        tables_to_join: List[str]
    ) -> Tuple[str, List[Any]]:
        """
        Generate base query logic using the configured mode.
        
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
            SecurityError: If security validation fails in strict mode
        """
        mode = self._determine_mode(config_params)
        
        if mode == QueryMode.SECURE:
            return generate_base_query_logic_secure(
                config_params, merge_keys, demographic_filters, behavioral_filters, tables_to_join
            )
        else:
            if self.strict_validation:
                # Validate parameters even in legacy mode
                errors = validate_secure(
                    config_params, demographic_filters, behavioral_filters, tables_to_join
                )
                if errors:
                    raise SecurityError(f"Security validation failed: {errors}")
            
            return generate_base_query_logic(
                config_params, merge_keys, demographic_filters, behavioral_filters, tables_to_join
            )
    
    def get_data_query(
        self,
        base_query_logic: str,
        params: List[Any],
        selected_tables: List[str],
        selected_columns: Dict[str, List[str]],
        allowed_tables: Optional[set] = None
    ) -> Tuple[Optional[str], Optional[List[Any]]]:
        """
        Generate data query using the configured mode.
        
        Args:
            base_query_logic: Base FROM/JOIN/WHERE clause
            params: Query parameters
            selected_tables: Tables to include
            selected_columns: Columns to select per table
            allowed_tables: Whitelist of allowed tables (for secure mode)
            
        Returns:
            Tuple of (complete SELECT query, parameters)
            
        Raises:
            QueryError: If query generation fails
        """
        mode = self._determine_mode({})
        
        if mode == QueryMode.SECURE:
            if allowed_tables is None:
                # Auto-generate allowed tables if not provided
                allowed_tables = set(selected_tables)
            
            return generate_data_query_secure(
                base_query_logic, params, selected_tables, selected_columns, allowed_tables
            )
        else:
            return generate_data_query(
                base_query_logic, params, selected_tables, selected_columns
            )
    
    def get_count_query(
        self,
        base_query_logic: str,
        params: List[Any],
        merge_keys: MergeKeys
    ) -> Tuple[Optional[str], Optional[List[Any]]]:
        """
        Generate count query (same for both modes).
        
        Args:
            base_query_logic: Base FROM/JOIN/WHERE clause
            params: Query parameters
            merge_keys: Merge strategy information
            
        Returns:
            Tuple of (COUNT query, parameters)
            
        Raises:
            QueryError: If query generation fails
        """
        mode = self._determine_mode({})
        
        if mode == QueryMode.SECURE:
            return generate_count_query_secure(base_query_logic, params, merge_keys)
        else:
            return generate_count_query(base_query_logic, params, merge_keys)
    
    def get_query_suite(
        self,
        config_params: Dict[str, Any],
        merge_keys: MergeKeys,
        demographic_filters: Dict[str, Any],
        behavioral_filters: List[Dict[str, Any]],
        tables_to_join: List[str],
        selected_columns: Optional[Dict[str, List[str]]] = None
    ) -> Tuple[str, str, List[Any]]:
        """
        Generate complete query suite (data + count queries).
        
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
        mode = self._determine_mode(config_params)
        
        if mode == QueryMode.SECURE:
            return generate_secure_query_suite(
                config_params, merge_keys, demographic_filters, behavioral_filters,
                tables_to_join, selected_columns
            )
        else:
            # Manual assembly for legacy mode
            base_query, params = self.get_base_query_logic(
                config_params, merge_keys, demographic_filters, behavioral_filters, tables_to_join
            )
            
            count_query, _ = self.get_count_query(base_query, params, merge_keys)
            
            data_query = ""
            if selected_columns:
                data_query, _ = self.get_data_query(
                    base_query, params, tables_to_join, selected_columns
                )
            
            return data_query or "", count_query or "", params
    
    def validate_parameters(
        self,
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
        if self.mode == QueryMode.SECURE or self.strict_validation:
            return validate_secure(
                config_params, demographic_filters, behavioral_filters, tables_to_join
            )
        else:
            # Basic validation for legacy mode
            errors = []
            
            if not isinstance(tables_to_join, list):
                errors.append("tables_to_join must be a list")
            
            if not isinstance(behavioral_filters, list):
                errors.append("behavioral_filters must be a list")
            
            return errors
    
    def set_mode(self, mode: QueryMode) -> None:
        """
        Change the query generation mode.
        
        Args:
            mode: New query generation mode
        """
        if mode == QueryMode.LEGACY:
            logging.warning("Switching to LEGACY mode with security vulnerabilities")
        
        self.mode = mode
    
    def _determine_mode(self, config_params: Dict[str, Any]) -> QueryMode:
        """
        Determine the actual mode to use based on configuration.
        
        Args:
            config_params: Configuration parameters
            
        Returns:
            Resolved query mode
        """
        if self.mode == QueryMode.AUTO:
            # Auto-select mode based on configuration or environment
            use_secure = config_params.get('use_secure_queries', True)
            return QueryMode.SECURE if use_secure else QueryMode.LEGACY
        
        return self.mode


# Global factory instance
_default_factory: Optional[QueryFactory] = None


def get_query_factory(
    mode: QueryMode = QueryMode.SECURE,
    strict_validation: bool = True
) -> QueryFactory:
    """
    Get the default query factory instance.
    
    Args:
        mode: Query generation mode (only used for first initialization)
        strict_validation: Whether to enforce strict validation
        
    Returns:
        QueryFactory instance
    """
    global _default_factory
    if _default_factory is None:
        _default_factory = QueryFactory(mode, strict_validation)
    return _default_factory


def reset_query_factory() -> None:
    """
    Reset the default query factory (useful for testing).
    """
    global _default_factory
    _default_factory = None


# Convenience functions that use the default factory
def generate_base_query(
    config_params: Dict[str, Any],
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str]
) -> Tuple[str, List[Any]]:
    """Convenience function using default factory."""
    factory = get_query_factory()
    return factory.get_base_query_logic(
        config_params, merge_keys, demographic_filters, behavioral_filters, tables_to_join
    )


def generate_query_suite(
    config_params: Dict[str, Any],
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str],
    selected_columns: Optional[Dict[str, List[str]]] = None
) -> Tuple[str, str, List[Any]]:
    """Convenience function using default factory."""
    factory = get_query_factory()
    return factory.get_query_suite(
        config_params, merge_keys, demographic_filters, behavioral_filters,
        tables_to_join, selected_columns
    )