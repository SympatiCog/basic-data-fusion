"""
Query generation module for Basic Data Fusion.

This module provides both secure and legacy SQL query generation functions
with a factory pattern for easy switching between modes.
"""

# Secure query functions (recommended)
from .query_secure import (
    generate_base_query_logic_secure,
    generate_data_query_secure,
    generate_count_query_secure,
    generate_secure_query_suite,
    validate_query_parameters as validate_secure_parameters
)

# Legacy query functions (deprecated, security vulnerabilities)
from .query_builder import (
    generate_base_query_logic,
    generate_data_query,
    generate_count_query,
    get_table_alias
)

# Query parameter handling
from .query_parameters import (
    export_query_parameters_to_toml,
    import_query_parameters_from_toml,
    validate_query_parameters,
    create_query_parameters_template
)

# Factory pattern for query generation
from .query_factory import (
    QueryFactory,
    QueryMode,
    get_query_factory,
    reset_query_factory,
    generate_base_query,  # Convenience function
    generate_query_suite  # Convenience function
)

__all__ = [
    # Secure query functions (recommended)
    'generate_base_query_logic_secure',
    'generate_data_query_secure',
    'generate_count_query_secure',
    'generate_secure_query_suite',
    'validate_secure_parameters',
    
    # Legacy query functions (deprecated)
    'generate_base_query_logic',
    'generate_data_query',
    'generate_count_query',
    'get_table_alias',
    
    # Query parameter handling
    'export_query_parameters_to_toml',
    'import_query_parameters_from_toml',
    'validate_query_parameters',
    'create_query_parameters_template',
    
    # Factory pattern
    'QueryFactory',
    'QueryMode',
    'get_query_factory',
    'reset_query_factory',
    'generate_base_query',
    'generate_query_suite',
]

# Version info
__version__ = "1.0.0"

# Default to secure mode
DEFAULT_QUERY_MODE = QueryMode.SECURE