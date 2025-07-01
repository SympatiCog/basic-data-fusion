"""
Database connection management for Basic Data Fusion.

This module provides a thread-safe database manager that handles
DuckDB connections with proper connection pooling and error handling.
"""

import logging
import threading
from contextlib import contextmanager
from typing import Optional, Any, Generator

import duckdb

from .exceptions import DatabaseError


class DatabaseManager:
    """
    Thread-safe database manager for DuckDB connections.
    
    This class provides a singleton pattern for database connections
    with proper thread safety and connection pooling.
    """
    
    _instance: Optional['DatabaseManager'] = None
    _lock = threading.Lock()
    
    def __new__(cls, connection_string: str = ':memory:'):
        """Singleton pattern to ensure only one database manager instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, connection_string: str = ':memory:'):
        """Initialize the database manager."""
        if hasattr(self, '_initialized'):
            return
            
        self.connection_string = connection_string
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self._connection_lock = threading.Lock()
        self._initialized = True
        
        logging.info(f"DatabaseManager initialized with connection: {connection_string}")
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Get a database connection, creating it if necessary.
        
        Returns:
            DuckDB connection object
            
        Raises:
            DatabaseError: If connection creation fails
        """
        if self._connection is None:
            with self._connection_lock:
                if self._connection is None:
                    try:
                        self._connection = duckdb.connect(
                            database=self.connection_string,
                            read_only=False
                        )
                        logging.info("Created new DuckDB connection")
                    except Exception as e:
                        raise DatabaseError(
                            f"Failed to create database connection: {e}",
                            context={'connection_string': self.connection_string}
                        )
        
        return self._connection
    
    @contextmanager
    def get_connection_context(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Context manager for database connections.
        Reuses the existing connection to avoid file handle exhaustion.
        
        Yields:
            DuckDB connection object
            
        Example:
            with db_manager.get_connection_context() as conn:
                result = conn.execute("SELECT * FROM table").fetchall()
        """
        connection = self.get_connection()
        try:
            yield connection
        except Exception as e:
            logging.error(f"Database operation failed: {e}")
            raise DatabaseError(f"Database operation failed: {e}")
    
    def execute_query(self, query: str, params: Optional[list] = None) -> Any:
        """
        Execute a query with parameters.
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
            
        Returns:
            Query result
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with self.get_connection_context() as conn:
                if params:
                    result = conn.execute(query, params)
                else:
                    result = conn.execute(query)
                return result.fetchall()
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                query=query,
                params=params
            )
    
    def execute_query_single(self, query: str, params: Optional[list] = None) -> Optional[Any]:
        """
        Execute a query and return a single result.
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
            
        Returns:
            Single query result or None
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with self.get_connection_context() as conn:
                if params:
                    result = conn.execute(query, params)
                else:
                    result = conn.execute(query)
                return result.fetchone()
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                query=query,
                params=params
            )
    
    def reset_connection(self):
        """Reset the database connection (useful for testing or error recovery)."""
        with self._connection_lock:
            if self._connection:
                try:
                    self._connection.close()
                except Exception as e:
                    logging.warning(f"Error closing database connection: {e}")
                finally:
                    self._connection = None
            logging.info("Database connection reset")
    
    def is_connected(self) -> bool:
        """Check if database connection is active."""
        return self._connection is not None
    
    def get_connection_info(self) -> dict:
        """Get information about the current database connection."""
        if not self._connection:
            return {'status': 'disconnected'}
        
        try:
            # Test connection with a simple query
            self._connection.execute("SELECT 1")
            return {
                'status': 'connected',
                'connection_string': self.connection_string,
                'database_type': 'duckdb'
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'connection_string': self.connection_string
            }


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager(connection_string: str = ':memory:') -> DatabaseManager:
    """
    Get the global database manager instance.
    
    Args:
        connection_string: Database connection string
        
    Returns:
        DatabaseManager instance
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager(connection_string)
    return _db_manager


def reset_database_manager():
    """Reset the global database manager (useful for testing)."""
    global _db_manager
    if _db_manager:
        _db_manager.reset_connection()
        _db_manager = None 