"""
Database connection utilities for Rosetta Compute Engine.

Provides connection pooling and session management for PostgreSQL.
"""

import logging
from contextlib import contextmanager
from typing import Generator, Any

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from compute.core.exceptions import DatabaseException

logger = logging.getLogger(__name__)

# Global connection pool
_connection_pool: pool.ThreadedConnectionPool | None = None


def init_connection_pool(min_conn: int = 1, max_conn: int = 10) -> pool.ThreadedConnectionPool:
    """
    Initialize the connection pool.
    
    Args:
        min_conn: Minimum number of connections to maintain
        max_conn: Maximum number of connections allowed
        
    Returns:
        The initialized connection pool
    """
    global _connection_pool
    
    if _connection_pool is not None:
        return _connection_pool
    
    from compute.config import get_config
    config = get_config()
    
    try:
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            **config.database.dsn
        )
        logger.info("Database connection pool initialized")
        return _connection_pool
    except psycopg2.Error as e:
        raise DatabaseException(f"Failed to initialize connection pool: {e}")


def get_connection_pool() -> pool.ThreadedConnectionPool:
    """Get or create the connection pool."""
    global _connection_pool
    
    if _connection_pool is None:
        return init_connection_pool()
    
    return _connection_pool


def close_connection_pool() -> None:
    """Close the connection pool."""
    global _connection_pool
    
    if _connection_pool is not None:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Database connection pool closed")


def get_db_connection() -> psycopg2.extensions.connection:
    """
    Get a database connection from the pool.
    
    Returns:
        A PostgreSQL connection
        
    Note:
        Caller is responsible for returning the connection to the pool.
    """
    pool = get_connection_pool()
    try:
        return pool.getconn()
    except psycopg2.Error as e:
        raise DatabaseException(f"Failed to get connection from pool: {e}")


def return_db_connection(conn: psycopg2.extensions.connection) -> None:
    """Return a connection to the pool."""
    pool = get_connection_pool()
    pool.putconn(conn)


class DatabaseSession:
    """
    Context manager for database sessions with automatic transaction handling.
    
    Usage:
        with DatabaseSession() as session:
            result = session.execute("SELECT * FROM sources")
            rows = session.fetchall()
    """
    
    def __init__(self, autocommit: bool = False):
        """
        Initialize session.
        
        Args:
            autocommit: If True, disable transaction management
        """
        self._conn: psycopg2.extensions.connection | None = None
        self._cursor: RealDictCursor | None = None
        self._autocommit = autocommit
    
    def __enter__(self) -> "DatabaseSession":
        """Acquire connection and cursor."""
        self._conn = get_db_connection()
        self._conn.autocommit = self._autocommit
        self._cursor = self._conn.cursor(cursor_factory=RealDictCursor)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Handle transaction commit/rollback and cleanup."""
        try:
            if exc_type is not None:
                # Rollback on exception
                if self._conn and not self._autocommit:
                    self._conn.rollback()
                    logger.warning(f"Transaction rolled back due to: {exc_val}")
            else:
                # Commit on success
                if self._conn and not self._autocommit:
                    self._conn.commit()
        finally:
            # Always cleanup
            if self._cursor:
                self._cursor.close()
            if self._conn:
                return_db_connection(self._conn)
        
        return False  # Don't suppress exceptions
    
    def execute(self, query: str, params: tuple | dict | None = None) -> "DatabaseSession":
        """
        Execute a query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Self for method chaining
        """
        if self._cursor is None:
            raise DatabaseException("Session not initialized. Use as context manager.")
        
        try:
            self._cursor.execute(query, params)
            return self
        except psycopg2.Error as e:
            raise DatabaseException(f"Query execution failed: {e}", {"query": query})
    
    def executemany(self, query: str, params_list: list) -> "DatabaseSession":
        """
        Execute a query with multiple parameter sets.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples/dicts
            
        Returns:
            Self for method chaining
        """
        if self._cursor is None:
            raise DatabaseException("Session not initialized. Use as context manager.")
        
        try:
            self._cursor.executemany(query, params_list)
            return self
        except psycopg2.Error as e:
            raise DatabaseException(f"Batch execution failed: {e}", {"query": query})
    
    def fetchone(self) -> dict | None:
        """Fetch one row as dict."""
        if self._cursor is None:
            raise DatabaseException("Session not initialized.")
        return self._cursor.fetchone()
    
    def fetchall(self) -> list[dict]:
        """Fetch all rows as list of dicts."""
        if self._cursor is None:
            raise DatabaseException("Session not initialized.")
        return self._cursor.fetchall()
    
    def fetchmany(self, size: int) -> list[dict]:
        """Fetch specified number of rows."""
        if self._cursor is None:
            raise DatabaseException("Session not initialized.")
        return self._cursor.fetchmany(size)
    
    @property
    def rowcount(self) -> int:
        """Get number of affected rows."""
        if self._cursor is None:
            return 0
        return self._cursor.rowcount
    
    @property
    def lastrowid(self) -> int | None:
        """Get last inserted row ID (if available)."""
        if self._cursor is None:
            return None
        # PostgreSQL doesn't support lastrowid directly
        # Use RETURNING clause in INSERT instead
        return None


@contextmanager
def transaction() -> Generator[DatabaseSession, None, None]:
    """
    Context manager for explicit transaction handling.
    
    Usage:
        with transaction() as session:
            session.execute("INSERT INTO ...")
            session.execute("UPDATE ...")
    """
    with DatabaseSession() as session:
        yield session
