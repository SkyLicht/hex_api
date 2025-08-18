import configparser
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SQLiteReadOnlyConnection:
    """
    A thread-safe SQLite read-only connection manager for FastAPI applications.
    Optimized for SELECT queries only.
    """
    
    def __init__(self, database_path: str = "database.db"):
        """
        Initialize the SQLite read-only connection manager.
        
        Args:
            database_path (str): Path to the SQLite database file
        """

        configs = configparser.ConfigParser()
        configs.read('configs/api_config.ini')
        if not configs.read('configs/api_config.ini'):
            raise ValueError("Config file not found")

        raw = configs.get('server_house', 'sfc_db')

        self.database_path = database_path
        self._local = threading.local()
        self._lock = threading.Lock()
        
        # Verify database exists and is accessible
        self._verify_database()

    
    def _verify_database(self):
        """Verify the database exists and is accessible."""
        try:
            with sqlite3.connect(f"file:{self.database_path}?mode=ro", uri=True) as conn:
                conn.execute("SELECT 1")
                logger.info(f"Read-only database connection verified: {self.database_path}")
        except sqlite3.Error as e:
            logger.error(f"Failed to access database in read-only mode: {e}")
            raise
    
    def _get_connection(self) -> sqlite3.Connection:
        """
        Get a thread-local read-only database connection.
        
        Returns:
            sqlite3.Connection: Thread-local read-only database connection
        """
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            try:
                # Open in read-only mode
                self._local.connection = sqlite3.connect(
                    f"file:{self.database_path}?mode=ro",
                    uri=True,
                    check_same_thread=False,
                    timeout=30.0
                )
                self._local.connection.row_factory = sqlite3.Row  # Enable dict-like access
                
                # Set read-only optimizations
                cursor = self._local.connection.cursor()
                cursor.execute("PRAGMA query_only = ON")  # Ensure read-only mode
                cursor.execute("PRAGMA temp_store = MEMORY")  # Use memory for temp storage
                cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB memory-mapped I/O
                cursor.execute("PRAGMA cache_size = -64000")  # 64MB cache
                cursor.close()
                
                logger.debug("Created new read-only database connection")
                print("Created new read-only database connection")
            except sqlite3.Error as e:
                logger.error(f"Failed to create read-only database connection: {e}")
                print(f"Failed to create read-only database connection: {e}")
                raise

        return self._local.connection
    
    @contextmanager
    def get_db_connection(self):
        """
        Context manager for read-only database connections.
        Automatically handles connection cleanup.
        
        Usage:
            async def some_endpoint():
                with db.get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM users")
                    return cursor.fetchall()
        """
        conn = None
        try:
            conn = self._get_connection()
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise
        # No commit needed for read-only operations
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results as a list of dictionaries.
        
        Args:
            query (str): SQL SELECT query
            params (tuple, optional): Query parameters
            
        Returns:
            List[Dict[str, Any]]: Query results as list of dictionaries
            
        Raises:
            ValueError: If query is not a SELECT statement
        """
        # Basic check to ensure it's a read operation
        if not query.strip().upper().startswith(('SELECT', 'WITH', 'PRAGMA')):
            raise ValueError("Only SELECT, WITH, and PRAGMA queries are allowed in read-only mode")
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Convert rows to dictionaries
            columns = [description[0] for description in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            
            return [dict(zip(columns, row)) for row in rows] if columns else []
    
    def execute_query_one(self, query: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
        """
        Execute a SELECT query and return the first result as a dictionary.
        
        Args:
            query (str): SQL SELECT query
            params (tuple, optional): Query parameters
            
        Returns:
            Dict[str, Any] or None: First result as dictionary or None if no results
            
        Raises:
            ValueError: If query is not a SELECT statement
        """
        # Basic check to ensure it's a read operation
        if not query.strip().upper().startswith(('SELECT', 'WITH', 'PRAGMA')):
            raise ValueError("Only SELECT, WITH, and PRAGMA queries are allowed in read-only mode")
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None
    
    def execute_query_raw(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """
        Execute a SELECT query and return raw results as a list of tuples.
        Useful for performance-critical operations where you don't need dictionaries.
        
        Args:
            query (str): SQL SELECT query
            params (tuple, optional): Query parameters
            
        Returns:
            List[tuple]: Query results as list of tuples
            
        Raises:
            ValueError: If query is not a SELECT statement
        """
        if not query.strip().upper().startswith(('SELECT', 'WITH', 'PRAGMA')):
            raise ValueError("Only SELECT, WITH, and PRAGMA queries are allowed in read-only mode")
        
        with self.get_db_connection() as conn:
            # Temporarily disable row factory for raw results
            original_row_factory = conn.row_factory
            conn.row_factory = None
            
            try:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                return cursor.fetchall()
            finally:
                conn.row_factory = original_row_factory
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get information about a table's structure.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            List[Dict[str, Any]]: Table structure information
        """
        return self.execute_query("PRAGMA table_info(?)", (table_name,))
    
    def get_table_names(self) -> List[str]:
        """
        Get all table names in the database.
        
        Returns:
            List[str]: List of table names
        """
        result = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [row['name'] for row in result]
    
    def get_view_names(self) -> List[str]:
        """
        Get all view names in the database.
        
        Returns:
            List[str]: List of view names
        """
        result = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='view'"
        )
        return [row['name'] for row in result]
    
    def count_rows(self, table_name: str, where_clause: str = "", params: Optional[tuple] = None) -> int:
        """
        Count rows in a table with optional WHERE clause.
        
        Args:
            table_name (str): Name of the table
            where_clause (str): Optional WHERE clause (without WHERE keyword)
            params (tuple, optional): Parameters for WHERE clause
            
        Returns:
            int: Number of rows
        """
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        result = self.execute_query_one(query, params)
        return result['count'] if result else 0
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name (str): Name of the table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        result = self.execute_query_one(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return result is not None
    
    def close_connection(self):
        """Close the thread-local connection if it exists."""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
            logger.debug("Closed read-only database connection")
    
    def close_all_connections(self):
        """Close all connections (useful for cleanup)."""
        with self._lock:
            self.close_connection()
    
    def __del__(self):
        """Cleanup when the object is destroyed."""
        try:
            self.close_all_connections()
        except:
            pass  # Ignore errors during cleanup

# Create a global instance for use in FastAPI
db = SQLiteReadOnlyConnection("C:\\Users\\abrah\\Desktop\\sfc_db\\lllll.db")  # Change the path as needed "C:\data\lbn_db\lllll.db"

# Dependency for FastAPI
def get_database():
    """
    FastAPI dependency to get read-only database instance.
    
    Usage:
        @app.get("/users")
        async def get_users(database: SQLiteReadOnlyConnection = Depends(get_database)):
            return database.execute_query("SELECT * FROM users")
    """
    return db