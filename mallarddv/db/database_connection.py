"""
Database connection handling for MallardDataVault.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple

import duckdb

from ..exceptions import DVSQLError

logger = logging.getLogger("mallarddv")


class DatabaseConnection:
    """
    Manages database connections and SQL execution for MallardDataVault.
    """

    def __init__(self, db_path: str):
        """
        Initialize with database path.

        Args:
            db_path: Path to the DuckDB database file or ":memory:" for in-memory database
        """
        self.db_path = db_path
        self.db = None

    def connect(self) -> "DatabaseConnection":
        """
        Connect to the database.

        Returns:
            Self reference for method chaining
        """
        self.db = duckdb.connect(self.db_path)
        return self

    def close(self) -> None:
        """
        Close the database connection.
        """
        if self.db:
            self.db.close()
            self.db = None

    def execute_sql_safely(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        description: str = "SQL operation",
        collect_errors: bool = True,
    ) -> Tuple[Optional[duckdb.DuckDBPyRelation], Optional[Tuple[str, str]]]:
        """
        Execute SQL with consistent error handling.

        Args:
            sql: SQL query to execute
            params: Optional parameters for the query
            description: Description of the operation for logging
            collect_errors: Whether to collect errors or raise exceptions

        Returns:
            Tuple of (result, error) where error is None if operation succeeded

        Raises:
            DVSQLError: If SQL execution fails and collect_errors is False
        """
        try:
            # DuckDB parameters should be passed as named parameters
            result = self.db.sql(sql, params=params) if params else self.db.sql(sql)
            return result, None
        except Exception as ex:
            error_msg = f"Error in {description}: {str(ex)}"
            logger.error(error_msg)
            if collect_errors:
                return None, (sql, str(ex))
            else:
                raise DVSQLError(error_msg, sql, ex)

    def sql(
        self, sql_str: str, params: Optional[List[Any]] = None
    ) -> duckdb.DuckDBPyRelation:
        """
        Execute a SQL query against the database.

        Args:
            sql_str: SQL query to execute
            params: Optional list of parameters for parameterized queries

        Returns:
            DuckDB relation object with query results

        Raises:
            DVSQLError: If SQL execution fails
        """
        result, error = self.execute_sql_safely(
            sql_str, params, "User SQL execution", False
        )
        return result

    def fetch_dict(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL and return results as a list of dictionaries.

        Args:
            sql: SQL query to execute
            params: Optional list of parameters for parameterized queries

        Returns:
            List of dictionaries where keys are column names and values are row values

        Raises:
            DVSQLError: If SQL execution fails
        """
        result, error = self.execute_sql_safely(sql, params, "Fetch dictionary", False)
        return [dict(zip(result.columns, rec)) for rec in result.fetchall()]

    def __enter__(self) -> "DatabaseConnection":
        """
        Context manager entry point.

        Returns:
            Self reference for use in context manager
        """
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Context manager exit point.

        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        self.close()
