"""
Adapter utilities for testing MallardDataVault.
"""

import duckdb
from typing import Optional, List, Dict, Any, Tuple


class LegacyDBAdapter:
    """
    Adapter class to maintain backward compatibility with tests that
    directly access the database connection.
    """

    def __init__(self, real_db: duckdb.DuckDBPyConnection):
        """
        Initialize with a DuckDB connection.

        Args:
            real_db: DuckDB connection
        """
        self.real_db = real_db

    def sql(self, query, *args, **kwargs):
        """
        Execute SQL through the DuckDB connection.

        Args:
            query: SQL query
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            Query result
        """
        if "params" in kwargs:
            return self.real_db.sql(query, params=kwargs["params"])
        elif len(args) > 0:
            return self.real_db.sql(query, *args)
        else:
            return self.real_db.sql(query)

    def fetch_dict(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL and return results as a list of dictionaries.

        Args:
            sql: SQL query to execute
            params: Optional parameters for the query

        Returns:
            List of dictionaries where keys are column names and values are row values
        """
        result = self.sql(sql, params=params) if params else self.sql(sql)
        return [dict(zip(result.columns, rec)) for rec in result.fetchall()]

    def execute_sql_safely(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        description: str = "SQL operation",
        collect_errors: bool = True,
    ) -> Tuple[Optional[duckdb.DuckDBPyRelation], Optional[Tuple[str, str]]]:
        """
        Execute SQL with error handling for testing.

        Args:
            sql: SQL query to execute
            params: Optional parameters for the query
            description: Description of the operation
            collect_errors: Whether to collect errors

        Returns:
            Tuple of (result, error)
        """
        try:
            result = self.sql(sql, params=params) if params else self.sql(sql)
            return result, None
        except Exception as ex:
            if collect_errors:
                return None, (sql, str(ex))
            else:
                raise Exception(f"Error in {description}: {str(ex)}")

    def close(self):
        """Close the database connection."""
        self.real_db.close()

    def connect(self):
        """Method stub for compatibility."""
        return self


def inject_test_db(mdv_instance, db_connection: duckdb.DuckDBPyConnection) -> None:
    """
    Inject a test database connection into a MallardDataVault instance.
    This is for testing purposes only.

    Args:
        mdv_instance: MallardDataVault instance
        db_connection: DuckDB connection to inject
    """
    # Create adapter that provides both legacy and new interfaces
    adapter = LegacyDBAdapter(db_connection)

    # Set the database adapter in the main MDV instance
    mdv_instance.db = adapter

    # Set the database adapter in all components
    mdv_instance.metadata_manager.db = adapter
    mdv_instance.schema_manager.db = adapter
    mdv_instance.hash_generator.db = (
        adapter  # Make sure hash generator gets the adapter
    )
    mdv_instance.hub_manager.db = adapter
    mdv_instance.link_manager.db = adapter
    mdv_instance.satellite_manager.db = adapter
    mdv_instance.etl_service.db = adapter
    mdv_instance.flow_executor.db = adapter

    # Ensure the hash generator's metadata manager also uses the adapter
    mdv_instance.hash_generator.metadata_manager.db = adapter
