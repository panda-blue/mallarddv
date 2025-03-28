"""
Metadata management for MallardDataVault.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

from ..db.database_connection import DatabaseConnection
from ..db.sql_templates import SQLTemplates
from ..exceptions import DVMetadataError

logger = logging.getLogger("mallarddv")


class MetadataManager:
    """
    Manages metadata tables and queries for MallardDataVault.
    """

    def __init__(self, db: DatabaseConnection):
        """
        Initialize with database connection.

        Args:
            db: Database connection
        """
        self.db = db

    def get_transitions(self, source_table: str) -> List[Dict[str, Any]]:
        """
        Retrieve mapping configuration between staging and data vault tables.

        Args:
            source_table: Name of the staging table to get mappings for

        Returns:
            List of mapping records containing field mappings between staging and data vault
        """
        return self.db.fetch_dict(
            sql=SQLTemplates.GET_TRANSITIONS, params=[source_table]
        )

    def get_tables(
        self, base_name: Optional[str] = None, rel_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve data vault table metadata based on optional filters.

        Args:
            base_name: Optional filter for the base entity name
            rel_type: Optional filter for relationship type (hub, link, hsat, etc.)

        Returns:
            List of data vault table metadata records
        """
        params = []
        where_clauses = []

        if base_name:
            where_clauses.append("base_name = ?")
            params.append(base_name)
        if rel_type:
            where_clauses.append("rel_type = ?")
            params.append(rel_type)

        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        sql = SQLTemplates.GET_TABLES.format(where_clause=where_clause)

        return self.db.fetch_dict(sql=sql, params=params)

    def init_metadata_tables(self, verbose: bool = False) -> List[Tuple[str, str]]:
        """
        Initialize metadata tables if they don't exist.

        Args:
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []

        # Create metadata tables
        if verbose:
            print(SQLTemplates.META_TABLES)
        _, error = self.db.execute_sql_safely(
            SQLTemplates.META_TABLES, description="Create metadata.tables"
        )
        if error:
            errors.append(error)

        if verbose:
            print(SQLTemplates.META_TRANSITIONS)
        _, error = self.db.execute_sql_safely(
            SQLTemplates.META_TRANSITIONS, description="Create metadata.transitions"
        )
        if error:
            errors.append(error)

        if verbose:
            print(SQLTemplates.META_RUNINFO)
        _, error = self.db.execute_sql_safely(
            SQLTemplates.META_RUNINFO, description="Create metadata.runinfo"
        )
        if error:
            errors.append(error)

        return errors

    def overwrite_metadata_from_files(
        self,
        meta_tables_path: Optional[str] = None,
        meta_transitions_path: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Execute SQL to replace the content of metadata.tables and
        metadata.transitions with content of files at provided locations.
        If no paths are provided, nothing is overwritten.

        Args:
            meta_tables_path: Path to file containing metadata for table metadata.tables
            meta_transitions_path: Path to file containing metadata for table metadata.transitions
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []

        if meta_tables_path:
            truncate_str = "TRUNCATE TABLE metadata.tables;"
            insert_str = f'INSERT INTO metadata.tables(base_name,rel_type,column_name,column_type,column_position,mapping) select * from read_csv("{meta_tables_path}")'

            if verbose:
                print(truncate_str)
            _, error = self.db.execute_sql_safely(
                truncate_str, description="Truncate metadata.tables"
            )
            if error:
                errors.append(error)

            if verbose:
                print(insert_str)
            _, error = self.db.execute_sql_safely(
                insert_str, description="Insert into metadata.tables"
            )
            if error:
                errors.append(error)

        if meta_transitions_path:
            truncate_str = "TRUNCATE TABLE metadata.transitions;"
            insert_str = f'INSERT INTO metadata.transitions(source_table,source_field,target_table,target_field,group_name,position,raw,transformation,transfer_type) select * from read_csv("{meta_transitions_path}");'

            if verbose:
                print(truncate_str)
            _, error = self.db.execute_sql_safely(
                truncate_str, description="Truncate metadata.transitions"
            )
            if error:
                errors.append(error)

            if verbose:
                print(insert_str)
            _, error = self.db.execute_sql_safely(
                insert_str, description="Insert into metadata.transitions"
            )
            if error:
                errors.append(error)

        return errors

    def get_next_run_id(self) -> int:
        """
        Get the next run ID.

        Returns:
            Next run ID
        """
        result = self.db.fetch_dict(SQLTemplates.GET_RUN_ID)
        return result[0]["run_id"]

    def check_previous_ingestion(
        self, source_table: str, file_path: str, status: str = "success"
    ) -> bool:
        """
        Check if a file has already been ingested successfully.

        Args:
            source_table: Source table name
            file_path: Path to the source file
            status: Status to check for (default: "success")

        Returns:
            True if the file has already been ingested successfully
        """
        result = self.db.fetch_dict(
            SQLTemplates.CHECK_INGESTION, params=[file_path, source_table, status]
        )
        return len(result) > 0

    def check_source_for_ingestion(self, source_table) -> bool:
        """
        Check if the source_table is a staging table to be loaded from file

        Args:
            source_table: Source table name

        Returns:
            True if the source_table should be loaded from file
        """

        result = self.db.fetch_dict(
            SQLTemplates.CHECK_SOURCE_FOR_INGESTION,
            params=[source_table],
        )

        return result[0]["to_load"]

    def register_run_info(
        self,
        source_table: str,
        run_id: int,
        file_path: str,
        status: str,
        message: str = "",
    ) -> None:
        """
        Register run information in the metadata.runinfo table.

        Args:
            source_table: Source table name
            run_id: Run ID
            file_path: Path to the source file
            status: Status of the run
            message: Optional message
        """
        load_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.db.sql(
            SQLTemplates.INSERT_RUNINFO,
            params=[source_table, run_id, load_time, file_path, status, message],
        )
