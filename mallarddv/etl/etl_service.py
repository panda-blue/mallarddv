"""
ETL operations for MallardDataVault.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

from ..db.database_connection import DatabaseConnection
from ..metadata.metadata_manager import MetadataManager
from ..exceptions import DVETLError

logger = logging.getLogger("mallarddv")


class ETLService:
    """
    Handles ETL operations for MallardDataVault.
    """

    def __init__(self, db: DatabaseConnection, metadata_manager: MetadataManager):
        """
        Initialize with database connection and metadata manager.

        Args:
            db: Database connection
            metadata_manager: Metadata manager
        """
        self.db = db
        self.metadata_manager = metadata_manager

    def load_file_to_staging(
        self,
        source_table: str,
        file_path: str,
        file_type: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Load data from a file into a staging table.

        Args:
            source_table: Name of the staging table to load into
            file_path: Path to the file to load
            file_type: Optional file type (inferred from file extension if not provided)
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []

        if verbose:
            print(f"Loading {file_path} into {source_table}")

        # Determine file type if not provided
        if not file_type:
            file_type = file_path.split(".")[-1]

        # Truncate staging table
        truncate_statement = f"TRUNCATE stg.{source_table};"
        if verbose:
            print(truncate_statement)

        _, error = self.db.execute_sql_safely(
            truncate_statement, description=f"Truncate staging table {source_table}"
        )
        if error:
            errors.append(error)
            return errors

        # Get staging table fields
        stg_fields_statement = f"SELECT column_name, column_type FROM metadata.tables WHERE base_name='{source_table}' AND rel_type='stg' AND mapping='c' ORDER BY column_position;"
        if verbose:
            print(stg_fields_statement)

        try:
            stg_fields = self.db.fetch_dict(stg_fields_statement)

            field_list = []
            mapping_dict = {}

            for c in stg_fields:
                column_name = c["column_name"]
                quote = " " in column_name
                field_list.append(
                    f"""{'"' if quote else ''}{column_name}{'"' if quote else ''}"""
                )

                mapping_dict[column_name] = c["column_type"]

            field_list_str = ", ".join(field_list)
            load_statement = f"""
            INSERT INTO stg.{source_table}
            (
                {field_list_str}
            )
            SELECT
            {field_list_str}
            FROM read_{file_type}("{file_path}", columns={json.dumps(mapping_dict)});"""

            if verbose:
                print(load_statement)

            _, error = self.db.execute_sql_safely(
                load_statement, description=f"Load {file_path} into stg.{source_table}"
            )
            if error:
                errors.append(error)
        except Exception as ex:
            errors.append((stg_fields_statement, str(ex)))

        return errors
