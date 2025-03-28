"""
MallardDataVault: A portable Data Vault implementation using DuckDB.
"""

import os
import logging
from typing import Dict, List, Optional, Any, Tuple

from .db.database_connection import DatabaseConnection
from .db.schema_manager import SchemaManager
from .metadata.metadata_manager import MetadataManager
from .datavault.hash_generator import HashViewGenerator
from .datavault.hub_manager import DVHubManager
from .datavault.link_manager import DVLinkManager
from .datavault.satellite_manager import DVSatelliteManager
from .etl.etl_service import ETLService
from .etl.flow_executor import FlowExecutor
from .utils.logging import configure_logging
from .exceptions import DVException

logger = logging.getLogger("mallarddv")


class MallardDataVault:
    """
    A portable implementation of Data Vault methodology using DuckDB.

    This class provides tools to create and manage a Data Vault model for data warehousing,
    including hubs, links, satellites, and associated metadata management.
    """

    def __init__(self, database_path: str, scripts_path: Optional[str] = None) -> None:
        """
        Initialize the MallardDataVault with a DuckDB database path.

        Args:
            database_path: Path to the DuckDB database file or ":memory:" for in-memory database
            scripts_path: Root path where scripts are written as sql files
        """
        configure_logging()

        self.db_path = database_path
        self.scripts_path = scripts_path or "models"

        # Initialize components
        self.db = DatabaseConnection(database_path)
        self.metadata_manager = MetadataManager(self.db)
        self.schema_manager = SchemaManager(self.db)
        self.hash_generator = HashViewGenerator(self.db, self.metadata_manager)
        self.hub_manager = DVHubManager(
            self.db, self.metadata_manager, self.hash_generator
        )
        self.link_manager = DVLinkManager(
            self.db, self.metadata_manager, self.hash_generator
        )
        self.satellite_manager = DVSatelliteManager(
            self.db, self.metadata_manager, self.hash_generator
        )
        self.etl_service = ETLService(self.db, self.metadata_manager)
        self.flow_executor = FlowExecutor(
            self.db,
            self.metadata_manager,
            self.schema_manager,
            self.hash_generator,
            self.hub_manager,
            self.link_manager,
            self.satellite_manager,
            self.etl_service,
        )

    def __enter__(self) -> "MallardDataVault":
        """
        Context manager entry point that connects to the database.

        Returns:
            Self reference for use in context manager
        """
        self.db.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Context manager exit point that closes the database connection.

        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        self.db.close()

    # For backwards compatibility with existing API

    def sql(self, sql_str: str, sql_args: Optional[List[object]] = None):
        """
        Execute a SQL query against the database.

        Args:
            sql_str: SQL query to execute
            sql_args: Optional list of parameters for parameterized queries

        Returns:
            DuckDB relation object with query results
        """
        return self.db.sql(sql_str, sql_args)

    def init_mallard_db(
        self,
        meta_only: Optional[bool] = True,
        meta_tables_path: Optional[str] = None,
        meta_transitions_path: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Initialize the Mallard Data Vault database.

        Args:
            meta_only: Only initialize schema and metadata tables
            meta_tables_path: Path to file containing table definitions
            meta_transitions_path: Path to file containing transition definitions
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []

        # Create schemas
        errors.extend(self.schema_manager.create_schemas(verbose=verbose))

        # Create metadata tables
        errors.extend(self.metadata_manager.init_metadata_tables(verbose=verbose))

        # Load metadata from files if provided
        errors.extend(
            self.metadata_manager.overwrite_metadata_from_files(
                meta_tables_path=meta_tables_path,
                meta_transitions_path=meta_transitions_path,
                verbose=verbose,
            )
        )

        if not meta_only:
            # Get metadata records for staging tables
            stg_records = self.metadata_manager.get_tables(rel_type="stg")

            # Create staging tables
            errors.extend(
                self.schema_manager.create_staging_table_from_metadata(
                    metadata_records=stg_records, verbose=verbose
                )
            )

            # Create hubs
            errors.extend(self.hub_manager.create_hub_from_metadata(verbose=verbose))

            # Create links
            errors.extend(self.link_manager.create_link_from_metadata(verbose=verbose))

            # Create satellites
            errors.extend(
                self.satellite_manager.create_sat_from_metadata(verbose=verbose)
            )

            # Create current views
            errors.extend(
                self.satellite_manager.create_current_sat_from_metadata(verbose=verbose)
            )

            # Apply view scripts in all schemas
            all_records = self.metadata_manager.get_tables()
            for schema in self.schema_manager.schema_list:
                errors.extend(
                    self.schema_manager.apply_script_from_metadata(
                        rel_type=f"{schema}_vw",
                        scripts_path=self.scripts_path,
                        metadata_records=all_records,
                        verbose=verbose,
                    )
                )

        return errors

    def compute_hash_view(
        self, stg_table: str, verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Create a hash view for a staging table.

        Args:
            stg_table: Name of staging table to create hash view for
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.hash_generator.compute_hash_view(stg_table, verbose)

    def load_related_hubs(
        self,
        stg_table: str,
        run_id: int,
        record_source: str,
        load_date_overwrite: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Load data from staging table into related hub tables.

        Args:
            stg_table: Name of the staging table
            run_id: Identifier for the current load process
            record_source: Identifier for the data source system
            load_date_overwrite: Optional date string to use instead of current timestamp
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.hub_manager.load_related_hubs(
            stg_table, run_id, record_source, load_date_overwrite, verbose
        )

    def load_related_links(
        self,
        stg_table: str,
        run_id: int,
        record_source: str,
        load_date_overwrite: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Load data from staging table into related link tables.

        Args:
            stg_table: Name of the staging table
            run_id: Identifier for the current load process
            record_source: Identifier for the data source system
            load_date_overwrite: Optional date string to use instead of current timestamp
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.link_manager.load_related_links(
            stg_table, run_id, record_source, load_date_overwrite, verbose
        )

    def load_related_sats(
        self,
        stg_table: str,
        run_id: int,
        record_source: str,
        load_date_overwrite: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Load data from staging table into related satellite tables.

        Args:
            stg_table: Name of the staging table
            run_id: Identifier for the current load process
            record_source: Identifier for the data source system
            load_date_overwrite: Optional date string to use instead of current timestamp
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.satellite_manager.load_related_sats(
            stg_table, run_id, record_source, load_date_overwrite, verbose
        )

    def create_hub_from_metadata(
        self, base_name: Optional[str] = None, verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Create hub tables based on metadata definitions.

        Args:
            base_name: Optional filter to create only hubs for a specific base entity
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.hub_manager.create_hub_from_metadata(base_name, verbose)

    def create_link_from_metadata(
        self,
        base_name: Optional[str] = None,
        rel_type: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Create link tables based on metadata definitions.

        Args:
            base_name: Optional filter to create only links for a specific base entity
            rel_type: Optional filter to create only links of a specific type (link, nhl)
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.link_manager.create_link_from_metadata(base_name, rel_type, verbose)

    def create_sat_from_metadata(
        self,
        base_name: Optional[str] = None,
        rel_type: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Create satellite tables based on metadata definitions.

        Args:
            base_name: Optional filter to create only satellites for a specific base entity
            rel_type: Optional filter to create only satellites of a specific type (hsat, lsat)
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.satellite_manager.create_sat_from_metadata(
            base_name, rel_type, verbose
        )

    def create_current_sat_from_metadata(
        self,
        base_name: Optional[str] = None,
        rel_type: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Create current value views for satellite tables.

        Args:
            base_name: Optional filter to create only views for satellites of a specific base entity
            rel_type: Optional filter to create only views for satellites of a specific type
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.satellite_manager.create_current_sat_from_metadata(
            base_name, rel_type, verbose
        )

    def create_staging_table_from_metadata(
        self, base_name: Optional[str] = None, verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Create staging tables based on metadata definitions.

        Args:
            base_name: Optional filter to create only specific entity
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        metadata_records = self.metadata_manager.get_tables(
            base_name=base_name, rel_type="stg"
        )
        return self.schema_manager.create_staging_table_from_metadata(
            metadata_records, verbose
        )

    def apply_script_from_metadata(
        self, rel_type: str, base_name: Optional[str] = None, verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Apply SQL scripts based on metadata records.

        Args:
            rel_type: Relationship type to apply
            base_name: Optional filter to create only specific entity
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        metadata_records = self.metadata_manager.get_tables(base_name=base_name)
        return self.schema_manager.apply_script_from_metadata(
            rel_type, self.scripts_path, metadata_records, verbose
        )

    def execute_flow(
        self,
        source_table: str,
        record_source: str,
        file_path: str = None,
        load_date_overwrite: str = None,
        force_load: bool = False,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Execute a complete Data Vault loading flow.

        Args:
            source_table: Name of the source table
            record_source: Data Vault run source identifier
            file_path: Optional path to file to load
            load_date_overwrite: Optional date string to use instead of current timestamp
            force_load: Whether to load the file even if already processed
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.flow_executor.execute_flow(
            source_table,
            record_source,
            file_path,
            load_date_overwrite,
            force_load,
            verbose,
        )

    def overwrite_metadata_from_files(
        self,
        meta_tables_path: Optional[str] = None,
        meta_transitions_path: Optional[str] = None,
        verbose: Optional[bool] = False,
    ) -> List[Tuple[str, str]]:
        """
        Replace the content of metadata tables with content from files.

        Args:
            meta_tables_path: Path to file containing metadata for table metadata.tables
            meta_transitions_path: Path to file containing metadata for table metadata.transitions
            verbose: Whether to print SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        return self.metadata_manager.overwrite_metadata_from_files(
            meta_tables_path, meta_transitions_path, verbose
        )
