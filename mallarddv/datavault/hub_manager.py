"""
Hub management for Data Vault.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

from ..db.database_connection import DatabaseConnection
from ..db.sql_templates import SQLTemplates
from ..metadata.metadata_manager import MetadataManager
from ..exceptions import DVEntityError
from .hash_generator import HashViewGenerator

logger = logging.getLogger("mallarddv")


class DVHubManager:
    """
    Creates and loads hub tables for Data Vault.
    """

    def __init__(
        self,
        db: DatabaseConnection,
        metadata_manager: MetadataManager,
        hash_generator: HashViewGenerator,
    ):
        """
        Initialize with database connection and metadata manager.

        Args:
            db: Database connection
            metadata_manager: Metadata manager
            hash_generator: Hash generator
        """
        self.db = db
        self.metadata_manager = metadata_manager
        self.hash_generator = hash_generator

    def create_hub_from_metadata(
        self, base_name: Optional[str] = None, verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Create hub tables based on metadata definitions.

        Args:
            base_name: Optional filter to create only hubs for a specific base entity
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []
        raw_records = self.metadata_manager.get_tables(
            base_name=base_name, rel_type="hub"
        )

        records = self.hash_generator.groupby(raw_records, keys=["base_name"])

        for base_name, business_keys in records.items():
            if verbose:
                print(f"Found a hub to create: {base_name}")

            bk_list: List[str] = []
            for bk in business_keys:
                quote = " " in bk["column_name"]

                bk_list.append(
                    f"""{'"' if quote else ''}{bk["column_name"]}_{'c' if len(business_keys)>1 else ''}bk{'"' if quote else ''} {bk["column_type"]}"""
                )

            sql_str = SQLTemplates.CREATE_HUB.format(
                base_name=base_name, business_keys=",\n               ".join(bk_list)
            )

            _, error = self.db.execute_sql_safely(
                sql_str, description=f"Create hub_{base_name}"
            )
            if error:
                errors.append(error)

        return errors

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
            stg_table: Name of the staging table containing the source data
            run_id: Identifier for the current load process
            record_source: Identifier for the data source system
            load_date_overwrite: Optional date string to use instead of current timestamp
            verbose: Whether to print generated SQL for debugging

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        load_date = (
            load_date_overwrite
            or f"""'{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}'"""
        )

        raw_records = self.metadata_manager.get_transitions(source_table=stg_table)
        records = [r for r in raw_records if r["target_table"].startswith("hub_")]

        bk_per_group = self.hash_generator.groupby(
            record_list=records, keys=["target_table", "group_name"]
        )

        errors: List[Tuple[str, str]] = []

        for hub_group, fields in bk_per_group.items():
            hub_name = hub_group.split(".")[0]
            group_name = hub_group.split(".")[1]

            hub_hash_key = f"{hub_name[4:]}_hk"

            business_keys = ",\n           ".join(
                [
                    f"SUB.{hk_field['source_field']} AS {hk_field['target_field']}"
                    for hk_field in fields
                ]
            )

            source_fields = ",\n           ".join(
                [f"src.{hk_field['source_field']}" for hk_field in fields]
            )

            insert_sql = SQLTemplates.INSERT_HUB.format(
                hub_name=hub_name,
                load_date=load_date,
                record_source=record_source,
                run_id=run_id,
                business_keys=business_keys,
                group_name=group_name,
                source_fields=source_fields,
                stg_table=stg_table,
                hub_hash_key=hub_hash_key,
            )

            if verbose:
                print(insert_sql)

            _, error = self.db.execute_sql_safely(
                insert_sql, description=f"Load hub {hub_name}"
            )
            if error:
                errors.append(error)

        return errors
