"""
Link management for Data Vault.
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


class DVLinkManager:
    """
    Creates and loads link tables for Data Vault.
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
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []
        raw_link_records = []

        if rel_type:
            raw_link_records = self.metadata_manager.get_tables(
                base_name=base_name, rel_type=rel_type
            )
        else:
            raw_link_records = self.metadata_manager.get_tables(
                base_name=base_name, rel_type="link"
            )
            raw_link_records.extend(
                self.metadata_manager.get_tables(base_name=base_name, rel_type="nhl")
            )

        records = self.hash_generator.groupby(
            raw_link_records, keys=["rel_type", "base_name"]
        )

        for rel_base_name, fields in records.items():
            sbn = rel_base_name.split(".")
            table_type = sbn[0]
            table_base_name = ".".join(sbn[1:])

            if verbose:
                print(f"Found a {table_type} to create: {table_base_name}")

            hk_list: List[str] = []
            dk_list: List[str] = []

            for field in fields:
                quote = " " in field["column_name"]

                if field["mapping"] == "ll":
                    hk_list.append(
                        f"""{'"' if quote else ''}{field["column_name"]}_hk{'"' if quote else ''} CHAR(40)"""
                    )
                else:
                    dk_list.append(
                        f"""{'"' if quote else ''}{field["column_name"]}_dk{'"' if quote else ''} {field["column_type"]}"""
                    )

            sql_str = SQLTemplates.CREATE_LINK.format(
                table_type=table_type,
                table_base_name=table_base_name,
                columns=",\n               ".join([*hk_list, *dk_list]),
            )

            _, error = self.db.execute_sql_safely(
                sql_str, description=f"Create {table_type}_{table_base_name}"
            )
            if error:
                errors.append(error)

        return errors

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

        records = [
            r
            for r in raw_records
            if r["target_table"].startswith("link_")
            or r["target_table"].startswith("nhl_")
        ]

        bk_per_group = self.hash_generator.groupby(
            record_list=records, keys=["target_table", "group_name"]
        )

        errors: List[Tuple[str, str]] = []

        for link_group, fields in bk_per_group.items():
            link_name = link_group.split(".")[0]
            group_name = link_group.split(".")[1]

            link_hashkey = (
                f"{link_name[5:] if link_name.startswith('l') else link_name[4:]}_hk"
            )

            link_fields = ",\n                ".join(
                [
                    f"SUB.{hk_field['source_field']}{'_hk' if hk_field['transfer_type']=='ll' else ''} AS {hk_field['target_field']}"
                    for hk_field in fields
                ]
            )

            source_fields = ",\n                   ".join(
                [
                    f"src.{hk_field['source_field']}{'_hk' if hk_field['transfer_type']=='ll' else ''}"
                    for hk_field in fields
                ]
            )

            insert_sql = SQLTemplates.INSERT_LINK.format(
                link_name=link_name,
                link_hashkey=link_hashkey,
                load_date=load_date,
                record_source=record_source,
                run_id=run_id,
                link_fields=link_fields,
                group_name=group_name,
                source_fields=source_fields,
                stg_table=stg_table,
            )

            if verbose:
                print(insert_sql)

            _, error = self.db.execute_sql_safely(
                insert_sql, description=f"Load link {link_name}"
            )
            if error:
                errors.append(error)

        return errors
