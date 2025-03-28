"""
Satellite management for Data Vault.
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


class DVSatelliteManager:
    """
    Creates and loads satellite tables for Data Vault.
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
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []
        raw_sat_records = []

        if rel_type:
            raw_sat_records = self.metadata_manager.get_tables(
                base_name=base_name, rel_type=rel_type
            )
        else:
            raw_sat_records = self.metadata_manager.get_tables(
                base_name=base_name, rel_type="hsat"
            )
            raw_sat_records.extend(
                self.metadata_manager.get_tables(base_name=base_name, rel_type="lsat")
            )

        records = self.hash_generator.groupby(
            raw_sat_records, keys=["rel_type", "base_name"]
        )

        for rel_base_name, fields in records.items():
            sbn = rel_base_name.split(".")
            table_type = sbn[0]
            table_base_name = ".".join(sbn[1:])

            if verbose:
                print(f"Found a {table_type} to create: {table_base_name}")

            hk_list: List[str] = []
            field_list: List[str] = []

            for field in fields:
                quote = " " in field["column_name"]

                if field["mapping"] == "hk":
                    hk_list.append(
                        f"""{'"' if quote else ''}{field["column_name"]}_hk{'"' if quote else ''} CHAR(40)"""
                    )
                else:
                    field_list.append(
                        f"""{'"' if quote else ''}{field["column_name"]}{'"' if quote else ''} {field["column_type"]}"""
                    )

            sql_str = SQLTemplates.CREATE_SAT.format(
                table_type=table_type,
                table_base_name=table_base_name,
                hub_key=hk_list[0] if len(hk_list) else "",
                comma=", " if len(field_list) else "",
                fields=",\n               ".join(field_list),
            )

            try:
                if len(hk_list) != 1:
                    raise DVEntityError(
                        f"Satellite {table_base_name} must have exactly one hub key"
                    )

                _, error = self.db.execute_sql_safely(
                    sql_str, description=f"Create {table_type}_{table_base_name}"
                )
                if error:
                    errors.append(error)
            except Exception as ex:
                errors.append((sql_str, str(ex)))

        return errors

    def create_current_sat_from_metadata(
        self,
        base_name: Optional[str] = None,
        rel_type: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Create current value views for satellite tables based on metadata definitions.

        Args:
            base_name: Optional filter to create only views for satellites of a specific base entity
            rel_type: Optional filter to create only views for satellites of a specific type (hsat, lsat)
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []
        raw_sat_records = []

        if rel_type:
            raw_sat_records = self.metadata_manager.get_tables(
                base_name=base_name, rel_type=rel_type
            )
        else:
            raw_sat_records = self.metadata_manager.get_tables(
                base_name=base_name, rel_type="hsat"
            )
            raw_sat_records.extend(
                self.metadata_manager.get_tables(base_name=base_name, rel_type="lsat")
            )

        records = self.hash_generator.groupby(
            raw_sat_records, keys=["rel_type", "base_name"]
        )

        for rel_base_name, fields in records.items():
            sbn = rel_base_name.split(".")
            table_type = sbn[0]
            table_base_name = ".".join(sbn[1:])

            if verbose:
                print(f"Found a {table_type} to create: {table_base_name}")

            hk_list: List[str] = []
            field_list: List[str] = []

            for field in fields:
                quote = " " in field["column_name"]

                if field["mapping"] == "hk":
                    hk_list.append(
                        f"""{'"' if quote else ''}{field["column_name"]}_hk{'"' if quote else ''}"""
                    )
                else:
                    field_list.append(
                        f"""{'"' if quote else ''}{field["column_name"]}{'"' if quote else ''}"""
                    )

            sql_str = SQLTemplates.CREATE_CURRENT_VIEW.format(
                table_type=table_type,
                table_base_name=table_base_name,
                hub_key=hk_list[0] if len(hk_list) else "",
                comma=",\n            " if len(field_list) else "",
                fields=",\n               ".join(field_list),
            )

            if verbose:
                print(sql_str)

            try:
                if len(hk_list) != 1:
                    raise DVEntityError(
                        f"Satellite {table_base_name} must have exactly one hub key"
                    )

                _, error = self.db.execute_sql_safely(
                    sql_str,
                    description=f"Create current view for {table_type}_{table_base_name}",
                )
                if error:
                    errors.append(error)
            except Exception as ex:
                errors.append((sql_str, str(ex)))

        return errors

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
            stg_table: Name of the staging table containing the source data
            run_id: Identifier for the current load process
            record_source: Identifier for the data source system
            load_date_overwrite: Optional date string to use instead of current timestamp
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        load_date = (
            load_date_overwrite
            or f"""'{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}'"""
        )

        raw_records = self.metadata_manager.get_transitions(source_table=stg_table)

        sat_load = [
            r for r in raw_records if r["transfer_type"] in ("sat_delta", "sat_full")
        ]

        errors: List[Tuple[str, str]] = []

        for sat in sat_load:
            if verbose:
                print(f"""Sat found: {sat["target_table"]}""")
            target_group = sat["group_name"]

            records = [
                r
                for r in raw_records
                if r["target_table"] == sat["target_table"]
                and r["group_name"] == target_group
                and r["transfer_type"] == "f"
            ]
            if verbose:
                print(f"Found {len(records)} field{'s' if len(records)>1 else ''}")

            bk_per_group = self.hash_generator.groupby(
                record_list=records, keys=["target_table", "group_name"]
            )

            fields = bk_per_group.get(f"""{sat["target_table"]}.{target_group}""", [])

            sat_name = sat["target_table"]
            group_name = target_group
            sat_hashkey = f"""{sat["target_field"]}_hk"""

            field_list = ",\n                ".join(
                [f"{hk_field['target_field']}" for hk_field in fields]
            )
            select_fields = ",\n                   ".join(
                [f"src.{hk_field['source_field']}" for hk_field in fields]
            )

            # Insert new records
            insert_new_sql = SQLTemplates.INSERT_SAT_NEW.format(
                sat_name=sat_name,
                sat_hashkey=sat_hashkey,
                load_date=load_date,
                source_field=sat["source_field"],
                hashdiff_field=(
                    f"{group_name}_hashdiff" if len(fields) else sat["source_field"]
                ),
                record_source=record_source,
                run_id=run_id,
                comma="," if len(fields) else "",
                field_list=field_list,
                select_fields=select_fields,
                stg_table=stg_table,
            )

            if verbose:
                print("Preparing for execution (new): ")
                print(insert_new_sql)

            _, error = self.db.execute_sql_safely(
                insert_new_sql, description=f"Load satellite {sat_name} (new records)"
            )
            if error:
                errors.append(error)

            # Insert deleted records for full satellites
            if sat["transfer_type"] == "sat_full":
                insert_del_sql = SQLTemplates.INSERT_SAT_DELETE.format(
                    sat_name=sat_name,
                    sat_hashkey=sat_hashkey,
                    load_date=load_date,
                    source_field=sat["source_field"],
                    hashdiff_field=(
                        f"{group_name}_hashdiff" if len(fields) else sat["source_field"]
                    ),
                    record_source=record_source,
                    run_id=run_id,
                    comma="," if len(fields) else "",
                    comma_if_fields="," if len(fields) else "",
                    field_list=field_list,
                    select_sat_fields=",\n                   ".join(
                        [f"sat.{hk_field['target_field']}" for hk_field in fields]
                    ),
                    stg_table=stg_table,
                )

                if verbose:
                    print("Preparing for execution (del):")
                    print(insert_del_sql)

                _, error = self.db.execute_sql_safely(
                    insert_del_sql,
                    description=f"Load satellite {sat_name} (deleted records)",
                )
                if error:
                    errors.append(error)

        return errors
