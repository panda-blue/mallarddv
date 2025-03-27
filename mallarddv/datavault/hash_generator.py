"""
Hash generation for Data Vault.
"""
import logging
import itertools as it
from typing import Dict, List, Optional, Any, Tuple

from ..db.database_connection import DatabaseConnection
from ..db.sql_templates import SQLTemplates
from ..metadata.metadata_manager import MetadataManager
from ..exceptions import DVSQLError

logger = logging.getLogger("mallarddv")


class HashViewGenerator:
    """
    Generates hash views for Data Vault.
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
    
    def hash_fields(self, name: str, fields: List[str]) -> str:
        """
        Generate SQL to create a hash value from a list of fields.
        
        Args:
            name: Name for the generated hash column
            fields: List of field names to include in the hash
            
        Returns:
            SQL expression that creates a SHA1 hash of concatenated field values
        """
        coalesce_expressions = [f"coalesce({f}::string,'')" for f in fields]
        concat_expression = f"concat_ws('||',{','.join(coalesce_expressions)})"
        
        return f"sha1(upper({concat_expression})) as {name}"
    
    def groupby(
        self, record_list: List[Dict[str, Any]], keys: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group records by concatenated key values.
        
        Args:
            record_list: List of dictionary records to group
            keys: List of dictionary keys to group by
            
        Returns:
            Dictionary with grouped records where keys are concatenated key values
            separated by dots and values are lists of matching records
        """
        return {
            k: list(l)
            for k, l in it.groupby(
                record_list, key=lambda r: f"{'.'.join([r[key] for key in keys])}"
            )
        }
    
    def compute_hash_view(
        self, stg_table: str, verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Create a hash view for a staging table to prepare data for data vault loading.
        
        This method generates SQL to create a view that contains all necessary hash keys
        and hash diffs required for loading data into hubs, links, and satellites.
        
        Args:
            stg_table: Name of staging table to create hash view for
            verbose: Whether to print generated SQL for debugging
            
        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        if verbose:
            print(f"Computing hash view for {stg_table}")
        
        errors = []
        records = self.metadata_manager.get_transitions(source_table=stg_table)
        
        # Base source fields
        cte_fields = list(
            set(
                [
                    f"""{(r["transformation"] or "#").replace("#",r["source_field"])} as {r["source_field"]}"""
                    for r in records
                    if not (r["raw"])
                    and r["transfer_type"] not in ("ll", "sat_delta", "sat_full")
                ]
            )
        )
        
        # Add raw fields
        cte_fields.extend(
            [
                f"""'{r["source_field"]}' as {r["source_field"]}"""
                for r in records
                if r["raw"]
            ]
        )
        
        if verbose:
            print(cte_fields)
        
        # Hub hash keys
        hash_field_list: List[str] = []
        
        hub_hash_groups = self.groupby(
            [r for r in records if r["transfer_type"] == "bk"], ["group_name"]
        )
        
        for k, g in hub_hash_groups.items():
            if verbose:
                print(f"Hub hash found: {k}")
            source_field_names = [c["source_field"] for c in g]
            hash_field_list.append(self.hash_fields(f"{k}_hk", source_field_names))
        
        link_hash_groups = self.groupby(
            [r for r in records if r["transfer_type"] in ("ll", "dk")], ["group_name"]
        )
        
        # Link hash keys
        for link_key, key_source in link_hash_groups.items():
            if verbose:
                print(f"Link key found: {link_key}")
            key_source_names: List[str] = []
            for ks in key_source:
                if verbose:
                    print(f"       {ks}")
                if ks["transfer_type"] == "ll":
                    for hhg in hub_hash_groups[ks["source_field"]]:
                        key_source_names.append(hhg["source_field"])
                else:
                    key_source_names.append(ks["source_field"])
            
            hash_field_list.append(
                self.hash_fields(f"{link_key}_hk", key_source_names)
            )
        
        # Satellite hash diff
        sat_hash_groups = self.groupby(
            [r for r in records if r["transfer_type"] == "f"], ["group_name"]
        )
        
        for sat_field, sat_source in sat_hash_groups.items():
            if verbose:
                print(f"Sat hash found: {sat_field}")
            source_field_names = [c["source_field"] for c in sat_source]
            hash_field_list.append(
                self.hash_fields(f"{sat_field}_hashdiff", source_field_names)
            )
        
        combined = [*hash_field_list, "*"]
        
        hash_view_sql = SQLTemplates.CREATE_HASH_VIEW.format(
            stg_table=stg_table,
            cte_fields=",\n               ".join(cte_fields),
            combined=",\n           ".join(combined)
        )
        
        if verbose:
            print(hash_view_sql)
        
        _, error = self.db.execute_sql_safely(
            hash_view_sql,
            description=f"Create hash view for {stg_table}"
        )
        if error:
            errors.append(error)
        
        return errors