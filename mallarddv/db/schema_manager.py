"""
Schema management for MallardDataVault.
"""
import logging
import os
from typing import Dict, List, Optional, Any, Tuple

from .database_connection import DatabaseConnection
from .sql_templates import SQLTemplates
from ..exceptions import DVSQLError

logger = logging.getLogger("mallarddv")


class SchemaManager:
    """
    Manages database schemas and tables for MallardDataVault.
    """
    
    schema_list = ["stg", "dv", "bv", "dm", "metadata"]

    def __init__(self, db: DatabaseConnection):
        """
        Initialize with database connection.
        
        Args:
            db: Database connection
        """
        self.db = db
    
    def create_schemas(self, verbose: bool = False) -> List[Tuple[str, str]]:
        """
        Create required schemas if they don't exist.
        
        Args:
            verbose: Whether to print additional information during execution
            
        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []
        
        for schema in self.schema_list:
            sql_str = SQLTemplates.CREATE_SCHEMA.format(schema=schema)
            if verbose:
                print(sql_str)
                
            _, error = self.db.execute_sql_safely(
                sql_str,
                description=f"Create schema {schema}"
            )
            if error:
                errors.append(error)
        
        return errors
    
    def create_staging_table_from_metadata(
        self, metadata_records: List[Dict[str, Any]], verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Create staging tables based on metadata definitions.
        
        Args:
            metadata_records: Metadata records for staging tables
            verbose: Whether to print additional information during execution
            
        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []
        
        # Group records by base_name
        base_name_groups = {}
        for record in metadata_records:
            base_name = record["base_name"]
            if base_name not in base_name_groups:
                base_name_groups[base_name] = []
            base_name_groups[base_name].append(record)
        
        # Create staging tables
        for base_name, records in base_name_groups.items():
            if verbose:
                print(f"Found a staging table to create: {base_name}")
            
            columns = []
            for record in records:
                if record["mapping"] == "c":
                    column_name = record["column_name"]
                    quote = " " in column_name
                    columns.append(
                        f"""{'"' if quote else ''}{column_name}{'"' if quote else ''} {record["column_type"]}"""
                    )
            
            if not columns:
                continue
                
            sql = SQLTemplates.CREATE_STAGING_TABLE.format(
                base_name=base_name,
                columns=",\n               ".join(columns)
            )
            
            if verbose:
                print(sql)
            
            _, error = self.db.execute_sql_safely(
                sql,
                description=f"Create staging table {base_name}"
            )
            if error:
                errors.append(error)
        
        return errors
    
    def apply_script_from_metadata(
        self, rel_type: str, scripts_path: str, 
        metadata_records: List[Dict[str, Any]], verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Apply SQL scripts based on metadata records.
        
        Args:
            rel_type: Relationship type (e.g., "stg_vw")
            scripts_path: Path to script files
            metadata_records: Metadata records
            verbose: Whether to print additional information during execution
            
        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []
        
        # Filter records by rel_type
        filtered_records = [r for r in metadata_records if r["rel_type"] == rel_type]
        
        # Group by base_name
        base_name_groups = {}
        for record in filtered_records:
            base_name = record["base_name"]
            if base_name not in base_name_groups:
                base_name_groups[base_name] = []
            base_name_groups[base_name].append(record)
        
        # Apply scripts
        for base_name, records in base_name_groups.items():
            if verbose:
                print(f"Found a {rel_type} to create: {base_name}")
            
            for record in records:
                sub_folder = record["column_name"]
                
                script_path = os.path.join(scripts_path, sub_folder, f"{base_name}.sql")
                if verbose:
                    print(script_path)
                
                try:
                    with open(script_path, "r") as script_file:
                        script = script_file.read()
                    
                    if verbose:
                        print(script)
                    
                    _, error = self.db.execute_sql_safely(
                        script,
                        description=f"Apply script {script_path}"
                    )
                    if error:
                        errors.append(error)
                except Exception as ex:
                    errors.append((script_path, str(ex)))
        
        return errors