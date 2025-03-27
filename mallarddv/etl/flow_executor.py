"""
Flow execution for MallardDataVault.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

from ..db.database_connection import DatabaseConnection
from ..db.schema_manager import SchemaManager
from ..metadata.metadata_manager import MetadataManager
from ..datavault.hash_generator import HashViewGenerator
from ..datavault.hub_manager import DVHubManager
from ..datavault.link_manager import DVLinkManager
from ..datavault.satellite_manager import DVSatelliteManager
from ..etl.etl_service import ETLService
from ..exceptions import DVETLError

logger = logging.getLogger("mallarddv")


class FlowExecutor:
    """
    Orchestrates complete data vault loading processes.
    """
    
    def __init__(
        self,
        db: DatabaseConnection,
        metadata_manager: MetadataManager,
        schema_manager: SchemaManager,
        hash_generator: HashViewGenerator,
        hub_manager: DVHubManager,
        link_manager: DVLinkManager,
        satellite_manager: DVSatelliteManager,
        etl_service: ETLService
    ):
        """
        Initialize with all required services.
        
        Args:
            db: Database connection
            metadata_manager: Metadata manager
            schema_manager: Schema manager
            hash_generator: Hash generator
            hub_manager: Hub manager
            link_manager: Link manager
            satellite_manager: Satellite manager
            etl_service: ETL service
        """
        self.db = db
        self.metadata_manager = metadata_manager
        self.schema_manager = schema_manager
        self.hash_generator = hash_generator
        self.hub_manager = hub_manager
        self.link_manager = link_manager
        self.satellite_manager = satellite_manager
        self.etl_service = etl_service
    
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
            source_table: Name of the source table to use
            record_source: Data Vault run source identifier
            file_path: Optional path to file to load
            load_date_overwrite: Optional date string to use instead of current timestamp
            force_load: Whether to load the file even if already processed
            verbose: Whether to print additional information during execution
            
        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []
        
        if verbose:
            print(
                f"{'FORCE ' if force_load else ''}LOAD {source_table} for {record_source} from {file_path}{f' with date overwrite {load_date_overwrite}' if load_date_overwrite else ''}"
            )
        
        # Check if already ingested
        if not force_load and file_path:
            try:
                if self.metadata_manager.check_previous_ingestion(source_table, file_path, "success"):
                    if verbose:
                        print(f"File {file_path} already ingested for {source_table}")
                    return errors
            except Exception as ex:
                errors.append((f"Check previous ingestion for {file_path}", str(ex)))
                return errors
        
        # Get run ID
        try:
            run_id = self.metadata_manager.get_next_run_id()
        except Exception as ex:
            errors.append(("Get next run ID", str(ex)))
            return errors
        
        # Register run start
        if verbose:
            print("Register run start")
        try:
            self.metadata_manager.register_run_info(
                source_table=source_table,
                run_id=run_id,
                file_path=file_path,
                status="start"
            )
        except Exception as ex:
            errors.append(("Register run start", str(ex)))
            return errors
        
        # Load file to staging if provided
        if file_path:
            if verbose:
                print("Load new data")
            errors.extend(
                self.etl_service.load_file_to_staging(
                    source_table=source_table,
                    file_path=file_path,
                    verbose=verbose
                )
            )
            if errors:
                self._register_run_end(source_table, run_id, file_path, "failure", errors)
                return errors
        
        # Compute hash view
        if verbose:
            print("Compute hash view")
        errors.extend(
            self.hash_generator.compute_hash_view(
                stg_table=source_table,
                verbose=verbose
            )
        )
        if errors:
            self._register_run_end(source_table, run_id, file_path, "failure", errors)
            return errors
        
        # Load hubs
        if verbose:
            print("Load hubs")
        errors.extend(
            self.hub_manager.load_related_hubs(
                stg_table=source_table,
                run_id=run_id,
                record_source=record_source,
                load_date_overwrite=load_date_overwrite,
                verbose=verbose
            )
        )
        if errors:
            self._register_run_end(source_table, run_id, file_path, "failure", errors)
            return errors
        
        # Load links
        if verbose:
            print("Load links")
        errors.extend(
            self.link_manager.load_related_links(
                stg_table=source_table,
                run_id=run_id,
                record_source=record_source,
                load_date_overwrite=load_date_overwrite,
                verbose=verbose
            )
        )
        if errors:
            self._register_run_end(source_table, run_id, file_path, "failure", errors)
            return errors
        
        # Load satellites
        if verbose:
            print("Load satellites")
        errors.extend(
            self.satellite_manager.load_related_sats(
                stg_table=source_table,
                run_id=run_id,
                record_source=record_source,
                load_date_overwrite=load_date_overwrite,
                verbose=verbose
            )
        )
        
        # Register run end
        self._register_run_end(
            source_table=source_table,
            run_id=run_id,
            file_path=file_path,
            status="success" if not errors else "failure",
            errors=errors
        )
        
        return errors
    
    def _register_run_end(
        self,
        source_table: str,
        run_id: int,
        file_path: str,
        status: str,
        errors: List[Tuple[str, str]] = None
    ) -> None:
        """
        Register the end of a run.
        
        Args:
            source_table: Source table name
            run_id: Run ID
            file_path: Path to the source file
            status: Status of the run
            errors: Optional list of errors
        """
        message = ""
        if errors:
            message = f"{len(errors)} errors occurred"
            if errors and len(errors) > 0:
                message += f": {errors[0][1]}"
                if len(errors) > 1:
                    message += f" and {len(errors) - 1} more"
        
        try:
            self.metadata_manager.register_run_info(
                source_table=source_table,
                run_id=run_id,
                file_path=file_path,
                status=status,
                message=message[:4095]  # Limit message length
            )
        except Exception as ex:
            logger.error(f"Failed to register run end: {str(ex)}")