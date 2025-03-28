import pytest
import duckdb
from datetime import datetime
from typing import Dict, List, Any

from mallarddv.mallarddv import MallardDataVault
from mallarddv.utils.test_adapter import inject_test_db


class TestMallardDataVaultIntegration:
    """Integration tests for MallardDataVault class using a real in-memory DuckDB database"""

    @pytest.fixture
    def setup_db(self):
        """Create an in-memory database with test schema"""
        # Connect to in-memory database
        db = duckdb.connect(":memory:")

        # Create metadata schema
        db.sql("CREATE SCHEMA metadata")
        db.sql(
            """
            CREATE TABLE metadata.transitions (
                source_table VARCHAR(255),
                source_field VARCHAR(255),
                target_table VARCHAR(255),
                group_name VARCHAR(255),
                target_field VARCHAR(255),
                position INTEGER,
                raw BOOLEAN,
                transformation VARCHAR(255),
                transfer_type VARCHAR(50)
            )
        """
        )

        db.sql(
            """
            CREATE TABLE metadata.tables (
                base_name VARCHAR(255),
                rel_type VARCHAR(50),
                column_name VARCHAR(255),
                column_type VARCHAR(50),
                column_position INTEGER,
                mapping VARCHAR(50)
            )
        """
        )

        # Create staging and data vault schemas
        db.sql("CREATE SCHEMA stg")
        db.sql("CREATE SCHEMA dv")
        db.sql("CREATE SCHEMA bv")

        # Create a test staging table
        db.sql(
            """
            CREATE TABLE stg.customer (
                id INTEGER,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255),
                created_date TIMESTAMP
            )
        """
        )

        # Insert test data
        db.sql(
            """
            INSERT INTO stg.customer VALUES
            (1, 'John', 'Doe', 'john.doe@example.com', '2023-01-01 10:00:00'),
            (2, 'Jane', 'Smith', 'jane.smith@example.com', '2023-01-02 11:00:00')
        """
        )

        # Insert metadata for mapping staging to data vault
        db.sql(
            """
            INSERT INTO metadata.transitions VALUES
            ('customer', 'id', 'hub_customer', 'customer', 'id_bk', 1, false, null, 'bk'),
            ('customer', 'first_name', 'hsat_customer_details', 'customer_details', 'first_name', 1, false, null, 'f'),
            ('customer', 'last_name', 'hsat_customer_details', 'customer_details', 'last_name', 2, false, null, 'f'),
            ('customer', 'email', 'hsat_customer_details', 'customer_details', 'email', 3, false, null, 'f'),
            ('customer', 'customer_hk', 'hsat_customer_details', 'customer_details', 'customer', 0, false, null, 'sat_delta')
        """
        )

        # Insert metadata for data vault tables
        db.sql(
            """
            INSERT INTO metadata.tables VALUES
            ('customer', 'hub', 'id', 'INTEGER', 1, 'pk'),
            ('customer_details', 'hsat', 'customer', 'INTEGER', 1, 'hk'),
            ('customer_details', 'hsat', 'first_name', 'VARCHAR(255)', 2, 'attr'),
            ('customer_details', 'hsat', 'last_name', 'VARCHAR(255)', 3, 'attr'),
            ('customer_details', 'hsat', 'email', 'VARCHAR(255)', 4, 'attr')
        """
        )

        return db

    @pytest.fixture
    def mdv(self, setup_db):
        """Create a MallardDataVault with the test database"""
        mdv = MallardDataVault(":memory:")
        inject_test_db(mdv, setup_db)
        return mdv

    def test_create_hub_from_metadata_integration(self, mdv):
        """Test creating a hub table from metadata"""
        # Create the hub
        errors = mdv.create_hub_from_metadata(verbose=True)

        # Check no errors occurred
        assert errors == []

        # Verify the table was created
        result = mdv.db.sql(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'hub_customer'"
        )
        assert result.fetchone() is not None

        # Check table structure
        result = mdv.db.sql("PRAGMA table_info('dv.hub_customer')")
        columns = {row[1]: row for row in result.fetchall()}

        assert "customer_hk" in columns
        assert "load_dts" in columns
        assert "record_source" in columns
        assert "run_id" in columns
        assert "id_bk" in columns

    def test_create_sat_from_metadata_integration(self, mdv):
        """Test creating a satellite table from metadata"""
        # Create the satellite
        errors = mdv.create_sat_from_metadata(verbose=True)

        # Check no errors occurred
        assert errors == []

        # Verify the table was created
        result = mdv.db.sql(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'hsat_customer_details'"
        )
        assert result.fetchone() is not None

        # Check table structure
        result = mdv.db.sql("PRAGMA table_info('dv.hsat_customer_details')")
        columns = {row[1]: row for row in result.fetchall()}

        assert "customer_hk" in columns
        assert "load_dts" in columns
        assert "del_flag" in columns
        assert "hash_diff" in columns
        assert "record_source" in columns
        assert "run_id" in columns
        assert "first_name" in columns
        assert "last_name" in columns
        assert "email" in columns

    def test_compute_hash_view_integration(self, mdv):
        """Test creating a hash view for staging tables"""
        # First create the hub needed for proper hash generation
        mdv.create_hub_from_metadata()

        # Create the hash view
        errors = mdv.compute_hash_view("customer", verbose=True)

        # Check no errors occurred
        assert errors == []

        # Verify the view was created
        result = mdv.db.sql(
            "SELECT table_name FROM information_schema.views WHERE table_name = 'customer_hash_vw'"
        )
        assert result.fetchone() is not None

        # Check that the view returns expected columns
        result = mdv.db.sql("SELECT * FROM stg.customer_hash_vw LIMIT 1")
        columns = result.columns

        assert "customer_hk" in columns
        assert "customer_details_hashdiff" in columns
        assert "id" in columns
        assert "first_name" in columns
        assert "last_name" in columns
        assert "email" in columns

    def test_end_to_end_data_vault_loading(self, mdv):
        """Test end-to-end data vault loading process"""
        # Create the hub table
        mdv.create_hub_from_metadata(verbose=True)

        # Create the satellite table
        mdv.create_sat_from_metadata(verbose=True)

        # Create the hash view
        mdv.compute_hash_view("customer", verbose=True)

        # Load hubs
        hub_errors = mdv.load_related_hubs("customer", 1, "TEST", verbose=True)
        assert hub_errors == []

        # Verify hub data was loaded
        result = mdv.db.sql("SELECT * FROM dv.hub_customer")
        hub_rows = result.fetchall()
        assert len(hub_rows) == 2

        # Load satellites
        sat_errors = mdv.load_related_sats("customer", 1, "TEST", verbose=True)
        assert sat_errors == []

        # Verify satellite data was loaded
        result = mdv.db.sql("SELECT * FROM dv.hsat_customer_details")
        sat_rows = result.fetchall()
        assert len(sat_rows) == 2

        # Create current view
        cv_errors = mdv.create_current_sat_from_metadata(verbose=True)
        assert cv_errors == []

        # Verify current view was created
        result = mdv.db.sql(
            "SELECT table_name FROM information_schema.views WHERE table_name = 'hsat_customer_details_cv'"
        )
        assert result.fetchone() is not None

        # Verify current view data
        result = mdv.db.sql("SELECT * FROM bv.hsat_customer_details_cv")
        cv_rows = result.fetchall()
        assert len(cv_rows) == 2

    def test_mdv_automated_creation(self):
        with MallardDataVault("./demo/demo.db", scripts_path="./demo/models") as mdv:
            errors = mdv.init_mallard_db(
                meta_only=False,
                meta_transitions_path="./demo/transitions.csv",
                meta_tables_path="./demo/tables.csv",
                verbose=True,
            )
            assert len(errors) == 0

    def test_mdv_automated_run(self):
        with MallardDataVault("demo/demo.db", scripts_path="./demo/models") as mdv:
            errors = mdv.execute_flow(
                "customer", f"demo-customer", f"demo/data/customer.csv", verbose=True
            )
            assert len(errors) == 0


if __name__ == "__main__":
    pytest.main(["-xvs", "test_dv_integration.py"])
