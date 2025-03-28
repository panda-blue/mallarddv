import os
import pytest
import duckdb
from unittest.mock import patch, MagicMock
from datetime import datetime
from typing import List, Dict, Any, Tuple

from mallarddv.mallarddv import MallardDataVault
from mallarddv.utils.test_adapter import inject_test_db


class TestMallardDataVault:
    """Tests for the MallardDataVault class"""

    @pytest.fixture
    def mock_db(self):
        """Create a mock database connection"""
        mock = MagicMock(spec=duckdb.DuckDBPyConnection)
        # Create a mock for sql method that can be chained
        mock_relation = MagicMock()
        mock_relation.columns = ["col1", "col2"]
        mock_relation.fetchall.return_value = [("value1", "value2")]
        mock.sql.return_value = mock_relation
        return mock

    @pytest.fixture
    def mdv(self, mock_db):
        """Create a MallardDataVault instance with a mock db"""
        mdv = MallardDataVault("in_memory")
        inject_test_db(mdv, mock_db)
        return mdv

    def test_init(self):
        """Test initialization sets database path"""
        mdv = MallardDataVault("test_db.duckdb")
        assert mdv.db_path == "test_db.duckdb"
        # In the refactored version, we create the database connection immediately
        # but don't connect to the database
        assert mdv.db.db is None

    def test_init_from_file(self):

        db_path = "./demo/demo.db"
        if os.path.isfile(db_path):
            os.remove(db_path)

        with MallardDataVault(db_path, "./demo/models/") as mdv:
            res = mdv.init_mallard_db(
                False,
                meta_tables_path="./demo/tables.csv",
                meta_transitions_path="./demo/transitions.csv",
            )

            assert len(res) == 0

    def test_hash_view_generation_from_file(self):
        db_path = "./demo/demo.db"
        with MallardDataVault(db_path, "./demo/models/") as mdv:
            res = []

            for source_table in ["customer", "product"]:
                res.extend(mdv.compute_hash_view(source_table))

            assert len(res) == 0

    def test_load_related_hubs_from_file(self):
        db_path = "./demo/demo.db"

        with MallardDataVault(db_path, "./demo/models/") as mdv:
            res = []

            for source_table in ["customer", "product"]:
                res.extend(mdv.load_related_hubs(source_table, 1, "demo-customer"))

            assert len(res) == 0

    def test_load_related_links_from_file(self):
        db_path = "./demo/demo.db"

        with MallardDataVault(db_path, "./demo/models/") as mdv:
            res = []

            for source_table in ["customer", "product"]:
                res.extend(mdv.load_related_links(source_table, 1, "demo-customer"))
                res.extend(mdv.load_related_links(source_table, 1, "demo-customer"))

            assert len(res) == 0

    def test_load_related_sats_from_file(self):
        db_path = "./demo/demo.db"

        with MallardDataVault(db_path, "./demo/models/") as mdv:
            res = []

            for source_table in ["customer", "product"]:
                res.extend(mdv.load_related_sats(source_table, 1, "demo-customer"))
                res.extend(mdv.load_related_sats(source_table, 1, "demo-customer"))

            assert len(res) == 0

    def test_enter(self, monkeypatch):
        """Test __enter__ connects to the database"""
        mock_connect = MagicMock()
        monkeypatch.setattr(duckdb, "connect", mock_connect)

        mdv = MallardDataVault("test_db.duckdb")
        result = mdv.__enter__()

        mock_connect.assert_called_once_with("test_db.duckdb")
        assert result is mdv

    def test_exit(self, mdv):
        """Test __exit__ closes the database connection"""
        mdv.__exit__(None, None, None)
        # The close method is now called on our original mock_db via the adapter
        assert mdv.db.real_db.close.call_count > 0

    def test_hash_fields(self, mdv):
        """Test _hash_fields method generates correct SQL for hashing fields"""
        # In the refactored version, hash_fields is in the hash_generator class
        result = mdv.hash_generator.hash_fields("test_hash", ["field1", "field2"])
        expected = "sha1(upper(concat_ws('||',coalesce(field1::string,''),coalesce(field2::string,'')))) as test_hash"
        assert result == expected

    def test_fetch_dict(self, mdv):
        """Test fetch_dict converts query results to dictionaries"""
        result = mdv.db.fetch_dict("SELECT * FROM test")
        assert result == [{"col1": "value1", "col2": "value2"}]

    def test_get_transitions(self, mdv):
        """Test get_transitions queries the metadata table correctly"""
        mdv.metadata_manager.get_transitions("test_table")

        # Check that the SQL call was made with the correct parameters
        called_sql = mdv.db.real_db.sql.call_args[0][0]
        called_params = mdv.db.real_db.sql.call_args[1].get('params')
        
        # With our refactoring, we now use parameterized queries
        assert "metadata.transitions" in called_sql
        assert "WHERE source_table = ?" in called_sql
        assert called_params == ["test_table"]
        assert (
            "order by source_table, target_table, group_name, position"
            in called_sql.lower()
        )

    def test_get_tables_with_no_filters(self, mdv):
        """Test get_tables with no filters"""
        mdv.metadata_manager.get_tables()

        # Check SQL doesn't have WHERE clause
        called_sql = mdv.db.real_db.sql.call_args[0][0]
        called_params = mdv.db.real_db.sql.call_args[1].get('params')
        assert "WHERE" not in called_sql
        assert called_params is None or called_params == []

    def test_get_tables_with_base_name(self, mdv):
        """Test get_tables with base_name filter"""
        mdv.metadata_manager.get_tables(base_name="test_base")

        # Check SQL has WHERE with base_name and params
        called_sql = mdv.db.real_db.sql.call_args[0][0]
        called_params = mdv.db.real_db.sql.call_args[1].get('params')
        assert "WHERE" in called_sql
        assert "base_name = ?" in called_sql
        assert called_params == ["test_base"]

    def test_get_tables_with_rel_type(self, mdv):
        """Test get_tables with rel_type filter"""
        mdv.metadata_manager.get_tables(rel_type="hub")

        # Check SQL has WHERE with rel_type and params
        called_sql = mdv.db.real_db.sql.call_args[0][0]
        called_params = mdv.db.real_db.sql.call_args[1].get('params')
        assert "WHERE" in called_sql
        assert "rel_type = ?" in called_sql
        assert called_params == ["hub"]

    def test_get_tables_with_both_filters(self, mdv):
        """Test get_tables with both filters"""
        mdv.metadata_manager.get_tables(base_name="test_base", rel_type="hub")

        # Check SQL has WHERE with both conditions and params
        called_sql = mdv.db.real_db.sql.call_args[0][0]
        called_params = mdv.db.real_db.sql.call_args[1].get('params')
        assert "WHERE" in called_sql
        assert "base_name = ?" in called_sql
        assert "AND" in called_sql
        assert "rel_type = ?" in called_sql
        assert called_params == ["test_base", "hub"]

    def test_groupby(self, mdv):
        """Test the groupby method correctly groups records"""
        records = [
            {"type": "hub", "name": "customer", "value": 1},
            {"type": "hub", "name": "customer", "value": 2},
            {"type": "hub", "name": "product", "value": 3},
            {"type": "link", "name": "order", "value": 4},
        ]

        # Group by type and name
        result = mdv.hash_generator.groupby(records, ["type", "name"])

        assert len(result) == 3
        assert "hub.customer" in result
        assert "hub.product" in result
        assert "link.order" in result
        assert len(result["hub.customer"]) == 2
        assert len(result["hub.product"]) == 1
        assert len(result["link.order"]) == 1

    def test_sql(self, mdv):
        """Test the sql method passes through to the db connection"""
        result = mdv.sql("SELECT * FROM test")
        # Our adapter passes None as the second argument when no params are provided
        mdv.db.real_db.sql.assert_called_once_with("SELECT * FROM test", None)
        assert result == mdv.db.real_db.sql.return_value

    def test_compute_hash_view(self, mdv):
        """Test compute_hash_view generates hash view SQL"""
        # Mock return values for dependencies
        mock_records = [
            {
                "source_field": "id",
                "target_field": "id_bk",
                "group_name": "customer",
                "raw": False,
                "transfer_type": "bk",
                "transformation": None,
            },
            {
                "source_field": "name",
                "target_field": "name",
                "group_name": "customer_details",
                "raw": False,
                "transfer_type": "f",
                "transformation": None,
            },
        ]

        with patch.object(mdv.metadata_manager, "get_transitions", return_value=mock_records):
            with patch.object(mdv.hash_generator, "hash_fields", return_value="HASH_EXPRESSION"):
                # Call the method
                mdv.compute_hash_view("test_table")

                # Check that sql was called with CREATE OR REPLACE VIEW
                called_sql = mdv.db.real_db.sql.call_args[0][0]
                assert "CREATE OR REPLACE VIEW stg.test_table_hash_vw" in called_sql
                assert "HASH_EXPRESSION" in called_sql

    def test_compute_hash_view_handles_errors(self, mdv):
        """Test compute_hash_view handles and returns errors"""
        # Make db.sql raise an exception
        mdv.db.real_db.sql.side_effect = Exception("Test error")

        with patch.object(mdv.metadata_manager, "get_transitions", return_value=[]):
            # Call the method
            errors = mdv.compute_hash_view("test_table")

            # Check errors were captured
            assert len(errors) == 1
            assert "Test error" in errors[0][1]

    # Additional tests for other methods can be added following similar patterns

    def test_load_related_hubs_sql_generation(self, mdv):
        """Test load_related_hubs generates correct SQL"""
        mock_records = [
            {
                "source_field": "id",
                "target_field": "id_bk",
                "target_table": "hub_customer",
                "group_name": "customer",
                "transfer_type": "bk",
            }
        ]

        with patch.object(mdv.metadata_manager, "get_transitions", return_value=mock_records):
            mdv.load_related_hubs("test_table", 123, "TEST_SOURCE")

            # Check SQL contains INSERT INTO statement
            called_sql = mdv.db.real_db.sql.call_args[0][0]
            assert "INSERT into dv.hub_customer" in called_sql
            assert "SUB.hk" in called_sql
            assert "LEFT OUTER JOIN dv.hub_customer hub" in called_sql
            assert "WHERE hub.customer_hk IS NULL" in called_sql

    def test_create_hub_from_metadata(self, mdv):
        """Test create_hub_from_metadata generates correct SQL"""
        mock_records = [
            {
                "base_name": "customer",
                "rel_type": "hub",
                "column_name": "id",
                "column_type": "VARCHAR(50)",
                "column_position": 1,
                "mapping": "pk",
            }
        ]

        with patch.object(mdv.metadata_manager, "get_tables", return_value=mock_records):
            mdv.create_hub_from_metadata()

            # Check SQL contains CREATE TABLE statement
            called_sql = mdv.db.real_db.sql.call_args[0][0]
            assert "CREATE TABLE IF NOT EXISTS dv.hub_customer" in called_sql
            assert "customer_hk CHAR(40) NOT NULL PRIMARY KEY" in called_sql
            assert "id_bk VARCHAR(50)" in called_sql


if __name__ == "__main__":
    pytest.main(["-xvs", "test_mallarddv.py"])
