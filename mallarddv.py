import duckdb
import itertools as it
import json
from typing import Tuple, List, Dict, Any, Optional, Union, Iterable

from datetime import datetime


class MallardDataVault:
    """
    A portable implementation of Data Vault methodology using DuckDB.

    This class provides tools to create and manage a Data Vault model for data warehousing,
    including hubs, links, satellites, and associated metadata management.
    """

    db_path: Optional[str] = None
    scripts_path: Optional[str] = "models"
    db: Optional[duckdb.DuckDBPyConnection] = None

    def __init__(self, database_path: str, scripts_path: Optional[str] = None) -> None:
        """
        Initialize the MallardDataVault with a DuckDB database path.

        Args:
            database_path (str): Path to the DuckDB database file or ":memory:" for in-memory database
            scripts_path (Optional[str], optional): Root path where scripts are written as sql files
        """
        self.db_path = database_path
        self.scripts_path = scripts_path or self.scripts_path

    def __enter__(self) -> "MallardDataVault":
        """
        Context manager entry point that connects to the database.

        Returns:
            Self reference for use in context manager
        """
        self.db = duckdb.connect(self.db_path)

        return self

    def init_mallard_db(
        self,
        meta_only: Optional[bool] = True,
        meta_tables_path: Optional[str] = None,
        meta_transitions_path: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """Initialise the Mallart Data Vault DB

        Args:
            meta_only (Optional[bool], optional): Only initialise schema and metadata tables. Defaults to True.
                                                  If False, it will attempt to create stg table and views,
                                                  Data Vault Hub, Link and Satellites, business current value satellites
            meta_tables_path (Optional[str], optional): path to file containing table definitions. Defaults to None.
            meta_transitions_path (Optional[str], optional): path to file containing transition definitions. Defaults to None.
            verbose (bool, optional): Wether to print prepared SQL for debugging. Defaults to False.

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []
        create_schema_template = "CREATE SCHEMA IF NOT EXISTS {schema};"
        meta_tables_template = """
        CREATE TABLE IF NOT EXISTS metadata.tables
        (
            base_name VARCHAR,
            rel_type VARCHAR,
            column_name VARCHAR,
            column_type VARCHAR,
            column_position INTEGER,
            mapping VARCHAR
        );
        """
        meta_transitions_template = """
        create table if not exists metadata.transitions
        (
            source_table varchar,
            source_field varchar,
            target_table varchar,
            target_field varchar,
            group_name varchar,
            position integer,
            raw boolean,
            transformation varchar,
            transfer_type varchar
        );
        """

        meta_runinfo_template = """
        create table if not exists metadata.runinfo
        (
            source_table varchar,
            run_id integer,
            log_date timestamp,
            source_file varchar,
            status varchar,
            message varchar
        );
        """

        for schema in ["stg", "dv", "bv", "dm", "metadata"]:
            sql_str = create_schema_template.format(schema=schema)

            if verbose:
                print(sql_str)

            try:
                self.db.sql(sql_str)
            except Exception as ex:
                errors.append((sql_str, repr(ex)))

        if verbose:
            print(meta_tables_template)
        try:
            self.sql(meta_tables_template)
        except Exception as ex:
            errors.append((meta_tables_template, repr(ex)))

        if verbose:
            print(meta_transitions_template)
        try:
            self.sql(meta_transitions_template)
        except Exception as ex:
            errors.append((meta_transitions_template, repr(ex)))

        if verbose:
            print(meta_runinfo_template)
        try:
            self.sql(meta_runinfo_template)
        except Exception as ex:
            errors.append((meta_runinfo_template, repr(ex)))

        errors.extend(
            self.overwrite_metadata_from_files(
                meta_transitions_path=meta_transitions_path,
                meta_tables_path=meta_tables_path,
                verbose=verbose,
            )
        )

        if not meta_only:

            errors.extend(self.create_staging_table_from_metadata(verbose=verbose))
            errors.extend(
                self.apply_script_from_metadata(rel_type="stg_vw", verbose=verbose)
            )

            errors.extend(self.create_hub_from_metadata(verbose=verbose))
            errors.extend(self.create_link_from_metadata(verbose=verbose))
            errors.extend(self.create_sat_from_metadata(verbose=verbose))

            errors.extend(self.create_current_sat_from_metadata(verbose=verbose))

        return errors

    def overwrite_metadata_from_files(
        self,
        meta_tables_path: Optional[str] = None,
        meta_transitions_path: Optional[str] = None,
        verbose: Optional[bool] = False,
    ) -> List[Tuple[str, str]]:
        """Execute SQL to replace the content of metadata.tables and
           metadata.transitions with content of files at provided locations
           if no paths are provided, nothing is overwritten
        Args:
            meta_tables_path (Optional[str], optional): path to file containing metadata for table metadata.tables. Defaults to None.
            meta_transitions_path (Optional[str], optional): path to file containing metadata for table metadata.transitions. Defaults to None.
            verbose (bool, optional): Wether to print prepared SQL for debugging. Defaults to False.

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """

        errors = []

        if meta_tables_path:
            truncate_str = f"TRUNCATE TABLE metadata.tables;"
            insert_str = f'INSERT INTO metadata.tables(base_name,rel_type,column_name,column_type,column_position,mapping) select * from read_csv("{meta_tables_path}")'

            if verbose:
                print(truncate_str)
            try:
                self.sql(truncate_str)
            except Exception as ex:
                errors.append((truncate_str, repr(ex)))

            if verbose:
                print(insert_str)
            try:
                self.sql(insert_str)
            except Exception as ex:
                errors.append((insert_str, repr(ex)))

        if meta_transitions_path:
            truncate_str = f"TRUNCATE TABLE metadata.transitions;"
            insert_str = f'INSERT INTO metadata.transitions(source_table,source_field,target_table,target_field,group_name,position,raw,transformation,transfer_type) select * from read_csv("{meta_transitions_path}");'

            if verbose:
                print(truncate_str)
            try:
                self.sql(truncate_str)
            except Exception as ex:
                errors.append((truncate_str, repr(ex)))

            if verbose:
                print(insert_str)
            try:
                self.sql(insert_str)
            except Exception as ex:
                errors.append((insert_str, repr(ex)))

            return errors

    def _hash_fields(self, name: str, fields: List[str]) -> str:
        """
        Generate SQL to create a hash value from a list of fields.

        Args:
            name: Name for the generated hash column
            fields: List of field names to include in the hash

        Returns:
            SQL expression that creates a SHA1 hash of concatenated field values
        """
        return f"""sha1(upper(concat_ws('||',{','.join([f"coalesce({f}::string,'')" for f in fields])}))) as {name}"""

    def _fetch_dict(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute SQL and return results as a list of dictionaries.

        Args:
            sql: SQL query to execute

        Returns:
            List of dictionaries where keys are column names and values are row values
        """
        r = self.db.sql(sql)
        return [dict(zip(r.columns, rec)) for rec in r.fetchall()]

    def _get_transitions(self, source_table: str) -> List[Dict[str, Any]]:
        """
        Retrieve mapping configuration between staging and data vault tables.

        Args:
            source_table: Name of the staging table to get mappings for

        Returns:
            List of mapping records containing field mappings between staging and data vault
        """
        sql = f"""
            select source_table,source_field,target_table,group_name,target_field,position,raw,transfer_type,transformation from metadata.transitions
            where source_table='{source_table}'
            order by source_table,target_table,group_name,position
            """
        return self._fetch_dict(sql=sql)

    def _get_tables(
        self, base_name: Optional[str] = None, rel_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve data vault table metadata based on optional filters.

        Args:
            base_name: Optional filter for the base entity name
            rel_type: Optional filter for relationship type (hub, link, hsat, etc.)

        Returns:
            List of data vault table metadata records
        """
        where_clause = bool(base_name) or bool(rel_type)
        and_clause = bool(base_name) and bool(rel_type)

        sql = f"""
        SELECT base_name,rel_type,column_name,column_type,column_position,mapping
        FROM metadata.tables
        {"WHERE " if where_clause else ''} {f"base_name = '{base_name}'" if base_name else ''}
        {"AND " if and_clause else ''} {f"rel_type = '{rel_type}'" if rel_type else ''}
        ORDER BY rel_type,base_name,mapping,column_position
        """

        return self._fetch_dict(sql=sql)

    def _groupby(
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

    def sql(
        self, sql_str: str, sql_args: Optional[List[object]] = None
    ) -> duckdb.DuckDBPyRelation:
        """
        Execute a SQL query against the database.

        Args:
            sql_str: SQL query to execute

        Returns:
            DuckDB relation object with query results
        """
        if sql_args is None:
            return self.db.sql(sql_str)
        else:
            return self.db.sql(sql_str, sql_args)

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
            print(f"computing hash view for {stg_table}")
        records = self._get_transitions(source_table=stg_table)

        # base source fields
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
        # add raw fields
        cte_fields.extend(
            [
                f"""'{r["source_field"]}' as {r["source_field"]}"""
                for r in records
                if r["raw"]
            ]
        )
        if verbose:
            print(cte_fields)
        # hub hash keys
        hash_field_list: List[str] = []

        hub_hash_groups = self._groupby(
            [r for r in records if r["transfer_type"] == "bk"], ["group_name"]
        )

        for k, g in hub_hash_groups.items():
            if verbose:
                print(f"hub hash found: {k}")
            source_field_names = [c["source_field"] for c in g]
            hash_field_list.append(self._hash_fields(f"{k}_hk", source_field_names))

        link_hash_groups = self._groupby(
            [r for r in records if r["transfer_type"] in ("ll", "dk")], ["group_name"]
        )

        # link hash keys
        for link_key, key_source in link_hash_groups.items():
            if verbose:
                print(f"link key found: {link_key}")
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
                self._hash_fields(f"{link_key}_hk", key_source_names)
            )

        # sat hash diff

        sat_hash_groups = self._groupby(
            [r for r in records if r["transfer_type"] == "f"], ["group_name"]
        )

        for sat_field, sat_source in sat_hash_groups.items():
            if verbose:
                print(f"sat hash found: {sat_field}")
            source_field_names = [c["source_field"] for c in sat_source]
            hash_field_list.append(
                self._hash_fields(f"{sat_field}_hashdiff", source_field_names)
            )

        combined = [*hash_field_list, "*"]

        hash_vw_sql = f'''CREATE OR REPLACE VIEW stg.{stg_table}_hash_vw AS
        (with cte as
            (
            SELECT
                {',\n               '.join(cte_fields)}
            FROM stg.{stg_table}
            )
        select
            {',\n           '.join(combined)}
        from cte)'''

        if verbose:
            print(hash_vw_sql)

        errors: List[Tuple[str, str]] = []

        try:
            self.db.sql(hash_vw_sql)
        except Exception as ex:
            errors.append((hash_vw_sql, repr(ex)))

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

        This method identifies hub tables related to the staging table based on metadata,
        generates SQL to load only new records, and executes the SQL.

        Args:
            stg_table: Name of the staging table containing the source data
            run_id: Identifier for the current load process
            record_source: Identifier for the data source system
            load_date_overwrite: Optional date string to use instead of current timestamp

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """

        load_date = (
            load_date_overwrite
            or f"""'{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}'"""
        )
        raw_records = self._get_transitions(source_table=stg_table)

        records = [r for r in raw_records if r["target_table"].startswith("hub_")]

        bk_per_group = self._groupby(
            record_list=records, keys=["target_table", "group_name"]
        )

        sql_to_execute: List[str] = []

        for hub_group, fields in bk_per_group.items():
            hub_name = hub_group.split(".")[0]
            group_name = hub_group.split(".")[1]

            hub_hash_key = f"{hub_name[4:]}_hk"

            insert_sql = f'''
            INSERT into dv.{hub_name}
            SELECT
                SUB.hk,
                {load_date} AS load_date,
                '{record_source}' AS record_source,
                {run_id} AS run_id,
                {',\n           '.join([f"SUB.{hk_field["source_field"]} AS {hk_field["target_field"]}" for hk_field in fields])}
            FROM (
                SELECT DISTINCT
                    src.{group_name}_hk as hk,
                    {',\n           '.join([f"src.{hk_field["source_field"]}" for hk_field in fields])}
                FROM stg.{stg_table}_hash_vw src
                    LEFT OUTER JOIN dv.{hub_name} hub
                    ON src.{group_name}_hk = hub.{hub_hash_key}
                WHERE hub.{hub_hash_key} IS NULL
            ) AS SUB;
            '''

            sql_to_execute.append(insert_sql)

        errors: List[Tuple[str, str]] = []
        for sql in sql_to_execute:
            if verbose:
                print(sql)
            try:
                self.db.sql(sql)
            except Exception as ex:
                errors.append((sql, repr(ex)))

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

        This method identifies link tables related to the staging table based on metadata,
        generates SQL to load only new records, and executes the SQL.

        Args:
            stg_table: Name of the staging table containing the source data
            run_id: Identifier for the current load process
            record_source: Identifier for the data source system
            load_date_overwrite: Optional date string to use instead of current timestamp

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        load_date = (
            load_date_overwrite
            or f"""'{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}'"""
        )

        raw_records = self._get_transitions(source_table=stg_table)

        records = [
            r
            for r in raw_records
            if r["target_table"].startswith("link_")
            or r["target_table"].startswith("nhl_")
        ]

        bk_per_group = self._groupby(
            record_list=records, keys=["target_table", "group_name"]
        )

        sql_to_execute: List[str] = []

        for link_group, fields in bk_per_group.items():
            link_name = link_group.split(".")[0]
            group_name = link_group.split(".")[1]

            link_hashkey = (
                f"{link_name[5:] if link_name.startswith('l') else link_name[4:]}_hk"
            )

            insert_sql = f'''
                INSERT INTO dv.{link_name}
                SELECT
                    {link_hashkey},
                    {load_date} as load_date,
                    '{record_source}' AS record_source,
                    {run_id} AS run_id,
                    {',\n                '.join([f"SUB.{hk_field["source_field"]}{'_hk' if hk_field["transfer_type"]=='ll' else ''} AS {hk_field["target_field"]}" for hk_field in fields])}
                FROM (
                    SELECT DISTINCT
                        src.{group_name}_hk as {link_hashkey},
                        {',\n                   '.join([f"src.{hk_field["source_field"]}{'_hk' if hk_field["transfer_type"]=='ll' else ''}" for hk_field in fields])}
                    FROM stg.{stg_table}_hash_vw src LEFT OUTER JOIN dv.{link_name} link
                        ON src.{group_name}_hk = link.{link_hashkey}
                    WHERE link.{link_hashkey} is null
                ) AS SUB;
                '''

            sql_to_execute.append(insert_sql)

        errors: List[Tuple[str, str]] = []
        for sql in sql_to_execute:
            try:
                if verbose:
                    print(sql)
                self.db.sql(sql)
            except Exception as ex:
                errors.append((sql, repr(ex)))

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

        This method identifies satellite tables related to the staging table based on metadata,
        generates SQL to load only changed records (for delta satellites) or both changed records
        and deleted flags (for full satellites), and executes the SQL.

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

        raw_records = self._get_transitions(source_table=stg_table)

        sat_load = [
            r for r in raw_records if r["transfer_type"] in ("sat_delta", "sat_full")
        ]

        sql_to_execute: List[str] = []

        for sat in sat_load:
            if verbose:
                print(f"""sat found: {sat["target_table"]}""")
            target_group = sat["group_name"]

            records = [
                r
                for r in raw_records
                if r["target_table"] == sat["target_table"]
                and r["group_name"] == target_group
                and r["transfer_type"] == "f"
            ]
            if verbose:
                print(f"found {len(records)} field{'s' if len(records)>1 else ''}")
            bk_per_group = self._groupby(
                record_list=records, keys=["target_table", "group_name"]
            )

            fields = bk_per_group.get(f"""{sat["target_table"]}.{target_group}""", [])

            sat_name = sat["target_table"]
            group_name = target_group
            sat_hashkey = f"""{sat["target_field"]}_hk"""

            insert_new_sql = f'''
            INSERT INTO dv.{sat_name} (
                {sat_hashkey},
                load_dts,
                del_flag,
                hash_diff,
                record_source,
                run_id{',' if len(fields) else ''}
                {',\n                '.join([f"{hk_field["target_field"]}" for hk_field in fields])}
                )
                SELECT DISTINCT
                    src.{sat["source_field"]},
                    {load_date},
                    false,
                    src.{f"{group_name}_hashdiff" if len(fields) else sat["source_field"]},
                    '{record_source}',
                    {run_id}{',' if len(fields) else ''}
                    {',\n                   '.join([f"src.{hk_field["source_field"]}" for hk_field in fields])}
                FROM stg.{stg_table}_hash_vw src
                where not exists (
                    with lr_hashdiff as (
                        select sat.{sat_hashkey}, sat.hash_diff, sat.del_flag
                        from dv.{sat_name} sat
                        where sat.{sat_hashkey} = src.{sat["source_field"]}
                        order by load_dts desc
                        limit 1
                    )
                    select 1 from lr_hashdiff
                    where lr_hashdiff.hash_diff = src.{f"{group_name}_hashdiff" if len(fields) else sat["source_field"]}
                        and lr_hashdiff.{sat_hashkey} = src.{sat["source_field"]}
                        and lr_hashdiff.del_flag = False
                );
            '''
            if verbose:
                print("preparing for execution (new): ")
            if verbose:
                print(insert_new_sql)

            sql_to_execute.append(insert_new_sql)

            if sat["transfer_type"] == "sat_full":
                insert_del_sql = f'''
            INSERT INTO dv.{sat_name} (
                {sat_hashkey},
                load_dts,
                del_flag,
                hash_diff,
                record_source,
                run_id{',' if len(fields) else ''}
                {',\n                '.join([f"{hk_field["target_field"]}" for hk_field in fields])}
                )
                with lr_hashdiff as (
                    select
                        hk,
                        hash_diff,
                        del_flag,
                        {',\n                '.join([f"{hk_field["target_field"]}" for hk_field in fields])}
                        from (
                            select
                                sat.{sat_hashkey} hk,
                                sat.hash_diff,
                                sat.del_flag,
                                row_number() over (partition by sat.{sat_hashkey} order by load_dts desc) as rn
                                {',' if len(fields) else ''}
                                {',\n                   '.join([f"{hk_field["target_field"]}" for hk_field in fields])}
                            from dv.{sat_name} sat
                        ) x
                        where x.rn = 1
                    )
                SELECT DISTINCT
                    sat.hk,
                    {load_date},
                    true,
                    sat.hash_diff,
                    '{record_source}',
                    {run_id}{',' if len(fields) else ''}
                    {',\n                   '.join([f"sat.{hk_field["target_field"]}" for hk_field in fields])}
                FROM lr_hashdiff sat
                LEFT JOIN stg.{stg_table}_hash_vw src ON sat.hk = src.{sat["source_field"]}
                WHERE src.{f"{group_name}_hashdiff" if len(fields) else sat["source_field"]} IS NULL
                AND sat.del_flag is False
                ;
                '''

                if verbose:
                    print("preparing for execution (del):")
                sql_to_execute.append(insert_del_sql)

        errors: List[Tuple[str, str]] = []
        for sql in sql_to_execute:
            try:
                self.db.sql(sql)
            except Exception as ex:
                errors.append((sql, repr(ex)))

        return errors

    def create_hub_from_metadata(
        self, base_name: Optional[str] = None, verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Create hub tables based on metadata definitions.

        This method retrieves hub table definitions from metadata and generates
        SQL to create the tables in the data vault schema.

        Args:
            base_name: Optional filter to create only hubs for a specific base entity
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        raw_records = self._get_tables(base_name=base_name, rel_type="hub")

        records = self._groupby(raw_records, keys=["base_name"])
        errors: List[Tuple[str, str]] = []
        for base_name, business_keys in records.items():
            if verbose:
                print(f"found a hub to create: {base_name}")

            bk_list: List[str] = []
            for bk in business_keys:
                quote = " " in bk["column_name"]

                bk_list.append(
                    f"""{'"' if quote else ''}{bk["column_name"]}_{'c' if len(business_keys)>1 else ''}bk{'"' if quote else ''} {bk["column_type"]}"""
                )

            sql_str = f'''
            CREATE TABLE IF NOT EXISTS dv.hub_{base_name} (
                {base_name}_hk CHAR(40) NOT NULL PRIMARY KEY,
                load_dts TIMESTAMP NOT NULL,
                record_source VARCHAR(255) NOT NULL,
                run_id INTEGER NOT NULL,
                {',\n               '.join(bk_list)}
            )
            '''

            try:
                self.db.sql(sql_str)
            except Exception as ex:
                errors.append((sql_str, repr(ex)))

        return errors

    def create_link_from_metadata(
        self,
        base_name: Optional[str] = None,
        rel_type: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Create link tables based on metadata definitions.

        This method retrieves link table definitions from metadata and generates
        SQL to create the tables in the data vault schema.

        Args:
            base_name: Optional filter to create only links for a specific base entity
            rel_type: Optional filter to create only links of a specific type (link, nhl)
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        if rel_type:
            raw_link_records = self._get_tables(base_name=base_name, rel_type=rel_type)
        else:
            raw_link_records = self._get_tables(base_name=base_name, rel_type="link")
            raw_link_records.extend(
                self._get_tables(base_name=base_name, rel_type="nhl")
            )

        records = self._groupby(raw_link_records, keys=["rel_type", "base_name"])

        errors: List[Tuple[str, str]] = []

        for rel_base_name, fields in records.items():

            sbn = rel_base_name.split(".")
            table_type = sbn[0]
            table_base_name = ".".join(sbn[1:])

            if verbose:
                print(f"found a {table_type} to create: {table_base_name}")

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

            sql_str = f'''
            CREATE TABLE IF NOT EXISTS dv.{table_type}_{table_base_name} (
                {table_base_name}_hk CHAR(40) NOT NULL PRIMARY KEY,
                load_dts TIMESTAMP NOT NULL,
                record_source VARCHAR(255) NOT NULL,
                run_id INTEGER NOT NULL,
                {',\n               '.join([*hk_list,*dk_list])}
            )
            '''

            try:
                self.db.sql(sql_str)
            except Exception as ex:
                errors.append((sql_str, repr(ex)))

        return errors

    def create_sat_from_metadata(
        self,
        base_name: Optional[str] = None,
        rel_type: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Create satellite tables based on metadata definitions.

        This method retrieves satellite table definitions from metadata and generates
        SQL to create the tables in the data vault schema.

        Args:
            base_name: Optional filter to create only satellites for a specific base entity
            rel_type: Optional filter to create only satellites of a specific type (hsat, lsat)
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        if rel_type:
            raw_sat_records = self._get_tables(base_name=base_name, rel_type=rel_type)
        else:
            raw_sat_records = self._get_tables(base_name=base_name, rel_type="hsat")
            raw_sat_records.extend(
                self._get_tables(base_name=base_name, rel_type="lsat")
            )

        records = self._groupby(raw_sat_records, keys=["rel_type", "base_name"])

        errors: List[Tuple[str, str]] = []

        for rel_base_name, fields in records.items():

            sbn = rel_base_name.split(".")
            table_type = sbn[0]
            table_base_name = ".".join(sbn[1:])

            if verbose:
                print(f"found a {table_type} to create: {table_base_name}")

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

            sql_str = f'''
            CREATE TABLE IF NOT EXISTS dv.{table_type}_{table_base_name} (
                {hk_list[0] if len(hk_list) else ''} NOT NULL,
                load_dts TIMESTAMP NOT NULL,
                del_flag BOOLEAN NOT NULL,
                hash_diff CHAR(40) NOT NULL,
                record_source VARCHAR(255) NOT NULL,
                run_id INTEGER NOT NULL{',' if len(field_list) else ''}
                {',\n               '.join(field_list)}
            )
            '''

            try:
                if len(hk_list) != 1:
                    raise ValueError("sat as 1! hk")
                self.db.sql(sql_str)
            except Exception as ex:
                errors.append((sql_str, repr(ex)))

        return errors

    def create_current_sat_from_metadata(
        self,
        base_name: Optional[str] = None,
        rel_type: Optional[str] = None,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """
        Create current value views for satellite tables based on metadata definitions.

        This method generates views in the business vault (bv) schema that show
        only the most recent valid version of each satellite record.

        Args:
            base_name: Optional filter to create only views for satellites of a specific base entity
            rel_type: Optional filter to create only views for satellites of a specific type (hsat, lsat)
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        if rel_type:
            raw_sat_records = self._get_tables(base_name=base_name, rel_type=rel_type)
        else:
            raw_sat_records = self._get_tables(base_name=base_name, rel_type="hsat")
            raw_sat_records.extend(
                self._get_tables(base_name=base_name, rel_type="lsat")
            )

        records = self._groupby(raw_sat_records, keys=["rel_type", "base_name"])

        errors: List[Tuple[str, str]] = []

        for rel_base_name, fields in records.items():

            sbn = rel_base_name.split(".")
            table_type = sbn[0]
            table_base_name = ".".join(sbn[1:])

            if verbose:
                print(f"found a {table_type} to create: {table_base_name}")

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

            sql_str = f'''
            CREATE OR REPLACE VIEW bv.{table_type}_{table_base_name}_cv as
            select
                    {hk_list[0] if len(hk_list) else ''},
                    load_dts,
                    del_flag,
                    hash_diff,
                    record_source,
                    run_id{',' if len(field_list) else ''}
                    {',\n               '.join(field_list)}
                from (
                    SELECT
                        row_number() over (partition by {hk_list[0] if len(hk_list) else ''} order by load_dts desc) as r,
                        {hk_list[0] if len(hk_list) else ''},
                        load_dts,
                        del_flag,
                        hash_diff,
                        record_source,
                        run_id{',' if len(field_list) else ''}
                        {',\n               '.join(field_list)}
                FROM dv.{table_type}_{table_base_name}
                ) x
                where x.r = 1
            ;
            '''

            if verbose:
                print(sql_str)

            try:
                if len(hk_list) != 1:
                    raise ValueError("sat as 1! hk")
                self.db.sql(sql_str)
            except Exception as ex:
                errors.append((sql_str, repr(ex)))

        return errors

    def create_staging_table_from_metadata(
        self, base_name: Optional[str] = None, verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Create staging tables based on metadata definitions.

        This method retrieves staging table definitions from metadata and generates
        SQL to create the tables in the staging schema.

        Args:
            base_name: Optional filter to create only specific entity
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """

        raw_records = self._get_tables(base_name=base_name, rel_type="stg")

        records = self._groupby(raw_records, keys=["base_name"])
        errors: List[Tuple[str, str]] = []
        for base_name, business_keys in records.items():
            if verbose:
                print(f"found a staging table to create: {base_name}")

            bk_list: List[str] = []
            for bk in business_keys:
                quote = " " in bk["column_name"]

                if bk["mapping"] == "c":
                    bk_list.append(
                        f"""{'"' if quote else ''}{bk["column_name"]}{'"' if quote else ''} {bk["column_type"]}"""
                    )

            sql_str = f'''
            CREATE TABLE IF NOT EXISTS stg.{base_name} (
                {',\n               '.join(bk_list)}
            )
            '''
            if verbose:
                print(sql_str)
            try:
                self.db.sql(sql_str)
            except Exception as ex:
                errors.append((sql_str, repr(ex)))

        return errors

    def apply_script_from_metadata(
        self, rel_type: str, base_name: Optional[str] = None, verbose: bool = False
    ) -> List[Tuple[str, str]]:
        """
        Create staging tables based on metadata definitions.

        This method retrieves staging table definitions from metadata and generates
        SQL to create the tables in the staging schema.

        Args:
            rel_type: relation type to apply (e.g. stg_vw)
            base_name: Optional filter to create only specific entity
            verbose: Whether to print additional information during execution

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """

        raw_records = self._get_tables(base_name=base_name, rel_type=rel_type)

        records = self._groupby(raw_records, keys=["base_name"])
        errors: List[Tuple[str, str]] = []

        for base_name, record_list in records.items():
            if verbose:
                print(f"found a staging view to create: {base_name}")

            for record in record_list:
                sub_folder = record["column_name"]

                script_path = f"{self.scripts_path}/{sub_folder}/{base_name}.sql"
                if verbose:
                    print(script_path)
                with open(script_path, "r") as script_file:
                    script = script_file.read()
                if verbose:
                    print(script)
                try:
                    self.db.sql(script)
                except Exception as ex:
                    errors.append((script, repr(ex)))

        return errors

    def execute_flow(
        self,
        source_table: str,
        record_source: str,
        file_path: str = None,
        load_date_overwrite: str = None,
        force_load: bool = False,
        verbose: bool = False,
    ) -> List[Tuple[str, str]]:
        """execute a flow based on entries in metadata.tables, metadata.flows and metadata.runinfo

        Args:
            source_table (str): name of the source_table to use from metadata.transitions
            record_source (str): the Data Vault run source
            file_path (str, optional): path of the file to load. Defaults to None.
            load_date_overwrite (str, optional): overwrite to apply to the Data Vault load_dts field. Defaults to None.
            force_load (bool, optional): If True, will load the file even though its reference exists in metadata.runinfo. Defaults to False.
            verbose (bool, optional): Whether to print additional information during execution. Defaults to False

        Returns:
            List of (SQL, error) tuples if errors occurred, otherwise empty list
        """
        errors = []

        if verbose:
            print(
                f"{'FORCE ' if force_load else ''}LOAD {source_table} for {record_source} from {file_path}{f' with date overwrite {load_date_overwrite}' if load_date_overwrite else ''}"
            )
        # get run_id:
        check_ingestion_statement = "select true as already_ingested from metadata.runinfo where source_file='{file_path}' and source_table='{source_table}' and status = '{status}';"
        run_id_statement = (
            "select coalesce(max(run_id),0) + 1 as run_id from metadata.runinfo;"
        )
        insert_statement = "insert into metadata.runinfo values ('{source_table}',{run_id},'{load_time}','{file_path}','{status}','{message}')"

        if not force_load:
            try:
                sql_str = check_ingestion_statement.format(
                    file_path=file_path,
                    source_table=source_table,
                    status="success",
                )
                if verbose:
                    print(sql_str)

                already_ran = self._fetch_dict(sql_str)
                if len(already_ran):
                    return errors

            except Exception as ex:
                errors.append((run_id_statement, repr(ex)))
                return errors

        try:
            if verbose:
                print(run_id_statement)
            run_id = self._fetch_dict(run_id_statement)[0]["run_id"]
        except Exception as ex:
            errors.append((run_id_statement, repr(ex)))
            return errors

        # register run_start:
        if verbose:
            print("   register start")
        try:
            insert_values = {
                "source_table": source_table,
                "run_id": run_id,
                "load_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "file_path": file_path,
                "status": "start",
                "message": "",
            }
            if verbose:
                print(insert_statement.format(**insert_values))
            self.sql(insert_statement.format(**insert_values))
        except Exception as ex:
            errors.append((insert_statement.format(**insert_values), repr(ex)))
            return errors

        if file_path:
            if verbose:
                print("   load new data")

            try:
                truncate_statement = f"TRUNCATE stg.{source_table};"
                if verbose:
                    print(truncate_statement)
                self.sql(truncate_statement)
            except Exception as ex:
                errors.append((run_id_statement, repr(ex)))
                return errors

            try:
                file_type = file_path.split(".")[-1]
                stg_fields_statement = f"select column_name,column_type from metadata.tables where base_name='{source_table}' and rel_type='stg' and mapping='c' order by column_position;"
                if verbose:
                    print(stg_fields_statement)
                stg_fields = self._fetch_dict(stg_fields_statement)

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
                select
                {field_list_str}
                from read_{file_type}("{file_path}",columns={json.dumps(mapping_dict)});"""
                if verbose:
                    print(load_statement)
                self.sql(load_statement)
            except Exception as ex:
                errors.append((run_id_statement, repr(ex)))
                return errors

        if verbose:
            print("   compute hash view")

        errors.extend(self.compute_hash_view(stg_table=source_table, verbose=verbose))

        if verbose:
            print("   load hubs")
        errors.extend(
            self.load_related_hubs(
                stg_table=source_table,
                run_id=run_id,
                record_source=record_source,
                load_date_overwrite=load_date_overwrite,
                verbose=verbose,
            )
        )
        if verbose:
            print("   load links")
        errors.extend(
            self.load_related_links(
                stg_table=source_table,
                run_id=run_id,
                record_source=record_source,
                load_date_overwrite=load_date_overwrite,
                verbose=verbose,
            )
        )
        if verbose:
            print("   load sats")
        errors.extend(
            self.load_related_sats(
                stg_table=source_table,
                run_id=run_id,
                record_source=record_source,
                load_date_overwrite=load_date_overwrite,
                verbose=verbose,
            )
        )

        if len(errors) == 0:
            insert_values = {
                "source_table": source_table,
                "run_id": run_id,
                "load_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "file_path": file_path,
                "status": "success",
                "message": "",
            }
        else:
            insert_values = {
                "source_table": source_table,
                "run_id": run_id,
                "load_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "file_path": file_path,
                "status": "failure",
                "message": f"{len(errors)} happend: {json.dumps([d[1] for d in errors])[0:4095]}",
            }
        # register run_start:

        try:

            self.sql(insert_statement.format(**insert_values))
        except Exception as ex:
            errors.append((insert_statement.format(**insert_values), repr(ex)))
            return errors

        return errors

    def __exit__(
        self, type: Optional[type], value: Optional[Exception], traceback: Any
    ) -> None:
        """
        Context manager exit point that closes the database connection.

        Args:
            type: Exception type if an exception was raised in the context
            value: Exception instance if an exception was raised in the context
            traceback: Exception traceback if an exception was raised in the context
        """
        self.db.close()
