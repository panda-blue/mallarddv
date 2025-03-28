"""
SQL templates for the MallardDataVault library.
"""


class SQLTemplates:
    """Centralized storage for SQL templates"""
    
    CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS {schema};"
    
    META_TABLES = """
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
    
    META_TRANSITIONS = """
    CREATE TABLE IF NOT EXISTS metadata.transitions
    (
        source_table VARCHAR,
        source_field VARCHAR,
        target_table VARCHAR,
        target_field VARCHAR,
        group_name VARCHAR,
        position INTEGER,
        raw BOOLEAN,
        transformation VARCHAR,
        transfer_type VARCHAR
    );
    """
    
    META_RUNINFO = """
    CREATE TABLE IF NOT EXISTS metadata.runinfo
    (
        source_table VARCHAR,
        run_id INTEGER,
        log_date TIMESTAMP,
        source_file VARCHAR,
        status VARCHAR,
        message VARCHAR
    );
    """
    
    GET_TRANSITIONS = """
    SELECT 
        source_table, source_field, target_table, group_name, 
        target_field, position, raw, transfer_type, transformation 
    FROM metadata.transitions
    WHERE source_table = ?
    ORDER BY source_table, target_table, group_name, position
    """
    
    GET_TABLES = """
    SELECT 
        base_name, rel_type, column_name, column_type, 
        column_position, mapping
    FROM metadata.tables
    {where_clause}
    ORDER BY rel_type, base_name, mapping, column_position
    """
    
    CHECK_INGESTION = """
    SELECT 
        TRUE AS already_ingested 
    FROM metadata.runinfo 
    WHERE source_file = ? 
    AND source_table = ? 
    AND status = ?
    """

    CHECK_SOURCE_FOR_INGESTION = """
    SELECT
        rel_type = 'stg' AS to_load
    FROM metadata.tables
    WHERE base_name = ?
    ORDER BY 1 ASC
    LIMIT 1
    """
    
    GET_RUN_ID = """
    SELECT 
        COALESCE(MAX(run_id), 0) + 1 AS run_id 
    FROM metadata.runinfo
    """
    
    INSERT_RUNINFO = """
    INSERT INTO metadata.runinfo 
    VALUES (?, ?, ?, ?, ?, ?)
    """
    
    # Hub SQL templates
    CREATE_HUB = """
    CREATE TABLE IF NOT EXISTS dv.hub_{base_name} (
        {base_name}_hk CHAR(40) NOT NULL PRIMARY KEY,
        load_dts TIMESTAMP NOT NULL,
        record_source VARCHAR(255) NOT NULL,
        run_id INTEGER NOT NULL,
        {business_keys}
    )
    """
    
    # Link SQL templates
    CREATE_LINK = """
    CREATE TABLE IF NOT EXISTS dv.{table_type}_{table_base_name} (
        {table_base_name}_hk CHAR(40) NOT NULL PRIMARY KEY,
        load_dts TIMESTAMP NOT NULL,
        record_source VARCHAR(255) NOT NULL,
        run_id INTEGER NOT NULL,
        {columns}
    )
    """
    
    # Satellite SQL templates
    CREATE_SAT = """
    CREATE TABLE IF NOT EXISTS dv.{table_type}_{table_base_name} (
        {hub_key} NOT NULL,
        load_dts TIMESTAMP NOT NULL,
        del_flag BOOLEAN NOT NULL,
        hash_diff CHAR(40) NOT NULL,
        record_source VARCHAR(255) NOT NULL,
        run_id INTEGER NOT NULL{comma}
        {fields}
    )
    """
    
    CREATE_CURRENT_VIEW = """
    CREATE OR REPLACE VIEW bv.{table_type}_{table_base_name}_cv as
    select
            {hub_key},
            load_dts,
            del_flag,
            hash_diff,
            record_source,
            run_id{comma}
            {fields}
        from (
            SELECT
                row_number() over (partition by {hub_key} order by load_dts desc) as r,
                {hub_key},
                load_dts,
                del_flag,
                hash_diff,
                record_source,
                run_id{comma}
                {fields}
        FROM dv.{table_type}_{table_base_name}
        ) x
        where x.r = 1
    ;
    """
    
    # Staging SQL templates
    CREATE_STAGING_TABLE = """
    CREATE TABLE IF NOT EXISTS stg.{base_name} (
        {columns}
    )
    """
    
    # Hash view SQL template
    CREATE_HASH_VIEW = """
    CREATE OR REPLACE VIEW stg.{stg_table}_hash_vw AS
    (with cte as
        (
        SELECT
            {cte_fields}
        FROM stg.{stg_table}
        )
    select
        {combined}
    from cte)
    """
    
    # Hub loading SQL template
    INSERT_HUB = """
    INSERT into dv.{hub_name}
    SELECT
        SUB.hk,
        {load_date} AS load_date,
        '{record_source}' AS record_source,
        {run_id} AS run_id,
        {business_keys}
    FROM (
        SELECT DISTINCT
            src.{group_name}_hk as hk,
            {source_fields}
        FROM stg.{stg_table}_hash_vw src
            LEFT OUTER JOIN dv.{hub_name} hub
            ON src.{group_name}_hk = hub.{hub_hash_key}
        WHERE hub.{hub_hash_key} IS NULL
    ) AS SUB;
    """
    
    # Link loading SQL template
    INSERT_LINK = """
    INSERT INTO dv.{link_name}
    SELECT
        {link_hashkey},
        {load_date} as load_date,
        '{record_source}' AS record_source,
        {run_id} AS run_id,
        {link_fields}
    FROM (
        SELECT DISTINCT
            src.{group_name}_hk as {link_hashkey},
            {source_fields}
        FROM stg.{stg_table}_hash_vw src LEFT OUTER JOIN dv.{link_name} link
            ON src.{group_name}_hk = link.{link_hashkey}
        WHERE link.{link_hashkey} is null
    ) AS SUB;
    """
    
    # Satellite loading SQL templates
    INSERT_SAT_NEW = """
    INSERT INTO dv.{sat_name} (
        {sat_hashkey},
        load_dts,
        del_flag,
        hash_diff,
        record_source,
        run_id{comma}
        {field_list}
        )
        SELECT DISTINCT
            src.{source_field},
            {load_date},
            false,
            src.{hashdiff_field},
            '{record_source}',
            {run_id}{comma}
            {select_fields}
        FROM stg.{stg_table}_hash_vw src
        where not exists (
            with lr_hashdiff as (
                select sat.{sat_hashkey}, sat.hash_diff, sat.del_flag
                from dv.{sat_name} sat
                where sat.{sat_hashkey} = src.{source_field}
                order by load_dts desc
                limit 1
            )
            select 1 from lr_hashdiff
            where lr_hashdiff.hash_diff = src.{hashdiff_field}
                and lr_hashdiff.{sat_hashkey} = src.{source_field}
                and lr_hashdiff.del_flag = False
        );
    """
    
    INSERT_SAT_DELETE = """
    INSERT INTO dv.{sat_name} (
        {sat_hashkey},
        load_dts,
        del_flag,
        hash_diff,
        record_source,
        run_id{comma}
        {field_list}
        )
        with lr_hashdiff as (
            select
                hk,
                hash_diff,
                del_flag,
                {field_list}
                from (
                    select
                        sat.{sat_hashkey} hk,
                        sat.hash_diff,
                        sat.del_flag,
                        row_number() over (partition by sat.{sat_hashkey} order by load_dts desc) as rn
                        {comma_if_fields}
                        {field_list}
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
            {run_id}{comma}
            {select_sat_fields}
        FROM lr_hashdiff sat
        LEFT JOIN stg.{stg_table}_hash_vw src ON sat.hk = src.{source_field}
        WHERE src.{hashdiff_field} IS NULL
        AND sat.del_flag is False
        ;
    """