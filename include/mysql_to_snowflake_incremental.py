import os
import pandas as pd
import mysql.connector
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

TABLE_CONFIG = {
    "customer": {"target": "CUSTOMER", "primary_key": "id", "incremental_column": "updated_at"},
    "invoice1": {"target": "INVOICE1", "primary_key": "id", "incremental_column": "updated_at"},
    "invoice2": {"target": "INVOICE2", "primary_key": "id", "incremental_column": "updated_at"},
}


def connect_mysql():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )


def connect_snowflake(schema="BRONZE"):
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=schema,
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def init_snowflake_objects(sf):
    cur = sf.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS BRONZE")
    cur.execute("CREATE SCHEMA IF NOT EXISTS SILVER")
    cur.execute("CREATE SCHEMA IF NOT EXISTS GOLD")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS BRONZE.ETL_WATERMARK (
            TABLE_NAME STRING PRIMARY KEY,
            LAST_VALUE STRING,
            LOAD_TYPE STRING,
            UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.close()


def table_exists(sf, table_name):
    cur = sf.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'BRONZE'
          AND TABLE_NAME = %s
    """, (table_name.upper(),))
    exists = cur.fetchone()[0] > 0
    cur.close()
    return exists


def get_watermark(sf, table_name):
    cur = sf.cursor()
    cur.execute("""
        SELECT LAST_VALUE
        FROM BRONZE.ETL_WATERMARK
        WHERE TABLE_NAME = %s
    """, (table_name.upper(),))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def update_watermark(sf, table_name, last_value, load_type="updated_at"):
    cur = sf.cursor()
    cur.execute("""
        MERGE INTO BRONZE.ETL_WATERMARK t
        USING (
            SELECT %s TABLE_NAME, %s LAST_VALUE, %s LOAD_TYPE
        ) s
        ON t.TABLE_NAME = s.TABLE_NAME
        WHEN MATCHED THEN UPDATE SET
            LAST_VALUE = s.LAST_VALUE,
            LOAD_TYPE = s.LOAD_TYPE,
            UPDATED_AT = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT (TABLE_NAME, LAST_VALUE, LOAD_TYPE)
            VALUES (s.TABLE_NAME, s.LAST_VALUE, s.LOAD_TYPE)
    """, (table_name.upper(), str(last_value), load_type))
    cur.close()


def read_incremental(mysql, table_name, incremental_column, watermark):
    if watermark is None:
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, mysql)

    query = f"SELECT * FROM {table_name} WHERE {incremental_column} > %s"
    return pd.read_sql(query, mysql, params=[watermark])


def create_target_if_needed(sf, df, target_table):
    if not table_exists(sf, target_table):
        df_copy = df.copy()
        df_copy.columns = [c.upper() for c in df_copy.columns]
        write_pandas(
            conn=sf,
            df=df_copy.head(0),
            table_name=target_table.upper(),
            schema="BRONZE",
            database=os.getenv("SNOWFLAKE_DATABASE"),
            auto_create_table=True,
            overwrite=False,
        )


def load_stage(sf, df, stage_table):
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]
    df["_INGESTED_AT"] = pd.Timestamp.utcnow()

    success, chunks, rows, output = write_pandas(
        conn=sf,
        df=df,
        table_name=stage_table.upper(),
        schema="BRONZE",
        database=os.getenv("SNOWFLAKE_DATABASE"),
        auto_create_table=True,
        overwrite=True,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for {stage_table}: {output}")
    return rows


def merge_stage_to_target(sf, stage_table, target_table, primary_key):
    cur = sf.cursor()
    cur.execute(f"DESC TABLE BRONZE.{stage_table.upper()}")
    columns = [row[0] for row in cur.fetchall()]

    pk = primary_key.upper()
    update_set = ", ".join([f"t.{col} = s.{col}" for col in columns if col != pk])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"s.{col}" for col in columns])

    merge_sql = f"""
        MERGE INTO BRONZE.{target_table.upper()} t
        USING BRONZE.{stage_table.upper()} s
        ON t.{pk} = s.{pk}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    cur.execute(merge_sql)
    cur.close()


def run_incremental_load():
    mysql = connect_mysql()
    sf = connect_snowflake("BRONZE")
    init_snowflake_objects(sf)

    try:
        for source_table, cfg in TABLE_CONFIG.items():
            target = cfg["target"]
            pk = cfg["primary_key"]
            inc_col = cfg["incremental_column"]
            stage = f"STG_{target}_INCREMENTAL"

            watermark = get_watermark(sf, target)
            df = read_incremental(mysql, source_table, inc_col, watermark)

            if df.empty:
                print(f"No new rows for {source_table}")
                continue

            create_target_if_needed(sf, df, target)
            loaded_rows = load_stage(sf, df, stage)
            merge_stage_to_target(sf, stage, target, pk)

            max_value = df[inc_col].max()
            update_watermark(sf, target, max_value, "updated_at")
            print(f"{source_table}: staged {loaded_rows} rows and merged into BRONZE.{target}")
    finally:
        mysql.close()
        sf.close()


if __name__ == "__main__":
    run_incremental_load()
