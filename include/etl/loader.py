import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from include.utils.database import DatabaseConnection


class SnowflakeLoader:

    @staticmethod
    def normalize_columns(df):
        df = df.copy()
        df.columns = [col.upper() for col in df.columns]
        return df

    @staticmethod
    def load_to_stage(df, stage_table):
        conn = DatabaseConnection.snowflake_connection()

        df = SnowflakeLoader.normalize_columns(df)

        success, chunks, rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name=stage_table.upper(),
            auto_create_table=True,
            overwrite=True
        )

        conn.close()

        if not success:
            raise Exception(f"Failed loading stage table {stage_table}")

        return rows

    @staticmethod
    def merge_stage_to_bronze(stage_table, target_table, primary_key):
        conn = DatabaseConnection.snowflake_connection()
        cursor = conn.cursor()

        stage_table = stage_table.upper()
        target_table = target_table.upper()
        primary_key = primary_key.upper()

        cursor.execute(f"DESC TABLE {stage_table}")
        columns = [row[0] for row in cursor.fetchall()]

        update_columns = [col for col in columns if col != primary_key]

        update_set = ", ".join([
            f"T.{col} = S.{col}" for col in update_columns
        ])

        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"S.{col}" for col in columns])

        merge_sql = f"""
            MERGE INTO {target_table} T
            USING {stage_table} S
            ON T.{primary_key} = S.{primary_key}
            WHEN MATCHED THEN UPDATE SET
                {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """

        cursor.execute(merge_sql)

        cursor.close()
        conn.close()

    @staticmethod
    def load_incremental(df, target_table, primary_key):
        if df.empty:
            return 0

        stage_table = f"STG_{target_table}_INCREMENTAL"

        loaded_rows = SnowflakeLoader.load_to_stage(df, stage_table)

        SnowflakeLoader.merge_stage_to_bronze(
            stage_table=stage_table,
            target_table=target_table,
            primary_key=primary_key
        )

        return loaded_rows