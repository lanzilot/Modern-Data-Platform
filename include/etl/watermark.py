from include.utils.database import DatabaseConnection


class WatermarkManager:

    @staticmethod
    def create_watermark_table():
        conn = DatabaseConnection.snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SALES_ANALYTICS.BRONZE.ETL_WATERMARK (
                TABLE_NAME STRING PRIMARY KEY,
                WATERMARK_COLUMN STRING,
                LAST_WATERMARK_VALUE STRING,
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.close()
        conn.close()

    @staticmethod
    def get_watermark(table_name, default_value="1900-01-01 00:00:00"):
        conn = DatabaseConnection.snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT LAST_WATERMARK_VALUE
            FROM SALES_ANALYTICS.BRONZE.ETL_WATERMARK
            WHERE TABLE_NAME = %s
        """, (table_name.upper(),))

        result = cursor.fetchone()

        cursor.close()
        conn.close()

        if result:
            return result[0]

        return default_value

    @staticmethod
    def update_watermark(table_name, watermark_column, last_value):
        conn = DatabaseConnection.snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("""
            MERGE INTO SALES_ANALYTICS.BRONZE.ETL_WATERMARK  t
            USING (
                SELECT 
                    %s AS TABLE_NAME,
                    %s AS WATERMARK_COLUMN,
                    %s AS LAST_WATERMARK_VALUE
            ) s
            ON t.TABLE_NAME = s.TABLE_NAME
            WHEN MATCHED THEN UPDATE SET
                t.WATERMARK_COLUMN = s.WATERMARK_COLUMN,
                t.LAST_WATERMARK_VALUE = s.LAST_WATERMARK_VALUE,
                t.UPDATED_AT = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT (
                TABLE_NAME,
                WATERMARK_COLUMN,
                LAST_WATERMARK_VALUE
            )
            VALUES (
                s.TABLE_NAME,
                s.WATERMARK_COLUMN,
                s.LAST_WATERMARK_VALUE
            )
        """, (
            table_name.upper(),
            watermark_column.upper(),
            str(last_value)
        ))

        cursor.close()
        conn.close()