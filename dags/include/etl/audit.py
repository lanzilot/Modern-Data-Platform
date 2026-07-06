from include.utils.database import DatabaseConnection


class AuditLogger:

    @staticmethod
    def create_audit_table():
        conn = DatabaseConnection.snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SALES_ANALYTICS.BRONZE.ETL_AUDIT_LOG (
                AUDIT_ID INTEGER AUTOINCREMENT,
                TABLE_NAME STRING,
                START_TIME TIMESTAMP_NTZ,
                END_TIME TIMESTAMP_NTZ,
                ROWS_EXTRACTED INTEGER,
                ROWS_LOADED INTEGER,
                STATUS STRING,
                ERROR_MESSAGE STRING,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.close()
        conn.close()

    @staticmethod
    def insert_audit_log(
        table_name,
        start_time,
        end_time,
        rows_extracted,
        rows_loaded,
        status,
        error_message=None
    ):
        conn = DatabaseConnection.snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO SALES_ANALYTICS.BRONZE.ETL_AUDIT_LOG (
                TABLE_NAME,
                START_TIME,
                END_TIME,
                ROWS_EXTRACTED,
                ROWS_LOADED,
                STATUS,
                ERROR_MESSAGE
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            table_name.upper(),
            start_time,
            end_time,
            rows_extracted,
            rows_loaded,
            status,
            error_message
        ))

        cursor.close()
        conn.close()