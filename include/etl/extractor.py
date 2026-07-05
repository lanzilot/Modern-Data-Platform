import pandas as pd
from include.utils.database import DatabaseConnection


class MySQLExtractor:

    @staticmethod
    def extract_incremental(table_name, watermark_column, last_watermark):

        conn = DatabaseConnection.mysql_connection()

        query = f"""
            SELECT *
            FROM {table_name}
            WHERE {watermark_column} > %s
            ORDER BY {watermark_column}
        """

        df = pd.read_sql(query, conn, params=[last_watermark])

        # Normalize column names
        df.columns = [c.lower() for c in df.columns]

        conn.close()

        return df