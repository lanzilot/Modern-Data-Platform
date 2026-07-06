from datetime import datetime

from include.etl.watermark import WatermarkManager
from include.etl.extractor import MySQLExtractor
from include.etl.validator import DataValidator
from include.etl.loader import SnowflakeLoader
from include.etl.audit import AuditLogger


TABLE_CONFIG = {
    "customer": {
        "target_table": "CUSTOMER",
        "primary_key": "id",
        "watermark_column": "last_updated"
    },
    "invoice1": {
        "target_table": "INVOICE1",
        "primary_key": "id",
        "watermark_column": "last_updated"
    },
    "invoice2": {
        "target_table": "INVOICE2",
        "primary_key": "id",
        "watermark_column": "last_updated"
    }
}


def run_bronze_load():
    WatermarkManager.create_watermark_table()
    AuditLogger.create_audit_table()

    for source_table, config in TABLE_CONFIG.items():
        start_time = datetime.now()
        rows_extracted = 0
        rows_loaded = 0

        try:
            print(f"Processing table: {source_table}")

            target_table = config["target_table"]
            primary_key = config["primary_key"]
            watermark_column = config["watermark_column"]

            last_watermark = WatermarkManager.get_watermark(
                table_name=target_table
            )

            df = MySQLExtractor.extract_incremental(
                table_name=source_table,
                watermark_column=watermark_column,
                last_watermark=last_watermark
            )

            rows_extracted = len(df)

            is_valid, issues = DataValidator.validate_dataframe(
                df=df,
                table_name=source_table,
                primary_key=primary_key
            )

            print(issues)

            if not is_valid:
                raise Exception(f"Validation failed for {source_table}: {issues}")

            rows_loaded = SnowflakeLoader.load_incremental(
                df=df,
                target_table=target_table,
                primary_key=primary_key
            )

            if rows_loaded > 0:
                max_watermark = df[watermark_column].max()

                WatermarkManager.update_watermark(
                    table_name=target_table,
                    watermark_column=watermark_column,
                    last_value=max_watermark
                )

            AuditLogger.insert_audit_log(
                table_name=target_table,
                start_time=start_time,
                end_time=datetime.now(),
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                status="SUCCESS"
            )

            print(f"Finished {source_table}: {rows_loaded} rows loaded")

        except Exception as e:
            AuditLogger.insert_audit_log(
                table_name=source_table,
                start_time=start_time,
                end_time=datetime.now(),
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                status="FAILED",
                error_message=str(e)
            )

            raise


if __name__ == "__main__":
    run_bronze_load()