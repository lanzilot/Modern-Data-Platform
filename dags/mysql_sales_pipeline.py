from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from include.etl.run_bronze_load import run_bronze_load


DBT_PROJECT_DIR = "/usr/local/airflow/dbt/sales_analytics_dbt"


default_args = {
    "owner": "rolan",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="railway_mysql_snowflake_dbt_pipeline",
    description="Railway MySQL to Snowflake Bronze, dbt Silver, Gold, and dbt tests",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["railway", "mysql", "snowflake", "dbt", "sales"],
) as dag:

    load_bronze = PythonOperator(
        task_id="load_railway_mysql_to_snowflake_bronze",
        python_callable=run_bronze_load,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging_silver",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select staging --profiles-dir {DBT_PROJECT_DIR}
        """,
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts_gold",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select marts --profiles-dir {DBT_PROJECT_DIR}
        """,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt test --select staging --profiles-dir {DBT_PROJECT_DIR}
        """,
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt test --select marts --profiles-dir {DBT_PROJECT_DIR}
        """,
    )

    load_bronze >> dbt_run_staging >> dbt_run_marts >> dbt_test_staging >> dbt_test_marts