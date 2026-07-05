# MySQL to Snowflake dbt Astronomer Medallion Pipeline

Pipeline:
1. MySQL customer, invoice1, invoice2
2. Python incremental loader to Snowflake BRONZE
3. dbt staging models to SILVER
4. dbt marts/star schema to GOLD
5. dbt tests

## Folder placement in Astronomer

Copy the folders into your Astro project:

- dags/mysql_snowflake_dbt_pipeline.py
- include/mysql_to_snowflake_incremental.py
- dbt/sales_analytics_dbt
- requirements.txt entries

## Required environment variables

Use .env locally or Astronomer Environment Variables in deployment.
See .env.example.

## Run locally inside Astro

astro dev restart

Then open Airflow and trigger:
mysql_snowflake_dbt_medallion_pipeline
