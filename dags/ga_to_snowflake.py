import os
import json
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup

from include.libs.schema_reg.base_schema_transforms import snowflake_load_column_string
from include.operators.google.analytics.gatogcsoperator import GAToGCSOperator


base_path = Path(__file__).parents[1]
table_schema_path = os.path.join(
    base_path, "include", "table_schemas", "snowflake", "database"
)
template_searchpath = os.path.join(base_path, "include", "templates")

# Update the variables below with your project info
GCS_CONN_ID = "google_cloud_default"
GCS_BUCKET = "gcs_bucket_name"
SNOWFLAKE_CONN_ID = "snowflake_default"
LOADING_DB = "loading"
LOADING_ROLE = "loader"
LOADING_WAREHOUSE = "loading"
TRANSFORM_DB = "transforming"
TRANSFORM_ROLE = "transformer"
TRANSFORM_WAREHOUSE = "transforming"

# These are folders under include/templates/
TRANSFORM_DB_FILE_FOLDER_NAME = "transform"
LOADING_DB_FILE_FOLDER_NAME = "staging"

loading_schema = "google_analytics"
transform_schema = "ga_schemas"
stage_gcs_root = "google_analytics"
property_id = "123456789"

with DAG(
    dag_id="google_analytics_to_snowflake",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    catchup=False,
    template_searchpath=template_searchpath,
) as dag:
    """
    The google_analytics_to_snowflake DAG transfers data from Google Analytics
    (GA) to Google Cloud Storage (GCS), and then loads the data to Snowflake
    staging tables. From staging, data is loaded to Snowflake transform tables.
    """
    with open(
        f"{table_schema_path}/{transform_schema}.json",
        "r",
    ) as f:
        table_schema = json.load(f)
        table_def = table_schema.get("definitions")
        tables = list(table_def.keys())

    start = DummyOperator(task_id="start")
    finish = DummyOperator(task_id="extract_finish")

    for table in tables:

        gcs_file_keypath = (
            f"{stage_gcs_root}/"
            f"{table}/"
            "{{execution_date.year}}/"
            "{{execution_date.month}}/"
            "{{execution_date.day}}/"
            f"{table}_"
            "{{ts_nodash}}.csv"
        )
        gcs_load_keypath = (
            f"{table}/"
            "{{execution_date.year}}/"
            "{{execution_date.month}}/"
            "{{execution_date.day}}/"
        )
        start_ds = "{{ yesterday_ds }}"
        end_ds = "{{ ds }}"
        table_props = table_def.get(f"{table}").get("properties")

        table_dimensions = table_def.get(f"{table}").get("dimensions")
        table_metrics = table_def.get(f"{table}").get("metrics")

        col_string = snowflake_load_column_string(table_props)

        """
        Create the Snowflake tables for GA data if they don't already exist
        """
        with TaskGroup(group_id=f"snowflake_{table}_create"):

            create_start = DummyOperator(
                task_id=f"snowflake_{table}_create_start"
            )

            create_tables = SnowflakeOperator(
                task_id=f"snowflake_create_{table}",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                autocommit=True,
                warehouse=TRANSFORM_WAREHOUSE,
                database=TRANSFORM_DB,
                role=TRANSFORM_ROLE,
                schema=transform_schema,
                params={
                    "transform_schema": transform_schema,
                    "transform_db": TRANSFORM_DB,
                    "table": table,
                    "table_schema": table_props,
                },
                sql=f"{TRANSFORM_DB_FILE_FOLDER_NAME}/create_tables.sql",
            )

            create_finish = DummyOperator(
                task_id=f"snowflake_{table}_create_finish"
            )

        """
        Extract GA data to GCS
        """
        with TaskGroup(group_id=f"{table}_extract"):

            extract_start = DummyOperator(task_id=f"{table}_extract_start")

            extract_to_gcs = GAToGCSOperator(
                task_id=f"upload_{table}_to_gcs",
                start_ds=start_ds,
                end_ds=end_ds,
                property_id=property_id,
                dimensions=table_dimensions,
                metrics=table_metrics,
                gcs_dest_path=gcs_file_keypath,
                ga_conn_id=GCS_CONN_ID,
                gcs_conn_id=GCS_CONN_ID,
                gcs_bucket_name=GCS_BUCKET,
            )

            extract_finish = DummyOperator(task_id=f"{table}_finish")

            """
            Load data from GCS to Snowflake and manage Snowflake environment
            """
            with TaskGroup(group_id="loading_from_gcs_to_snowflake"):
                loading_db_params = {
                    "loading_schema": loading_schema,
                    "loading_db": LOADING_DB,
                    "table": table,
                    "table_schema": table_props,
                }

                prepare_staging = SnowflakeOperator(
                    task_id=f"create_staging_{table}",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    autocommit=True,
                    warehouse=LOADING_WAREHOUSE,
                    database=LOADING_DB,
                    role=LOADING_ROLE,
                    schema=loading_schema,
                    params=loading_db_params,
                    sql=f"{LOADING_DB_FILE_FOLDER_NAME}/create_staging_tables.sql",
                )

                load_staging = SnowflakeOperator(
                    task_id=f"load_{table}_to_snowflake",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    autocommit=True,
                    warehouse=LOADING_WAREHOUSE,
                    database=LOADING_DB,
                    role=LOADING_ROLE,
                    schema=loading_schema,
                    params={
                        "loading_schema": loading_schema,
                        "loading_db": LOADING_DB,
                        "table": table,
                        "table_schema": table_props,
                        "col_string": col_string,
                        "stage_name": "STAGE_GOOGLE_ANALYTICS",
                    },
                    sql=f"{LOADING_DB_FILE_FOLDER_NAME}/copy_into_staging_tables.sql",
                )

                cluster_keys = (
                    table_def.get(f"{table}").get(
                        "cluster_keys", {}).get("columns", [])
                )
                merge_tables = SnowflakeOperator(
                    task_id=f"merge_{table}",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    autocommit=True,
                    warehouse=TRANSFORM_WAREHOUSE,
                    database=TRANSFORM_DB,
                    role=TRANSFORM_ROLE,
                    schema=transform_schema,
                    params={
                        "loading_db": LOADING_DB,
                        "loading_schema": loading_schema,
                        "transform_schema": transform_schema,
                        "transform_db": TRANSFORM_DB,
                        "table": table,
                        "table_schema": table_props,
                        "cluster_keys": cluster_keys,
                    },
                    sql=f"{TRANSFORM_DB_FILE_FOLDER_NAME}/upsert_tables.sql",
                )

                cleanup_staging = SnowflakeOperator(
                    task_id=f"cleanup_staging_{table}",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                    autocommit=True,
                    warehouse=LOADING_WAREHOUSE,
                    database=LOADING_DB,
                    role=LOADING_ROLE,
                    schema=loading_schema,
                    params=loading_db_params,
                    sql=f"{LOADING_DB_FILE_FOLDER_NAME}/cleanup_staging_tables.sql",
                )

                chain(
                    start,
                    create_start,
                    create_tables,
                    create_finish,
                    extract_start,
                    extract_to_gcs,
                    extract_finish,
                    prepare_staging,
                    load_staging,
                    merge_tables,
                    cleanup_staging,
                    finish,
                )
