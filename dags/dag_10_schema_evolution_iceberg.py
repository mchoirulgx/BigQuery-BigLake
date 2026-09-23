"""
===============================================================================
DAG: gcs_parquet_to_iceberg_dts_orchestration
Stage: Stage 5 - Schema Evolution Workaround
File: dags/dag_10_schema_evolution_iceberg.py

PURPOSE:
  Workaround for BigQuery DTS Limitation:
  BigQuery DTS and load jobs do not natively support schema evolution
  (`ALLOW_FIELD_ADDITION`) when targeting BigLake Managed Apache Iceberg tables.
  This DAG inspects incoming Parquet file schemas in GCS landing zones using PyArrow,
  identifies new columns, executes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in BigQuery,
  and only then triggers BigQuery DTS to ingest the data without errors.

WHAT TO DO:
  1. Set up Airflow Connections:
     - 'google_cloud_default': GCP Connection with BigQuery and Storage permissions.
  2. Configure Variables:
     - Replace '<YOUR_BUCKET_NAME>', '<YOUR_GCS_PREFIX>/', '<YOUR_PROJECT_ID>',
       '<YOUR_DATASET_ID>', '<YOUR_TABLE_NAME>', and '<YOUR_DTS_TRANSFER_CONFIG_ID>'.
  3. Deploy:
     - Copy this file to your Airflow / Cloud Composer `dags/` folder.
  4. Test:
     - Run `scripts/simulate_mysql_producer.py` to upload sample Parquet data with new columns.
     - Trigger this DAG in Airflow UI.
     - Verify newly added columns in BigQuery Studio schema tab.

DEMO VS. PRODUCTION NOTES:
  - Idempotency: Uses `ADD COLUMN IF NOT EXISTS` to ensure retries do not fail if
    a column was already created.
  - PyArrow Type Mapping: Automatically translates PyArrow `date32`, `timestamp[us]`,
    `int64`, and `float64` to appropriate BigQuery SQL types.
===============================================================================
"""

from datetime import datetime, timezone
from io import BytesIO
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryDataTransferServiceStartTransferRunsOperator
from airflow.providers.google.cloud.sensors.bigquery_dts import BigQueryDataTransferServiceTransferRunSensor
import pyarrow.parquet as pq
import pyarrow.types as types

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'gcs_parquet_to_iceberg_dts_orchestration',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['schema-evolution', 'iceberg', 'dts', 'gcs'],
) as dag:

    @task
    def check_and_evolve_schema(bucket: str, prefix: str, project_id: str, dataset_id: str, table_id: str):
        """
        Reads GCS Parquet metadata across all landed files, compares schema
        against BigQuery Iceberg table, and dynamically alters the BigQuery table schema
        if new columns exist.
        """
        
        def map_pyarrow_to_bq_type(pa_type) -> str:
            """Maps PyArrow data types to BigQuery SQL data types."""
            if types.is_string(pa_type) or types.is_large_string(pa_type):
                return "STRING"
            elif types.is_integer(pa_type):
                return "INT64"
            elif types.is_floating(pa_type):
                return "FLOAT64"
            elif types.is_boolean(pa_type):
                return "BOOL"
            elif types.is_timestamp(pa_type):
                return "TIMESTAMP"
            elif types.is_date(pa_type):
                return "DATE"
            elif types.is_decimal(pa_type):
                return "NUMERIC"
            else:
                raise ValueError(f"Unsupported PyArrow type for auto-schema evolution: {pa_type}")

        # 1. Inspect PyArrow schema across ALL Parquet files in GCS landing zone
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        blob_names = gcs_hook.list(bucket_name=bucket, prefix=prefix)
        parquet_blobs = [b for b in blob_names if b.endswith('.parquet')]
        if not parquet_blobs:
            print("No files found in landing zone.")
            return

        # Compute cumulative schema union across all files
        unified_fields = {}
        for blob_name in parquet_blobs:
            file_bytes = gcs_hook.download(bucket_name=bucket, object_name=blob_name)
            schema = pq.read_schema(BytesIO(file_bytes))
            for field in schema:
                if field.name not in unified_fields:
                    unified_fields[field.name] = field.type
        parquet_cols = set(unified_fields.keys())
        
        # 2. Inspect current BigQuery Iceberg schema
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        client = bq_hook.get_client(project_id=project_id)
        table_ref = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        bq_cols = {field.name for field in table_ref.schema}
        
        # 3. Compute schema difference and construct DDL
        new_cols = parquet_cols - bq_cols
        if new_cols:
            alter_statements = []
            for col in new_cols:
                pa_type = unified_fields[col]
                bq_type = map_pyarrow_to_bq_type(pa_type)
                print(f"[SCHEMA EVOLUTION] New column detected: '{col}' ({pa_type}) -> BQ Type: '{bq_type}'")
                alter_statements.append(f"ADD COLUMN IF NOT EXISTS `{col}` {bq_type}")
                
            query = f"ALTER TABLE `{project_id}.{dataset_id}.{table_id}` {', '.join(alter_statements)}"
            
            # Execute DDL query
            client.query_and_wait(query)
            print(f"Successfully executed DDL: {query}")
        else:
            print("No schema changes detected.")

    # Task 1: Check schema & alter table
    schema_task = check_and_evolve_schema(
        bucket='<YOUR_BUCKET_NAME>', 
        prefix='<YOUR_GCS_PREFIX>/',
        project_id='<YOUR_PROJECT_ID>',
        dataset_id='<YOUR_DATASET_ID>',
        table_id='<YOUR_TABLE_NAME>'
    )

    # Task 2: Trigger BigQuery Data Transfer Service
    trigger_dts = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="trigger_dts_transfer",
        project_id='<YOUR_PROJECT_ID>',
        transfer_config_id="<YOUR_DTS_TRANSFER_CONFIG_ID>", 
        location='<YOUR_LOCATION>',
        requested_run_time={"seconds": "{{ data_interval_end.int_timestamp }}"},
        gcp_conn_id='google_cloud_default',
    )

    # Task 3: Wait for DTS transfer run to complete
    wait_for_dts = BigQueryDataTransferServiceTransferRunSensor(
        task_id="wait_for_dts_transfer",
        project_id='<YOUR_PROJECT_ID>',
        transfer_config_id="<YOUR_DTS_TRANSFER_CONFIG_ID>",
        location='<YOUR_LOCATION>',
        run_id="{{ task_instance.xcom_pull(task_ids='trigger_dts_transfer', key='return_value').name.split('/')[-1] }}",
        expected_statuses={"SUCCEEDED"},
        poke_interval=30,
        timeout=1200,
        mode='reschedule',
        gcp_conn_id='google_cloud_default',
    )

    # Execution Flow
    schema_task >> trigger_dts >> wait_for_dts
