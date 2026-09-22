"""
===============================================================================
DAG: mysql_to_bq_lakehouse_unified
Stage: Stage 3 - Using DTS to Ingest into Iceberg
File: dags/dag_07_dts_parquet_to_iceberg.py

PURPOSE:
  Orchestrates end-to-end data ingestion from MySQL to a BigLake Managed Iceberg
  table (`dts_managed_users`) via BigQuery Data Transfer Service (DTS), then
  triggers a scheduled query to merge changes into the native table (`dts_native_users`).

WHAT TO DO:
  1. Set up Airflow Connections:
     - 'google_cloud_default': GCP Connection with BigQuery, Storage, and DTS scopes.
     - 'mysql_default': Connection to source MySQL database.
  2. Configure Variables:
     - Replace PROJECT_ID, GCS_BUCKET, BQ_DATASET_ID.
     - Retrieve DTS Transfer Config IDs from the GCP Console (BigQuery > Data Transfers)
       and populate DTS_PARQUET_CONFIG_NAME and DTS_MERGE_CONFIG_NAME.
  3. Deploy:
     - Copy to your Airflow / Cloud Composer `dags/` folder.
  4. Execution:
     - Trigger manually or via daily schedule.

DEMO VS. PRODUCTION NOTES:
  - IMPORTANT ORCHESTRATION LESSON:
    `BigQueryDataTransferServiceStartTransferRunsOperator` triggers the transfer
    asynchronously and completes immediately (within a few seconds).
  - In this basic Stage 3 demo, tasks are chained directly:
    `task_extract_upload >> task_run_dts_parquet >> task_run_dts_merge`.
  - In production, placing `task_run_dts_merge` immediately after the DTS trigger
    can cause a race condition if DTS has not finished copying files.
  - To see the hardened pattern using `BigQueryDataTransferServiceTransferRunSensor`
    to wait for DTS completion, see Stage 4: `dag_09_reference_pipeline_date_prefix.py`.
===============================================================================
"""

from datetime import timedelta
from io import BytesIO
import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryDataTransferServiceStartTransferRunsOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# --- CONFIGURATION ---
PROJECT_ID = '<your-project-id>'
GCS_BUCKET = '<your-bucket-name>'
GCS_LANDING_FOLDER = 'batch_landing_zone/'
BQ_DATASET_ID = '<your-dataset>'
BQ_MANAGED_TABLE = 'dts_managed_users'

# DTS Transfer Config resource paths (projects/{project_id}/locations/{location}/transferConfigs/{config_id})
DTS_PARQUET_CONFIG_NAME = 'projects/<your-project-id>/locations/<your-region>/transferConfigs/<your-parquet-transfer-config-id>'
DTS_MERGE_CONFIG_NAME = 'projects/<your-project-id>/locations/<your-region>/transferConfigs/<your-merge-scheduled-query-config-id>'

jkt_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'mysql_to_bq_lakehouse_unified',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily
    start_date=pendulum.datetime(2026, 8, 25, tz="Asia/Jakarta"),
    catchup=False,
    tags=['batch', 'mysql', 'gcs', 'iceberg', 'dts'],
) as dag:

    def mysql_to_parquet_gcs(**kwargs):
        execution_date = kwargs.get('data_interval_end').in_timezone(jkt_tz)
        yesterday = execution_date.subtract(days=1)
        
        start_win = yesterday.start_of('day').to_datetime_string() 
        end_win = yesterday.end_of('day').to_datetime_string()     
        
        # 1. Check Target Table Status in BigQuery (Full or Incremental)
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
        check_query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_MANAGED_TABLE}`"
        
        is_empty = True
        try:
            records = bq_hook.get_records(check_query)
            if records and records[0][0] > 0:
                is_empty = False
                print(f"Table contains {records[0][0]} rows. Proceeding with Incremental Load.")
            else:
                print("Table is empty. Proceeding with Full Load.")
        except Exception as e:
            print(f"Table check failed: {e}. Proceeding with Full Load.")
            is_empty = True

        # 2. Construct dynamic SQL Query
        if is_empty:
            query = "SELECT id, name, email, created_at, updated_at, deleted_at FROM users"
            filename_suffix = "full_load"
        else:
            query = f"SELECT id, name, email, created_at, updated_at, deleted_at FROM users WHERE updated_at >= '{start_win}' AND updated_at <= '{end_win}'"
            filename_suffix = yesterday.strftime('%Y%m%d')

        print(f"Executing Query: {query}")

        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        engine = mysql_hook.get_sqlalchemy_engine()
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print("No data found. Skipping.")
            return "skipped"
            
        for col in ['created_at', 'updated_at', 'deleted_at']:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
        # 3. In-memory Parquet conversion with microsecond timestamps
        parquet_buffer = BytesIO()
        df.to_parquet(
            parquet_buffer, 
            engine='pyarrow', 
            index=False,
            coerce_timestamps='us',          
            allow_truncated_timestamps=True 
        )
        
        # 4. Upload to GCS
        filename = f"{GCS_LANDING_FOLDER}batch_users_{filename_suffix}.parquet"
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=filename,
            data=parquet_buffer.getvalue(),
            mime_type='application/octet-stream'
        )

    # TASK 1: Generate & Upload Parquet
    task_extract_upload = PythonOperator(
        task_id='extract_and_upload_parquet',
        python_callable=mysql_to_parquet_gcs,
    )

    # TASK 2: Trigger DTS for Parquet Ingestion
    task_run_dts_parquet = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id='run_dts_parquet_to_managed_table',
        transfer_config_name=DTS_PARQUET_CONFIG_NAME,
        gcp_conn_id='google_cloud_default',
    )

    # TASK 3: Trigger DTS Scheduled Query for Merging
    task_run_dts_merge = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id='run_dts_scheduled_query_merge',
        transfer_config_name=DTS_MERGE_CONFIG_NAME,
        gcp_conn_id='google_cloud_default',
    )

    # Pipeline execution order
    task_extract_upload >> task_run_dts_parquet >> task_run_dts_merge
