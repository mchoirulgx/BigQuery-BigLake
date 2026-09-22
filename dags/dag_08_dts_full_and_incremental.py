"""
===============================================================================
DAG: mysql_to_gcs_parquet_batch
Stage: Stage 4 - Incremental Loading
File: dags/dag_08_dts_full_and_incremental.py

PURPOSE:
  Automates data extraction from MySQL into GCS Parquet files, dynamically
  deciding between an initial Full Load (all records) and daily Incremental Loads
  (last 24 hours of updates) based on destination table emptiness in BigQuery.

WHAT TO DO:
  1. Configure Airflow Connections:
     - 'google_cloud_default' (or GCP_CONN_ID): GCP Connection with BigQuery & GCS scopes.
     - 'mysql_conn' (or MYSQL_CONN_ID): Source database connection credentials.
  2. Set Variables / Config:
     - Update GCS_BUCKET, BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID.
  3. Deploy:
     - Copy this file to your Airflow / Cloud Composer `dags/` folder.
  4. Execution:
     - On first run: table is empty -> DAG executes Full Load (`batch_users_full_load.parquet`).
     - On subsequent runs: table has rows -> DAG executes Incremental Load (`batch_users_incremental_YYYYMMDD.parquet`).

DEMO VS. PRODUCTION NOTES:
  - Timestamp Coercion: Enforces `coerce_timestamps='us'` to prevent PyArrow's default
    nanoseconds from causing BigQuery load errors.
  - Scaling Tip: On multi-million-row initial loads, replace monolithic `pd.read_sql`
    with chunked streaming (`chunksize=50000`) to prevent worker Out-Of-Memory (OOM) errors.
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
from airflow.providers.mysql.hooks.mysql import MySqlHook

# --- CONFIGURATION ---
GCS_BUCKET = '<your-bucket-name>'
GCS_LANDING_FOLDER = 'batch_landing_zone/'
MYSQL_CONN_ID = 'mysql_conn'
GCP_CONN_ID = 'google_cloud_default'
BQ_PROJECT_ID = '<your-project-id>'
BQ_DATASET_ID = '<your-dataset>'
BQ_TABLE_ID = 'dts_managed_users'

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'mysql_to_gcs_parquet_batch',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    start_date=pendulum.datetime(2026, 8, 26, tz="Asia/Jakarta"),
    catchup=False,
    tags=['batch', 'mysql', 'gcs', 'parquet', 'iceberg'],
) as dag:

    def mysql_to_parquet_gcs(**kwargs):
        # 1. Define Time Window (Previous Day)
        execution_date = kwargs.get('data_interval_end').in_timezone(local_tz)
        yesterday = execution_date.subtract(days=1)
        
        start_win = yesterday.start_of('day').to_datetime_string() 
        end_win = yesterday.end_of('day').to_datetime_string()     
        
        # 2. Check Target Table Status in BigQuery (Full or Incremental)
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        check_query = f"SELECT COUNT(1) FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`"
        
        is_empty = True
        try:
            records = bq_hook.get_records(check_query)
            if records and records[0][0] > 0:
                is_empty = False
                print(f"Table contains {records[0][0]} rows. Proceeding with Incremental Load.")
            else:
                print("Table is empty. Proceeding with Full Load.")
        except Exception as e:
            print(f"Table check failed (table might not exist): {e}. Proceeding with Full Load.")
            is_empty = True

        # 3. Construct SQL Query dynamically
        if is_empty:
            query = """
                SELECT id, name, email, created_at, updated_at, deleted_at 
                FROM users
            """
            filename_suffix = "full_load"
        else:
            query = f"""
                SELECT id, name, email, created_at, updated_at, deleted_at 
                FROM users 
                WHERE updated_at >= '{start_win}' AND updated_at <= '{end_win}'
            """
            filename_suffix = f"incremental_{yesterday.strftime('%Y%m%d')}"
            
        print(f"Executing Query: {query}")

        # 4. Extract from MySQL
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        engine = mysql_hook.get_sqlalchemy_engine()
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print("No data found for the defined window. Skipping.")
            return "skipped"
            
        # 5. Handle Datetimes and Force Microsecond Precision
        for col in ['created_at', 'updated_at', 'deleted_at']:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
        parquet_buffer = BytesIO()
        df.to_parquet(
            parquet_buffer, 
            engine='pyarrow', 
            index=False,
            coerce_timestamps='us',          # Critical: BigQuery does not support nanoseconds
            allow_truncated_timestamps=True 
        )
        
        # 6. Upload to GCS Landing Zone
        filename = f"{GCS_LANDING_FOLDER}batch_users_{filename_suffix}.parquet"
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=filename,
            data=parquet_buffer.getvalue(),
            mime_type='application/octet-stream'
        )
        print(f"Successfully uploaded {filename}")

    task_extract_upload = PythonOperator(
        task_id='extract_and_upload_parquet',
        python_callable=mysql_to_parquet_gcs,
    )
