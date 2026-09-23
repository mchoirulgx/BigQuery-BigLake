"""
===============================================================================
DAG: mysql_to_lakehouse_migration
Stage: Stage 2 - Simple Ingestion
File: dags/dag_05_mysql_direct_sql.py

PURPOSE:
  Demonstrates an educational prototype pipeline that extracts incremental data (via
  updated_at watermark) from a relational MySQL database and ingests it directly into a BigLake Managed
  Iceberg table (`history_users`) using BigQuery SQL DML and JSON UNNEST,
  followed by an upsert MERGE into the native BigQuery serving table (`managed_main_users`).

WHAT TO DO:
  1. Configure Airflow Connections:
     - 'google_cloud_default' (or set GCP_CONN_ID): GCP Connection with BigQuery scopes.
     - 'mysql_conn' (or set MYSQL_CONN_ID): Connection to source MySQL database.
  2. Set Variables / Config:
     - Update PROJECT_ID, DATASET, ICEBERG_HISTORY_TABLE, and NATIVE_MAIN_TABLE.
  3. Deploy:
     - Copy this file to your Airflow / Cloud Composer `dags/` folder.
  4. Execution:
     - Trigger manually or run on daily schedule.

DEMO VS. PRODUCTION NOTES:
  - This DAG uses `UNNEST(JSON_QUERY_ARRAY(@json_str))` to bypass the BigQuery Load
    API's lack of support for direct Iceberg table loading.
  - NOTE ON SCALING: This technique is suitable for small demo payloads (< 5,000 rows).
    For production scale, refer to Stage 4 (`dag_09_reference_pipeline_date_prefix.py`),
    which stages Parquet files in GCS and uses BigQuery DTS to avoid query parameter size limits.
===============================================================================
"""

from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

# --- CONFIGURATION ---
PROJECT_ID = '<your-project-id>'
DATASET = '<your-dataset>'
ICEBERG_HISTORY_TABLE = f'{PROJECT_ID}.{DATASET}.history_users' 
NATIVE_MAIN_TABLE = f'{PROJECT_ID}.{DATASET}.managed_main_users' 

MYSQL_CONN_ID = 'mysql_conn'
GCP_CONN_ID = 'google_cloud_default'

# Set Timezone
local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'mysql_to_lakehouse_migration',
    default_args=default_args,
    description='Pipeline for moving data from MySQL to BQ for migration',
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    start_date=pendulum.datetime(2026, 8, 20, tz="Asia/Jakarta"),
    catchup=False,
    tags=['elt', 'mysql', 'iceberg', 'migration'],
) as dag:

    # ==========================================
    # TASK 1: MySQL -> Iceberg Managed (History)
    # ==========================================
    def extract_and_append(**kwargs):
        # 1. Define Time Window (Previous Day)
        execution_date = kwargs.get('data_interval_end').in_timezone(local_tz)
        yesterday = execution_date.subtract(days=1)
        
        start_window = yesterday.start_of('day').to_datetime_string() 
        end_window = yesterday.end_of('day').to_datetime_string()     
        
        print(f"Extracting migration data from {start_window} to {end_window}")
        
        # 2. Extract from MySQL
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        engine = mysql_hook.get_sqlalchemy_engine()
        
        query = f"""
            SELECT id, name, email, created_at, updated_at, deleted_at 
            FROM users 
            WHERE updated_at >= '{start_window}' AND updated_at <= '{end_window}'
        """
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print("No new data found for migration window. Skipping BQ load.")
            return "skipped"
            
        # 3. Handle NULL values and Convert to JSON
        for col in ['created_at', 'updated_at', 'deleted_at']:
            df[col] = df[col].astype(str).replace({'NaT': None, 'nan': None})
            
        json_data = df.to_json(orient='records')
        
        # 4. Direct DML into BigLake Iceberg using JSON UNNEST
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from google.cloud import bigquery
        
        insert_query = f"""
            INSERT INTO `{ICEBERG_HISTORY_TABLE}` (id, name, email, created_at, updated_at, deleted_at)
            SELECT 
                JSON_VALUE(json_row, '$.id'),
                JSON_VALUE(json_row, '$.name'),
                JSON_VALUE(json_row, '$.email'),
                CAST(JSON_VALUE(json_row, '$.created_at') AS TIMESTAMP),
                CAST(JSON_VALUE(json_row, '$.updated_at') AS TIMESTAMP),
                CAST(JSON_VALUE(json_row, '$.deleted_at') AS TIMESTAMP)
            FROM UNNEST(JSON_QUERY_ARRAY(@json_str, '$')) AS json_row
        """
        
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq_hook.get_client(project_id=PROJECT_ID)
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("json_str", "STRING", json_data)]
        )
        job = client.query(insert_query, job_config=job_config)
        job.result()

    task_extract_load = PythonOperator(
        task_id='extract_mysql_to_iceberg',
        python_callable=extract_and_append,
    )

    # ==========================================
    # TASK 2: MERGE Iceberg -> Native Main
    # ==========================================
    merge_query = f"""
    MERGE `{NATIVE_MAIN_TABLE}` T
    USING (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT 
          *, 
          ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM `{ICEBERG_HISTORY_TABLE}`
        -- Scan optimization: Only read the last 2 days of history
        WHERE updated_at >= TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME('Asia/Jakarta'), INTERVAL 2 DAY))
      )
      WHERE rn = 1
    ) S
    ON T.id = S.id
    
    -- Scenario 1: Soft Delete to Hard Delete
    WHEN MATCHED AND S.deleted_at IS NOT NULL THEN 
      DELETE
      
    -- Scenario 2: Update existing record
    WHEN MATCHED AND S.deleted_at IS NULL THEN
      UPDATE SET 
        T.name = S.name, 
        T.email = S.email, 
        T.updated_at = S.updated_at
        
    -- Scenario 3: Insert new record
    WHEN NOT MATCHED AND S.deleted_at IS NULL THEN
      INSERT (id, name, email, created_at, updated_at, deleted_at) 
      VALUES (S.id, S.name, S.email, S.created_at, S.updated_at, S.deleted_at)
    """

    task_merge_main = BigQueryInsertJobOperator(
        task_id='merge_iceberg_to_native',
        configuration={
            "query": {
                "query": merge_query,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # Dependency Flow
    task_extract_load >> task_merge_main
