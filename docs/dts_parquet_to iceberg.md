# Modern Data Lakehouse: Batch Data Migration via GCS Parquet and BigQuery DTS

## Architecture Overview

This documentation outlines an end-to-end data pipeline for migrating data via Full and Incremental Batch loads from MySQL to a BigQuery Data Lakehouse[cite: 1]. To optimize extraction speed and avoid overloading the Airflow worker's memory and storage, this architecture utilizes in-memory Parquet conversion and BigQuery Data Transfer Service (DTS) for native Iceberg ingestion[cite: 1].

### The Pipeline Flow:
1.  **Extraction (Airflow):** Dynamically queries MySQL. If the target BigQuery table is empty, it performs a **Full Load**. If data exists, it performs an **Incremental Load** for the last 24 hours of data.
2.  **Transformation (Pandas In-Memory):** Converts the extracted data into a Parquet format entirely in memory (`BytesIO`), specifically coercing timestamps to microseconds to ensure BigQuery compatibility[cite: 1].
3.  **Landing Zone (GCS):** Uploads the Parquet file to a Cloud Storage bucket[cite: 1].
4.  **Staging/History Layer (BigQuery DTS):** BigQuery DTS automatically detects new Parquet files, ingests them into a BigLake Managed Iceberg Table, and updates the Iceberg metadata[cite: 1].
5.  **Main/Gold Layer (Scheduled Query):** A standard SQL `MERGE` operation applies the historical data changes (inserts, updates, soft-deletes) into a native BigQuery serving table[cite: 1].

---

## 1. Prerequisites & GCP Setup

1.  **BigLake Connection:** Create an external data connection in BigQuery (e.g., `<your-region>.<your-connection-id>`)[cite: 1].
2.  **Cloud Storage Bucket:** Create a bucket with two distinct folders[cite: 1]:
    *   `gs://<your-bucket-name>/batch_landing_zone/` (For raw Parquet drops)
    *   `gs://<your-bucket-name>/batch_iceberg_managed/` (For BigLake Iceberg metadata/storage)

---

## 2. BigQuery Table Schemas (DDL)

Execute the following queries in BigQuery to set up the two-tier architecture[cite: 1].

### 2.1. Target 1: BigLake Managed Iceberg Table (History Layer)
This table acts as the destination for BigQuery DTS[cite: 1]. 

```sql
CREATE OR REPLACE TABLE `<your-project-id>.<your-dataset>.dts_managed_users`
(
    id STRING,
    name STRING,
    email STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
)
PARTITION BY DATE(created_at) 
WITH CONNECTION `<your-project-id>.<your-region>.<your-connection-id>`
OPTIONS (
    file_format = 'PARQUET', 
    table_format = 'ICEBERG', 
    storage_uri = 'gs://<your-bucket-name>/batch_iceberg_managed/'
);
```

### 2.2. Target 2: BigQuery Native Table (Main Layer)
This is the final serving layer where data is physically merged[cite: 1].

```sql
CREATE OR REPLACE TABLE `<your-project-id>.<your-dataset>.dts_native_users`
(
    id STRING,
    name STRING,
    email STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
)
PARTITION BY DATE(created_at);
```

---

## 3. Apache Airflow Orchestration (Automated Full & Incremental)

This DAG extracts the data, applies the crucial `coerce_timestamps='us'` fix for PyArrow (preventing BigQuery `TIMESTAMP_NANOS` invalid argument errors), and streams it to GCS[cite: 1]. It automatically checks if the target table is empty to decide between a Full Load or an Incremental Load.

### Airflow DAG Code (`mysql_to_gcs_parquet_batch.py`)

```python
from datetime import timedelta
import pendulum
import pandas as pd
from io import BytesIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# --- CONFIGURATION ---
GCS_BUCKET = '<your-bucket-name>'
GCS_LANDING_FOLDER = 'batch_landing_zone/'
MYSQL_CONN_ID = '<your-mysql-conn-id>'
GCP_CONN_ID = '<your-gcp-conn-id>'
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
    schedule_interval='0 0 * * *', # Runs daily at midnight
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
        
        # 2. Check Target Table Status in BigQuery
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
```

---

## 4. BigQuery Data Transfer Service (DTS) Setup

Once the first Parquet file is in the landing zone, configure DTS via the BigQuery UI to automate the Iceberg ingestion[cite: 1]:

1.  Navigate to **Data transfers** > **Create Transfer**[cite: 1].
2.  **Source:** Google Cloud Storage[cite: 1].
3.  **Transfer config name:** `Ingest_MySQL_Parquet_to_Iceberg_Batch`
4.  **Schedule:** Set to `00:30` (Allowing Airflow time to complete the GCS upload)[cite: 1].
5.  **Destination dataset:** `<your-dataset>`[cite: 1].
6.  **Destination table:** `dts_managed_users`[cite: 1].
7.  **Cloud Storage URI:** `gs://<your-bucket-name>/batch_landing_zone/*.parquet`
8.  **Write preference:** `APPEND`[cite: 1].
9.  **File format:** `PARQUET`[cite: 1].
10. Check **Delete source files after transfer** if you want to keep the landing zone clean[cite: 1].

---

## 5. Merging to Native Table (Scheduled Query)

Set up a BigQuery Scheduled Query to run at `01:00` (after DTS completes) to apply the incremental batch logic (Inserts, Updates, and Soft Deletes) to the Native Table[cite: 1].

```sql
MERGE `<your-project-id>.<your-dataset>.dts_native_users` T
USING (
  SELECT * EXCEPT(rn) 
  FROM (
    SELECT 
      *, 
      ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) as rn
    FROM `<your-project-id>.<your-dataset>.dts_managed_users`
    -- Optimization: Only scan the last 2 days of history
    WHERE updated_at >= TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME('Asia/Jakarta'), INTERVAL 2 DAY))
  ) 
  WHERE rn = 1
) S 
ON T.id = S.id

-- Scenario 1: Process Soft Deletes
WHEN MATCHED AND S.deleted_at IS NOT NULL THEN 
  DELETE
  
-- Scenario 2: Process Updates
WHEN MATCHED AND S.deleted_at IS NULL THEN 
  UPDATE SET 
    T.name = S.name, 
    T.email = S.email, 
    T.updated_at = S.updated_at
    
-- Scenario 3: Process New Inserts
WHEN NOT MATCHED AND S.deleted_at IS NULL THEN 
  INSERT (id, name, email, created_at, updated_at, deleted_at) 
  VALUES (S.id, S.name, S.email, S.created_at, S.updated_at, S.deleted_at);
```