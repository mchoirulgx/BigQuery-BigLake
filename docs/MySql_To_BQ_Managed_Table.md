# Modern Data Lakehouse: Moving Data from MySQL to BigQuery for Migration

## Architecture Overview

This documentation outlines the architecture and implementation of a data pipeline designed for **moving data from a MySQL database to a Google Cloud BigQuery-based Data Lakehouse for migration purposes**. 

The architecture strictly follows a two-tier BigQuery layer approach to optimize storage costs, query performance, and ensure data accuracy during the migration window:
1.  **History Layer (Bronze/Staging):** Uses **Apache Iceberg (BigLake Managed Table)** backed by Google Cloud Storage (GCS). This layer acts as an append-only staging area for all incoming data and historical state changes during the migration.
2.  **Main Layer (Gold/Production):** Uses **BigQuery Native Tables**. This layer reflects the final, as-is state of the data by merging changes from the History layer and cleaning up any soft-deleted records.

### Key Technologies
*   **Source:** MySQL
*   **Storage & Compute:** Google Cloud Storage (GCS), BigQuery, BigLake Connection
*   **Orchestration:** Apache Airflow
*   **Processing:** Python (Pandas) & Standard SQL DML

---

## 1. Data Type Mapping (MySQL to BigQuery)

When migrating data from MySQL to BigQuery, it is crucial to map the data types correctly, as BigQuery uses a different type system. Below is a comprehensive comparison table for standard data types:

| MySQL Data Type | BigQuery Equivalent | Notes |
| :--- | :--- | :--- |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `INT64` | BigQuery handles all integers as 64-bit integers. |
| `VARCHAR`, `CHAR`, `TEXT`, `LONGTEXT` | `STRING` | BigQuery `STRING` values must be UTF-8 encoded. |
| `DATETIME`, `TIMESTAMP` | `TIMESTAMP` or `DATETIME` | Use `TIMESTAMP` if you need absolute point-in-time (includes timezone context). Use `DATETIME` for civil time (no timezone). |
| `DATE` | `DATE` | Maps directly. |
| `FLOAT`, `DOUBLE` | `FLOAT64` | BigQuery uses 64-bit floating-point numbers. |
| `DECIMAL`, `NUMERIC` | `NUMERIC` or `BIGNUMERIC` | Use `NUMERIC` for exact precision (up to 38 digits). Use `BIGNUMERIC` for extreme precision needs. |
| `TINYINT(1)`, `BOOLEAN`, `BOOL` | `BOOL` | Represents `TRUE` or `FALSE`. |
| `JSON` | `JSON` or `STRING` | BigQuery has native `JSON` support, but storing it as `STRING` is common for staging layers before parsing. |
| `BLOB`, `BINARY` | `BYTES` | Used for raw binary data. |

---

## 2. Prerequisites & GCP Setup

Before deploying the pipeline, ensure the following Google Cloud components are configured:
1.  **BigLake Connection:** Create an external data connection in BigQuery (e.g., `<your-region>.<your-connection-id>`).
2.  **IAM Permissions:** The Service Account running Apache Airflow must have `BigQuery Connection Admin` or `BigQuery Connection User` roles to utilize the BigLake connection.
3.  **Cloud Storage Bucket:** A dedicated GCS bucket to store the Iceberg Parquet files and metadata (e.g., `gs://<your-bucket-name>/<your-folder>/`).

---

## 3. Database Schema (DDL)

### 3.1. Source: MySQL Table
The source table utilizes a `deleted_at` column to track soft deletes during the migration phase.

```sql
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    deleted_at DATETIME NULL
);
```

### 3.2. Target 1: BigQuery Iceberg Managed Table (History Layer)
This table stores every state change. It uses the Iceberg format for direct DML support (`INSERT`) while keeping data on affordable GCS storage. It is partitioned by `created_at`.

```sql
CREATE OR REPLACE TABLE `<your-project-id>.<your-dataset>.history_users`
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
    storage_uri = 'gs://<your-bucket-name>/<your-folder_partitioned>/' 
);
```
> **Note on Partitioning:** Iceberg uses *Hidden Partitioning*. You will not see explicit Hive-style directory structures (e.g., `created_at=2026-08-20`) in GCS. Instead, Iceberg tracks partition mapping within its metadata files, enabling significantly faster query planning.

### 3.3. Target 2: BigQuery Native Table (Main Layer)
This table acts as the serving layer for analytics. It merges data from the History layer to reflect the final migrated state.

```sql
CREATE OR REPLACE TABLE `<your-project-id>.<your-dataset>.managed_main_users`
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

## 4. Apache Airflow Orchestration (DAG)

The DAG extracts data modified in the last 24 hours and orchestrates the migration load process. 

### Why JSON UNNEST for Iceberg Loading?
Standard BigQuery Load APIs (often used by `pandas.to_gbq`) **do not support loading data into Iceberg tables**. Attempting to do so results in a `403 Delegate Permission` error. The most robust workaround is converting the Pandas DataFrame to a JSON string and executing a direct SQL `INSERT` using `UNNEST(JSON_QUERY_ARRAY())`.

### Airflow DAG Code (`mysql_to_lakehouse_migration.py`)

```python
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

MYSQL_CONN_ID = '<your-mysql-conn-id>'
GCP_CONN_ID = '<your-gcp-conn-id>'

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
    schedule_interval='0 0 * * *', # Runs daily at midnight
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

    task_extract_load >> task_merge_main
```