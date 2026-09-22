# End-to-End Data Pipeline: MySQL to BigQuery (Iceberg Staging & Native Upsert)

## 1. Overview

This repository contains a complete automated data pipeline integrating MySQL, Google Cloud Storage (GCS), BigLake Iceberg, and BigQuery Native tables.

### Architecture Flow

1. **Source:** MySQL Database.
2. **Extraction:** Apache Airflow extracts delta data using Pandas, enforces a strict PyArrow schema, and uploads Parquet files to GCS in date-prefixed folders.
3. **Ingestion (Staging):** BigQuery Data Transfer Service (DTS) detects the new Parquet files in GCS and synchronizes them into a **BigLake Iceberg** staging table.
4. **Transformation (Final):** Airflow triggers a BigQuery `MERGE` operation to upsert the staging data into a **BigQuery Native** production table, handling inserts, updates, and soft-deletes safely.

---

## 2. BigQuery Infrastructure Setup (Table DDLs)

Before running the Airflow pipeline, you must execute the following Data Definition Language (DDL) queries in the BigQuery SQL Workspace.

### A. Staging Table (BigLake Iceberg)

This acts as a staging layer. BigQuery Iceberg tables require a Cloud Resource Connection. Ensure you have created a connection in BigQuery (e.g., `my-connection`) and granted it access to your GCS bucket.

```sql
CREATE TABLE IF NOT EXISTS `[YOUR_PROJECT_ID].[YOUR_DATASET_ID].staging_iceberg_transaksi`
(
    id_transaksi STRING,
    jumlah FLOAT64,
    status STRING,
    tanggal_transaksi DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
)
WITH CONNECTION `[YOUR_PROJECT_ID].[YOUR_REGION].[YOUR_CONNECTION_NAME]`
OPTIONS (
    file_format = 'PARQUET',
    table_format = 'ICEBERG',
    storage_uri = 'gs://[YOUR_GCS_BUCKET]/[YOUR_ICEBERG_DATA_FOLDER]/'
);
```

### B. Final Production Table (BigQuery Native)

This is the managed table where historical data is kept and the `MERGE` operation happens.

```sql
CREATE TABLE IF NOT EXISTS `[YOUR_PROJECT_ID].[YOUR_DATASET_ID].final_transaksi`
(
    id_transaksi STRING,
    jumlah FLOAT64,
    status STRING,
    tanggal_transaksi DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY id_transaksi;
```

---

## 3. Google Cloud Storage (GCS) Setup

Create a bucket to act as your landing zone for the Parquet files extracted from MySQL.

- **Bucket Name:** `[YOUR_GCS_BUCKET]`
- **Folder Path:** `data_transaksi/`

Airflow will automatically create subfolders daily, resulting in paths like:

```
gs://[YOUR_GCS_BUCKET]/data_transaksi/export_20260915/transaksi_export_20260915.parquet
```

---

## 4. BigQuery Data Transfer Service (DTS) Setup

Set up the DTS to continuously load GCS data into your Iceberg table.

1. Go to **Google Cloud Console > BigQuery > Data Transfers**.
2. Click **+ CREATE TRANSFER**.
3. **Source Type:** Select "Google Cloud Storage".
4. **Transfer Config Name:** e.g., `DTS_GCS_to_Iceberg`.
5. **Schedule Options:** Set to "On-demand" or disable the schedule, as Airflow will trigger this.
6. **Destination Settings:**
   - Dataset: `[YOUR_DATASET_ID]`
   - Destination Table: `staging_iceberg_transaksi`
7. **Data Source Details:**
   - Cloud Storage URI: `gs://[YOUR_GCS_BUCKET]/data_transaksi/export_{run_time+7h|"%Y%m%d"}/*.parquet`
     *(Note: The `{run_time+7h}` macro offsets the default UTC time to match the GMT+7 local time folder created by Airflow.)*
   - File Format: `PARQUET`.
     *(Note for Iceberg: DTS will synchronize the metadata. There is no `WRITE_TRUNCATE` option for Iceberg tables.)*
8. Click **SAVE**.
9. Copy the **Configuration ID** (e.g., `6aa9c338-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) and paste it into the `DTS_CONFIG_ID` variable in your Airflow code.

---

## 5. Full Airflow DAG Code

Create a file named `banking_pipeline_soft_delete.py` in your Airflow `dags/` folder and paste the following code.

```python
import pendulum
import uuid
import random
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryDataTransferServiceStartTransferRunsOperator
from airflow.providers.google.cloud.sensors.bigquery_dts import BigQueryDataTransferServiceTransferRunSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ================= CONFIGURATION =================
GCP_CONN_ID = "google_cloud_default"
MYSQL_CONN_ID = "mysql_default"
GCS_BUCKET = "[YOUR_GCS_BUCKET]"
PROJECT_ID = "[YOUR_PROJECT_ID]"
DATASET_ID = "[YOUR_DATASET_ID]"
DTS_CONFIG_ID = "[YOUR_DTS_CONFIG_ID]"
REGION = "asia-southeast2"

# Timezone
wib_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'banking_pipeline_soft_delete',
    default_args=default_args,
    description='Pipeline Soft Delete MySQL -> GCS -> DTS Iceberg -> BQ Native',
    schedule_interval='0 3 * * *',  # 03:00 AM Local Time
    start_date=pendulum.datetime(2026, 9, 14, tz=wib_tz),
    catchup=False,
    tags=['banking', 'iceberg', 'soft-delete'],
) as dag:

    # ---------------------------------------------------------
    # TASK 1: Generate Dummy Data (For Testing Only)
    # ---------------------------------------------------------
    @task
    def generate_dummy_data(ds=None):
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # 1. 700 NEW INSERTS
        insert_data = []
        for _ in range(700):
            trx_id = f"TRX-{uuid.uuid4().hex[:8]}"
            insert_data.append((
                trx_id, random.uniform(100, 10000), 'SUCCESS',
                ds, f"{ds} 09:00:00", f"{ds} 09:00:00", None
            ))
        cursor.executemany(
            """INSERT INTO transaksi_bank
            (id_transaksi, jumlah, status, tanggal_transaksi, created_at, updated_at, deleted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""", insert_data
        )

        # 2. 200 SOFT UPDATES
        cursor.execute("SELECT id_transaksi FROM transaksi_bank WHERE deleted_at IS NULL LIMIT 200")
        ids_to_update = [row[0] for row in cursor.fetchall()]
        if ids_to_update:
            format_strings = ','.join(['%s'] * len(ids_to_update))
            cursor.execute(
                f"UPDATE transaksi_bank SET jumlah = jumlah + 700, updated_at = '{ds} 11:30:00' WHERE id_transaksi IN ({format_strings})",
                tuple(ids_to_update)
            )

        # 3. 100 SOFT DELETES
        cursor.execute("SELECT id_transaksi FROM transaksi_bank WHERE deleted_at IS NULL LIMIT 100 OFFSET 200")
        ids_to_delete = [row[0] for row in cursor.fetchall()]
        if ids_to_delete:
            format_strings = ','.join(['%s'] * len(ids_to_delete))
            cursor.execute(
                f"UPDATE transaksi_bank SET deleted_at = '{ds} 14:00:00', updated_at = '{ds} 14:00:00' WHERE id_transaksi IN ({format_strings})",
                tuple(ids_to_delete)
            )

        conn.commit()
        cursor.close()
        conn.close()

    # ---------------------------------------------------------
    # TASK 2: Check Initial vs Delta Load
    # ---------------------------------------------------------
    @task
    def check_initial_load(ds=None, **kwargs):
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)

        # Check if native table is empty
        query = f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.final_transaksi`"
        try:
            res = list(client.query(query).result())
            count = res[0]['cnt']
        except Exception:
            count = 0

        # No casting required. PyArrow will handle the schema strictly.
        base_sql = """
        SELECT id_transaksi, jumlah, status, tanggal_transaksi, created_at, updated_at, deleted_at
        FROM transaksi_bank
        """
        if count == 0:
            sql_query = base_sql
            is_init = True
        else:
            sql_query = f"{base_sql} WHERE DATE(updated_at) = '{ds}'"
            is_init = False

        return {"sql": sql_query, "is_initial": is_init}

    # ---------------------------------------------------------
    # TASK 3: Extract MySQL to GCS (Pandas + PyArrow)
    # ---------------------------------------------------------
    @task
    def extract_mysql_to_gcs(data_interval_end=None, **kwargs):
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        from io import BytesIO
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        ti = kwargs['ti']
        sql_query = ti.xcom_pull(task_ids='check_initial_load')['sql']

        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        engine = mysql_hook.get_sqlalchemy_engine()
        df = pd.read_sql(sql_query, engine)
        if df.empty:
            print("No data to extract.")
            return

        # 1. Clean Data Types
        df['id_transaksi'] = df['id_transaksi'].astype(str)
        df['jumlah'] = pd.to_numeric(df['jumlah'], errors='coerce').astype(float)
        df['status'] = df['status'].astype(str)
        df['tanggal_transaksi'] = pd.to_datetime(df['tanggal_transaksi'], errors='coerce').dt.date

        # 2. Enforce UTC on Timestamps for BigQuery Compatibility
        waktu_kolom = ['created_at', 'updated_at', 'deleted_at']
        for col in waktu_kolom:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')
            else:
                df[col] = df[col].dt.tz_convert('UTC')

        # 3. Explicit PyArrow Schema (Prevents INVALID_ARGUMENT errors in BQ)
        schema = pa.schema([
            pa.field('id_transaksi', pa.string()),
            pa.field('jumlah', pa.float64()),
            pa.field('status', pa.string()),
            pa.field('tanggal_transaksi', pa.date32()),
            pa.field('created_at', pa.timestamp('us', tz='UTC')),
            pa.field('updated_at', pa.timestamp('us', tz='UTC')),
            pa.field('deleted_at', pa.timestamp('us', tz='UTC'))
        ])

        table = pa.Table.from_pandas(df, schema=schema)
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)

        # 4. Upload to GCS
        date_folder = data_interval_end.in_timezone('Asia/Jakarta').strftime('%Y%m%d')
        filename = f"data_transaksi/export_{date_folder}/transaksi_export_{date_folder}.parquet"

        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=filename,
            data=parquet_buffer.getvalue(),
            mime_type='application/octet-stream'
        )

    # ---------------------------------------------------------
    # TASK 4 & 5: Trigger and Wait for DTS
    # ---------------------------------------------------------
    trigger_dts = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="trigger_dts",
        project_id=PROJECT_ID,
        transfer_config_id=DTS_CONFIG_ID,
        location=REGION,
        requested_run_time={"seconds": "{{ data_interval_end.int_timestamp }}"},
        gcp_conn_id=GCP_CONN_ID,
    )

    wait_for_dts = BigQueryDataTransferServiceTransferRunSensor(
        task_id="wait_for_dts",
        project_id=PROJECT_ID,
        transfer_config_id=DTS_CONFIG_ID,
        location=REGION,
        run_id="{{ task_instance.xcom_pull(task_ids='trigger_dts', key='return_value').name.split('/')[-1] }}",
        expected_statuses={"SUCCEEDED"},
        poke_interval=30,
        timeout=1200,
        gcp_conn_id=GCP_CONN_ID,
    )

    # ---------------------------------------------------------
    # TASK 6: Upsert (MERGE) Staging Iceberg to Native Final
    # ---------------------------------------------------------
    merge_sql = f"""
    {{% set is_init = ti.xcom_pull(task_ids='check_initial_load')['is_initial'] %}}
    {{% set date_filter = "1=1" if is_init else "DATE(updated_at) = '{{{{ ds }}}}'" %}}

    MERGE `{PROJECT_ID}.{DATASET_ID}.final_transaksi` T
    USING (
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.staging_iceberg_transaksi`
        WHERE {{{{ date_filter }}}}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY id_transaksi ORDER BY updated_at DESC) = 1
    ) S
    ON T.id_transaksi = S.id_transaksi
    WHEN MATCHED THEN
        UPDATE SET
            T.jumlah = S.jumlah,
            T.status = S.status,
            T.tanggal_transaksi = S.tanggal_transaksi,
            T.updated_at = S.updated_at,
            T.deleted_at = S.deleted_at
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id_transaksi, jumlah, status, tanggal_transaksi, created_at, updated_at, deleted_at)
        VALUES (S.id_transaksi, S.jumlah, S.status, S.tanggal_transaksi, S.created_at, S.updated_at, S.deleted_at)
    """

    upsert_to_final = BigQueryInsertJobOperator(
        task_id='upsert_to_final',
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": merge_sql,
                "useLegacySql": False,
            }
        }
    )

    # ---------------------------------------------------------
    # DEPENDENCIES
    # ---------------------------------------------------------
    dummy_task = generate_dummy_data()
    load_config = check_initial_load()
    extract_task = extract_mysql_to_gcs()
    dummy_task >> load_config >> extract_task >> trigger_dts >> wait_for_dts >> upsert_to_final
```

---

## 6. How to Re-Run the Pipeline (Skipping Dummy Data)

If a task fails and you want to resume the pipeline without regenerating the dummy data in MySQL, follow these steps:

1. **Open Airflow UI**: Navigate to the DAG `banking_pipeline_soft_delete`.
2. **Go to Grid/Graph View**: Click on the task that failed (e.g., `extract_mysql_to_gcs`).
3. **Clear the Task**: Click the Clear button.
4. **Include Downstream**: Make sure the Downstream option is checked so that all subsequent tasks (DTS, Upsert) are also reset.
5. **Confirm**: Click OK / Clear.

Airflow will automatically resume execution from the cleared task. The `generate_dummy_data` task will remain marked as Success and will be skipped.

---

## 7. Critical Engineering Notes

**A. Iceberg DTS Sync**

Unlike native BigQuery tables, BigLake Iceberg tables do not support the `WRITE_TRUNCATE` configuration via DTS. The DTS merely registers and synchronizes the Parquet file metadata from GCS into the Iceberg manifest.

**B. PyArrow Strict Schema Enforcement**

BigQuery is strictly typed. Relying on Pandas automatic inference will lead to `INVALID_ARGUMENT` errors. To solve this, we explicitly define the schema in the `extract_mysql_to_gcs` task:

- `pa.date32()`: Ensures Pandas dates are not interpreted as strings, matching BigQuery's `DATE` type.
- `pa.timestamp('us', tz='UTC')`: Ensures timestamps are parsed as `INT64` (microseconds) in UTC, exactly as BigQuery expects.

**C. Timezone Synchronization**

To prevent "No files found" errors in DTS:

- Airflow creates folders localized to GMT+7 (e.g., `data_interval_end.in_timezone('Asia/Jakarta')`).
- The DTS Cloud Storage URI dynamically adds 7 hours to its UTC runtime: `export_{run_time+7h|"%Y%m%d"}`. This perfectly aligns the DTS search query with Airflow's output folder.
