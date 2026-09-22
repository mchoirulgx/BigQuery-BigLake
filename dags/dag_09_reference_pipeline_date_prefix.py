"""
===============================================================================
DAG: banking_pipeline_soft_delete
Stage: Stage 4 - Incremental Loading (Reference Pipeline)
File: dags/dag_09_reference_pipeline_date_prefix.py

PURPOSE:
  CANONICAL REFERENCE PIPELINE DEMO:
  Orchestrates an end-to-end robust Lakehouse ingestion pipeline:
  1. Generates / extracts CDC data from MySQL (`transaksi_bank`).
  2. Dynamically decides between Full Load and Delta Load.
  3. Coerces timestamps to UTC microsecond precision (`coerce_timestamps='us'`).
  4. Writes Parquet to date-partitioned GCS folders (`data_transaksi/export_YYYYMMDD/`).
  5. Triggers BigQuery DTS to append data into BigLake Managed Iceberg table.
  6. USES SENSOR: Waits for DTS run to SUCCEED using `BigQueryDataTransferServiceTransferRunSensor`.
  7. Executes an idempotent BigQuery MERGE into the native serving table (`final_transaksi`)
     with QUALIFY ROW_NUMBER deduplication and soft-delete tracking.

WHAT TO DO:
  1. Configure Airflow Connections:
     - 'google_cloud_default' (GCP_CONN_ID): Connection with BigQuery, Storage, and DTS scopes.
     - 'mysql_default' (MYSQL_CONN_ID): Connection to source MySQL database.
  2. Set Variables / Config:
     - Replace PROJECT_ID, DATASET_ID, GCS_BUCKET, DTS_CONFIG_ID, and REGION.
  3. Execute DDLs:
     - Run `sql/04_ddl_reference_staging_and_native.sql` in BigQuery Studio.
  4. Set up DTS Transfer in BigQuery:
     - Source URI: `gs://<YOUR_BUCKET>/data_transaksi/export_{run_time+7h|"%Y%m%d"}/*.parquet`
     - Destination Table: `staging_iceberg_transaksi`
     - Schedule: On-Demand
  5. Deploy:
     - Copy this file to your Airflow / Cloud Composer `dags/` folder.
  6. Execution:
     - Trigger the DAG in Airflow UI.

DEMO VS. PRODUCTION NOTES:
  - ZERO-DELETION GCS ARCHIVE (SCENARIO B):
    * This pipeline implements Scenario B: files are NEVER deleted from GCS.
    * Date-partitioned prefixing (`export_YYYYMMDD/`) ensures DTS only ingests
      the active window, maintaining a full immutable raw audit trail on Cloud Storage.
  - Eliminates Race Conditions: The sensor ensures the MERGE task never executes
    before DTS finishes copying data and updating Iceberg manifests.
  - Microsecond UTC timestamps prevent `INVALID_ARGUMENT` errors.
  - Production Cost Tip: In the MERGE statement, adding target partition bounds
    (`AND T.tanggal_transaksi = S.tanggal_transaksi`) prevents full scans on `final_transaksi`.
===============================================================================
"""

import random
import uuid
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryDataTransferServiceStartTransferRunsOperator
from airflow.providers.google.cloud.sensors.bigquery_dts import BigQueryDataTransferServiceTransferRunSensor
import pendulum

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
        from io import BytesIO
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

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

        # 4. Upload to GCS in date-prefixed folder
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
       -- Partition pruning optimization
       AND T.tanggal_transaksi = S.tanggal_transaksi
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
