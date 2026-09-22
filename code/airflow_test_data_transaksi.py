import pendulum
import uuid
import random
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryDataTransferServiceStartTransferRunsOperator
from airflow.providers.google.cloud.sensors.bigquery_dts import BigQueryDataTransferServiceTransferRunSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ================= KONFIGURASI =================
GCP_CONN_ID = "google_cloud_default"
MYSQL_CONN_ID = "mysql_default"
GCS_BUCKET = "biglake_data"
PROJECT_ID = "imposing-league-374504"
DATASET_ID = "bqlake_data"
DTS_CONFIG_ID = "6aa9c338-0000-2faa-a278-582429cdbf78" # Ganti dengan Config ID dari Console

# Timezone WIB
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
    schedule_interval='0 3 * * *', # Jam 3 Pagi WIB
    start_date=pendulum.datetime(2026, 9, 14, tz=wib_tz),
    catchup=False,
    tags=['banking', 'iceberg', 'soft-delete'],
) as dag:

    # ---------------------------------------------------------
    # TASK 1: Generate Dummy Data (Insert, Soft Update, Soft Delete)
    # ---------------------------------------------------------
    @task
    def generate_dummy_data(ds=None):
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # 1. 700 INSERT BARU
        insert_data = []
        for _ in range(700):
            trx_id = f"TRX-{uuid.uuid4().hex[:8]}"
            # created_at & updated_at diisi logical_date (ds)
            insert_data.append((
                trx_id, random.uniform(100, 10000), 'SUCCESS', 
                ds, f"{ds} 09:00:00", f"{ds} 09:00:00", None
            ))
        cursor.executemany(
            """INSERT INTO transaksi_bank 
               (id_transaksi, jumlah, status, tanggal_transaksi, created_at, updated_at, deleted_at) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            insert_data
        )

        # 2. 200 SOFT UPDATE (Ambil 200 ID secara acak)
        cursor.execute("SELECT id_transaksi FROM transaksi_bank WHERE deleted_at IS NULL LIMIT 200")
        ids_to_update = [row[0] for row in cursor.fetchall()]
        if ids_to_update:
            format_strings = ','.join(['%s'] * len(ids_to_update))
            # Update jumlah dan updated_at
            cursor.execute(
                f"""UPDATE transaksi_bank 
                    SET jumlah = jumlah + 700, updated_at = '{ds} 11:30:00' 
                    WHERE id_transaksi IN ({format_strings})""",
                tuple(ids_to_update)
            )

        # 3. 100 SOFT DELETE (Ambil 100 ID lain yang belum di-delete)
        cursor.execute("SELECT id_transaksi FROM transaksi_bank WHERE deleted_at IS NULL LIMIT 100 OFFSET 200")
        ids_to_delete = [row[0] for row in cursor.fetchall()]
        if ids_to_delete:
            format_strings = ','.join(['%s'] * len(ids_to_delete))
            # Set deleted_at dan trigger updated_at
            cursor.execute(
                f"""UPDATE transaksi_bank 
                    SET deleted_at = '{ds} 14:00:00', updated_at = '{ds} 14:00:00' 
                    WHERE id_transaksi IN ({format_strings})""",
                tuple(ids_to_delete)
            )
            
        conn.commit()
        cursor.close()
        conn.close()

    # ---------------------------------------------------------
    # TASK 2: Cek Initial Load atau Delta Load
    # ---------------------------------------------------------
    @task
    def check_initial_load(ds=None, **kwargs):
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        
        # Cek apakah tabel staging BQ masih kosong
        query = f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.staging_iceberg_transaksi`"
        try:
            res = list(client.query(query).result())
            count = res[0]['cnt']
        except Exception:
            count = 0

        # FIX: Hapus semua CAST AS CHAR pada kolom tanggal dan waktu.
        # Biarkan MySQL dan PyArrow mengekspornya dalam bentuk aslinya (Date/Timestamp).
        base_sql = """
            SELECT 
                id_transaksi, 
                CAST(jumlah AS DOUBLE) as jumlah, 
                status, 
                tanggal_transaksi, 
                created_at, 
                updated_at, 
                deleted_at 
            FROM transaksi_bank
        """
            
        # Logika SQL Extraction
        if count == 0:
            sql_query = base_sql
            is_init = True
        else:
            sql_query = f"{base_sql} WHERE DATE(updated_at) = '{ds}'"
            is_init = False
            
        return {"sql": sql_query, "is_initial": is_init}

    # Execute Python Tasks
    dummy_task = generate_dummy_data()
    load_config = check_initial_load()
    dummy_task >> load_config # Pastikan dummy dibuat sebelum di-cek

# ---------------------------------------------------------
    # TASK 3: Extract MySQL to GCS (Pandas + Explicit PyArrow Schema)
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
            print("Tidak ada data untuk diekstrak.")
            return

        # 1. Standarisasi tipe data dasar di Pandas
        df['id_transaksi'] = df['id_transaksi'].astype(str)
        df['jumlah'] = pd.to_numeric(df['jumlah'], errors='coerce').astype(float)
        df['status'] = df['status'].astype(str)
        
        # 2. Standarisasi DATE (Tanggal tanpa jam)
        df['tanggal_transaksi'] = pd.to_datetime(df['tanggal_transaksi'], errors='coerce').dt.date

        # 3. FIX: Konversi SEMUA kolom waktu ke Datetime + Timezone UTC
        waktu_kolom = ['created_at', 'updated_at', 'deleted_at']
        for col in waktu_kolom:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Memaksa penambahan UTC agar BigQuery mendeteksinya sebagai INT64 Timestamp
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')
            else:
                df[col] = df[col].dt.tz_convert('UTC')

        # ========================================================
        # 4. FIX: MEMAKSA SCHEMA PYARROW AGAR SESUAI DENGAN BIGQUERY
        # ========================================================
        # pa.date32() = tipe DATE di BigQuery
        # pa.timestamp('us', tz='UTC') = tipe TIMESTAMP di BigQuery (INT64)
        schema = pa.schema([
            pa.field('id_transaksi', pa.string()),
            pa.field('jumlah', pa.float64()),
            pa.field('status', pa.string()),
            pa.field('tanggal_transaksi', pa.date32()),
            pa.field('created_at', pa.timestamp('us', tz='UTC')),
            pa.field('updated_at', pa.timestamp('us', tz='UTC')),
            pa.field('deleted_at', pa.timestamp('us', tz='UTC'))
        ])

        # 5. Convert DF ke PyArrow Table dengan Schema ketat
        table = pa.Table.from_pandas(df, schema=schema)

        # 6. Tulis ke Parquet Buffer
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)

        # 7. Upload Parquet ke GCS (Sesuai dengan path DTS Anda)
        date_folder = data_interval_end.in_timezone('Asia/Jakarta').strftime('%Y%m%d')
        filename = f"data_transaksi/export_{date_folder}/transaksi_export_{date_folder}.parquet"

        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=filename,
            data=parquet_buffer.getvalue(),
            mime_type='application/octet-stream'
        )

    # Inisialisasi Task
    extract_task = extract_mysql_to_gcs()

    # ---------------------------------------------------------
    # TASK 4 & 5: Trigger dan Wait DTS
    # ---------------------------------------------------------
    trigger_dts = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="trigger_dts",
        project_id=PROJECT_ID,
        transfer_config_id=DTS_CONFIG_ID,
        location='asia-southeast2',
        # MENGGUNAKAN data_interval_end AGAR SINKRON DENGAN {run_time+7h} DI DTS
        requested_run_time={"seconds": "{{ data_interval_end.int_timestamp }}"}, 
        gcp_conn_id=GCP_CONN_ID,
    )

    wait_for_dts = BigQueryDataTransferServiceTransferRunSensor(
        task_id="wait_for_dts",
        project_id=PROJECT_ID,
        transfer_config_id=DTS_CONFIG_ID,
        location = 'asia-southeast2',
        run_id="{{ task_instance.xcom_pull(task_ids='trigger_dts', key='return_value').name.split('/')[-1] }}",
        expected_statuses={"SUCCEEDED"},
        poke_interval=30,
        timeout=1200,
        gcp_conn_id=GCP_CONN_ID,
    )

    # ---------------------------------------------------------
    # TASK 6: Upsert (MERGE) Staging Iceberg to Native Final
    # ---------------------------------------------------------
    merge_sql = """
    {% set is_init = ti.xcom_pull(task_ids='check_initial_load')['is_initial'] %}
    {% set date_filter = "1=1" if is_init else "DATE(updated_at) = '" ~ ds ~ "'" %}

    MERGE `{{ params.project_id }}.{{ params.dataset_id }}.final_transaksi` T
    USING (
        SELECT *
        FROM `{{ params.project_id }}.{{ params.dataset_id }}.staging_iceberg_transaksi`
        WHERE {{ date_filter }}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY id_transaksi ORDER BY updated_at DESC) = 1
    ) S
    ON T.id_transaksi = S.id_transaksi
    WHEN MATCHED THEN UPDATE SET
        T.jumlah = S.jumlah,
        T.status = S.status,
        T.tanggal_transaksi = S.tanggal_transaksi,
        T.updated_at = S.updated_at,
        T.deleted_at = S.deleted_at
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id_transaksi, jumlah, status, tanggal_transaksi, created_at, updated_at, deleted_at)
        VALUES (S.id_transaksi, S.jumlah, S.status, S.tanggal_transaksi, S.created_at, S.updated_at, S.deleted_at)"""

    upsert_to_final = BigQueryInsertJobOperator(
        task_id='upsert_to_final',
        gcp_conn_id=GCP_CONN_ID,
        params={"project_id": PROJECT_ID, "dataset_id": DATASET_ID},
        configuration={
            "query": {
                "query": merge_sql,
                "useLegacySql": False,
            }
        },
    )

# ---------------------------------------------------------
    # DEFINE DAG DEPENDENCIES
    # ---------------------------------------------------------
    load_config >> extract_task >> trigger_dts >> wait_for_dts >> upsert_to_final