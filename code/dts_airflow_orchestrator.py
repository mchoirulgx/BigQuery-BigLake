from datetime import timedelta, datetime
import pendulum
import pandas as pd
import time
import logging
import uuid
import random
from faker import Faker
from io import BytesIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryDataTransferServiceStartTransferRunsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.email import EmailOperator

PROJECT_ID = 'imposing-league-374504'
GCS_BUCKET = 'biglake_data'
GCS_LANDING_FOLDER = 'dts_landing_zone/'
GCS_DATA_FOLDER = 'dts_iceberg_managed'
BQ_DATASET_ID = 'bqlake_data'
BQ_MANAGED_TABLE = 'dts_managed_users'

# Resource name untuk masing-masing DTS config di GCP Console
DTS_PARQUET_CONFIG_NAME = '6a8ff824-0000-28e9-86dd-fc41166cee39'
DTS_MERGE_CONFIG_NAME = '6a910ab3-0000-22bd-9513-9898fbb54bdd'

EMAIL_RECIPIENT = 'hasan.muttaqien@gmail.com'
jkt_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'Hassan',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'mysql_to_gcs_parquet_and_dts_pipeline_v3',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2026, 8, 25, tz="Asia/Jakarta"),
    catchup=False,
) as dag:

    past_run_time = {
        "seconds": "{{ (macros.datetime.utcnow() - macros.timedelta(seconds=10)).timestamp() | int }}"
    }

    common_config = {
        "gcp_conn_id": "google_cloud_default",
        "location": "asia-southeast2",
        "execution_timeout": timedelta(minutes=60),
        "requested_run_time": past_run_time,
        "project_id": "imposing-league-374504"
    }

    # ==========================================
    # HELPER FUNCTIONS UNTUK METRICS
    # ==========================================
    def get_bq_row_count():
        bq_hook = BigQueryHook(
            gcp_conn_id='google_cloud_default', 
            use_legacy_sql=False,
            location='asia-southeast2'
        )
        query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_MANAGED_TABLE}`"
        try:
            records = bq_hook.get_records(query)
            return f"{records[0][0]} rows"
        except Exception as e:
            logging.error(f"BQ Count Error: {e}")
            return f"Error: {e}" 

    def get_gcs_metrics():
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        client = gcs_hook.get_conn()
        bucket = client.bucket(GCS_BUCKET)
        blobs = bucket.list_blobs(prefix=GCS_DATA_FOLDER)
        
        total_size = sum(blob.size for blob in blobs if blob.name.endswith('.parquet'))
        return {'total_size_kb': round(total_size / 1024, 2)}

    # ==========================================
    # PYTHON CALLABLES (GENERATE data & METRICS)
    # ==========================================
    def generate_mysql_data(**kwargs):
        fake = Faker('id_ID')
        
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        target_date = datetime.now() - timedelta(days=1)
        timestamp_str = target_date.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"--- Memulai Simulasi Data untuk Tanggal: {timestamp_str} ---")

        # 1. INSERT 1000 DATA BARU
        insert_query = """
            INSERT INTO users (id, name, email, created_at, updated_at, deleted_at)
            VALUES (%s, %s, %s, %s, %s, NULL)
        """
        new_users = []
        for _ in range(1000):
            new_users.append((
                str(uuid.uuid4()),
                fake.name(),
                fake.email(),
                timestamp_str,
                timestamp_str
            ))
        
        cursor.executemany(insert_query, new_users)
        conn.commit()
        print("✅ Berhasil Insert 1000 data baru.")

        # 2. AMBIL DATA EKSISTING UNTUK UPDATE & DELETE
        cursor.execute("SELECT id FROM users WHERE deleted_at IS NULL ORDER BY created_at ASC")
        all_active_ids = [row[0] for row in cursor.fetchall()]
        
        if len(all_active_ids) > 0:
            delete_ids = all_active_ids[:100]
            remaining_ids = all_active_ids[len(delete_ids):]
            
            if len(remaining_ids) >= 200:
                update_ids = random.sample(remaining_ids, 200)
            else:
                update_ids = remaining_ids

            # 3. UPDATE 200 DATA
            if update_ids:
                update_query = """
                    UPDATE users 
                    SET name = %s, email = %s, updated_at = %s 
                    WHERE id = %s
                """
                update_data = []
                for uid in update_ids:
                    update_data.append((
                        fake.name() + " (Updated)", 
                        fake.email(), 
                        timestamp_str, 
                        uid
                    ))
                cursor.executemany(update_query, update_data)
                conn.commit()
                print(f"✅ Berhasil Update {len(update_ids)} data eksisting.")

            # 4. SOFT DELETE 100 DATA TERLAMA
            if delete_ids:
                delete_query = """
                    UPDATE users 
                    SET deleted_at = %s, updated_at = %s 
                    WHERE id = %s
                """
                delete_data = [(timestamp_str, timestamp_str, uid) for uid in delete_ids]
                cursor.executemany(delete_query, delete_data)
                conn.commit()
                print(f"✅ Berhasil Soft Delete {len(delete_ids)} data terlama eksisting.")
        else:
            print("⚠️ Data eksisting kosong. Update & Delete dilewati.")

        cursor.close()
        conn.close()
        print("--- Simulasi Selesai! ---")

    def wait_30_seconds(**kwargs):
        print("Menunggu 30 detik untuk memastikan proses asinkron BQ DTS selesai...")
        time.sleep(30)

    def pre_check_metrics(**kwargs):
        return {
            'gcs': get_gcs_metrics(),
            'bq_count': get_bq_row_count()
        }
        
    def check_after_insert_metrics(**kwargs):
        return {
            'gcs': get_gcs_metrics(),
            'bq_count': get_bq_row_count()
        }
        
    def post_check_metrics(**kwargs):
        return {
            'gcs': get_gcs_metrics(),
            'bq_count': get_bq_row_count()
        }

    def generate_email_content(**kwargs):
        ti = kwargs['ti']
        pre_data = ti.xcom_pull(task_ids='task_pre_check')
        after_insert_data = ti.xcom_pull(task_ids='task_check_after_insert')
        post_data = ti.xcom_pull(task_ids='task_post_check')
        
        def render_gcs_html_table(gcs_data):
            return f"""
            <table style='border-collapse: collapse; width: 60%; font-family: Arial;'>
                <tr>
                    <td style='border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2;'><b>Total Parquet Size</b></td>
                    <td style='border: 1px solid #ddd; padding: 8px; font-weight: bold;'>{gcs_data['total_size_kb']} KB</td>
                </tr>
            </table>
            """

        html_content = f"""
        <h2 style='color: #2e6c80;'>Data Lakehouse Pipeline Report</h2>
        
        <h3>1. GCS Storage (Before Pipeline)</h3>
        {render_gcs_html_table(pre_data['gcs'])}
        
        <br>
        <table style='border-collapse: collapse; width: 60%; font-family: Arial;'>
            <tr>
                <td style='border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2;'><b>2. Row Count (Before Insert)</b></td>
                <td style='border: 1px solid #ddd; padding: 8px;'>{pre_data['bq_count']}</td>
            </tr>
        </table>
        
        <hr style="border: 1px solid #eee; margin: 20px 0;">
        
        <h3>3. GCS Storage (After Insert/Merge)</h3>
        {render_gcs_html_table(after_insert_data['gcs'])}
        
        <br>
        <table style='border-collapse: collapse; width: 60%; font-family: Arial;'>
            <tr>
                <td style='border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2;'><b>4. Row Count (After Insert/Merge)</b></td>
                <td style='border: 1px solid #ddd; padding: 8px;'>{after_insert_data['bq_count']}</td>
            </tr>
        </table>

        <hr style="border: 1px solid #eee; margin: 20px 0;">
        
        <h3>5. GCS Storage (After Delete)</h3>
        {render_gcs_html_table(post_data['gcs'])}
        
        <br>
        <table style='border-collapse: collapse; width: 60%; font-family: Arial;'>
            <tr>
                <td style='border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2;'><b>6. Row Count (After Delete)</b></td>
                <td style='border: 1px solid #ddd; padding: 8px;'>{post_data['bq_count']}</td>
            </tr>
        </table>
        
        <br>
        <p><i>Report generated automatically by Apache Airflow.</i></p>
        """
        return html_content

    # ==========================================
    # EKSTRAKSI MYSQL TO PARQUET
    # ==========================================
    def mysql_to_parquet_gcs(**kwargs):
        execution_date = kwargs.get('data_interval_end').in_timezone(jkt_tz)
        yesterday = execution_date.subtract(days=1)
        
        start_win = yesterday.start_of('day').to_datetime_string()
        end_win = yesterday.end_of('day').to_datetime_string()
        
        bq_count_str = get_bq_row_count()
        is_empty = True if "0" in bq_count_str or "Error" in bq_count_str else False
        
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
            return "skipped"
            
        for col in ['created_at', 'updated_at', 'deleted_at']:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
        parquet_buffer = BytesIO()
        df.to_parquet(
            parquet_buffer, 
            engine='pyarrow', 
            index=False,
            coerce_timestamps='us',
            allow_truncated_timestamps=True
        )
        
        filename = f"{GCS_LANDING_FOLDER}/users_{filename_suffix}.parquet"
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=filename,
            data=parquet_buffer.getvalue(),
            mime_type='application/octet-stream'
        )

    # ==========================================
    # DEFINISI TASKS & OPERATORS
    # ==========================================
    
    task_generate_data = PythonOperator(
        task_id='generate_mysql_data',
        python_callable=generate_mysql_data,
    )

    task_pre_check = PythonOperator(
        task_id='task_pre_check',
        python_callable=pre_check_metrics,
    )

    task_extract_upload = PythonOperator(
        task_id='extract_and_upload_parquet',
        python_callable=mysql_to_parquet_gcs,
    )

    task_run_dts_parquet = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id='run_dts_parquet_to_managed_table',
        transfer_config_id=DTS_PARQUET_CONFIG_NAME,
        **common_config
    )

    task_wait_after_parquet = PythonOperator(
        task_id='wait_30s_after_parquet',
        python_callable=wait_30_seconds,
    )

    task_run_dts_merge = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id='run_dts_scheduled_query_merge',
        transfer_config_id=DTS_MERGE_CONFIG_NAME,
        **common_config
    )
    
    task_wait_after_merge = PythonOperator(
        task_id='wait_30s_after_merge',
        python_callable=wait_30_seconds,
    )
    
    task_check_after_insert = PythonOperator(
        task_id='task_check_after_insert',
        python_callable=check_after_insert_metrics,
    )

    # Catatan Perubahan: 
    # Menggunakan fungsi TO_JSON_STRING(t) untuk mencocokkan KESELURUHAN baris.
    # Cara ini mengabaikan `id` dan mencegah BigQuery menghapus lebih dari 100 baris 
    delete_query = f"""
        DELETE FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_MANAGED_TABLE}` t
        WHERE TO_JSON_STRING(t) IN (
            SELECT TO_JSON_STRING(t2)
            FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_MANAGED_TABLE}` t2
            ORDER BY t2.created_at ASC
            LIMIT 100
        )
    """
    task_delete_oldest = BigQueryInsertJobOperator(
        task_id='delete_100_oldest',
        configuration={"query": {"query": delete_query, "useLegacySql": False}},
        gcp_conn_id='google_cloud_default',
    )
    
    task_post_check = PythonOperator(
        task_id='task_post_check',
        python_callable=post_check_metrics,
    )
    
    task_prepare_email = PythonOperator(
        task_id='prepare_email_content',
        python_callable=generate_email_content,
    )

    task_send_email = EmailOperator(
        task_id='send_email_report',
        to=EMAIL_RECIPIENT,
        subject='BigLake Migration Report - {{ ds }}',
        html_content="{{ ti.xcom_pull(task_ids='prepare_email_content') }}",
        conn_id='smtp_default'
    )

    # ==========================================
    # URUTAN EKSEKUSI (PIPELINE)
    # ==========================================
    (
        task_generate_data >> 
        task_pre_check >> 
        task_extract_upload >> 
        task_run_dts_parquet >> 
        task_wait_after_parquet >>   
        task_run_dts_merge >> 
        task_wait_after_merge >>     
        task_check_after_insert >>
        task_delete_oldest >> 
        task_post_check >>
        task_prepare_email >>
        task_send_email
    )