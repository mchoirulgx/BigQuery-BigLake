from datetime import timedelta
import pendulum
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.email import EmailOperator

# ==========================================
# KONFIGURASI
# ==========================================
PROJECT_ID = 'imposing-league-374504'
GCS_BUCKET = 'biglake_data'
GCS_DATA_FOLDER = 'dummy_iceberg'
BQ_DATASET_ID = 'bqlake_data'
BQ_MANAGED_TABLE = 'managed_users_dummy '

EMAIL_RECIPIENT = 'hasan.muttaqien@gmail.com'
jkt_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'Hassan',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Membuat DAG terpisah
with DAG(
    'bq_delete_200k_oldest_rows_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *', # Berjalan setiap jam 2 pagi
    start_date=pendulum.datetime(2026, 8, 25, tz="Asia/Jakarta"),
    catchup=False,
) as dag:

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
    # PYTHON CALLABLES (METRICS & EMAIL)
    # ==========================================
    def pre_check_metrics(**kwargs):
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
        <h2 style='color: #2e6c80;'>BigQuery Cleanup Pipeline Report (200k Rows)</h2>
        
        <h3>1. GCS & BQ Metrics (Before Deletion)</h3>
        {render_gcs_html_table(pre_data['gcs'])}
        <br>
        <table style='border-collapse: collapse; width: 60%; font-family: Arial;'>
            <tr>
                <td style='border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2;'><b>Row Count (Before Delete)</b></td>
                <td style='border: 1px solid #ddd; padding: 8px;'>{pre_data['bq_count']}</td>
            </tr>
        </table>
        
        <hr style="border: 1px solid #eee; margin: 20px 0;">
        
        <h3>2. GCS & BQ Metrics (After Deletion)</h3>
        {render_gcs_html_table(post_data['gcs'])}
        <br>
        <table style='border-collapse: collapse; width: 60%; font-family: Arial;'>
            <tr>
                <td style='border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2;'><b>Row Count (After Delete 200,000 Oldest)</b></td>
                <td style='border: 1px solid #ddd; padding: 8px;'>{post_data['bq_count']}</td>
            </tr>
        </table>
        
        <br>
        <p><i>Report generated automatically by Apache Airflow.</i></p>
        """
        return html_content

    # ==========================================
    # DEFINISI TASKS & OPERATORS
    # ==========================================
    task_pre_check = PythonOperator(
        task_id='task_pre_check',
        python_callable=pre_check_metrics,
    )

    # Menghapus persis 200.000 data terlama menggunakan pendekatan TO_JSON_STRING
    delete_query = f"""
        DELETE FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_MANAGED_TABLE}` t
        WHERE TO_JSON_STRING(t) IN (
            SELECT TO_JSON_STRING(t2)
            FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_MANAGED_TABLE}` t2
            ORDER BY t2.created_at ASC
            LIMIT 200000
        )
    """
    
    task_delete_200k_oldest = BigQueryInsertJobOperator(
        task_id='delete_200k_oldest_rows',
        configuration={"query": {"query": delete_query, "useLegacySql": False}},
        gcp_conn_id='google_cloud_default',
        location='asia-southeast2',
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
        subject='BQ Cleanup Report (200k Rows) - {{ ds }}',
        html_content="{{ ti.xcom_pull(task_ids='prepare_email_content') }}",
        conn_id='smtp_default'
    )

    # ==========================================
    # URUTAN EKSEKUSI (PIPELINE)
    # ==========================================
    (
        task_pre_check >> 
        task_delete_200k_oldest >> 
        task_post_check >>
        task_prepare_email >>
        task_send_email
    )