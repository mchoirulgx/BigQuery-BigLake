import logging
from google.cloud import bigquery
from google.cloud import storage

logging.basicConfig(level=logging.INFO)

PROJECT_ID = "imposing-league-374504"
BQ_DATASET_ID = "bqlake_data"
BQ_MANAGED_TABLE = "managed_users_dummy"
GCS_BUCKET = "biglake_data"
GCS_DATA_FOLDER = "dummy_iceberg"
LOCATION = "asia-southeast2"

# PROJECT_ID = 'imposing-league-374504'
# GCS_BUCKET = 'biglake_data'
# GCS_DATA_FOLDER = 'dummy_iceberg'
# BQ_DATASET_ID = 'bqlake_data'
# BQ_MANAGED_TABLE = 'managed_users_dummy'

def get_bq_row_count():
    bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_MANAGED_TABLE}`"
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        row = list(results)[0]
        return f"{row[0]} rows"
    except Exception as e:
        logging.error(f"BQ Count Error: {e}")
        return f"Error: {e}"


def get_gcs_metrics():
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=GCS_DATA_FOLDER)
    print(storage_client, bucket, blobs)
    total_size = sum(blob.size for blob in blobs if blob.name.endswith('.parquet'))
    return {'total_size_kb': round(total_size / 1024, 2)}


if __name__ == "__main__":
    print(get_bq_row_count())
    print(get_gcs_metrics())