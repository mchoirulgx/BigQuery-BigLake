#!/usr/bin/env python3
"""
===============================================================================
SCRIPT: simulate_mysql_producer.py
Stage: Stage 5 - Schema Evolution Workaround
File: scripts/simulate_mysql_producer.py

PURPOSE:
  Simulates a source data producer generating Parquet files in Google Cloud
  Storage (GCS). It demonstrates schema drift by introducing new columns
  (`order_date` as DATE, `created_at` as TIMESTAMP, `item_count` as INT64)
  to test whether Airflow's pre-flight schema evolution automatically updates
  the BigLake Managed Iceberg table schema before DTS triggers.

WHAT TO DO:
  1. Set your configuration:
     - Update BUCKET_NAME and GCS_DESTINATION_PATH.
     - Ensure you are authenticated with GCP (`gcloud auth application-default login`).
  2. Run the script:
     python3 scripts/simulate_mysql_producer.py
  3. Verify in GCS:
     gcloud storage ls gs://<YOUR_BUCKET_NAME>/<YOUR_GCS_PREFIX>/
  4. Trigger the schema evolution DAG (`dag_10_schema_evolution_iceberg.py`) in
     Airflow to observe automatic column addition in BigQuery.
===============================================================================
"""

import os
from datetime import date, datetime
import pandas as pd
from google.cloud import storage

# --- Configuration ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "<YOUR_PROJECT_ID>")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "<YOUR_BUCKET_NAME>")
GCS_PREFIX = os.getenv("GCS_PREFIX", "source_parquet_data")
FILE_NAME = "sample_orders_drift.parquet"
GCS_DESTINATION_PATH = f"{GCS_PREFIX}/{FILE_NAME}"

def generate_and_upload():
    # Mock data containing new columns with explicit data types
    data = [
        {
            "order_id": 101,
            "customer_id": 501,
            "status": "completed",
            # New Column 1: DATE type
            "order_date": date(2026, 9, 16),
            # New Column 2: TIMESTAMP type
            "created_at": datetime(2026, 9, 16, 14, 30, 0),
            # New Column 3: INT64 type
            "item_count": 3
        },
        {
            "order_id": 102,
            "customer_id": 502,
            "status": "pending",
            "order_date": date(2026, 9, 17),
            "created_at": datetime(2026, 9, 17, 9, 15, 0),
            "item_count": 5
        }
    ]

    # Create DataFrame
    df = pd.DataFrame(data)

    # Ensure explicit datetime casting to avoid falling back to string types
    df['order_date'] = pd.to_datetime(df['order_date']).dt.date
    df['created_at'] = pd.to_datetime(df['created_at'])

    # Save to local temporary Parquet file
    df.to_parquet(FILE_NAME, engine="pyarrow", index=False)

    # Upload to Cloud Storage
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(GCS_DESTINATION_PATH)
    blob.upload_from_filename(FILE_NAME)

    print(f"Successfully uploaded: gs://{BUCKET_NAME}/{GCS_DESTINATION_PATH}")
    os.remove(FILE_NAME)

if __name__ == "__main__":
    generate_and_upload()
