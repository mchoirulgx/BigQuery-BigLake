import pandas as pd
from google.cloud import storage
from datetime import date, datetime
import os

# Configuration
BUCKET_NAME = "biglake_data"
FILE_NAME = "sample_data.parquet"
GCS_DESTINATION_PATH = f"dts_landing_zone_test_modified_table/{FILE_NAME}"

# Sample data
data = [
    {"order_id": 15, "customer_id": 115, "status": "shipped", "volume" : 2.3, "order_date": date(2026, 9, 16),"created_at": datetime(2026, 9, 16, 14, 30, 0)},
    {"order_id": 16, "customer_id": 116, "status": "pending", "volume" : 3.8, "order_date": date(2026, 9, 16),"created_at": datetime(2026, 9, 16, 14, 30, 0)}
]

# Convert to DataFrame and save as Parquet locally
df = pd.DataFrame(data)
df.to_parquet(FILE_NAME, engine="pyarrow", index=False)

# Upload to GCS
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
blob = bucket.blob(GCS_DESTINATION_PATH)
blob.upload_from_filename(FILE_NAME)

print(f"Successfully uploaded {GCS_DESTINATION_PATH} to gs://{BUCKET_NAME}/")

# Clean up local file
os.remove(FILE_NAME)