# BigLake Data Lakehouse POC: Hive-Partitioned Parquet to Managed Iceberg

This document outlines the Proof of Concept (POC) for migrating raw, Hive-partitioned Parquet data stored in Google Cloud Storage (GCS) into a fully transactional **BigLake Managed Table** utilizing the **Apache Iceberg** metadata format.

## Public Data Source Reference
For this POC, we are using the public NYC Taxi dataset provided by Google.
*   **Source URI:** `gs://biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab_2021/data/`
*   **Structure:** Parquet files organized using Hive Partitioning (`data_file_year=YYYY/data_file_month=MM`).

---

## Step-by-Step Implementation

### Step 1: Copy All Public Data and Cleanse Corrupted Partitions
To simulate a real-world scenario with massive data volumes, copy the entire public dataset into your own GCS bucket using Cloud Shell, and then immediately remove any corrupted partitions to prevent schema mapping errors.

*Run these commands sequentially in Google Cloud Shell:*
```bash
# 1. Copy ALL partitioned data from the public bucket to your staging folder
gcloud storage cp -r gs://biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab_2021/data/* gs://{bucket_name}/poc_raw_data/nyc_taxi/

# 2. Delete the corrupted 'null' partition folder to prevent BigQuery INT64 schema errors
gcloud storage rm -r gs://{bucket_name}/poc_raw_data/nyc_taxi/data_file_year=null
```

### Step 2: Create the Staging Table (External / Unmanaged)
We must first create a read-only bridge (External Table) so BigQuery understands how to read the raw Parquet files and interpret the Hive directory structure as columns.

*Run this DDL in BigQuery:*
```sql
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset}.{table}`
WITH PARTITION COLUMNS (
    data_file_year INT64,
    data_file_month INT64
)
WITH CONNECTION `asia-southeast2.biglake-data-connection`
OPTIONS (
    format = 'PARQUET',
    hive_partition_uri_prefix = 'gs://{bucket_name}/poc_raw_data/nyc_taxi/',
    -- Using a wildcard to target the year 2000 folder and bypass null directories
    uris = ['gs://{bucket_name}/poc_raw_data/nyc_taxi/data_file_year=2000/*']
);
```

### Step 3: Materialize into a BigLake Managed Iceberg Table
Instruct BigQuery to read from the staging table and write a fully managed Apache Iceberg structure (data + metadata) to a new, empty GCS destination. BigQuery will now take full ownership of this new location.

*Run this DDL in BigQuery:*
```sql
CREATE OR REPLACE TABLE `{project_id}.{dataset}.managed_nyc_taxi_iceberg`
WITH CONNECTION `asia-southeast2.biglake-data-connection`
OPTIONS (
    file_format = 'PARQUET',
    table_format = 'ICEBERG',
    storage_uri = 'gs://{bucket_name}/managed_nyc_taxi_iceberg/'
) AS 
SELECT * FROM `{project_id}.{dataset}.{table}`;
```

### Step 4: Validate DML Capabilities
Once Step 3 completes successfully, the table is officially a BigLake Managed Table. You can now execute standard SQL CRUD operations natively against the Iceberg table, which will update the metadata seamlessly in GCS.

*Testing a SELECT operation (Read & Aggregate):*
```sql
-- Example: Querying and aggregating data from the managed Iceberg table
SELECT 
    passenger_count, 
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue
FROM `{project_id}.{dataset}.managed_nyc_taxi_iceberg`
WHERE data_file_year = 2022 AND data_file_month = 2
GROUP BY passenger_count
ORDER BY total_trips DESC;
```

*Testing an UPDATE operation:*
```sql
-- Example: Updating records natively within the Iceberg format
UPDATE `{project_id}.{dataset}.managed_nyc_taxi_iceberg`
SET passenger_count = 1
WHERE passenger_count IS NULL 
  AND data_file_year = 2022;
```

*Testing a DELETE operation:*
```sql
-- Example: Deleting specific records natively
DELETE FROM `{project_id}.{dataset}.managed_nyc_taxi_iceberg`
WHERE total_amount < 0;
```