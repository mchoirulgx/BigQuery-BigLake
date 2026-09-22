# Stage 1: Concept — Why BigLake and Apache Iceberg?
## Part 1.3: Hands-On POC: Migrating Hive-Partitioned Parquet to Managed Iceberg

> **Section Overview:**  
> This hands-on Proof of Concept (POC) demonstrates migrating raw, unmanaged Hive-partitioned Parquet data from Google Cloud Storage into a fully transactional **BigLake Managed Table** utilizing the **Apache Iceberg** table format. You will verify ACID DML capabilities (`UPDATE`, `DELETE`, `SELECT`) directly in GCS without standing up a Spark cluster.

---

## Standalone Code Assets
* **SQL Scripts:** [`sql/02_ddl_poc_nyc_taxi_iceberg.sql`](../../sql/02_ddl_poc_nyc_taxi_iceberg.sql)

---

## Public Data Source Reference
For this POC, we use the public NYC Taxi dataset provided by Google:
* **Source URI:** `gs://biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab_2021/data/`
* **Directory Structure:** Parquet files organized by Hive Partitioning (`data_file_year=YYYY/data_file_month=MM`).

---

## Step-by-Step Implementation

### Step 1: Copy Public Data & Cleanse Corrupted Partitions
Copy the public dataset to your own GCS bucket using Cloud Shell, and remove any corrupted partition directories (`data_file_year=null`) to prevent BigQuery schema mapping errors.

*Run in Google Cloud Shell:*
```bash
# 1. Copy ALL partitioned data from the public bucket to your staging folder
gcloud storage cp -r gs://biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab_2021/data/* gs://<YOUR_BUCKET_NAME>/poc_raw_data/nyc_taxi/

# 2. Delete the corrupted 'null' partition folder to prevent BigQuery INT64 schema errors
gcloud storage rm -r gs://<YOUR_BUCKET_NAME>/poc_raw_data/nyc_taxi/data_file_year=null
```

---

### Step 2: Create the Staging Table (External / Unmanaged)
Create an External Table bridge so BigQuery can read raw Parquet files and interpret the Hive directory structure as columns (`data_file_year`, `data_file_month`).

*Run in BigQuery Studio:*
```sql
CREATE OR REPLACE EXTERNAL TABLE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.staging_nyc_taxi_raw`
WITH PARTITION COLUMNS (
    data_file_year INT64,
    data_file_month INT64
)
WITH CONNECTION `<YOUR_REGION>.<YOUR_CONNECTION_ID>`
OPTIONS (
    format = 'PARQUET',
    hive_partition_uri_prefix = 'gs://<YOUR_BUCKET_NAME>/poc_raw_data/nyc_taxi/',
    uris = ['gs://<YOUR_BUCKET_NAME>/poc_raw_data/nyc_taxi/*']
);
```

---

### Step 3: Materialize into a BigLake Managed Iceberg Table
Instruct BigQuery to read from the staging table and write an Apache Iceberg table format (data + metadata manifests) to a new, empty GCS destination. BigQuery takes ownership of this location as the table catalog.

*Run in BigQuery Studio:*
```sql
CREATE OR REPLACE TABLE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.managed_nyc_taxi_iceberg`
WITH CONNECTION `<YOUR_REGION>.<YOUR_CONNECTION_ID>`
OPTIONS (
    file_format = 'PARQUET',
    table_format = 'ICEBERG',
    storage_uri = 'gs://<YOUR_BUCKET_NAME>/managed_nyc_taxi_iceberg/'
) AS 
SELECT * FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.staging_nyc_taxi_raw`;
```

---

### Step 4: Validate ACID DML Capabilities
Once Step 3 completes, standard SQL CRUD operations can be executed against the Iceberg table. BigQuery will update metadata manifests and generate new Parquet files seamlessly in GCS.

#### 1. Aggregate Query (Read OLAP):
```sql
SELECT 
    passenger_count, 
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.managed_nyc_taxi_iceberg`
WHERE data_file_year = 2021 AND data_file_month = 2
GROUP BY passenger_count
ORDER BY total_trips DESC;
```

#### 2. Row-Level UPDATE:
```sql
-- Iceberg allows direct row modification without rewriting untouched partitions
UPDATE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.managed_nyc_taxi_iceberg`
SET passenger_count = 1
WHERE passenger_count IS NULL 
  AND data_file_year = 2021;
```

#### 3. Row-Level DELETE:
```sql
-- Safely purge negative fare errors natively
DELETE FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.managed_nyc_taxi_iceberg`
WHERE total_amount < 0;
```

---

## Next Steps in Stage 1:
Now that the BigLake connection is active and IAM permissions are configured, test it with Google Cloud's public NYC Taxi dataset:  
**[04_time_travel_and_fail-safe.md](04_time_travel_and_fail-safe.md)**


## Conclusion of Stage 1
You have validated:
1. The structural differences between Parquet, Hive, and Iceberg.
2. The security model and Cloud Resource connection delegation in BigLake.
3. Converting unmanaged Hive Parquet files into a fully transactional BigLake Managed Iceberg table.
4. Understanding how BigLake Managed Iceberg table file is used and processed.

Proceed to **Stage 2: Simple Ingestion**:  
**[../02_simple_ingestion/04_simple_parquet_external_table.md](../02_simple_ingestion/04_simple_parquet_external_table.md)**
