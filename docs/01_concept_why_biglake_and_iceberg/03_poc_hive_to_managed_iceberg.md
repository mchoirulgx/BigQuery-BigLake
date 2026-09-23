# Stage 1: Concept — Why BigLake and Apache Iceberg?
## Part 1.3: Hands-On POC: Migrating Hive-Partitioned Parquet to Managed Iceberg

> **Section Overview:**  
> This hands-on POC demonstrates migrating raw, unmanaged Hive-partitioned Parquet data from Google Cloud Storage into a fully transactional **BigLake Managed Table** in BigQuery, utilizing the **Apache Iceberg** table format. You will verify ACID DML capabilities (`UPDATE`, `DELETE`, `SELECT`) directly in GCS without standing up a Spark cluster.
>
> Intendend scenario:
> - You have existing data in Hive structure, and wanted to migrate to BigQuery as soon as possible without ingesting into BigQuery's native table.
> - You have existing data in relational DBs or CSV, and wanted to migrate to BigQuery as modern Lakehouse instead of native table.

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
Create an External Table bridge so BigQuery can read raw Parquet files and interpret the Hive directory structure as columns (`data_file_year`, `data_file_month`) [1](https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs).

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
Instruct BigQuery to read from the staging table and write an Apache Iceberg table format (data + metadata manifests) to a new, empty GCS destination. BigQuery takes ownership of this location as the table catalog [2](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#create-tables).

*Run in BigQuery Studio:*
```sql
CREATE OR REPLACE TABLE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.managed_nyc_taxi_iceberg`
WITH CONNECTION `<YOUR_REGION>.<YOUR_CONNECTION_ID>`
PARTITION BY RANGE_BUCKET(data_file_month, GENERATE_ARRAY(1, 12, 1))
OPTIONS (
    file_format = 'PARQUET',
    table_format = 'ICEBERG',
    storage_uri = 'gs://<YOUR_BUCKET_NAME>/managed_nyc_taxi_iceberg/'
) AS 
SELECT * FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.staging_nyc_taxi_raw`;
```
*(Integer range partitioning prunes partition scans during analytics queries) [3](https://cloud.google.com/bigquery/docs/creating-integer-range-partitions).*

---

### Step 4: Validate ACID DML Capabilities
Once Step 3 completes, standard SQL CRUD operations can be executed against the Iceberg table. BigQuery will update metadata manifests and generate new Parquet files seamlessly in GCS [4](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#dml).

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
-- Iceberg allows direct row modification without rewriting untouched partitions [5](https://iceberg.apache.org/spec/#row-level-delete-schemas)
UPDATE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.managed_nyc_taxi_iceberg`
SET passenger_count = 1
WHERE passenger_count IS NULL 
  AND data_file_year = 2021;
```

#### 3. Row-Level DELETE:
```sql
-- Safely purge negative fare errors natively [4](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#dml)
DELETE FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.managed_nyc_taxi_iceberg`
WHERE total_amount < 0;
```

---

## Next Steps in Stage 1:
Now that the BigLake connection is active and IAM permissions are configured, test it with Google Cloud's public NYC Taxi dataset:  
**[04_time_travel_and_fail-safe.md](04_time_travel_and_fail-safe.md)**

---

## References

1. [Google Cloud BigQuery: Hive partitioned queries over Cloud Storage](https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs)
2. [Google Cloud BigQuery: Create Apache Iceberg managed tables](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#create-tables)
3. [Google Cloud BigQuery: Integer range partitioned tables](https://cloud.google.com/bigquery/docs/creating-integer-range-partitions)
4. [Google Cloud BigQuery: DML operations on BigLake Iceberg tables](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#dml)
5. [Apache Iceberg: Row-Level Mutations & Position Deletes](https://iceberg.apache.org/spec/#row-level-delete-schemas)
