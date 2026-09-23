-- ============================================================================
-- SCRIPT: 02_ddl_poc_nyc_taxi_iceberg.sql
-- Stage: Stage 1 - Concept: Why BigLake and Iceberg?
-- File: sql/02_ddl_poc_nyc_taxi_iceberg.sql
--
-- PURPOSE:
--   Proof of Concept demonstrating migration from public NYC Taxi Hive-partitioned
--   Parquet data to a BigLake Managed Iceberg table, followed by DML validation.
--
-- WHAT TO DO:
--   1. Replace placeholders:
--      - `{project_id}`: Your Google Cloud Project ID.
--      - `{dataset_id}`: Your BigQuery Dataset ID.
--      - `{region}`: GCP region (e.g., `us-central1`, `asia-southeast2`).
--      - `{bucket_name}`: Your Cloud Storage Bucket.
--      - `{connection_id}`: Your BigLake Cloud Resource connection ID.
--   2. In Step 1, copy sample data to your GCS bucket:
--      gcloud storage cp -r gs://biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab_2021/data/* gs://{bucket_name}/poc_raw_data/nyc_taxi/
--   3. Execute the statements in sequence in BigQuery Studio.
--
-- KEY PATTERN:
--   Converts Hive-partitioned Parquet files into an Apache Iceberg table managed
--   by BigQuery, enabling ACID transactions and full DML (INSERT, UPDATE, DELETE).
-- ============================================================================

-- Step 2: Create External Table over Hive-Partitioned GCS Parquet Files
-- (Note: Wildcard is 'gs://{bucket_name}/poc_raw_data/nyc_taxi/*' to cover all partitions)
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.tlc_yellow_trips_2022_external`
WITH PARTITION COLUMNS (
    data_file_year INT64,
    data_file_month INT64
)
OPTIONS (
    format = 'PARQUET',
    hive_partition_uri_prefix = 'gs://{bucket_name}/poc_raw_data/nyc_taxi/',
    uris = ['gs://{bucket_name}/poc_raw_data/nyc_taxi/*']
);

-- Step 3: Create BigLake Managed Iceberg Table from External Table via CTAS
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.tlc_yellow_trips_2022_managed`
WITH CONNECTION `{project_id}.{region}.{connection_id}`
PARTITION BY RANGE_BUCKET(data_file_month, GENERATE_ARRAY(1, 12, 1))
OPTIONS (
    file_format = 'PARQUET',
    table_format = 'ICEBERG'
) AS
SELECT * 
FROM `{project_id}.{dataset_id}.tlc_yellow_trips_2022_external`;

-- Step 4: DML Validation Queries

-- Query partitioned data with predicate pushdown
SELECT 
    vendor_id, 
    pickup_datetime, 
    passenger_count, 
    trip_distance, 
    total_amount
FROM `{project_id}.{dataset_id}.tlc_yellow_trips_2022_managed`
WHERE data_file_year = 2022 AND data_file_month = 2
LIMIT 10;

-- Test INSERT into Iceberg table
INSERT INTO `{project_id}.{dataset_id}.tlc_yellow_trips_2022_managed` 
(vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, total_amount, data_file_year, data_file_month)
VALUES 
(1, CURRENT_DATETIME(), CURRENT_DATETIME(), 2, 5.4, 25.0, 2022, 2);

-- Test UPDATE on Iceberg table
UPDATE `{project_id}.{dataset_id}.tlc_yellow_trips_2022_managed`
SET total_amount = total_amount + 5.0
WHERE data_file_year = 2022 AND data_file_month = 2 AND vendor_id = 1;

-- Test DELETE on Iceberg table
DELETE FROM `{project_id}.{dataset_id}.tlc_yellow_trips_2022_managed`
WHERE data_file_year = 2022 AND data_file_month = 2 AND total_amount > 1000;
