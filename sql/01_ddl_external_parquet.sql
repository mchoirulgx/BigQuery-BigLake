-- ============================================================================
-- SCRIPT: 01_ddl_external_parquet.sql
-- Stage: Stage 2 - Simple Ingestion
-- File: sql/01_ddl_external_parquet.sql
--
-- PURPOSE:
--   Defines an External Table over raw Parquet files in Cloud Storage (GCS)
--   and demonstrates materializing it into a Native BigQuery table via CTAS.
--
-- WHAT TO DO:
--   1. Replace placeholders:
--      - `{project_id}`: Your Google Cloud Project ID.
--      - `{dataset_id}`: Your BigQuery Dataset ID.
--      - `{bucket_name}`: Your Cloud Storage Bucket containing Parquet files.
--   2. Ensure the Parquet files exist in `gs://{bucket_name}/data_transaksi/*.parquet`.
--   3. Execute in the BigQuery Studio Console or via `bq query`.
--
-- KEY PATTERN:
--   External tables over raw Parquet are read-only and immutable. To enable
--   DML (UPDATE/DELETE/MERGE), data must be materialized into BigLake Iceberg
--   or Native BigQuery tables.
-- ============================================================================

-- Step 1: Create BigQuery External Table pointing to GCS Parquet files
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.staging_data_transaksi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{bucket_name}/data_transaksi/*.parquet']
);

-- Step 2: Query the external table directly
SELECT * 
FROM `{project_id}.{dataset_id}.staging_data_transaksi` 
LIMIT 10;

-- Step 3: Materialize into a native BigQuery table for high-performance analytics
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.final_data_transaksi` AS
SELECT * 
FROM `{project_id}.{dataset_id}.staging_data_transaksi`;
