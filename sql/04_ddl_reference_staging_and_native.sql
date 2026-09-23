-- ============================================================================
-- SCRIPT: 04_ddl_reference_staging_and_native.sql
-- Stage: Stage 4 - Incremental Loading (Reference Pipeline)
-- File: sql/04_ddl_reference_staging_and_native.sql
--
-- PURPOSE:
--   Defines the production-grade table schemas for the canonical transaction
--   pipeline demo:
--   1. `staging_iceberg_transaksi`: BigLake Managed Iceberg staging table.
--   2. `final_transaksi`: Partitioned and clustered BigQuery native serving table.
--
-- WHAT TO DO:
--   1. Replace placeholders:
--      - `{project_id}`: Your Google Cloud Project ID.
--      - `{dataset_id}`: Your BigQuery Dataset ID.
--      - `{region}`: GCP region (e.g., `asia-southeast2`).
--      - `{connection_id}`: Your BigLake connection ID.
--   2. Execute DDL statements in BigQuery Studio prior to running DTS.
--
-- KEY PATTERN:
--   Both tables are partitioned by business date (`tanggal_transaksi`) and
--   clustered by `(status, id_transaksi)` to optimize query performance and
--   cost for downstream analytics.
-- ============================================================================

-- Step 1: Create BigLake Managed Iceberg Staging Table
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.staging_iceberg_transaksi`
(
  id_transaksi INT64,
  jumlah NUMERIC,
  status STRING,
  tanggal_transaksi DATE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  deleted_at TIMESTAMP
)
WITH CONNECTION `{project_id}.{region}.{connection_id}`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG'
)
PARTITION BY DATE(tanggal_transaksi)
CLUSTER BY status, id_transaksi;

-- Step 2: Create Final BigQuery Native Serving Table
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.final_transaksi`
(
  id_transaksi INT64,
  jumlah NUMERIC,
  status STRING,
  tanggal_transaksi DATE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  deleted_at TIMESTAMP
)
PARTITION BY DATE(tanggal_transaksi)
CLUSTER BY status, id_transaksi;

-- Step 3 : Create Source Table
CREATE TABLE IF NOT EXISTS transaksi_bank (
    id_transaksi VARCHAR(50) PRIMARY KEY,
    jumlah DOUBLE, -- Bisa diganti DECIMAL(15,2) jika ini nilai uang yang butuh presisi pasti
    status VARCHAR(50),
    tanggal_transaksi DATE,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    deleted_at DATETIME NULL
);