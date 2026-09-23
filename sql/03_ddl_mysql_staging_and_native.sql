-- ============================================================================
-- SCRIPT: 03_ddl_mysql_staging_and_native.sql
-- Stage: Stage 2 - Simple Ingestion
-- File: sql/03_ddl_mysql_staging_and_native.sql
--
-- PURPOSE:
--   Defines the two-tier table architecture for the MySQL-to-BigLake demo:
--   1. `history_users`: BigLake Managed Iceberg table (Bronze/History layer).
--   2. `main_users`: Native BigQuery table (Gold/Serving layer).
--   Also provides the MERGE query to synchronize changes between layers.
--
-- WHAT TO DO:
--   1. Replace placeholders:
--      - `{project_id}`: Your Google Cloud Project ID.
--      - `{dataset_id}`: Your BigQuery Dataset ID.
--      - `{region}`: GCP region (e.g., `asia-southeast2`).
--      - `{connection_id}`: Your BigLake connection ID.
--   2. Execute DDL statements in BigQuery Studio before triggering the Airflow DAG.
--
-- KEY PATTERN:
--   Demonstrates a 2-tier Medallion architecture where the historical audit
--   trail is kept in open Iceberg format, while high-speed BI queries hit
--   clustered native BigQuery tables.
-- ============================================================================

-- Step 1: Create BigLake Managed Iceberg Staging / History Table
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.history_users`
(
  id INT64,
  name STRING,
  email STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  deleted_at TIMESTAMP
)
WITH CONNECTION `{project_id}.{region}.{connection_id}`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG'
)
PARTITION BY DATE(created_at);

-- Step 2: Create Native Serving Table with Clustering
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.main_users`
(
  id INT64,
  name STRING,
  email STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  deleted_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY id;

-- Step 3: Upsert & Sync MERGE Statement (from Staging to Native)
MERGE `{project_id}.{dataset_id}.main_users` T
USING (
  SELECT *
  FROM `{project_id}.{dataset_id}.history_users`
  -- Filter recent updates (e.g. last 2 days)
  WHERE updated_at >= TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME('Asia/Jakarta'), INTERVAL 2 DAY))
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 1
) S
ON T.id = S.id
   -- Strict equi-join on partition column enables BigQuery partition pruning
   AND DATE(T.created_at) = DATE(S.created_at)
WHEN MATCHED AND S.deleted_at IS NOT NULL THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET 
    T.name = S.name,
    T.email = S.email,
    T.updated_at = S.updated_at,
    T.deleted_at = S.deleted_at
WHEN NOT MATCHED THEN
  INSERT (id, name, email, created_at, updated_at, deleted_at)
  VALUES (S.id, S.name, S.email, S.created_at, S.updated_at, S.deleted_at);
