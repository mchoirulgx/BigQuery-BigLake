-- ============================================================================
-- SCRIPT: 05_merge_staging_to_native.sql
-- Stage: Stage 4 - Incremental Loading (Reference Pipeline)
-- File: sql/05_merge_staging_to_native.sql
--
-- PURPOSE:
--   Synchronizes records from `staging_iceberg_transaksi` to `final_transaksi`
--   via BigQuery MERGE with deduplication and target table partition pruning.
--
-- WHAT TO DO:
--   1. This query is designed to be executed directly by Airflow's
--      `BigQueryInsertJobOperator` with templated parameters (`params`).
--   2. For manual execution in BigQuery Studio, replace `{{ params.* }}` and
--      `{{ ds }}` with literal values.
--
-- KEY PATTERN:
--   - QUALIFY ROW_NUMBER() guarantees idempotency even if GCS or DTS contains duplicates.
--   - Target partition pruning (AND T.tanggal_transaksi = S.tanggal_transaksi)
--     prevents full table scans on the target table, ensuring predictable query costs.
--   - Soft deletes are preserved (T.deleted_at = S.deleted_at) for audit compliance.
-- ============================================================================

MERGE `{{ params.project_id }}.{{ params.dataset_id }}.final_transaksi` T
USING (
  SELECT *
  FROM `{{ params.project_id }}.{{ params.dataset_id }}.staging_iceberg_transaksi`
  WHERE 
    {% if params.is_initial %}
      1=1
    {% else %}
      DATE(updated_at) = '{{ ds }}'
    {% endif %}
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id_transaksi ORDER BY updated_at DESC) = 1
) S
ON T.id_transaksi = S.id_transaksi
   -- CRITICAL COST OPTIMIZATION: Prunes partitions on the target table
   AND T.tanggal_transaksi = S.tanggal_transaksi
WHEN MATCHED THEN
  UPDATE SET
    T.jumlah = S.jumlah,
    T.status = S.status,
    T.tanggal_transaksi = S.tanggal_transaksi,
    T.updated_at = S.updated_at,
    T.deleted_at = S.deleted_at
WHEN NOT MATCHED BY TARGET THEN
  INSERT (id_transaksi, jumlah, status, tanggal_transaksi, created_at, updated_at, deleted_at)
  VALUES (S.id_transaksi, S.jumlah, S.status, S.tanggal_transaksi, S.created_at, S.updated_at, S.deleted_at);
