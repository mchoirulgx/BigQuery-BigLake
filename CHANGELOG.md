# Changelog

All notable changes to the **BigLake, Apache Iceberg, and BigQuery Pipeline Playbook** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.1.0] - 2026-09-23

### Summary
Comprehensive code review remediation bringing all SQL scripts, Airflow DAGs, and documentation into 100% compliance with Google Cloud Well-Architected Framework and BigLake Iceberg best practices.

### Fixed & Hardened
- **P0 Schema Alignment (`sql/04_ddl_reference_staging_and_native.sql`)**:
  - Aligned transaction identifier `id_transaksi` from `INT64` to `STRING` and `jumlah` from `NUMERIC` to `FLOAT64`, eliminating fatal DTS schema mismatch exceptions and achieving 100% parity with PyArrow schemas, MySQL source tables, and documentation.
- **P0 Stage 5 Schema Evolution Hardening (`dags/dag_10_schema_evolution_iceberg.py`)**:
  - Replaced ambient `gcsfs` with enterprise `GCSHook` using Airflow connection credentials.
  - Implemented multi-file schema union across all landed Parquet files to detect schema additions across batch splits.
  - Replaced parse-time `datetime.now()` evaluation with dynamic Jinja templating `{{ data_interval_end.int_timestamp }}`.
  - Added `BigQueryDataTransferServiceTransferRunSensor` in `reschedule` mode to guarantee DTS ingestion completes before downstream dependencies.
  - Replaced silent fallback to `STRING` with defensive validation and explicit error handling for unsupported PyArrow types.
- **P0 Stage 3 Ingestion Race Condition (`dags/dag_07_dts_parquet_to_iceberg.py`)**:
  - Inserted `BigQueryDataTransferServiceTransferRunSensor` (`mode='reschedule'`) between the asynchronous DTS trigger and the downstream MERGE query, preventing premature MERGE execution against uncommitted Iceberg data.
- **P1 Partition Pruning Optimization (`sql/03_ddl_mysql_staging_and_native.sql`)**:
  - Replaced broken non-equi join timestamp range with strict equi-join `AND DATE(T.created_at) = DATE(S.created_at)`, enabling BigQuery to prune partitions dynamically.
- **P1 Airflow Security & Connection Integrity (`dags/dag_09_reference_pipeline_date_prefix.py`)**:
  - Refactored `check_initial_load` to use `BigQueryHook` and Airflow connections instead of ambient unmanaged client instantiations.
  - Added `mode='reschedule'` to `BigQueryDataTransferServiceTransferRunSensor` to prevent Airflow worker slot starvation.
- **P1 Documentation Accuracy & DTS Destination Constraint (`docs/03_dts_to_iceberg/06_dts_parquet_migration.md`)**:
  - Clarified that GCS staging is mandatory because the native DTS MySQL connector only supports native BigQuery tables and cannot target BigLake Managed Apache Iceberg tables.
- **P1 Navigation & Repository Structure Synchronization (`README.md`)**:
  - Corrected numbering drift across `README.md` table and repository layout tree to exactly match physical file paths on disk.
- **P2 CDC Terminology Precision Across Documentation & DAGs**:
  - Re-labeled all claims of "MySQL CDC" to **"Timestamp-Based Incremental Extraction"** across `docs/02_simple_ingestion/05_mysql_to_iceberg_direct_sql.md`, `docs/04_incremental_loading/09_reference_pipeline_date_prefix_sensor.md`, `README.md`, and DAG docstrings (`dag_05`, `dag_09`). Retained clean batch extraction logic without adding unnecessary streaming CDC infrastructure.
- **P2 Iceberg POC Partitioning (`sql/02_ddl_poc_nyc_taxi_iceberg.sql`, `docs/01_concept_why_biglake_and_iceberg/03_poc_hive_to_managed_iceberg.md`)**:
  - Added explicit `PARTITION BY RANGE_BUCKET(data_file_month, GENERATE_ARRAY(1, 12, 1))` to the CTAS statement to demonstrate true partition pruning on BigLake Iceberg tables.
- **Producer Script Reliability (`scripts/simulate_mysql_producer.py`)**:
  - Added `coerce_timestamps='us'` to ensure microsecond timestamp resolution and wrapped Cloud Storage upload in `try...finally` to ensure reliable temporary file cleanup.

---

## [2.0.0] - 2026-09-22

### Summary
Major architectural restructuring and codebase hardening. Transformed an unorganized collection of 10 loose Markdown notes into a production-grade, progressive 5-stage Data Lakehouse Playbook and guidance demo for Google Cloud Platform.

---

### Added

#### 1. Progressive 5-Stage Documentation Hierarchy (`docs/`)
- **Stage 1: Concepts & Architecture Foundations (`docs/01_concept_why_biglake_and_iceberg/`)**
  - `01_biglake_iceberg_concepts.md`: Deep-dive comparison of Parquet, Hive Partitioning, and Iceberg metadata layers; ACID transaction mechanics; Time Travel snapshot queries.
  - `02_biglake_connection_and_iam.md`: Cloud Resource Connection setup, IAM role delegation (`roles/storage.objectAdmin` vs. `roles/bigquery.connectionUser`), and cross-project permissions.
  - `03_poc_hive_to_managed_iceberg.md`: Hands-on migration POC using the public NYC Taxi dataset to validate row-level `UPDATE` and `DELETE` on BigLake Iceberg.
- **Stage 2: Simple Ingestion (`docs/02_simple_ingestion/`)**
  - `04_simple_parquet_external_table.md`: External Parquet tables over GCS; PyArrow-to-BigQuery data type mapping matrix; read-only limitations.
  - `05_mysql_to_iceberg_direct_sql.md`: Educational direct SQL ingestion pattern via JSON UNNEST and 2-tier Medallion MERGE upsert.
- **Stage 3: DTS to Iceberg (`docs/03_dts_to_iceberg/`)**
  - `06_dts_parquet_migration.md`: Preserving pipeline consistency via BigQuery Data Transfer Service (DTS) using GCS landing zones; introduces **Scenario A (Post-Ingestion File Deletion)** vs. **Scenario B (Filename & Prefix Detection Without Deletion)**; PyArrow microsecond timestamp precision.
  - `07_dts_parquet_airflow_orchestration.md`: Orchestrating DTS via Airflow API; Managing Staging Lifecycles (File Deletion vs. Retention); Iceberg snapshot retention; automated BigLake Garbage Collection lifecycle.
- **Stage 4: Incremental Loading & Canonical Reference Implementation (`docs/04_incremental_loading/`)**
  - `08_dts_full_and_incremental_load.md`: **Scenario B in Action**: Dynamic cold-start detection (target table row count check) using distinct filename signatures (`batch_users_full_load.parquet` vs `batch_users_incremental_YYYYMMDD.parquet`) without deleting historical files.
  - `09_reference_pipeline_date_prefix_sensor.md`: **Canonical Reference Implementation** (`transaksi_bank`): Production embodiment of **Scenario B (Zero File Deletion Archive)** with date-prefixed folders (`export_YYYYMMDD/`), DTS runtime macros, `BigQueryDataTransferServiceTransferRunSensor`, and partition-pruned `MERGE`.
- **Stage 5: Schema Evolution Workarounds (`docs/05_schema_evolution/`)**
  - `10_automated_schema_evolution_iceberg.md`: Resolving DTS's lack of auto-schema evolution for Iceberg destinations via PyArrow metadata inspection and automated DDL migration.

#### 2. Standalone Apache Airflow DAGs (`dags/`)
- `dag_05_mysql_direct_sql.py`: Stage 2 prototype extracting MySQL CDC batches and loading into Iceberg via BigQuery SQL JSON UNNEST.
- `dag_07_dts_parquet_to_iceberg.py`: Stage 3 pipeline triggering DTS via `StartTransferRunsOperator` and executing a downstream Medallion `MERGE`.
- `dag_08_dts_full_and_incremental.py`: Stage 4 pipeline featuring dynamic cold-start branching (full initial extract vs. 24h incremental delta).
- `dag_09_reference_pipeline_date_prefix.py`: Stage 4 production-grade canonical reference pipeline with date-isolated folders, strict PyArrow serialization, and DTS run polling sensor.
- `dag_10_schema_evolution_iceberg.py`: Stage 5 automated schema drift handler comparing Parquet metadata with BigQuery schemas and applying `ALTER TABLE` DDL before DTS execution.
- *All DAGs include standardized instructional header comments explaining architectural purpose, execution prerequisites, and configuration variables.*

#### 3. Standalone BigQuery SQL Scripts (`sql/`)
- `01_ddl_external_parquet.sql`: External table DDL and CTAS templates for raw Parquet files.
- `02_ddl_poc_nyc_taxi_iceberg.sql`: External staging and BigLake Managed Iceberg DDL with sample ACID update/delete DML.
- `03_ddl_mysql_staging_and_native.sql`: Stage 2 staging and native table DDL.
- `04_ddl_reference_staging_and_native.sql`: Stage 4 reference partitioned staging and native serving table DDL.
- `05_merge_staging_to_native.sql`: Parameterized, partition-pruned BigQuery `MERGE` script.

#### 4. Simulation Utilities (`scripts/`)
- `scripts/simulate_mysql_producer.py`: Synthetic generator simulating MySQL banking transactions and day-over-day schema drift (Day 1 standard columns vs. Day 2 addition of `lokasi_cabang` and `tipe_transaksi`).

#### 5. Navigation & Repository Infrastructure
- `README.md`: Central landing portal featuring executive architectural context, visual Mermaid roadmap, complete documentation index, repository layout, and quickstart cheat sheet.
- `.gitignore`: Configured to exclude Python bytecode and `__pycache__` directories.

---

### Fixed

- **BigLake GCS Bucket IAM Role**:
  - *Previous*: Documentation specified `roles/storage.objectViewer` for the BigLake Connection Service Account.
  - *Fix*: Corrected to **`roles/storage.objectAdmin`** on the underlying GCS bucket. BigLake Managed Tables require object creation, metadata rewrite, and object deletion permissions to execute Iceberg commits and automated Garbage Collection (cleaning up expired data files and manifest lists).
- **BigLake Connection User Delegation**:
  - *Fix*: Documented the mandatory requirement for **`roles/bigquery.connectionUser`** on the Cloud Resource Connection for developers, Airflow worker identities, and automated pipelines.
- **NYC Taxi POC Partition Wildcard**:
  - *Previous*: DDL hardcoded Hive partition discovery to `data_file_year=2000/*`.
  - *Fix*: Corrected URI to `gs://<bucket>/poc_raw_data/nyc_taxi/*` to allow BigQuery Hive partition autodiscovery to register all partition years (e.g., 2021/2022).
- **DTS Async Execution Race Condition in Airflow**:
  - *Previous*: Pipelines triggered DTS via `StartTransferRunsOperator` and immediately executed downstream `MERGE` queries while DTS was still transferring files in the background.
  - *Fix*: Implemented **`BigQueryDataTransferServiceTransferRunSensor`** in Stage 4 reference pipelines to poll for terminal `SUCCEEDED` status before triggering downstream queries.
- **BigQuery MERGE Performance Anti-Pattern (Partition Scanning)**:
  - *Previous*: `MERGE` join condition only matched on primary key `T.id_transaksi = S.id_transaksi`, forcing BigQuery to scan all historical partitions in the target table.
  - *Fix*: Added partition predicate **`AND T.tanggal_transaksi = S.tanggal_transaksi`** in the `ON` clause to activate BigQuery partition pruning and dramatically reduce slot consumption and query scan costs.
- **Schema Evolution DDL Non-Idempotency**:
  - *Previous*: Dynamic DDL executed raw `ALTER TABLE <table_id> ADD COLUMN <column_name> <type>`, causing pipeline crashes on task retries if the column had already been added.
  - *Fix*: Updated to **`ALTER TABLE <table_id> ADD COLUMN IF NOT EXISTS <column_name> <type>`**.
- **Python Date Syntax Error in Simulation Code**:
  - *Previous*: Code contained invalid octal literal syntax in Python 3 (`datetime(2026, 9, 17, 09, 15, 0)`).
  - *Fix*: Corrected to `datetime(2026, 9, 17, 9, 15, 0)`.
- **PyArrow Timestamp Resolution for BigLake Iceberg**:
  - *Fix*: Documented and applied mandatory `coerce_timestamps='us'` configuration when serializing timestamps with PyArrow to prevent schema mismatch errors against BigQuery Iceberg `TIMESTAMP` columns.
- **Typographical and Markdown Hygiene**:
  - Cleaned up duplicated words (e.g. `connconn`), stray markdown artifacts (trailing `d`), and informal citation tags across all documentation files.

---

### Removed

- Deleted 10 legacy, unorganized root Markdown files:
  - `BQ Managed Table.md`
  - `BQ Managed Table Test.md`
  - `MySql_To_BQ_Managed_Table.md`
  - `Parquet data.md`
  - `Readme.md`
  - `automated-schema-evolution-gcs-bigquery-iceberg.md`
  - `dts_parquet_migration_docs.md`
  - `dts_parquet_to iceberg.md`
  - `dts_parquet_to_iceberg_with_airflow.md`
  - `dts_to_bq_managed_table_with_date_prefix.md`
