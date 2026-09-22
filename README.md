# MySQL → BigQuery Data Lakehouse: Documentation Index

This repository is a knowledge base of guides, proof-of-concepts, and production-ready pipeline blueprints for building a **Data Lakehouse on Google Cloud**, migrating data from **MySQL** into **BigQuery**, using **Apache Airflow**, **BigQuery Data Transfer Service (DTS)**, **Google Cloud Storage (GCS)**, and **Apache Iceberg (BigLake Managed Tables)**.

Every pipeline in this repo follows the same core architectural pattern:

```
MySQL (source) → Parquet (in-memory/GCS) → BigLake Iceberg Table (History/Staging Layer)
              → MERGE / UPSERT → BigQuery Native Table (Main/Serving Layer)
```

## Repository Structure

```
.
├── README.md          ← you are here (index)
├── docs/              ← architecture guides, comparisons, and pipeline write-ups
└── code/              ← runnable Airflow DAGs and standalone test/helper scripts
```

- **`docs/`** explains the *why* and *how* — architecture decisions, DDL, setup steps, troubleshooting.
- **`code/`** contains the *actual* Python/Airflow implementations referenced by some of the docs below.
- Not every doc has a corresponding code file (some only contain inline SQL/DDL snippets); the mapping table in each section below tells you which ones do.

---

## 1. Foundations & Core Concepts

Read these first to understand the building blocks used across every pipeline in this repo.

| Doc | Code | What it covers |
| --- | --- | --- |
| [`docs/biglake-connection-setup.md`](./docs/biglake-connection-setup.md) | — | Step-by-step guide to creating a **BigLake Connection** (Cloud Resource) in BigQuery, granting the connection's service account IAM access (`Storage Object Viewer`) on GCS, and troubleshooting the two most common setup errors. |
| [`docs/BQ-Managed-Table.md`](./docs/BQ-Managed-Table.md) | — | Conceptual deep-dive comparing **Apache Parquet**, **Hive Partitioning**, and **Apache Iceberg** — pros, cons, and a comparison table covering DML support, ACID transactions, time travel, and schema evolution. |
| [`docs/Parquet-data.md`](./docs/Parquet-data.md) | — | How to create **BigLake external tables** over raw Parquet, a SQL Server → BigQuery type mapping matrix, a CRUD capability matrix across storage formats, and troubleshooting "matched no files" errors. |

---

## 2. Proof of Concept / Testing

| Doc | Code | What it covers |
| --- | --- | --- |
| [`docs/BQ-Managed-Table-Test.md`](./docs/BQ-Managed-Table-Test.md) | — | Hands-on POC using the public **NYC Taxi dataset**: copying Hive-partitioned Parquet into your own bucket, creating an external staging table, materializing it into a **BigLake Managed Iceberg Table**, and validating native `SELECT`, `UPDATE`, `DELETE`. |

---

## 3. Migration Pipeline Blueprints (MySQL → BigQuery)

| Doc | Code | Pipeline style / key differentiator |
| --- | --- | --- |
| [`docs/MySql_To_BQ_Managed_Table.md`](./docs/MySql_To_BQ_Managed_Table.md) | — | One-time/CDC migration. Loads directly into Iceberg via `INSERT ... UNNEST(JSON_QUERY_ARRAY())` (no DTS/GCS hop). Includes full MySQL → BigQuery type mapping. |
| [`docs/dts_parquet_migration_docs.md`](./docs/dts_parquet_migration_docs.md) | — | CDC (24h window). Airflow → in-memory Parquet → GCS → DTS ingests into Iceberg → Scheduled Query `MERGE`s into Native table. |
| [`docs/dts_parquet_to-iceberg.md`](./docs/dts_parquet_to-iceberg.md) | — | Full + Incremental batch. Airflow checks if the target table is empty to decide Full vs. Incremental Load, then hands off to DTS + scheduled `MERGE`. |
| [`docs/dts_parquet_to_iceberg_with_airflow.md`](./docs/dts_parquet_to_iceberg_with_airflow.md) | [`code/dts_airflow_orchestrator.py`](./code/dts_airflow_orchestrator.py) | Unified, Airflow-triggered pipeline (no BigQuery-side cron). The code version extends the doc's concept with dummy-data generation, pre/post row-count + GCS-size metrics, an oldest-row cleanup step, and an emailed run report. |
| [`docs/dts_to_bq_managed_table_with_date_prefix.md`](./docs/dts_to_bq_managed_table_with_date_prefix.md) | [`code/airflow_test_data_transaksi.py`](./code/airflow_test_data_transaksi.py) | Date-prefixed folders, soft-delete banking example (`transaksi_bank`). Strict PyArrow schema enforcement, DTS sensor to wait for completion, dummy-data generator (700 insert / 200 update / 100 soft-delete). |

---

## 4. Automation & Schema Management

| Doc | Code | What it covers |
| --- | --- | --- |
| [`docs/automated-schema-evolution-gcs-bigquery-iceberg.md`](./docs/automated-schema-evolution-gcs-bigquery-iceberg.md) | [`code/airflow_test_add_delete_column.py`](./code/airflow_test_add_delete_column.py) · [`code/test-delete-add-tcolumn.py`](./code/test-delete-add-tcolumn.py) | Solves the gap where DTS doesn't support `ALLOW_FIELD_ADDITION` on Iceberg tables. The DAG (`airflow_test_add_delete_column.py`) inspects incoming Parquet metadata, diffs it against the BigQuery schema, and runs `ALTER TABLE ... ADD COLUMN` automatically before triggering DTS. `test-delete-add-tcolumn.py` is the companion script that uploads a test Parquet file with new columns to trigger the evolution. |

---

## How to Navigate This Repo

- **New to the lakehouse pattern?** Start with Section 1 (`docs/` foundations).
- **Want to see it work end-to-end first?** Try Section 2's NYC Taxi POC.
- **Building a real pipeline?** Pick the blueprint in Section 3 that matches your load pattern, then open its matching file in `code/` if one exists.
- **Hitting schema drift issues?** Jump to Section 4 — doc + code both included.

All `<placeholder>` / `[YOUR_...]` values in the SQL and Python snippets must be replaced with your actual GCP project ID, dataset, bucket names, and connection IDs before running.
