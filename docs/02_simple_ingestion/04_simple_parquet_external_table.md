# Stage 2: Simple Ingestion
## Part 2.1: BigLake External Tables over Raw Parquet

> **Section Overview:**  
> This section demonstrates the simplest pattern for exposing Cloud Storage data to BigQuery: creating a BigLake External Table directly over raw Parquet files. This approach allows SQL querying with zero data copying, while explaining why external Parquet tables are immutable and how they map data types.

---

## Standalone Code Assets
* **SQL Scripts:** [`sql/01_ddl_external_parquet.sql`](../../sql/01_ddl_external_parquet.sql)

---

## Step 1: Data Type Mapping (Source to BigQuery)

When exporting relational data (such as from SQL Server or MySQL) into Parquet using PyArrow, data types are mapped into BigQuery types <a href="https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#parquet_conversions" target="_blank">[1]</a>:

| Source System Type | Parquet Logical Type (PyArrow) | Destination (BigQuery) | Technical Notes |
| :--- | :--- | :--- | :--- |
| `INT` | `INT32` | `INTEGER` | Standard 32-bit integer. |
| `DATETIME` / `DATETIME2` | `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS` | `TIMESTAMP` | Converted to UTC epoch. (Note: BigQuery `DATETIME` is timezone-naive; `TIMESTAMP` is UTC-anchored). |
| `DECIMAL(18,2)` | `DECIMAL128(18,2)` | `NUMERIC` | Exact precision fixed-point arithmetic (prevents floating-point rounding errors). |
| `BIT` / `TINYINT(1)` | `BOOLEAN` | `BOOLEAN` | Standard boolean. |
| `VARCHAR(N)` | `STRING` | `STRING` | Variable-length UTF-8 text. |

---

## Step 2: External Table Creation DDL

Run the following SQL in BigQuery Studio to map your GCS Parquet path to BigQuery using the BigLake Cloud Resource connection <a href="https://cloud.google.com/bigquery/docs/create-cloud-storage-table-biglake" target="_blank">[2]</a>:

```sql
CREATE OR REPLACE EXTERNAL TABLE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.parquet_data`
WITH CONNECTION `<YOUR_REGION>.<YOUR_CONNECTION_ID>`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://<YOUR_BUCKET_NAME>/sql_server_data/*.parquet']
);
```

> [!TIP]
> Always use a wildcard (`*.parquet`) so that as new Parquet files land in the folder, BigQuery discovers them automatically without DDL modifications.

---

## Step 3: Immutability and CRUD Limitations

External tables over raw Parquet files are strictly **Read-Only** and do not support DML statements <a href="https://cloud.google.com/bigquery/docs/external-tables#limitations" target="_blank">[3]</a>:

| Storage Format | Read (SELECT) | Insert (ADD) | Update (MODIFY) | Delete (REMOVE) | Primary Use Case |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Standard Parquet** | ✅ Yes | ⚠️ No direct SQL (must upload new files) | ❌ No | ❌ No | Immutable historical data, raw data lake ingestion. |
| **Hive-Partitioned** | ✅ Yes (Partition Pruning) | ⚠️ No direct SQL (add new folders/files) | ❌ No (requires full partition overwrite) | ❌ No (requires dropping partition) | Massive time-series, log files. |
| **Apache Iceberg** | ✅ Yes (Time Travel) | ✅ Yes (Native SQL `INSERT`) | ✅ Yes (Native SQL `UPDATE`) | ✅ Yes (Native SQL `DELETE`) | Mutable data lakehouse, ACID transactions, CDC. |

---

## Step 4: Materializing to Native BigQuery for DML

If row-level mutations are needed from an immutable external Parquet table, the data must be materialized into a Native BigQuery table (or an Iceberg table) via CTAS <a href="https://cloud.google.com/bigquery/docs/tables#create-table-query" target="_blank">[4]</a> <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax" target="_blank">[5]</a>:

```sql
-- Materialize into a native table
CREATE OR REPLACE TABLE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.transaksi_native` AS
SELECT * FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.parquet_data`;

-- Perform DML
UPDATE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.transaksi_native`
SET IsProcessed = TRUE 
WHERE TransactionID = 10000000;
```

---

## ⚠️ Troubleshooting

### Error: Matched No Files
**Error Message:** 
```text
Failed to expand table parquet_data with file pattern gs://.../*.parquet: matched no files.
```

**Cause:** 
The BigLake connection SA has read permissions, but the specific URI pattern did not match any files (often due to subfolder nesting typos).

**Solution:**
Verify the exact folder path using `gcloud storage ls gs://<YOUR_BUCKET_NAME>/sql_server_data/`.

---

## Next Steps in Stage 2:
To ingest mutable relational data directly from MySQL into a transactional Iceberg staging table using SQL DML, see:  
**[05_mysql_to_iceberg_direct_sql.md](05_mysql_to_iceberg_direct_sql.md)**

---

## References

1. <a href="https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#parquet_conversions" target="_blank">Google Cloud - Parquet type conversions in BigQuery</a>
2. <a href="https://cloud.google.com/bigquery/docs/create-cloud-storage-table-biglake" target="_blank">Google Cloud - Create a BigLake Cloud Storage table</a>
3. <a href="https://cloud.google.com/bigquery/docs/external-tables#limitations" target="_blank">Google Cloud - External table limitations in BigQuery</a>
4. <a href="https://cloud.google.com/bigquery/docs/tables#create-table-query" target="_blank">Google Cloud - Create a table from a query result</a>
5. <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax" target="_blank">Google Cloud - Data manipulation language (DML) statements</a>
