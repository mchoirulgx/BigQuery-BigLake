# Creating BigLake External Tables (Parquet Format)

The Parquet format is highly optimal for analytics due to its columnar binary compression. BigLake can automatically read Parquet files along with their metadata and complex data types via schema inferencing.

## Step 1: Data Type Mapping (SQL Server to BigQuery)

When exporting data from source systems like SQL Server into Parquet and then reading it in BigQuery, the data types are automatically mapped. Here is the conversion matrix:

| Source System (SQL Server) | Parquet Logical Type (PyArrow) | Destination (BigQuery) | Notes |
| :--- | :--- | :--- | :--- |
| `INT` | `INT32` | `INTEGER` | Standard 32-bit integer. |
| `DATETIME` / `DATETIME2` | `TIMESTAMP_MILLIS` | `TIMESTAMP` | Preserves millisecond precision. |
| `DECIMAL(18,2)` | `DECIMAL128(18,2)` | `NUMERIC` | Prevents floating-point errors (crucial for financials). |
| `BIT` | `BOOLEAN` | `BOOLEAN` | True/False mapping. |
| `VARCHAR(N)` | `STRING` | `STRING` | Encoded as UTF-8. |

## Step 2: External Table Creation DDL

Use the following SQL in the BigQuery Editor to map GCS to BigLake. Make sure to use a *wildcard* (`*.parquet`) so the table automatically recognizes new data if additional Parquet files are uploaded to the directory.

```sql
CREATE OR REPLACE EXTERNAL TABLE `imposing-league-374504.bqlake_data.parquet_data`
WITH CONNECTION `asia-southeast2.biglake-data-connectionconn`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://biglake_data/sql_server_data/*.parquet']
);
```

## Step 3: CRUD Capabilities and Limitations by Format

Before attempting to modify data, it is critical to understand the capabilities and limitations of different data lake storage formats in BigQuery.

| Storage Format | Read (SELECT) | Insert (ADD) | Update (MODIFY) | Delete (REMOVE) | Primary Use Case |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Standard Parquet** | ✅ Yes | ⚠️ No direct SQL (must upload new files) | ❌ No | ❌ No | Immutable historical data, raw data lake ingestion. |
| **Hive-Partitioned** | ✅ Yes (Partition Pruning) | ⚠️ No direct SQL (add new folders/files) | ❌ No (requires full partition overwrite) | ❌ No (requires dropping partition) | Large-scale time-series data, daily log files. |
| **Apache Iceberg** | ✅ Yes (Supports Time Travel) | ✅ Yes (Native SQL `INSERT`) | ✅ Yes (Native SQL `UPDATE`) | ✅ Yes (Native SQL `DELETE`) | Mutable data lakehouse, ACID transactions, CDC workloads. |

> **Fundamental Rule:** External tables with raw Parquet format are strictly *Read-Only* (Immutable). BigQuery does not support native SQL `INSERT`, `UPDATE`, or `DELETE` operations directly on these tables.

## Step 4: DML / CRUD Lifecycle on Parquet Data

To perform a CRUD cycle using SQL on standard Parquet data, the data must first be materialized into a Native BigQuery table.

**Materialization (CTAS):**
```sql
CREATE OR REPLACE TABLE `imposing-league-374504.bqlake_data.transaksi_native` AS
SELECT * FROM `imposing-league-374504.bqlake_data.parquet_data`;
```

Once the table is native, standard DML operations can be executed:
```sql
-- READ
SELECT * FROM `imposing-league-374504.bqlake_data.transaksi_native` LIMIT 10;

-- UPDATE
UPDATE `imposing-league-374504.bqlake_data.transaksi_native`
SET IsProcessed = TRUE WHERE TransactionID = 10000000;
```

---

## ⚠️ Parquet DDL Troubleshooting

### Error: Matched No Files
**Error Message:** 
`Failed to expand table parquet_data with file pattern gs://.../*.parquet: matched no files.`

**Cause:** 
The IAM permissions are correct, but the BigQuery engine could not find any `.parquet` files at the specified path. This is most often caused by a typo or a duplicated folder name in the `uris` parameter.

**Solution:**
Validate the actual folder structure in the Cloud Storage Console.
*Incorrect:* `uris = ['gs://biglake_data/sql_server_data/sql_server_data/*.parquet']` (Duplicated folder)
*Correct:* `uris = ['gs://biglake_data/sql_server_data/*.parquet']`