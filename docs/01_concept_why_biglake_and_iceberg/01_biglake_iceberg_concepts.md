# Stage 1: Concept — Why BigLake and Apache Iceberg?
## Part 1.1: Foundations of Lakehouse Architecture (Parquet vs. Hive vs. Iceberg)

> **Section Overview:**  
> This section introduces the core architectural consideration behind combining Google Cloud BigQuery, BigLake, and Apache Iceberg. It explains three structural layers of a Data Lakehouse: the physical file format (Parquet), directory organization (Hive Partitioning), and the transactional metadata table format (Apache Iceberg).

---

---

## 1. Apache Parquet (Physical Storage Layer / File Format)

**Definition:**
Apache Parquet is an open-source, columnar storage format [1](https://parquet.apache.org/docs/). Unlike CSV or JSON, which store data row-by-row, Parquet stores data by column. In a data architecture, Parquet represents the physical foundation; it is the actual binary file resting on your disk or inside Google Cloud Storage (GCS).

**Pros:**
* **High-Level Compression:** Because data types within a single column are uniform, compression algorithms (like Snappy or GZIP) operate highly efficiently, drastically reducing storage costs [1](https://parquet.apache.org/docs/).
* **Analytical Query Performance (OLAP):** Extremely fast for aggregation queries. If you execute `SELECT total_amount FROM table`, the engine only reads the `total_amount` column from the disk and ignores the rest (Column Pruning) [1](https://parquet.apache.org/docs/).
* **Complex Schema Support:** Capable of storing nested and array data types natively.

**Cons:**
* **Immutable:** Parquet files are final once written. You cannot execute an `UPDATE` or `DELETE` on a specific row within the file. You must read the data into memory, modify it, and overwrite the entire file.
* **Poor for OLTP:** Highly inefficient for transactional row-level lookups.

---

## 2. Hive Partitioning (Directory Organization Layer)

**Definition:**
Hive Partitioning is not a file format or application; it is a **standardized directory naming convention** used in cloud storage or HDFS [2](https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs). This method breaks massive datasets into hierarchical folders based on key-value pairs (partition columns).  
*Example GCS Path:* `gs://bucket_data/transactions/year=2024/month=08/day=15/data.parquet`

**Pros:**
* **Aggressive Partition Pruning:** Allows SQL engines (like BigQuery or Spark) to skip irrelevant data. If you run `WHERE year = 2024 AND month = 08`, the engine only opens that specific folder, cutting query scanning costs by up to 90% [2](https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs).
* **Logical Organization:** Simplifies data lifecycle management (e.g., easily deleting an entire folder for the year 2020 to save storage space).

**Cons:**
* **No Native DML Operations:** You cannot perform row-level `UPDATE`s on Hive data. To modify records, you must overwrite the entire partition folder with new Parquet files.
* **The Small File Problem:** Over-partitioning (e.g., down to the hour/minute level) generates millions of tiny kilobyte-sized Parquet files. This degrades engine performance due to excessive metadata reading overhead.
* **Rigidity:** Changing your partition strategy (e.g., from date-based to region-based) requires reading and rewriting the entire data lake.

---

## 3. Apache Iceberg (Logical Table Format Layer / Metadata Layer)

**Definition:**
Apache Iceberg is an open table format that sits **on top of** your Parquet files [3](https://iceberg.apache.org/spec/). Iceberg acts as the "brain" or ledger (manifest), precisely tracking metadata: it knows exactly where every Parquet file is located, when it was created, its schema version, and which rows are active or deleted.

**Pros:**
* **ACID Transactions (Full DML):** Brings relational database capabilities to the data lake. You can safely execute `INSERT`, `UPDATE`, `DELETE`, and `MERGE` [4](https://iceberg.apache.org/docs/latest/reliability/). When updating, Iceberg does not alter the old Parquet file; it writes a new file and intelligently updates its metadata pointer.
* **Time Travel:** Allows you to run queries to view the data's state at a specific point in time in the past (e.g., querying data as-of last week before an accidental deletion) [5](https://cloud.google.com/bigquery/docs/time-travel).
* **Hidden Partitioning:** Manages logical partitioning behind the scenes based on metadata [6](https://iceberg.apache.org/docs/latest/evolution/). Users do not need to know the complex folder structure; they just filter by standard columns (e.g., `WHERE transaction_date = '2024-08-15'`), and Iceberg translates it.
* **Seamless Schema Evolution:** You can add, drop, rename, or reorder columns, or change data types (e.g., integer to bigint) safely without ever breaking historical data or rewriting the entire table [6](https://iceberg.apache.org/docs/latest/evolution/).

**Cons:**
* **Maintenance Overhead:** Because Iceberg tracks every change via metadata files, it requires routine maintenance (like *Compaction* for small files, and *Snapshot Expiration* to remove obsolete metadata). *Note: In BigLake Managed Tables, Google Cloud handles this automatically.* [7](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#automatic-maintenance)
* **Initial Complexity:** The initial setup and architectural understanding are more complex than simply dumping Parquet files into a bucket.

---

## Architecture Comparison Table

| Feature / Parameter | Apache Parquet | Hive Partitioning | Apache Iceberg |
| :--- | :--- | :--- | :--- |
| **Primary Function** | Binary storage file. | Folder organization method. | Transactional table format layer. |
| **Native DML (CRUD)** | ❌ No (Immutable) | ❌ No (Requires full partition overwrite) | ✅ Yes (Supports `UPDATE`/`DELETE`/`MERGE`) [4](https://iceberg.apache.org/docs/latest/reliability/) |
| **ACID Transactions** | ❌ No | ❌ No | ✅ Yes (Serial Isolation) [4](https://iceberg.apache.org/docs/latest/reliability/) |
| **Time Travel** | ❌ No | ❌ No | ✅ Yes (Metadata Snapshot-based) [5](https://cloud.google.com/bigquery/docs/time-travel) |
| **Schema Evolution** | ⚠️ Limited (Add columns only) | ❌ Difficult (Can break old partitions) | ✅ Full (Add, drop, rename, change types) [6](https://iceberg.apache.org/docs/latest/evolution/) |
| **Partitioning Logic** | ❌ Not applicable | ✅ Manual (Hardcoded in directory URLs) | ✅ Automatic (Hidden Partitioning) [6](https://iceberg.apache.org/docs/latest/evolution/) |
| **Best Use Case** | Long-term archival, read-heavy workloads. | Massive, static historical log/sensor data. | Dynamic Data Lakehouse environments (incremental ELT). |

---

## Next Steps in Stage 1:
- To set up the required BigLake Cloud Resource connection and IAM permissions, proceed to:  
  **[02_biglake_connection_and_iam.md](02_biglake_connection_and_iam.md)**
- To test creating and querying a BigLake Managed Iceberg table hands-on, see:  
  **[03_poc_hive_to_managed_iceberg.md](03_poc_hive_to_managed_iceberg.md)**

---

## References

1. [Apache Parquet Official Documentation](https://parquet.apache.org/docs/)
2. [Google Cloud BigQuery: Hive partitioned queries over Cloud Storage](https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs)
3. [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
4. [Apache Iceberg: ACID Transactions & Concurrency](https://iceberg.apache.org/docs/latest/reliability/)
5. [Google Cloud BigQuery: Time Travel and Fail-safe](https://cloud.google.com/bigquery/docs/time-travel)
6. [Apache Iceberg: Partitioning and Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/)
7. [Google Cloud BigQuery: Apache Iceberg Managed Tables & Automatic Maintenance](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#automatic-maintenance)
