# Guide to Setting Up BigLake Connection in Google Cloud

As a best practice in data lake architecture, BigLake requires a Connection (Cloud Resource) so BigQuery can delegate data read access to Google Cloud Storage (GCS) securely using a Service Account, without exposing individual user credentials.

## Step 1: Creating a BigLake Connection

1. Open the **BigQuery Console**.
2. In the Explorer panel, click **+ ADD** -> choose **Connections to external data sources**.
3. In the **Connection type** dropdown, select:
   **Agent Platform remote models, remote functions, Lakehouse and Spanner (Cloud Resource)**
4. Change the **Location type** to **Region**.
5. Select your primary operational region (e.g., `asia-southeast2` for Jakarta) to optimize query latency and avoid data egress costs.
6. Name the connection (example: `biglake-data-connectionconn`).
7. Click **Create Connection**.
8. Open the newly created connection in the Explorer panel, then copy the generated **Service Account ID** (ending with `@gcp-sa-bigquery-condel.iam.gserviceaccount.com`).

## Step 2: IAM (Identity and Access Management) Configuration

The Service Account associated with the BigLake connection must be granted permissions to read objects in the target GCS bucket.

1. Open the **Cloud Storage > Buckets** page.
2. Navigate to your target bucket (example: `gs://biglake_data`).
3. Go to the **Permissions** tab and click **+ GRANT ACCESS**.
4. *Paste* the **Service Account ID** into the *New principals* field.
5. Select the role: **Cloud Storage** -> **Storage Object Viewer**.
6. Click **Save**.
   *(Note: IAM propagation typically takes about 1-2 minutes before BigQuery can utilize the new permissions).*

---

## ⚠️ Common Setup Troubleshooting

### Error 1: Invalid Connection Name Format
**Error Message:** 
`Connection name should conform to the pattern: projects/{project_id=*}/locations/{location_id=*}/connections/{connection_id=*}`

**Cause:** 
In the BigQuery DDL query, referencing the Connection by its name alone is insufficient. The BigQuery engine requires the region information.

**Solution:**
Add the region (location) prefix in front of the connection name in the `WITH CONNECTION` clause.
*Incorrect:* `WITH CONNECTION 'biglake-data-connectionconn'`
*Correct:* `WITH CONNECTION 'asia-southeast2.biglake-data-connectionconn'`

### Error 2: Permission Denied (Globbing File Pattern)
**Error Message:** 
`Access Denied: Permission denied while globbing file pattern. <service-account> does not have storage.objects.list access...`

**Cause:** 
The BigLake connection is invoked in the DDL, but its Service Account has not been granted permission (or the permission hasn't propagated yet) to read the contents of the GCS Bucket.

**Solution:**
Follow **Step 2** above to assign the **Storage Object Viewer** role at the bucket level. Wait 1-2 minutes before running the DDL again.


# Comprehensive Guide to Data Lakehouse Architecture: Parquet, Hive, and Apache Iceberg

When building a large-scale Data Lakehouse architecture, it is crucial to understand the distinct roles of the physical storage format, the directory organization method, and the logical table format layer. This document breaks down these three fundamental concepts, detailing their strengths and weaknesses.

---

## 1. Apache Parquet (Physical Storage Layer / File Format)

**Definition:**
Apache Parquet is an open-source, columnar storage format. Unlike CSV or JSON, which store data row-by-row, Parquet stores data by column. In a data architecture, Parquet represents the physical foundation; it is the actual binary file resting on your disk or inside Google Cloud Storage (GCS).

**Pros:**
*   **High-Level Compression:** Because data types within a single column are uniform, compression algorithms (like Snappy or GZIP) operate highly efficiently, drastically reducing storage costs.
*   **Analytical Query Performance (OLAP):** Extremely fast for aggregation queries. If you execute `SELECT total_amount FROM table`, the engine only reads the `total_amount` column from the disk and ignores the rest (a process known as Column Pruning).
*   **Complex Schema Support:** Capable of storing nested and array data types natively.

**Cons:**
*   **Immutable:** Parquet files are final once written. You cannot execute an `UPDATE` or `DELETE` on a specific row within the file. You must read the data into memory, modify it, and overwrite the entire file.
*   **Poor for OLTP:** Highly inefficient for transactional row-level lookups.

---

## 2. Hive Partitioning (Directory Organization Layer)

**Definition:**
Hive Partitioning is not a file or an application; it is a **standardized directory naming convention** used in cloud storage or HDFS. This method breaks massive datasets into hierarchical folders based on key-value pairs (partition columns). 
*Example GCS Path:* `gs://bucket_data/transactions/year=2024/month=08/day=15/data.parquet`

**Pros:**
*   **Aggressive Partition Pruning:** Allows SQL engines (like BigQuery or Spark) to skip irrelevant data. If you run `WHERE year = 2024 AND month = 08`, the engine only opens that specific folder, cutting query scanning costs by up to 90%.
*   **Logical Organization:** Simplifies data lifecycle management (e.g., easily deleting an entire folder for the year 2020 to save storage space).

**Cons:**
*   **No Native DML Operations:** You cannot perform row-level `UPDATE`s on Hive data. To modify records, you must overwrite the entire partition folder with new Parquet files.
*   **The Small File Problem:** Over-partitioning (e.g., down to the hour/minute level) generates millions of tiny kilobyte-sized Parquet files. This cripples engine performance due to massive metadata reading overhead.
*   **Rigidity:** Changing your partition strategy (e.g., from date-based to region-based) requires reading and rewriting the entire data lake.

---

## 3. Apache Iceberg (Logical Table Format Layer / Metadata Layer)

**Definition:**
Apache Iceberg is an open table format that sits **on top of** your Parquet files. Iceberg acts as the "brain" or ledger (manifest), precisely tracking metadata: it knows exactly where every Parquet file is located, when it was created, its schema version, and which rows are active or deleted.

**Pros:**
*   **ACID Transactions (Full DML):** Brings relational database capabilities to the data lake. You can safely execute `INSERT`, `UPDATE`, `DELETE`, and `MERGE`. When updating, Iceberg does not alter the old Parquet file; it writes a new file and intelligently updates its metadata pointer.
*   **Time Travel:** Allows you to run queries to view the data's state at a specific point in time in the past (e.g., querying data as-of last week before a massive deletion occurred).
*   **Hidden Partitioning:** Manages logical partitioning behind the scenes based on metadata. Users do not need to know the complex folder structure; they just filter by standard columns (e.g., `WHERE transaction_date = '2024-08-15'`), and Iceberg translates it.
*   **Seamless Schema Evolution:** You can add, drop, rename, or reorder columns, or change data types (e.g., integer to bigint) safely without ever breaking historical data or rewriting the entire table.

**Cons:**
*   **Maintenance Overhead:** Because Iceberg tracks every change via metadata files, it requires routine maintenance (like *Compaction* for small files, and *Snapshot Expiration* to remove obsolete metadata). *Note: In BigLake Managed Tables, Google handles this automatically.*
*   **Initial Complexity:** The initial setup and architectural understanding are more complex than simply dumping Parquet files into a bucket.

---

## Architecture Comparison Table

| Feature / Parameter | Apache Parquet | Hive Partitioning | Apache Iceberg |
| :--- | :--- | :--- | :--- |
| **Primary Function** | Binary storage file. | Folder organization method. | Transactional table format layer. |
| **Native DML (CRUD)** | ❌ No (Immutable) | ❌ No (Requires full partition overwrite) | ✅ Yes (Supports `UPDATE`/`DELETE`/`MERGE`) |
| **ACID Transactions** | ❌ No | ❌ No | ✅ Yes (Serial Isolation) |
| **Time Travel** | ❌ No | ❌ No | ✅ Yes (Metadata Snapshot-based) |
| **Schema Evolution** | ⚠️ Limited (Add columns only) | ❌ Difficult (Can break old partitions) | ✅ Full (Add, drop, rename, change types) |
| **Partitioning Logic** | ❌ Not applicable | ✅ Manual (Hardcoded in directory URLs) | ✅ Automatic (Hidden Partitioning) |
| **Best Use Case** | Long-term archival, read-heavy workloads. | Massive, static historical log/sensor data. | Dynamic Data Lakehouse environments (CDC). |