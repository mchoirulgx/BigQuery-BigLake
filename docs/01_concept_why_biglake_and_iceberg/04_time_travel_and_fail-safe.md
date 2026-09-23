# Time Travel & Fail-Safe on BigQuery Iceberg Managed Tables

## Overview

When you run `DELETE` on a BigQuery Iceberg managed table, the affected rows disappear from query results immediately, but the underlying Parquet data files are **not** removed from Cloud Storage right away. BigQuery only writes new metadata marking those rows as logically deleted; the old data files remain on disk so that **time travel** can restore them if needed <a href="https://cloud.google.com/bigquery/docs/time-travel" target="_blank">[1]</a>.

This is why you can observe row count decreasing while physical storage size stays flat or even increases for a while — old files are still retained, and new metadata/delete markers are added on top of them.

## Default Retention: 7-Day Time Travel Window

Every BigQuery dataset has a **time travel window**, which defaults to **7 days** <a href="https://cloud.google.com/bigquery/docs/time-travel" target="_blank">[1]</a> <a href="https://cloud.google.com/bigquery/docs/datasets" target="_blank">[3]</a>. During this window:

- Deleted or modified data can be recovered using `FOR SYSTEM_TIME AS OF`.
- The physical files backing that data remain in the GCS bucket associated with the Iceberg table's `storage_uri`.
- Storage size reflects live rows **plus** anything still inside the retention window, not just the current logical row count.

## Fail-Safe: The Extra Layer (Native Tables Only)

For regular (native) BigQuery tables, once the time travel window ends, there's an additional, **fixed 7-day fail-safe period**. During fail-safe:

- Data is no longer accessible via normal queries or `FOR SYSTEM_TIME AS OF`.
- Only Google Cloud Support can recover the data, and only in genuine emergency/disaster-recovery scenarios.
- Fail-safe **cannot be disabled, shortened, or configured** — it's a fixed safety net on top of whatever time travel window you set <a href="https://cloud.google.com/bigquery/docs/time-travel" target="_blank">[1]</a> <a href="https://cloud.google.com/bigquery/docs/restore-deleted-tables" target="_blank">[4]</a>.

So for a native table, total retention before physical garbage collection = time travel window (2–7 days) + fail-safe (fixed 7 days), i.e. up to 14 days in the worst case.

### Important Exception: Iceberg Managed Tables Do Not Use Fail-Safe

**Apache Iceberg managed tables (BigLake managed tables) do not support the fail-safe window** <a href="https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#data-retention" target="_blank">[2]</a>. This is a key difference from native BigQuery tables:

| Table type | Time travel window | Extra fail-safe | Total retention before GC |
|---|---|---|---|
| Native BigQuery table | 2–7 days (configurable) | Fixed 7 days | Time travel + 7 days |
| Iceberg managed table | 2–7 days (configurable) | Not supported | Time travel window only |

For an Iceberg managed table, Google's documentation states:

> "Automatic compaction and clustering are performed on the data files in the bucket. After the expiration of the time travel window, data files are garbage collected." <a href="https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#data-retention" target="_blank">[2]</a>

This means garbage collection for an Iceberg managed table happens **right after** the time travel window expires — there is no additional 7-day buffer waiting on top of it.

## Configuring the Time Travel Window

The time travel window can be set between **2 and 7 days**, in multiples of 24 hours (48, 72, 96, 120, 144, or 168 hours) <a href="https://cloud.google.com/bigquery/docs/time-travel" target="_blank">[1]</a> <a href="https://cloud.google.com/bigquery/docs/datasets" target="_blank">[3]</a>. Lowering it to the minimum shortens how long deleted data's old files stay billable/physically present, which is most impactful under **physical (compressed) storage billing**, since time travel storage is billed at active-storage rates in that model <a href="https://cloud.google.com/bigquery/docs/time-travel" target="_blank">[1]</a>.

**Set it when creating a new dataset:**

```sql
CREATE SCHEMA `PROJECT_ID.DATASET_ID`
OPTIONS (
  max_time_travel_hours = 48  -- minimum allowed = 48 hours (2 days)
);
```

**Update an existing dataset:**

```sql
ALTER SCHEMA `PROJECT_ID.DATASET_ID`
SET OPTIONS (
  max_time_travel_hours = 48
);
```

**Using the `bq` CLI:**

```bash
bq update --max_time_travel_hours=48 PROJECT_ID:DATASET_ID
```

Notes:
- The value must be a multiple of 24 hours; other values are rejected.
- This setting applies at the **dataset level** and affects every table inside it, including Iceberg managed tables.
- Reducing the window doesn't retroactively shrink data already inside a longer retention period from before the change — it only affects data modified after the setting takes effect.
- If your dataset uses **logical (uncompressed) billing** (the default), time travel storage cost is already bundled into the base rate, so the direct cost savings are smaller — but files still get garbage collected sooner.

## Full Lifecycle of a Deleted Row (Iceberg Managed Table, 2-Day Window)

| Stage | What happens | Queryable? | Recoverable? |
|---|---|---|---|
| t = 0 | `DELETE` executed; row hidden logically; old files remain in GCS | No | Yes, via `FOR SYSTEM_TIME AS OF` |
| t = 0 → 2 days | Inside time travel window; physical size stays inflated | No | Yes |
| t = 2 days | Time travel window expires; automatic garbage collection removes orphaned files | No | No |

No fail-safe stage applies here, unlike a native table which would add 7 more days after this point.

## Exception: Dropping the Entire Table

If you drop the **entire table** (not just rows), the underlying data files are **not** automatically garbage collected once the time travel window passes — this scenario requires separate manual handling, such as deleting the GCS objects directly or applying lifecycle rules on the bucket <a href="https://cloud.google.com/bigquery/docs/time-travel" target="_blank">[1]</a> <a href="https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#data-retention" target="_blank">[2]</a>.

## Verifying Physical Files in GCS

Two limitations to know first:

- `INFORMATION_SCHEMA.TABLE_STORAGE` does **not** include Iceberg managed tables, so you can't check `time_travel_physical_bytes` through that view for these tables.
- Iceberg managed tables show as **0 bytes** in the BigQuery Console or standard APIs, because the actual data lives in your own GCS bucket, not inside BigQuery's managed storage <a href="https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#data-retention" target="_blank">[2]</a> <a href="https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#storage-billing" target="_blank">[5]</a>.

To verify retained/duplicated files directly:

```bash
# 1. Resolve the table's storage_uri
bq query --use_legacy_sql=false '
SELECT table_name,
       REGEXP_EXTRACT(ddl, r"storage_uri\s*=\s*\"([^\"]+)\"") AS storage_uri
FROM `PROJECT_ID.DATASET_ID`.INFORMATION_SCHEMA.TABLES
WHERE table_name = "TABLE_NAME"'

# 2. Check total physical size and file count in the bucket
gsutil du -sh gs://BUCKET_NAME/TABLE_PATH/data
gsutil ls -l gs://BUCKET_NAME/TABLE_PATH/data/**

# 3. Inspect the latest Iceberg snapshot metadata
gsutil ls gs://BUCKET_NAME/TABLE_PATH/metadata/*.metadata.json | sort | tail -n 1
```

For a precise diff between physically present files and files referenced by the active snapshot, use `pyiceberg`:

```python
from pyiceberg.table import StaticTable

table = StaticTable.from_metadata("gs://BUCKET_NAME/TABLE_PATH/metadata/v<N>.metadata.json")
snapshot = table.current_snapshot()
active_files = {
    entry.data_file.file_path
    for manifest in snapshot.manifests(table.io)
    for entry in manifest.fetch_manifest_entry(table.io)
}
```

Any file present in the bucket but missing from `active_files` is a file still being retained purely because of the time travel window — this is the "duplicated" storage you're seeing before garbage collection runs.

## Billing Note

Automatic compaction and garbage collection for Iceberg managed tables is billed as **Data Compute Units (DCU)**, not as standard BigQuery storage cost <a href="https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#data-retention" target="_blank">[2]</a> <a href="https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#storage-billing" target="_blank">[5]</a>.

## Key Takeaways

- Deleted rows aren't physically removed instantly — their files persist for the full time travel window.
- Default time travel window is 7 days; it can be lowered to a minimum of 2 days to save storage cost sooner.
- Fail-safe (a fixed extra 7 days) applies to native BigQuery tables but **not** to Iceberg managed tables.
- For Iceberg managed tables, garbage collection happens immediately after the time travel window expires — no additional waiting period.
- Because Iceberg managed tables aren't covered by `INFORMATION_SCHEMA.TABLE_STORAGE` and show 0 bytes in the Console, physical storage must be verified directly against the GCS bucket.

## Conclusion of Stage 1
You have validated:
1. The structural differences between Parquet, Hive, and Iceberg.
2. The security model and Cloud Resource connection delegation in BigLake.
3. Converting unmanaged Hive Parquet files into a fully transactional BigLake Managed Iceberg table.
4. Understanding how BigLake Managed Iceberg table file is used and processed.

Proceed to **Stage 2: Simple Ingestion**:  
**[../02_simple_ingestion/04_simple_parquet_external_table.md](../02_simple_ingestion/04_simple_parquet_external_table.md)**

---

## References

1. <a href="https://cloud.google.com/bigquery/docs/time-travel" target="_blank">Google Cloud - Data retention with time travel and fail-safe</a>
2. <a href="https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#data-retention" target="_blank">Google Cloud - Manage BigLake Iceberg tables (Data Retention & Maintenance)</a>
3. <a href="https://cloud.google.com/bigquery/docs/datasets" target="_blank">Google Cloud - Create and manage datasets (Time travel window option)</a>
4. <a href="https://cloud.google.com/bigquery/docs/restore-deleted-tables" target="_blank">Google Cloud - Restore deleted tables</a>
5. <a href="https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#storage-billing" target="_blank">Google Cloud - BigLake managed table pricing and storage billing</a>
