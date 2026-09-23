# Stage 1: Concept — Why BigLake and Apache Iceberg?
## Part 1.2: Setting Up BigLake Connection & IAM Security

> **Section Overview:**  
> BigLake allows BigQuery to access data stored in Google Cloud Storage (GCS) securely using a centralized Cloud Resource Connection [1](https://cloud.google.com/bigquery/docs/create-cloud-resource-connections). This eliminates the need to distribute individual user keys or open up public bucket permissions. This guide walks through setting up the connection, configuring the necessary IAM roles for both external tables and managed Iceberg tables, and resolving common setup errors.

---

## Step 1: Creating a BigLake Connection

1. Open the **BigQuery Console**.
2. In the Explorer panel, click **+ ADD** -> choose **Connections to external data sources**.
3. In the **Connection type** dropdown, select:
   **Lakehouse, Cloud Resource, remote models and Spanner**
4. Set the **Location type** to **Region**.
5. Select your primary operational region (e.g., `asia-southeast2` for Jakarta, or `us-central1`) matching your GCS bucket to avoid cross-region egress latency and costs [2](https://cloud.google.com/bigquery/docs/locations).
6. Set the **Connection ID** (e.g., `biglake-data-connection`).
7. Click **Create Connection**.
8. Open the newly created connection in the Explorer panel, then copy the generated **Service Account ID** (ending with `@gcp-sa-bigquery-condel.iam.gserviceaccount.com`) [1](https://cloud.google.com/bigquery/docs/create-cloud-resource-connections).

---

## Step 2: IAM (Identity and Access Management) Configuration

The Service Account associated with the BigLake connection must be granted appropriate permissions on the target GCS bucket.

> [!IMPORTANT]
> **Crucial IAM Distinction: External Tables vs. BigLake Managed Iceberg Tables**
> - **Read-Only External Tables**: Granting **Storage Object Viewer** (`roles/storage.objectViewer`) is sufficient because BigQuery only reads Parquet files.
> - **BigLake Managed Iceberg Tables (`table_format = 'ICEBERG'`)**: BigQuery acts as the catalog and storage manager. When executing DDL (`CREATE TABLE ... AS SELECT`), DML (`INSERT`, `UPDATE`, `DELETE`, `MERGE`), or DTS ingestion, BigQuery must create, overwrite, and delete Parquet files and Iceberg metadata manifests (`.metadata.json`, `.avro`).  
>   Therefore, the connection service account **must be granted `roles/storage.objectAdmin`** (or `roles/storage.objectUser`) on the bucket or data folder [3](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#grant-storage-access).

### Steps to Grant Permissions on GCS:
1. Open the **Cloud Storage > Buckets** page.
2. Select your target bucket (e.g., `gs://<YOUR_BUCKET_NAME>`).
3. Go to the **Permissions** tab and click **+ GRANT ACCESS**.
4. Paste the **Connection Service Account ID** (`@gcp-sa-bigquery-condel.iam.gserviceaccount.com`) into *New principals*.
5. Select the role:
   - For Managed Iceberg tables: **Cloud Storage** -> **Storage Object Admin** (`roles/storage.objectAdmin`) [3](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#grant-storage-access).
   - For read-only external tables: **Cloud Storage** -> **Storage Object Viewer** (`roles/storage.objectViewer`).
6. Click **Save**.
   *(Note: IAM propagation typically takes 1–2 minutes before BigQuery can utilize the new permissions).*

### Granting Connection Access to Airflow / Users:
Any user, service account, or Airflow worker executing queries with `WITH CONNECTION` must be granted the **BigQuery Connection User** role (`roles/bigquery.connectionUser`) on the connection resource [4](https://cloud.google.com/bigquery/docs/create-cloud-resource-connections#authorize_the_connection):
```bash
gcloud resource-manager bindings add \
    --project="<YOUR_PROJECT_ID>" \
    --member="serviceAccount:<AIRFLOW_OR_USER_SA>@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
    --role="roles/bigquery.connectionUser"
```

---

## ⚠️ Common Setup Troubleshooting

### Error 1: Invalid Connection Name Format
**Error Message:** 
```text
Connection name should conform to the pattern: projects/{project_id=*}/locations/{location_id=*}/connections/{connection_id=*}
```

**Cause:** 
In BigQuery DDL queries, referencing the connection by its ID alone without the region prefix is insufficient [5](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_statement).

**Solution:**
Include the project and location prefix (or at least `region.connection_id`) in the `WITH CONNECTION` clause:
* *Incorrect:* `WITH CONNECTION 'biglake-data-connection'`
* *Correct:* `WITH CONNECTION 'asia-southeast2.biglake-data-connection'`
* *Fully Qualified:* `WITH CONNECTION 'projects/<PROJECT_ID>/locations/asia-southeast2/connections/biglake-data-connection'`

---

### Error 2: Permission Denied (Globbing File Pattern)
**Error Message:** 
```text
Access Denied: Permission denied while globbing file pattern. <service-account> does not have storage.objects.list access...
```

**Cause:** 
The BigLake connection service account has not been granted permission on the target GCS bucket, or the IAM binding has not propagated yet.

**Solution:**
Follow **Step 2** above to assign the **Storage Object Admin** (or **Storage Object Viewer**) role at the bucket level. Wait 1–2 minutes and retry.

---

### Error 3: Access Denied to BigLake Managed Table (Write / DML Failure)
**Error Message:** 
```text
Access Denied: Service account does not have storage.objects.create or storage.objects.delete access...
```

**Cause:** 
The connection service account was only assigned `Storage Object Viewer` on a table configured with `table_format = 'ICEBERG'`.

**Solution:**
Upgrade the bucket role for the connection service account to **Storage Object Admin** (`roles/storage.objectAdmin`).

---

## Next Steps in Stage 1:
Now that the BigLake connection is active and IAM permissions are configured, test it with Google Cloud's public NYC Taxi dataset:  
**[03_poc_hive_to_managed_iceberg.md](03_poc_hive_to_managed_iceberg.md)**

---

## References

1. [Google Cloud BigQuery: Create and manage Cloud Resource connections](https://cloud.google.com/bigquery/docs/create-cloud-resource-connections)
2. [Google Cloud BigQuery: Dataset locations and cross-region considerations](https://cloud.google.com/bigquery/docs/locations)
3. [Google Cloud BigQuery: Apache Iceberg managed tables — Grant Storage Access](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#grant-storage-access)
4. [Google Cloud BigQuery: Authorize users to use Cloud Resource connections](https://cloud.google.com/bigquery/docs/create-cloud-resource-connections#authorize_the_connection)
5. [Google Cloud BigQuery: BigQuery DDL syntax for external and BigLake tables](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_statement)
