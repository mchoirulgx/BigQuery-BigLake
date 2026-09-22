from datetime import datetime, timezone
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryDataTransferServiceStartTransferRunsOperator
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.types as types
import gcsfs

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'gcs_parquet_to_iceberg_dts_orchestration',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    @task
    def check_and_evolve_schema(bucket: str, prefix: str, project_id: str, dataset_id: str, table_id: str):
        
        # Helper function to map PyArrow types to BigQuery types
        def map_pyarrow_to_bq_type(pa_type):
            if types.is_string(pa_type) or types.is_large_string(pa_type):
                return "STRING"
            elif types.is_integer(pa_type):
                return "INT64"
            elif types.is_floating(pa_type):
                return "FLOAT64"
            elif types.is_boolean(pa_type):
                return "BOOL"
            elif types.is_timestamp(pa_type):
                return "TIMESTAMP"
            elif types.is_date(pa_type):
                return "DATE"
            elif types.is_decimal(pa_type):
                return "NUMERIC"
            else:
                # Fallback for complex types (structs, lists) or unknown types
                return "STRING" 

        # 1. Read the Parquet schema from the GCS landing zone
        fs = gcsfs.GCSFileSystem()
        files = fs.glob(f"gs://{bucket}/{prefix}*.parquet")
        if not files:
            print("No new files to process.")
            return
            
        # Read the metadata of the first file
        parquet_schema = pq.read_schema(f"gs://{files[0]}")
        parquet_cols = set(parquet_schema.names)
        
        # 2. Read the current BigQuery Iceberg table schema
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        client = bq_hook.get_client(project_id=project_id)
        table_ref = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        bq_cols = {field.name for field in table_ref.schema}
        
        # 3. Compare schemas and generate DDL if new columns exist
        new_cols = parquet_cols - bq_cols
        if new_cols:
            alter_statements = []
            for col in new_cols:
                # Get the PyArrow type for the specific column
                pa_type = parquet_schema.field(col).type
                # Map it to BigQuery type
                bq_type = map_pyarrow_to_bq_type(pa_type)
                alter_statements.append(f"ADD COLUMN {col} {bq_type}")
                
            query = f"ALTER TABLE `{project_id}.{dataset_id}.{table_id}` {', '.join(alter_statements)}"
            
            # Execute the DDL to evolve the Iceberg schema
            client.query_and_wait(query)
            print(f"Schema updated successfully: {query}")
        else:
            print("Schemas match. No DDL required.")

    # Task 1: Check and alter table if needed
    schema_task = check_and_evolve_schema(
        bucket='biglake_data', 
        prefix='dts_landing_zone_test_modified_table/',
        project_id='imposing-league-374504',
        dataset_id='bqlake_data',
        table_id='test_order_table'
    )

    # Task 2: Trigger the BigQuery Data Transfer Service
    trigger_dts = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="trigger_dts_transfer",
        project_id='imposing-league-374504',
        transfer_config_id="6aaa2142-0000-2f1f-b86a-34c7e918f963", 
        location='asia-southeast2',
        requested_run_time={"seconds": int(datetime.now(timezone.utc).timestamp())},
    )

    schema_task >> trigger_dts