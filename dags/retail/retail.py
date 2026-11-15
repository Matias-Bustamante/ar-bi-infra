from airflow import DAG 
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator 
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.standard.operators.python import  ShortCircuitOperator
from datetime import datetime, timedelta
import json
from pathlib import Path 
import os 
from services.gcp.config import ( 
    BUCKET_NAME, 
    SOURCE_OBJECTS, 
    PROCESSED_PREFIX, 
    GCP_CONN_ID
)


def check_file_bucket()->bool: 
    hook=GCSHook(gcp_conn_id=GCP_CONN_ID)
    return hook.exists(bucket_name=BUCKET_NAME, object_name=SOURCE_OBJECTS)

def _resolve_data_dir() -> Path:
    env_dir = os.environ.get("AIRFLOW_DATA_DIR")
    if env_dir:
        return Path(env_dir)
    composer_dir = Path("/home/airflow/gcs/data")
    if composer_dir.exists():
        return composer_dir
    return Path("/home/airflow/data")

DATA_DIR = _resolve_data_dir()
SCHEMA_PATH = DATA_DIR / "schema" / "retail" / "schema.json"
with SCHEMA_PATH.open(encoding="utf-8") as f: 
    schema=json.load(f)

default_args= { 
    "owner":"matias", 
    "depends_on_past":False, 
    "email_on_failure": False, 
    "retries": 1, 
    "retry_delay":timedelta(seconds=30) 
}

with DAG(
dag_id="local_to_gcs", 
default_args=default_args, 
description="Insertar datos a cloud storage", 
start_date=datetime(2026,11,15), 
schedule=None, 
catchup=False,  
max_active_runs=1, 
dagrun_timeout=timedelta(minutes=1), 
tags=["csv", "retail"]
) as dag: 

    

    check_file_in_bucket=ShortCircuitOperator(
            task_id='check_file_in_bucket', 
            python_callable=check_file_bucket, 
        )


    gcs_to_bigquery=GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery", 
        bucket="retail-online", 
        source_objects=["retail-online/online_retail"], 
        gcp_conn_id="google_cloud_default", 
        destination_project_dataset_table="data-engineer-509817.retail_online.stg_retail_online",
        schema_fields=schema, 
        autodetect=False, 
        source_format="CSV", 
        skip_leading_rows=1, 
        write_disposition="WRITE_TRUNCATE"
    )
    archive_file=GCSToGCSOperator(
            task_id="chek_file", 
            gcp_conn_id=GCP_CONN_ID, 
            source_bucket=BUCKET_NAME, 
            source_object=SOURCE_OBJECTS, 
            destination_bucket=BUCKET_NAME, 
            destination_object=f"{PROCESSED_PREFIX}/online_retail_{{{{ts_nodash}}}}.csv", 
            move_object=True 
        )
    

    check_file_in_bucket >>gcs_to_bigquery >> archive_file