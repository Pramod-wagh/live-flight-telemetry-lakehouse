"""
Airflow DAG: Live Flight Telemetry Lakehouse Pipeline
Extracts live flights from OpenSky Network API, loads raw micro-batch to MinIO,
and triggers PySpark Iceberg transformation + PostgreSQL serving sync.
"""

from datetime import datetime, timedelta
import json
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from minio import Minio
import io

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

def extract_and_upload_raw_to_minio(**context):
    """Fetch live aircraft states from OpenSky API and store raw JSON in MinIO bronze bucket."""
    execution_dt = context.get('logical_date') or datetime.utcnow()
    timestamp_str = execution_dt.strftime('%Y%m%d_%H%M%S')
    filename = f"flights_raw_{timestamp_str}.json"
    
    # 1. Fetch from OpenSky Network API
    url = "https://opensky-network.org/api/states/all"
    headers = {'User-Agent': 'FlightDataEngineeringPipeline/1.0'}
    
    print(f"Requesting live flight states from {url}...")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    payload = response.json()
    
    raw_data_bytes = json.dumps(payload).encode('utf-8')
    data_stream = io.BytesIO(raw_data_bytes)
    data_length = len(raw_data_bytes)
    
    # 2. Connect to MinIO
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    minio_client = Minio(
        minio_endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "adminpassword123"),
        secure=False
    )
    
    bucket_name = os.getenv("MINIO_RAW_BUCKET", "flight-raw")
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    
    # Upload timestamped raw batch
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=filename,
        data=data_stream,
        length=data_length,
        content_type="application/json"
    )
    print(f"Successfully uploaded raw data to MinIO: s3a://{bucket_name}/{filename}")
    
    # Also upload as latest.json for convenience
    data_stream.seek(0)
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name="latest.json",
        data=data_stream,
        length=data_length,
        content_type="application/json"
    )
    
    # Pass s3 URI to downstream task
    raw_s3_path = f"s3a://{bucket_name}/{filename}"
    context['ti'].xcom_push(key='raw_s3_path', value=raw_s3_path)
    return raw_s3_path

with DAG(
    dag_id='live_flight_telemetry_pipeline',
    default_args=default_args,
    description='End-to-End Live Flight Data Lakehouse with OpenSky, MinIO, Iceberg, and Postgres',
    schedule_interval='*/5 * * * *', # runs every 5 minutes
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: Ingest live flight states into MinIO bronze storage
    fetch_raw_flights_task = PythonOperator(
        task_id='extract_and_upload_raw_to_minio',
        python_callable=extract_and_upload_raw_to_minio,
        provide_context=True,
    )

    # Task 2: PySpark Iceberg + Postgres Serving Sync
    spark_iceberg_task = BashOperator(
        task_id='pyspark_iceberg_and_postgres_sync',
        bash_command="""
        RAW_PATH="{{ ti.xcom_pull(task_ids='extract_and_upload_raw_to_minio', key='raw_s3_path') }}"
        python /opt/spark-apps/ingest_flight_telemetry.py "$RAW_PATH"
        """,
    )

    fetch_raw_flights_task >> spark_iceberg_task
