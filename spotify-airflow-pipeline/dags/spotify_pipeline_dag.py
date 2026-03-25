import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Import your worker functions from the scripts folder
from scripts.extract_tracks import extract_spotify_data, load_to_s3

# Default settings for our DAG
default_args = {
    'owner': 'mcdesmond',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 15), 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'spotify_to_s3_pipeline',
    default_args=default_args,
    description='Extracts recent Spotify tracks and loads raw JSON to S3',
    schedule_interval='0 */3 * * *',  
    catchup=False
) as dag:

    # Task 1: Extract from Spotify
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_spotify_data,
    )

    # Task 2: Load to S3
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_s3,
    )

    # Task 3: Transfer raw JSON from AWS S3 over to Google Cloud Storage
    transfer_s3_to_gcs = S3ToGCSOperator(
        task_id='transfer_s3_to_gcs',
        bucket=os.getenv("S3_BUCKET_NAME"),   
        prefix='raw_data/',                 
        dest_gcs='gs://spotify-landing-zone/', 
        aws_conn_id='aws_default',          
        gcp_conn_id='gcp_default',          
        replace=True,
    )

    # Task 4: Load the JSON from GCS directly into BigQuery
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket='spotify-landing-zone',
        source_objects=['raw_data/*'],               
        destination_project_dataset_table='spotify_raw.listening_history', 
        source_format='NEWLINE_DELIMITED_JSON', 
        write_disposition='WRITE_APPEND',   
        autodetect=True,                    
        gcp_conn_id='gcp_default',
    )

    # Set the Dependencies 
    extract_task >> load_task >> transfer_s3_to_gcs >> load_gcs_to_bq