import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Import ALL worker functions from the scripts folder
from scripts.extract_tracks import extract_spotify_data, load_to_s3
from scripts.extract_artists import extract_spotify_artists, load_artists_to_s3

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 15), 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spotify_to_s3_pipeline',
    default_args=default_args,
    description='Extracts Spotify tracks and artists, loading them to S3, GCS, and BigQuery',
    schedule='0 */3 * * *',  
    catchup=False
) as dag:

    # ==========================================
    # TRACKS PIPELINE TASKS
    # ==========================================
    extract_tracks_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_spotify_data,
    )

    load_tracks_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_s3,
    )

    transfer_tracks_s3_to_gcs = S3ToGCSOperator(
        task_id='transfer_tracks_s3_to_gcs',
        bucket=os.getenv("S3_BUCKET_NAME"),   
        prefix='raw_data/tracks/',             # Keep tracks isolated
        dest_gcs='gs://spotify-landing-zone/tracks/', 
        aws_conn_id='aws_default',          
        gcp_conn_id='gcp_default',          
        replace=True,
    )

    load_tracks_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_tracks_gcs_to_bq',
        bucket='spotify-landing-zone',
        source_objects=['tracks/*'],               
        destination_project_dataset_table='spotify_raw.listening_history', 
        source_format='NEWLINE_DELIMITED_JSON', 
        write_disposition='WRITE_APPEND',   
        autodetect=True,                    
        gcp_conn_id='gcp_default',
    )

    # ==========================================
    # ARTISTS PIPELINE TASKS
    # ==========================================
    extract_artists_task = PythonOperator(
        task_id='extract_artists',
        python_callable=extract_spotify_artists,
    )

    load_artists_task = PythonOperator(
        task_id='load_artists_to_s3',
        python_callable=load_artists_to_s3,
    )

    transfer_artists_s3_to_gcs = S3ToGCSOperator(
        task_id='transfer_artists_s3_to_gcs',
        bucket=os.getenv("S3_BUCKET_NAME"),   
        prefix='raw_data/artists/',            # Target the artists folder
        dest_gcs='gs://spotify-landing-zone/artists/', 
        aws_conn_id='aws_default',          
        gcp_conn_id='gcp_default',          
        replace=True,
    )

    load_artists_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_artists_gcs_to_bq',
        bucket='spotify-landing-zone',
        source_objects=['artists/*'],               
        destination_project_dataset_table='spotify_raw.artists', # New BQ Table
        source_format='NEWLINE_DELIMITED_JSON', 
        write_disposition='WRITE_APPEND',   
        autodetect=True,                    
        gcp_conn_id='gcp_default',
    )

    # ==========================================
    # SET DEPENDENCIES (THE ARCHITECTURE)
    # ==========================================
    
    # 1. The Tracks Flow
    extract_tracks_task >> load_tracks_task >> transfer_tracks_s3_to_gcs >> load_tracks_gcs_to_bq
    
    # 2. The Artists Flow (Branches off the track extraction)
    extract_tracks_task >> extract_artists_task >> load_artists_task >> transfer_artists_s3_to_gcs >> load_artists_gcs_to_bq