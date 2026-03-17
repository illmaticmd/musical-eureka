import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import boto3

def extract_spotify_data(**kwargs):
    """Pulls recent tracks and formats the data as NDJSON for BigQuery."""
    print("Authenticating with Spotify...")
    # Point to the cache file we placed in the dags folder
    cache_path = '/opt/airflow/dags/.cache' 
    auth_manager = SpotifyOAuth(scope="user-read-recently-played", cache_path=cache_path)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    
    # 1. Pull the raw nested dictionary from Spotify
    raw_response = sp.current_user_recently_played(limit=50)
    
    # 2. Extract ONLY the list of tracks (ignoring API metadata)
    track_list = raw_response.get('items', [])
    print(f"Pulled {len(track_list)} tracks.")
    
    # 3. Convert that list into a single NDJSON formatted string
    ndjson_string = "\n".join([json.dumps(track) for track in track_list])
    
    # 4. Return the NDJSON string to XCom for your S3 upload task
    return ndjson_string

def load_to_s3(**kwargs):
    """Pulls data from the previous task and uploads it to AWS S3."""
    # Pull the NDJSON string passed from the extract task using XCom
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    
    bucket_name = os.getenv("S3_BUCKET_NAME")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"spotify_history_{timestamp}.json"
    
    print(f"Uploading {file_name} to S3...")
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"raw_data/{file_name}",
        Body=data  # <--- CHANGED: Just pass the raw NDJSON string here
    )
    print("Upload complete!")


# Default settings for our DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 15), # Start today
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spotify_to_s3_pipeline',
    default_args=default_args,
    description='Extracts recent Spotify tracks and loads raw JSON to S3',
    schedule='0 */3 * * *',  # <--- Runs at minute 0 past every 3rd hour
    catchup=False
)


# Define the Tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_spotify_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_to_s3,
    dag=dag,
)

#Transfer raw JSON from AWS S3 over to Google Cloud Storage
transfer_s3_to_gcs = S3ToGCSOperator(
    task_id='transfer_s3_to_gcs',
    bucket=os.getenv("S3_BUCKET_NAME"),   # Replace with your S3 bucket
    prefix='raw_data/',                 # The folder in S3 where your JSONs live (or leave blank for root)
    dest_gcs='gs://spotify-landing-zone/', # Replace with your new GCS bucket
    aws_conn_id='aws_default',          # Assuming you set this up for S3 earlier
    gcp_conn_id='gcp_default',          # The GCP connection you just made
    replace=True,
    dag=dag
)

#Load the JSON from GCS directly into BigQuery
load_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_gcs_to_bq',
    bucket='spotify-landing-zone',
    source_objects=['raw_data/*'],               # Grabs everything that just landed in the bucket
    destination_project_dataset_table='spotify_raw.listening_history', # dataset.table
    source_format='NEWLINE_DELIMITED_JSON', # BigQuery's preferred JSON format
    write_disposition='WRITE_APPEND',   # Adds new data to the bottom of the table
    autodetect=True,                    # Automatically figures out the schema from the JSON
    gcp_conn_id='gcp_default',
    dag=dag
)









# Set the Dependencies (This is what makes it a pipeline!)
extract_task >> load_task >> transfer_s3_to_gcs >> load_gcs_to_bq