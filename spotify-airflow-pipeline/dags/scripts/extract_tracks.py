import os
import json
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import boto3

def extract_spotify_data(**kwargs):
    """Pulls recent tracks and formats the data as NDJSON for BigQuery."""
    print("Authenticating with Spotify...")
    cache_path = '/opt/airflow/dags/.cache' 
    auth_manager = SpotifyOAuth(scope="user-read-recently-played", cache_path=cache_path)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    
    # 1. Pull the raw nested dictionary from Spotify
    raw_response = sp.current_user_recently_played(limit=50)
    track_list = raw_response.get('items', [])
    print(f"Pulled {len(track_list)} tracks.")
    
    # 2. DROP THE NOISY SCHEMA-DRIFT COLUMNS
    for item in track_list:
        if 'track' in item and 'album' in item['track']:
            # This safely removes the release date fields from the dictionary
            item['track']['album'].pop('release_date', None)
            item['track']['album'].pop('release_date_precision', None)
    
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
        Key=f"raw_data/tracks/{file_name}",
        Body=data  
    )
    print("Upload complete!")