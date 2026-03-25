import os
import json
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import boto3

def extract_spotify_artists(**kwargs):
    """Parses track data for Artist IDs, fetches genres, and formats as NDJSON."""
    ti = kwargs['ti']
    
    # 1. Pull the raw track data passed from the extract_data task
    track_ndjson = ti.xcom_pull(task_ids='extract_data')
    
    # 2. Parse the NDJSON to get a list of unique artist IDs
    artist_ids = set()
    for line in track_ndjson.split('\n'):
        if line.strip():
            # spotipy's recently played payload has a 'track' dictionary
            item_data = json.loads(line)
            track = item_data.get('track', {})
            
            # Loop through all artists on the track and grab their IDs
            for artist in track.get('artists', []):
                if 'id' in artist and artist['id'] is not None:
                    artist_ids.add(artist['id'])
                
    # Convert the set back to a list
    artist_ids = list(artist_ids)
    print(f"Found {len(artist_ids)} unique artists.")
    
    # 3. Authenticate with Spotify
    cache_path = '/opt/airflow/dags/.cache' 
    auth_manager = SpotifyOAuth(scope="user-read-recently-played", cache_path=cache_path)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    
    # 4. Fetch artist details in batches of 50 (Spotify API limit)
    extracted_artists = []
    for i in range(0, len(artist_ids), 50):
        batch = artist_ids[i:i + 50]
        artists_response = sp.artists(batch)
        
        for artist in artists_response['artists']:
            if artist:
                artist_info = {
                    "artist_id": artist['id'],
                    "artist_name": artist['name'],
                    "genres": artist['genres']  # This remains a list of strings
                }
                extracted_artists.append(artist_info)
                
    # 5. Convert to NDJSON string for BigQuery
    artists_ndjson = "\n".join([json.dumps(artist) for artist in extracted_artists])
    return artists_ndjson


def load_artists_to_s3(**kwargs):
    """Pulls artist data from the previous task and uploads it to AWS S3."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_artists')
    
    bucket_name = os.getenv("S3_BUCKET_NAME")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"spotify_artists_{timestamp}.json"
    
    print(f"Uploading {file_name} to S3...")
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    
    # Notice we put this in a different folder: raw_data/artists/
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"raw_data/artists/{file_name}",
        Body=data  
    )
    print("Artist upload complete!")