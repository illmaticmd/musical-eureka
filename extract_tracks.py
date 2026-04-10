import os
import json
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from google.cloud import secretmanager, bigquery
from dotenv import load_dotenv

# -------------------------------------------------------------------
# CREDENTIALS
# Locally: reads from your .env file (ENV=local)
# GitHub Actions / Cloud: reads from Google Secret Manager
# -------------------------------------------------------------------

def get_secret(secret_id, project_id=None):
    """Pull a secret from Google Secret Manager."""
    project_id = project_id or os.environ["GCP_PROJECT_ID"]
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def load_credentials():
    """Load Spotify credentials from .env (local) or Secret Manager (cloud)."""
    if os.getenv("ENV") == "local":
        load_dotenv()
        return {
            "SPOTIPY_CLIENT_ID": os.getenv("SPOTIPY_CLIENT_ID"),
            "SPOTIPY_CLIENT_SECRET": os.getenv("SPOTIPY_CLIENT_SECRET"),
            "SPOTIPY_REDIRECT_URI": os.getenv("SPOTIPY_REDIRECT_URI"),
        }
    else:
        return {
            "SPOTIPY_CLIENT_ID": get_secret("SPOTIPY_CLIENT_ID"),
            "SPOTIPY_CLIENT_SECRET": get_secret("SPOTIPY_CLIENT_SECRET"),
            "SPOTIPY_REDIRECT_URI": get_secret("SPOTIPY_REDIRECT_URI"),
        }


# -------------------------------------------------------------------
# ARTIST CONFIG
# Swap this file out per client — or point to a Google Sheet later
# -------------------------------------------------------------------

def load_artist_config():
    """Load artist-specific config from artist_config.json."""
    config_path = os.path.join(os.path.dirname(__file__), "artist_config.json")
    with open(config_path, "r") as f:
        return json.load(f)


# -------------------------------------------------------------------
# EXTRACT
# -------------------------------------------------------------------

def extract_spotify_data(**kwargs):
    """Pulls recent tracks and formats the data as NDJSON for BigQuery."""
    print("Loading credentials...")
    creds = load_credentials()

    os.environ["SPOTIPY_CLIENT_ID"] = creds["SPOTIPY_CLIENT_ID"]
    os.environ["SPOTIPY_CLIENT_SECRET"] = creds["SPOTIPY_CLIENT_SECRET"]
    os.environ["SPOTIPY_REDIRECT_URI"] = creds["SPOTIPY_REDIRECT_URI"]

    print("Authenticating with Spotify...")
    cache_path = os.getenv("SPOTIPY_CACHE_PATH", ".cache")
    auth_manager = SpotifyOAuth(
        scope="user-read-recently-played",
        cache_path=cache_path
    )
    sp = spotipy.Spotify(auth_manager=auth_manager)

    # 1. Pull raw data from Spotify
    raw_response = sp.current_user_recently_played(limit=50)
    track_list = raw_response.get("items", [])
    print(f"Pulled {len(track_list)} tracks.")

    # 2. Clean up messy date fields
    for item in track_list:
        if "track" in item and "album" in item["track"]:
            raw_date = item["track"]["album"].get("release_date", "")
            if raw_date:
                item["track"]["album"]["release_year"] = raw_date[:4]
            item["track"]["album"]["release_date"] = ""
            item["track"]["album"]["release_date_precision"] = ""

    # 3. Convert to NDJSON string
    ndjson_string = "\n".join([json.dumps(track) for track in track_list])

    # Pass to XCom if running in Airflow, otherwise just return
    return ndjson_string


# -------------------------------------------------------------------
# LOAD DIRECTLY TO BIGQUERY
# -------------------------------------------------------------------

def load_to_bigquery(**kwargs):
    """Pulls track data from previous task and loads directly into BigQuery."""
    config = load_artist_config()

    # Pull NDJSON from XCom (Airflow) or call extract directly (standalone)
    if "ti" in kwargs:
        data = kwargs["ti"].xcom_pull(task_ids="extract_data")
    else:
        data = extract_spotify_data()

    if not data:
        print("No data to load.")
        return

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = config.get("bigquery_dataset", "spotify_data")
    table_id = config.get("bigquery_tracks_table", "tracks")
    full_table = f"{project_id}.{dataset_id}.{table_id}"

    print(f"Loading tracks to BigQuery table: {full_table}")

    bq_client = bigquery.Client(project=project_id)

    # Parse NDJSON into list of dicts
    rows = [json.loads(line) for line in data.strip().split("\n") if line.strip()]

    # Add ingestion timestamp to each row
    ingested_at = datetime.utcnow().isoformat()
    for row in rows:
        row["ingested_at"] = ingested_at

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )

    job = bq_client.load_table_from_json(rows, full_table, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Successfully loaded {len(rows)} tracks into {full_table}.")


# -------------------------------------------------------------------
# STANDALONE ENTRY POINT
# For testing outside of Airflow — just run: python extract_tracks.py
# -------------------------------------------------------------------

if __name__ == "__main__":
    load_to_bigquery()
