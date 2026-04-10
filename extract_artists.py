import os
import json
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from google.cloud import secretmanager, bigquery
from dotenv import load_dotenv

# -------------------------------------------------------------------
# CREDENTIALS
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
            "SPOTIPY_REFRESH_TOKEN": os.getenv("SPOTIPY_REFRESH_TOKEN"),
        }
    else:
        return {
            "SPOTIPY_CLIENT_ID": get_secret("SPOTIPY_CLIENT_ID"),
            "SPOTIPY_CLIENT_SECRET": get_secret("SPOTIPY_CLIENT_SECRET"),
            "SPOTIPY_REDIRECT_URI": get_secret("SPOTIPY_REDIRECT_URI"),
            "SPOTIPY_REFRESH_TOKEN": get_secret("SPOTIPY_REFRESH_TOKEN"),
        }


def get_spotify_client(creds):
    """Create a Spotify client using refresh token — no browser needed."""
    auth_manager = SpotifyOAuth(
        client_id=creds["SPOTIPY_CLIENT_ID"],
        client_secret=creds["SPOTIPY_CLIENT_SECRET"],
        redirect_uri=creds["SPOTIPY_REDIRECT_URI"],
        scope="user-read-recently-played",
    )
    token_info = auth_manager.refresh_access_token(creds["SPOTIPY_REFRESH_TOKEN"])
    return spotipy.Spotify(auth=token_info["access_token"])


# -------------------------------------------------------------------
# ARTIST CONFIG
# -------------------------------------------------------------------

def load_artist_config():
    """Load artist-specific config from artist_config.json."""
    config_path = os.path.join(os.path.dirname(__file__), "artist_config.json")
    with open(config_path, "r") as f:
        return json.load(f)


# -------------------------------------------------------------------
# EXTRACT
# -------------------------------------------------------------------

def extract_spotify_artists(**kwargs):
    """Parses track data for Artist IDs, fetches genres, formats as NDJSON."""

    # Pull track data from XCom (Airflow) or run extract directly (standalone)
    if "ti" in kwargs:
        track_ndjson = kwargs["ti"].xcom_pull(task_ids="extract_data")
    else:
        # Import inline to avoid circular imports when running standalone
        from extract_tracks import extract_spotify_data
        track_ndjson = extract_spotify_data()

    if not track_ndjson:
        print("No track data found.")
        return ""

    # 1. Parse NDJSON to get unique artist IDs
    artist_ids = set()
    for line in track_ndjson.split("\n"):
        if line.strip():
            item_data = json.loads(line)
            track = item_data.get("track", {})
            for artist in track.get("artists", []):
                if "id" in artist and artist["id"] is not None:
                    artist_ids.add(artist["id"])

    artist_ids = list(artist_ids)
    print(f"Found {len(artist_ids)} unique artists.")

    # 2. Authenticate with Spotify using refresh token
    print("Loading credentials...")
    creds = load_credentials()
    sp = get_spotify_client(creds)

    # 3. Fetch artist details in batches of 50 (Spotify API limit)
    extracted_artists = []
    for i in range(0, len(artist_ids), 50):
        batch = artist_ids[i:i + 50]
        artists_response = sp.artists(batch)

        for artist in artists_response["artists"]:
            if artist:
                artist_info = {
                    "artist_id": artist["id"],
                    "artist_name": artist["name"],
                    "genres": artist["genres"],
                    "followers": artist.get("followers", {}).get("total", 0),
                    "popularity": artist.get("popularity", 0),
                }
                extracted_artists.append(artist_info)

    # 4. Convert to NDJSON string
    artists_ndjson = "\n".join([json.dumps(artist) for artist in extracted_artists])
    return artists_ndjson


# -------------------------------------------------------------------
# LOAD DIRECTLY TO BIGQUERY
# -------------------------------------------------------------------

def load_artists_to_bigquery(**kwargs):
    """Pulls artist data from previous task and loads directly into BigQuery."""
    config = load_artist_config()

    # Pull from XCom (Airflow) or call extract directly (standalone)
    if "ti" in kwargs:
        data = kwargs["ti"].xcom_pull(task_ids="extract_artists")
    else:
        data = extract_spotify_artists()

    if not data:
        print("No artist data to load.")
        return

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = config.get("bigquery_dataset", "spotify_data")
    table_id = config.get("bigquery_artists_table", "artists")
    full_table = f"{project_id}.{dataset_id}.{table_id}"

    print(f"Loading artists to BigQuery table: {full_table}")

    bq_client = bigquery.Client(project=project_id)

    # Parse NDJSON into list of dicts
    rows = [json.loads(line) for line in data.strip().split("\n") if line.strip()]

    # Add ingestion timestamp
    ingested_at = datetime.utcnow().isoformat()
    for row in rows:
        row["ingested_at"] = ingested_at

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )

    job = bq_client.load_table_from_json(rows, full_table, job_config=job_config)
    job.result()

    print(f"Successfully loaded {len(rows)} artists into {full_table}.")


# -------------------------------------------------------------------
# STANDALONE ENTRY POINT
# For testing outside of Airflow — just run: python extract_artists.py
# -------------------------------------------------------------------

if __name__ == "__main__":
    load_artists_to_bigquery()
