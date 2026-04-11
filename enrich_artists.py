import os
import json
from google.cloud import bigquery
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
from google.cloud import secretmanager


def get_secret(secret_id, project_id=None):
    project_id = project_id or os.environ["GCP_PROJECT_ID"]
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def load_credentials():
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
    auth_manager = SpotifyOAuth(
        client_id=creds["SPOTIPY_CLIENT_ID"],
        client_secret=creds["SPOTIPY_CLIENT_SECRET"],
        redirect_uri=creds["SPOTIPY_REDIRECT_URI"],
        scope="user-read-recently-played",
    )
    token_info = auth_manager.refresh_access_token(creds["SPOTIPY_REFRESH_TOKEN"])
    return spotipy.Spotify(auth=token_info["access_token"])


def enrich_unknown_artists():
    project_id = os.getenv("GCP_PROJECT_ID")
    bq_client = bigquery.Client(project=project_id)

    # 1. Pull all artists with no genres from BigQuery
    query = f"""
        SELECT DISTINCT artist_id, artist_name
        FROM `{project_id}.spotify_raw.artists`
        WHERE ARRAY_LENGTH(genres) = 0
    """
    unknown_artists = list(bq_client.query(query).result())
    print(f"Found {len(unknown_artists)} artists with no genres.")

    if not unknown_artists:
        print("No unknown artists to enrich.")
        return

    # 2. Connect to Spotify
    creds = load_credentials()
    sp = get_spotify_client(creds)

    # 3. For each unknown artist infer genre from related artists
    enriched = []
    for row in unknown_artists:
        artist_id = row["artist_id"]
        artist_name = row["artist_name"]

        try:
            related = sp.artist_related_artists(artist_id)

            # Collect all genres from related artists
            all_genres = []
            for related_artist in related["artists"]:
                all_genres.extend(related_artist.get("genres", []))

            if all_genres:
                # Pick the most common genre
                inferred_genre = max(set(all_genres), key=all_genres.count)
                print(f"{artist_name} → inferred genre: {inferred_genre}")
            else:
                inferred_genre = None
                print(f"{artist_name} → no genre found from related artists")

            enriched.append({
                "artist_id": artist_id,
                "inferred_genre": inferred_genre
            })

        except Exception as e:
            print(f"Error enriching {artist_name}: {e}")
            continue

    # 4. Save inferred genres to a new BigQuery table
    if enriched:
        table = f"{project_id}.spotify_raw.artist_genre_enrichment"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )
        job = bq_client.load_table_from_json(enriched, table, job_config=job_config)
        job.result()
        print(f"Saved {len(enriched)} enriched genres to {table}")


if __name__ == "__main__":
    enrich_unknown_artists()
