import os
import json
import requests
import time
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


def get_genre_from_spotify(sp, artist_id, artist_name):
    """Try to get genre from Spotify search."""
    try:
        results = sp.search(q=f"artist:{artist_name}", type="artist", limit=5)
        artists_found = results["artists"]["items"]
        for found_artist in artists_found:
            if found_artist["id"] == artist_id:
                if found_artist.get("genres"):
                    return found_artist["genres"][0]
            if found_artist["name"].lower() == artist_name.lower():
                if found_artist.get("genres"):
                    return found_artist["genres"][0]
    except Exception as e:
        print(f"Spotify search error for {artist_name}: {e}")
    return None


def get_genre_from_musicbrainz(artist_name):
    """Fall back to MusicBrainz for genre lookup."""
    try:
        # MusicBrainz requires a user agent
        headers = {"User-Agent": "SpotifyPipeline/1.0 (mcdesmondd@gmail.com)"}
        url = f"https://musicbrainz.org/ws/2/artist/?query=artist:{requests.utils.quote(artist_name)}&fmt=json&limit=3"
        response = requests.get(url, headers=headers)
        time.sleep(1)  # MusicBrainz rate limit — 1 request per second

        if response.status_code == 200:
            data = response.json()
            artists = data.get("artists", [])
            for artist in artists:
                # Check name matches closely
                if artist.get("name", "").lower() == artist_name.lower():
                    tags = artist.get("tags", [])
                    if tags:
                        # Tags are sorted by vote count — take the top one
                        top_tag = sorted(tags, key=lambda x: x.get("count", 0), reverse=True)[0]
                        return top_tag["name"]
    except Exception as e:
        print(f"MusicBrainz error for {artist_name}: {e}")
    return None


def enrich_unknown_artists():
    project_id = os.getenv("GCP_PROJECT_ID")
    bq_client = bigquery.Client(project=project_id)

    # 1. Pull artists with no genres
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

    creds = load_credentials()
    sp = get_spotify_client(creds)

    enriched = []
    for row in unknown_artists:
        artist_id = row["artist_id"]
        artist_name = row["artist_name"]

        # Try Spotify first
        genre = get_genre_from_spotify(sp, artist_id, artist_name)

        # Fall back to MusicBrainz
        if not genre:
            genre = get_genre_from_musicbrainz(artist_name)
            if genre:
                print(f"{artist_name} → {genre} (MusicBrainz)")
            else:
                print(f"{artist_name} → no genre found")
        else:
            print(f"{artist_name} → {genre} (Spotify)")

        enriched.append({
            "artist_id": artist_id,
            "inferred_genre": genre
        })

    # 2. Save to BigQuery
    if enriched:
        table = f"{project_id}.spotify_raw.artist_genre_enrichment"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        job = bq_client.load_table_from_json(enriched, table, job_config=job_config)
        job.result()
        print(f"Saved {len(enriched)} enriched genres to {table}")


if __name__ == "__main__":
    enrich_unknown_artists()