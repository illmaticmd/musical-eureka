WITH tracks AS (
    SELECT DISTINCT
        track.id AS track_id,
        track.name AS track_name,
        track.popularity AS popularity,
        played_at,
        track.artists[SAFE_OFFSET(0)].id AS primary_artist_id
    FROM `spotify-data-pipeline-490402.spotify_raw.listening_history`
),

artists AS (
    SELECT DISTINCT
        a.artist_id,
        a.artist_name,
        -- First try Spotify's own genre
        -- Then fall back to inferred genre from related artists
        -- Then fall back to Unknown
        COALESCE(
            NULLIF(a.genres[SAFE_OFFSET(0)], ''),
            e.inferred_genre,
            'Unknown'
        ) AS primary_genre
    FROM `spotify-data-pipeline-490402.spotify_raw.artists` a
    LEFT JOIN `spotify-data-pipeline-490402.spotify_raw.artist_genre_enrichment` e
        ON a.artist_id = e.artist_id
)

SELECT
    t.played_at,
    t.track_id,
    t.track_name,
    a.artist_name,
    a.primary_genre,
    t.popularity
FROM tracks t
LEFT JOIN artists a
    ON t.primary_artist_id = a.artist_id
