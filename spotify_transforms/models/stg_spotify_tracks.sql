WITH raw_data AS (
    SELECT * FROM `spotify-data-pipeline-490402.spotify_raw.listening_history`
)

-- Adding DISTINCT here drops the duplicate rows caused by overlapping Airflow runs
SELECT DISTINCT
    track.id AS track_id,
    track.name AS track_name,
    -- Extract the first artist's name from the nested array
    track.artists[SAFE_OFFSET(0)].name AS primary_artist,
    track.album.name AS album_name,
    track.popularity AS popularity_score,
    track.duration_ms AS duration_ms,
    -- Cast the played_at string to a proper timestamp
    CAST(played_at AS TIMESTAMP) AS played_at_timestamp
FROM 
    raw_data
-- Filter out any null records just in case
WHERE 
    track.id IS NOT NULL