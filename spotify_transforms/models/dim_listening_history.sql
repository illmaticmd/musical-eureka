WITH tracks AS (
    SELECT 
        -- Extracting basic track info
        track.id AS track_id,
        track.name AS track_name,
        track.popularity AS popularity,
        played_at,
        
        -- Spotify tracks can have multiple artists. We grab the ID of the first (primary) artist.
        track.artists[SAFE_OFFSET(0)].id AS primary_artist_id
        
    FROM `your_project_id.spotify_raw.listening_history`
),

artists AS (
    SELECT 
        artist_id,
        artist_name,
        
        -- Artists have multiple genres. We grab the first one. 
        -- If they have no genres, we label it 'Unknown' instead of leaving a null blank.
        COALESCE(genres[SAFE_OFFSET(0)], 'Unknown') AS primary_genre
        
    FROM `your_project_id.spotify_raw.artists`
)

-- Join them together to create the final, flat table for Power BI
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