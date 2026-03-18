WITH artist_albums AS (
    SELECT 
        a.name AS artist_name,
        count(ab.album_name) AS album_count,
        ROUND(COALESCE(avg(ab.play_count::int), 0), 2) AS avg_plays_per_album
    FROM {{ ref('stg_pg__topartists') }} a 
    LEFT JOIN {{ ref('stg_pg__topalbums') }} ab ON a.name = ab.artist_name
    GROUP BY a.name
)

SELECT 
    artist_name,
    album_count,
    avg_plays_per_album,
    CASE 
        WHEN album_count >= 3 THEN 'deep'
        WHEN album_count = 2 THEN 'moderate'
        ELSE 'surface'
    END AS listening_depth
FROM artist_albums