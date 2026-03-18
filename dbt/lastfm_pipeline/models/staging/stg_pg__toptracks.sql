SELECT 
    title,
    artist,
    duration,
    play_count
FROM {{ source('pg', 'toptracks' )}}