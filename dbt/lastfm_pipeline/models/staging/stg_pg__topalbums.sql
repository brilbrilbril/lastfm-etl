SELECT 
    artist_name,
    album_name,
    play_count,
    rank
FROM {{ source('pg', 'topalbums') }}