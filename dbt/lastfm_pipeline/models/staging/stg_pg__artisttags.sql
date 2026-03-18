SELECT
    artist_name,
    tag_name,
    count
FROM {{ source('pg', 'artisttags') }}