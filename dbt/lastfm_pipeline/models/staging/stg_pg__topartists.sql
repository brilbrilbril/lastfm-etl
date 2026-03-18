SELECT
    name,
    play_count,
    rank
FROM {{ source('pg', 'topartists') }}