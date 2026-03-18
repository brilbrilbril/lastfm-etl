WITH duration_bucket AS (
    SELECT
        CASE
            WHEN duration < 180 THEN 'short'
	    WHEN duration BETWEEN 180 AND 300 THEN 'medium'
	ELSE 'long'
    END AS duration_category,
    	play_count
    FROM {{ ref('stg_pg__toptracks') }}
)
SELECT
    duration_category,
    count(*) as track_count,
    avg(play_count::int) as avg_play_count
FROM duration_bucket
GROUP BY duration_category