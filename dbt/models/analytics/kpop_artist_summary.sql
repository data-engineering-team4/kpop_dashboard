WITH artist AS (
    SELECT * FROM {{ ref('src_kpop_artist') }}
)
SELECT 
    id, 
    name, 
    genres,
    popularity, 
    followers_total
FROM artist