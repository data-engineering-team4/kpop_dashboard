WITH src_kpop_artist AS (
  SELECT * FROM {{ source('dev', 'artist_data') }}
)
SELECT 
  id, 
  name, 
  genres,
  popularity, 
  followers_total
FROM src_kpop_artist