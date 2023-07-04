WITH src_kpop_track AS(
  SELECT * FROM {{ source('dev', 'track_data') }}
)
SELECT 
  id,
  REPLACE(artists[0]['id'],'"', '') artist_id,
  name
FROM src_kpop_track