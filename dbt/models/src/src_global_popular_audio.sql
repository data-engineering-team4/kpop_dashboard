WITH src_global_popular_audio AS(
  SELECT * FROM {{ source('dev', 'global_famous_audio_data') }}
)
SELECT 
  id,
  acousticness, 
  danceability, 
  energy, 
  liveness, 
  loudness / -8 as loudness, 
  mode, 
  speechiness, 
  tempo / 130 as tempo,
  valence 
FROM src_global_popular_audio