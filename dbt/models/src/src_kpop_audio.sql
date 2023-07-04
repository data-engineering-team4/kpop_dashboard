WITH src_kpop_audio AS(
  SELECT * FROM {{ source('dev', 'audio_data') }}
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
FROM src_kpop_audio