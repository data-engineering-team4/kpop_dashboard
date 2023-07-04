-- Dashboard #4
WITH audio AS(
  SELECT * FROM {{ ref('src_global_popular_audio') }}
)
SELECT 
  audio.id,
  audio.acousticness, 
  audio.danceability, 
  audio.energy, 
  audio.liveness, 
  audio.loudness, 
  audio.speechiness, 
  audio.tempo, 
  audio.valence
FROM audio 
