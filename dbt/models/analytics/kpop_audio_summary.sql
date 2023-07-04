-- Dashboard #2,4
WITH track AS(
  SELECT id FROM {{ ref('src_kpop_track') }}
),
chart AS(
  SELECT * FROM {{ ref('src_country_weekly_chart') }}
),
audio AS(
  SELECT * FROM {{ ref('src_kpop_audio') }}
)
SELECT 
  chart.country_code, 
  chart.artist_names, 
  chart.track_name, 
  audio.acousticness, 
  audio.danceability, 
  audio.energy, 
  audio.liveness, 
  audio.loudness, 
  audio.speechiness, 
  audio.tempo, 
  audio.valence
FROM track
JOIN audio ON track.id = audio.id
JOIN chart ON track.id = chart.track_id

