-- Dashboard #3 
WITH track AS(
  SELECT * FROM {{ ref('src_kpop_track') }}
),
artist AS(
  SELECT * FROM {{ ref('src_kpop_artist') }}
),
chart AS(
  SELECT * FROM {{ ref('src_global_yearly_chart') }}
  UNION 
  SELECT * FROM {{ ref('src_global_weekly_chart') }}
)
SELECT DISTINCT
  artist.id as artist_id, 
  artist.name as artist_name, 
  artist.genres as artist_genres,
  artist.followers_total as artist_followers,
  artist.popularity as artist_popularity,
  track.name as track_name,
  chart.streams as streams,
  chart.peak_rank as peak_rank,
  chart.rank as rank,
  chart.chart_date as chart_date
FROM track
LEFT JOIN artist ON artist.id = track.artist_id
LEFT JOIN chart ON chart.track_id = track.id
