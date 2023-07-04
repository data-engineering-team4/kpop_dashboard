WITH src_country_weekly_chart AS(
  SELECT * FROM {{ source('dev', 'country_weekly_chart') }}
)
SELECT
  rank,
  track_id,
  track_name,
  artist_names,
  streams,
  country_code,
  DATE(chart_date) as chart_date
FROM src_country_weekly_chart