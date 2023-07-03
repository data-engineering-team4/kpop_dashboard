WITH source_data AS (
    SELECT rank,track_id,track_name,artist_names,streams,country_code,chart_date
    FROM dev.raw.country_weekly_chart
)
SELECT *
FROM source_data
