WITH source_data AS (
    SELECT rank, track_id, track_name, artist_names, streams, chart_date
    FROM dev.raw.global_weekly_chart
)
SELECT *
FROM source_data