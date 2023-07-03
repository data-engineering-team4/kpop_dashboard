SELECT track_id, country_code, artist_names, track_name
FROM {{ source('dev', 'country_weekly_chart') }}