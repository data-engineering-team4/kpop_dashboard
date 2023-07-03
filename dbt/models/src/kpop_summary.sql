WITH track_ids AS (
    SELECT id
    FROM dev.raw.track_data
),

chart_ids AS (
    SELECT track_id, country_code, artist_names, track_name
    FROM dev.raw.country_weekly_chart
),

audio_ids AS (
    SELECT id, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo
    FROM dev.raw.audio_data
),

matched_ids AS (
    SELECT chart.track_id
    FROM track_ids kpop
    JOIN chart_ids chart ON kpop.id = chart.track_id
)

SELECT chart.country_code, chart.artist_names, chart.track_name, audio.danceability, audio.energy, audio.loudness, audio.speechiness, audio.acousticness, audio.liveness, audio.valence, audio.tempo
FROM track_ids kpop
JOIN chart_ids chart ON kpop.id = chart.track_id
JOIN audio_ids audio ON kpop.id = audio.id
WHERE kpop.id IN (SELECT track_id FROM matched_ids)

