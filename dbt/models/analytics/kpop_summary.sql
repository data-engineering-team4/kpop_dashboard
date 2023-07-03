SELECT chart.country_code, chart.artist_names, chart.track_name, audio.danceability, audio.energy, audio.loudness, audio.speechiness, audio.acousticness, audio.liveness, audio.valence, audio.tempo
FROM {{ ref('src_track_ids') }} kpop
JOIN {{ ref('src_chart_ids') }} chart ON kpop.id = chart.track_id
JOIN {{ ref('src_audio_ids') }} audio ON kpop.id = audio.id
WHERE kpop.id IN (SELECT track_id FROM {{ ref('src_matched_ids') }})

