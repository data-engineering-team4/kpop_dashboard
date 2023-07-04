SELECT id, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo
FROM {{ source('dev', 'audio_data') }}