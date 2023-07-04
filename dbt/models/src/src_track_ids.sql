SELECT id
FROM {{ source('dev', 'track_data') }}