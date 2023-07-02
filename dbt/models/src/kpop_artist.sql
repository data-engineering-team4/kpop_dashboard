WITH source_data AS (
    SELECT id, name, popularity, followers_total
    FROM dev.raw.artist_data
)
SELECT *
FROM source_data