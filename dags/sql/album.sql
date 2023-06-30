USE SCHEMA raw;
CREATE or replace STAGE raw_data_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://kpop-analysis/raw_data/spotify/api/';
CREATE TABLE IF NOT EXISTS album_data
(
    album_type VARCHAR
    , artists ARRAY
    , href VARCHAR
    , id VARCHAR
    , images ARRAY
    , name VARCHAR
    , release_date DATE
    , release_date_precision VARCHAR
    , total_tracks INT
    , type VARCHAR
    , uri VARCHAR
);
COPY INTO album_data
FROM (
    SELECT
        $1:album_type::VARCHAR AS album_type,
        $1:artists::ARRAY AS artists,
        $1:href::VARCHAR AS href,
        $1:id::VARCHAR AS id,
        $1:images::ARRAY AS images,
        $1:name::VARCHAR AS name,
        $1:release_date::DATE AS release_date,
        $1:release_date_precision::VARCHAR AS release_date_precision,
        $1:total_tracks::INT AS total_tracks,
        $1:type::VARCHAR AS type,
        $1:uri::VARCHAR AS uri
    FROM '@raw_data_stage/spotify/api/albums/{ymd}/albums.json'
)
FILE_FORMAT = (TYPE = JSON);
