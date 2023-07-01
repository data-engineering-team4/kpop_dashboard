USE SCHEMA raw;
CREATE or replace STAGE raw_data_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://kpop-analysis/raw_data/';
CREATE TABLE IF NOT EXISTS artist_data
(
    external_urls_spotify VARCHAR
    , followers_href VARCHAR
    , followers_total INT
    , genres ARRAY
    , href VARCHAR
    , id VARCHAR
    , images ARRAY
    , name VARCHAR
    , popularity INT
    , type VARCHAR
    , uri VARCHAR
);
COPY INTO artist_data
FROM (
    SELECT
        $1:external_urls:spotify,
        $1:followers.href,
        $1:followers:total,
        $1:genres,
        $1:href,
        $1:id,
        $1:images,
        $1:name,
        $1:popularity,
        $1:type,
        $1:uri
    FROM '@raw_data_stage/spotify/api/artists/{ymd}/'
    )
FILE_FORMAT = (TYPE = JSON);