USE SCHEMA raw;
-- 파일 임시 저장 위치 (stage) 생성
CREATE or replace STAGE raw_data_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://kpop-analysis/raw_data/';
CREATE OR REPLACE TABLE track_data (
    artists VARIANT
    , available_markets ARRAY
    , disc_number INTEGER
    , duration_ms INTEGER
    , explicit BOOLEAN
    , external_urls_spotify VARCHAR
    , href VARCHAR
    , id VARCHAR
    , is_local BOOLEAN
    , name VARCHAR
    , preview_url VARCHAR
    , track_number INTEGER
    , type VARCHAR(16)
    , uri VARCHAR
);
COPY INTO track_data
FROM (
    SELECT
        $1:artists,
        $1:available_markets,
        $1:disc_number,
        $1:duration_ms,
        $1:explicit,
        $1:external_urls.spotify,
        $1:href,
        $1:id,
        $1:is_local,
        $1:name,
        $1:preview_url,
        $1:track_number,
        $1:type,
        $1:uri
    FROM '@raw_data_stage/spotify/api/tracks/{ymd}/'
    )
FILE_FORMAT = (TYPE = JSON);