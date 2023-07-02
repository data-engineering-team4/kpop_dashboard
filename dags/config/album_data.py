{
    'schema': 'raw',
    'table': 'album_data',
    'sqls': {
        'load': """
            USE SCHEMA raw;
            CREATE or replace STAGE raw_data_stage
                STORAGE_INTEGRATION = s3_int
                URL = 's3://kpop-analysis/raw_data/';
            CREATE OR REPLACE TABLE album_data (
                album_group VARCHAR
                , album_type VARCHAR
                , artists VARIANT
                , available_markets ARRAY
                , external_urls_spotify VARCHAR
                , href VARCHAR
                , id VARCHAR
                , images ARRAY
                , name VARCHAR
                , release_date DATE
                , release_date_precision VARCHAR
                , total_tracks INTEGER
                , type VARCHAR
                , uri VARCHAR
            );
            COPY INTO album_data
            FROM (
                SELECT
                    $1:album_group,
                    $1:album_type,
                    $1:artists,
                    $1:available_markets,
                    $1:external_urls.spotify,
                    $1:href,
                    $1:id,
                    $1:images,
                    $1:name,
                    $1:release_date,
                    $1:release_date_precision,
                    $1:total_tracks,
                    $1:type,
                    $1:uri
                FROM '@raw_data_stage/spotify/api/albums/{ymd}/'
                )
            FILE_FORMAT = (TYPE = JSON);
    """}
}