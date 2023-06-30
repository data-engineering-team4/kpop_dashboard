{
    'schema': 'raw',
    "table": "artist_data",
    "sqls": {
        "load": """
            USE SCHEMA raw;
            CREATE or replace STAGE raw_data_stage
                STORAGE_INTEGRATION = s3_int
                URL = 's3://kpop-analysis/raw_data/spotify/api/';
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
                    $1:external_urls:spotify::VARCHAR AS external_urls_spotify,
                    $1:followers.href::VARCHAR AS followers_href,
                    $1:followers:total::INT AS followers_total,
                    $1:genres::ARRAY AS genres,
                    $1:href::VARCHAR AS href,
                    $1:id::VARCHAR AS id,
                    $1:images::ARRAY AS images,
                    $1:name::VARCHAR AS name,
                    $1:popularity::INT AS popularity,
                    $1:type::VARCHAR AS type,
                    $1:uri::VARCHAR AS uri
                FROM '@raw_data_stage/spotify/api/artists/{ymd}/artists.json'
            )
            FILE_FORMAT = (TYPE = JSON);
        """
    }
}