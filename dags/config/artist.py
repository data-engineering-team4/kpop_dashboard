{
    'schema': 'raw',
    "table": "artist_data",
    "sqls": {
        "load": f"""
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
        INSERT INTO artist_data
        SELECT
            artist.value:external_urls:spotify::VARCHAR AS external_urls_spotify,
            artist.value:followers:href::VARCHAR AS followers_href,
            artist.value:followers:total::INT AS followers_total,
            artist.value:genres::ARRAY AS genres,
            artist.value:href::VARCHAR AS href,
            artist.value:id::VARCHAR AS id,
            artist.value:images::ARRAY AS images,
            artist.value:name::VARCHAR AS name,
            artist.value:popularity::INT AS popularity,
            artist.value:type::VARCHAR AS type,
            artist.value:uri::VARCHAR AS uri
        FROM (
            SELECT PARSE_JSON($1) AS json_data
            FROM '@raw_data_stage/spotify/api/artists/{ymd}/artists.json'
        ), LATERAL FLATTEN(input => json_data:artists) artist;

        """
    }
}