{
    'schema': 'raw',
    "table": "track_data",
    "sqls": {
        "load": """
            USE SCHEMA raw;
            CREATE or replace STAGE raw_data_stage
                STORAGE_INTEGRATION = s3_int
                URL = 's3://kpop-analysis/raw_data/spotify/api/';
            CREATE TABLE IF NOT EXISTS track_data
            (
                album_id VARCHAR
                , artists ARRAY
                , disc_number INT
                , duration_ms INT
                , explicit BOOLEAN
                , href VARCHAR
                , id VARCHAR
                , name VARCHAR
                , popularity INT
                , track_number INT
                , type VARCHAR
                , uri VARCHAR
            );
            COPY INTO track_data
            FROM (
                SELECT
                    $1:album_id::VARCHAR AS album_id,
                    $1:artists::ARRAY AS artists,
                    $1:disc_number::INT AS disc_number,
                    $1:duration_ms::INT AS duration_ms,
                    $1:explicit::BOOLEAN AS explicit,
                    $1:href::VARCHAR AS href,
                    $1:id::VARCHAR AS id,
                    $1:name::VARCHAR AS name,
                    $1:popularity::INT AS popularity,
                    $1:track_number::INT AS track_number,
                    $1:type::VARCHAR AS type,
                    $1:uri::VARCHAR AS uri
                FROM '@raw_data_stage/spotify/api/tracks/{ymd}/tracks.json'
            )
            FILE_FORMAT = (TYPE = JSON);
                    """
    }
}