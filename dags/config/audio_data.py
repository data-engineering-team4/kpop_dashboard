{
    'schema': 'raw',
    'table': 'audio_data',
    'sqls': {
        'load': """
            USE SCHEMA raw;
            
            -- 파일 임시 저장 위치 (stage) 생성
            CREATE or replace STAGE raw_data_stage
                STORAGE_INTEGRATION = s3_int
                URL = 's3://kpop-analysis/raw_data/';
            
            CREATE OR REPLACE TABLE audio_data (
                danceability FLOAT,
                energy FLOAT,
                key INTEGER,
                loudness FLOAT,
                mode INTEGER,
                speechiness FLOAT,
                acousticness FLOAT,
                instrumentalness FLOAT,
                liveness FLOAT,
                valence FLOAT,
                tempo FLOAT,
                type VARCHAR(16),
                id VARCHAR,
                uri VARCHAR,
                track_href VARCHAR,
                analysis_url VARCHAR,
                duration_ms INTEGER,
                time_signature INTEGER
            );
            
            COPY INTO audio_data
            FROM (
                SELECT
                    $1:danceability,
                    $1:energy,
                    $1:key,
                    $1:loudness,
                    $1:mode,
                    $1:speechiness,
                    $1:acousticness,
                    $1:instrumentalness,
                    $1:liveness,
                    $1:valence,
                    $1:tempo,
                    $1:type,
                    $1:id,
                    $1:uri,
                    $1:track_href,
                    $1:analysis_url,
                    $1:duration_ms,
                    $1:time_signature
                FROM '@raw_data_stage/spotify/api/audio_features/{ymd}/'
            )
            FILE_FORMAT = (TYPE = JSON);
    """}
}