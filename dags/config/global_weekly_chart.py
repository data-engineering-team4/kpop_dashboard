{
    'schema': 'raw',
    'table': 'global_weekly_chart',
    'sqls' : {
        'load_sql' : """
                        BEGIN;

                        CREATE TEMPORARY TABLE temp_table AS 
                        SELECT 
                            $1 AS rank, 
                            $2 AS track_id, 
                            $3 AS artist_names, 
                            $4 AS track_name, 
                            $5 AS peak_rank, 
                            $6 AS previous_rank, 
                            $7 AS weeks_on_chart, 
                            $8 AS streams, 
                            $9 AS chart_date
                        FROM @raw.transformed_data_stage_csv/spotify/chart/{date}/{target_file_pattern};
                        
                        DELETE FROM raw.global_weekly_chart
                        WHERE chart_date = '{date}';

                        INSERT INTO raw.global_weekly_chart
                        SELECT t.* 
                        FROM temp_table t;

                        COMMIT;
                    """,
        'get_top_10':"""
                        SELECT DISTINCT track_id
                        FROM raw.global_weekly_chart
                        WHERE rank <= 10
                        LIMIT 100
                        ;
                    """,
        'load_top_10':"""
                        USE SCHEMA raw; 
                        BEGIN;

                        COPY INTO GLOBAL_FAMOUS_AUDIO_DATA 
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
                                    $1:time_signature,
                                    '{date}' as recorded_at
                                FROM @raw_data_stage/spotify/api/{key}/{date}/)
                        FILE_FORMAT = (TYPE = JSON)
                        ;

                        COMMIT;
        
        """
    },
    'dag_params':{
        'table_name':'global_weekly_chart',
        'source_file_pattern' : 'regional-global-weekly-{date}.csv',
        'target_file_pattern' : 'global-weekly-{date}.csv'  
    }
}