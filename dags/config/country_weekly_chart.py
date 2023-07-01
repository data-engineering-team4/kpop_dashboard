{
    'schema': 'raw',
    'table': 'country_weekly_chart',
    'sqls' : {
        'test_sql' : """
            SELECT 
                $1 AS rank, 
                $2 AS track_id, 
                $3 AS artist_names, 
                $4 AS track_name, 
                $5 AS peak_rank, 
                $6 AS previous_rank, 
                $7 AS weeks_on_chart, 
                $8 AS streams, 
                $9 AS country_code, 
                $10 AS chart_date
            FROM @raw.transformed_data_stage_csv/spotify/chart/{date}/{target_file_pattern};
    """ ,
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
                                $9 AS country_code, 
                                $10 AS chart_date
                            FROM @raw.transformed_data_stage_csv/spotify/chart/{date}/{target_file_pattern};
                            
                            DELETE FROM raw.country_weekly_chart 
                            WHERE chart_date = '{date}';

                            INSERT INTO raw.country_weekly_chart
                            SELECT t.* 
                            FROM temp_table t;

                        COMMIT;
                    """
    },
    'dag_params':{
        'table_name':'country_weekly_chart',
        'source_file_pattern' : 'regional-*-weekly-{date}.csv',
        'target_file_pattern' : 'country-weekly-{date}.csv',       
    }
         
}