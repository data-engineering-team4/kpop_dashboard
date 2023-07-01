{
    'schema': 'raw',
    'table': 'global_yearly_chart',
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
                            to_timestamp(to_char(to_date($9, 'yyyy-mm-dd'), 'yyyy-mm-dd')) AS chart_date,
                            $10 AS days_on_chart
                        FROM @raw.transformed_data_stage_csv/spotify/chart/{date}/{target_file_pattern};
                        
                        DELETE FROM raw.global_yearly_chart
                        WHERE chart_date = '{date}';

                        INSERT INTO raw.global_yearly_chart
                        SELECT t.* 
                        FROM temp_table t;

                        COMMIT;
                    """
    },
    'dag_params':{
        'table_name':'global_yearly_chart',
        'source_file_pattern' : 'regional-global-daily-{date}.csv',
        'target_file_pattern' : 'global-yearly-{date}.csv'
    }
}