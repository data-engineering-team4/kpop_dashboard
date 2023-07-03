{
    'schema': 'raw',
    'table': 'reddit_comments_data',
    'sqls': {
        'load_sql': """
                        USE SCHEMA raw;
                        BEGIN;

                        CREATE TEMPORARY TABLE temp_table AS
                        SELECT
                            $1 AS type,
                            $2 AS post_id,
                            $3 AS body,
                            $4 AS created
                        FROM @raw.raw_data_stage/reddit/api/comments/{YMD}/{comment_type}_reddit_comments.csv;

                        -- 중복된 레코드를 삭제
                        DELETE FROM temp_table where type='post_type';

                        DELETE FROM reddit_comments_data
                        WHERE post_id IN (
                            SELECT post_id
                            FROM temp_table
                            GROUP BY post_id
                            HAVING COUNT(*) > 1
                        );

                        INSERT INTO reddit_comments_data
                        SELECT t.*
                        from temp_table t;

                        COMMIT;
                    """
    }
}