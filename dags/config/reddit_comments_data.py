{
    'schema': 'raw',
    'table': 'reddit_posts_data',
    'sqls': {
        'load_sql': """
                        USE SCHEMA raw;
                        BEGIN;

                        CREATE TEMPORARY TABLE temp_table AS
                        SELECT
                            $1 AS type,
                            $2 AS title,
                            $3 AS score,
                            $4 AS id,
                            $5 AS subreddit,
                            $6 AS url,
                            $7 AS num_comments,
                            $8 AS body,
                            $9 AS created
                        FROM @raw.raw_data_stage/reddit/api/posts/{YMD}/{post_type}_reddit_posts.csv;

                        -- 중복된 레코드를 삭제
                        DELETE FROM temp_table where type='type';

                        DELETE FROM reddit_posts_data
                        WHERE id IN (
                            SELECT id
                            FROM temp_table
                            GROUP BY id
                            HAVING COUNT(*) > 1
                        );

                        INSERT INTO reddit_posts_data
                        SELECT t.*
                        from temp_table t;


                        COMMIT;
                    """
    }
}