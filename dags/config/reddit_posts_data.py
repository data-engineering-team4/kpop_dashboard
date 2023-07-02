USE SCHEMA raw;

    BEGIN;

    -- 새로운 임시 테이블 생성
    CREATE TEMPORARY TABLE reddit_posts_temp_table (
        type VARCHAR,
        title VARCHAR,
        score INT,
        id VARCHAR,
        subreddit VARCHAR,
        url VARCHAR,
        num_comments INT,
        body VARCHAR,
        created TIMESTAMP_LTZ
    );

    COPY INTO reddit_posts_temp_table
    FROM '@raw_data_stage/reddit/api/posts/{YMD}/{post_type}_reddit_posts.csv'
    FILE_FORMAT = (
        SKIP_HEADER = 1
    );

    -- 중복된 레코드를 삭제
    DELETE FROM reddit_posts_data
    WHERE id IN (
        SELECT id
        FROM reddit_posts_temp_table
        GROUP BY id
        HAVING COUNT(*) > 1
    );

    INSERT INTO reddit_posts_data
    SELECT t.*
    from reddit_posts_temp_table t;

    COMMIT;
