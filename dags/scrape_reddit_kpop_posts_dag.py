import datetime
import os
import glob
import re

import praw
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from prawcore import ResponseException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from operators.upload_files_to_s3_operator import UploadFilesToS3Operator

# Reddit API 연결 정보
REDDIT_CLIENT_ID = Variable.get('client_id', default_var=None)
REDDIT_CLIENT_SECRET = Variable.get('client_secret', default_var=None)
REDDIT_USER_AGENT = Variable.get('user_agent', default_var=None)

NOW = datetime.datetime.now()
YMD = str(NOW.year) + '-' + str(NOW.month).zfill(2) + '-' + str(NOW.day).zfill(2)

def remove_special_characters(text):
    # URL 패턴 정규 표현식
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    cleaned_text = re.sub(url_pattern, '', text)

    # 특수 문자 및 줄바꿈 제거 정규 표현식
    pattern = r'[^\w\s]'

    # 특수 문자 및 줄바꿈 제거
    cleaned_text = re.sub(pattern, '', cleaned_text)

    # 줄바꿈 제거
    cleaned_text = cleaned_text.replace('\n', ' ')

    return cleaned_text

def scrape_reddit_data(reddit, post_type, limit=1000):
    subreddit = reddit.subreddit('kpopthoughts')

    # logging configuration
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    logger.info(f"Scraping Reddit Submissions from r/{subreddit.display_name} - {post_type}")

    try:
        if post_type not in ['hot', 'new', 'top']:
            raise ValueError(f"Invalid post type: {post_type}")

        subreddit_posts = getattr(subreddit, post_type)(limit=limit)
        posts = []

        for post in subreddit_posts:
            dt = datetime.datetime.fromtimestamp(post.created)
            posts.append([
                post_type, post.title, post.score, post.id, post.subreddit,
                post.url, post.num_comments, post.selftext, dt
            ])

        posts_df = pd.DataFrame(posts, columns=[
            'type', 'title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'])

        posts_df['title'] = posts_df['title'].apply(remove_special_characters)
        posts_df['body'] = posts_df['body'].apply(remove_special_characters)

        posts_df.to_csv(f'/tmp/{post_type}_reddit_posts.csv', encoding='utf-8', index=False)
        logger.info("Successfully scraped %d submissions from r/%s - %s", len(posts_df), subreddit, post_type)

        return posts_df['id'].tolist()

    except ResponseException as e:
        logger.info(f"Error occurred while scraping Reddit submissions: {e}")


def scrape_reddit_comments(reddit, task_instance, post_type, **kwargs):
    # 로그 설정
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    logger.info("Scraping Reddit Comments")

    comments = []

    try:
        posts_df = task_instance.xcom_pull(task_ids=f"scrape_reddit_data_tasks.scrape_reddit_data_{post_type}")

        if posts_df is None:
            raise ValueError("No data available for scraping comments.")

        for submission_id in posts_df:
            submission = reddit.submission(id=submission_id)

            logger.info(f"Scraping comments from submission {submission_id}")

            submission.comments.replace_more(limit=None)

            for comment in submission.comments.list():
                if isinstance(comment, praw.models.Comment):
                    dt = datetime.datetime.fromtimestamp(comment.created)
                    if comment.body not in ('[removed]', '[deleted]'):   # 삭제된 경우 skip
                        comments.append([post_type, submission_id, comment.body, dt])

            logger.info(f"Successfully scraped comments from submission {submission_id}")

        comments_df = pd.DataFrame(comments, columns=['post_type', 'submission_id', 'body', 'created'])
        comments_df['body'] = comments_df['body'].apply(remove_special_characters)

        comments_df.to_csv(f'/tmp/{post_type}_reddit_comments.csv', encoding='utf-8', index=False)
        logger.info(f"Successfully scraped {len(comments_df)} comments")

    except ResponseException as e:
        logger.info(f"Error occurred while scraping Reddit comments: {e}")


def create_upsert_post_task(task_id, post_type, schema, table):
    return SnowflakeOperator(
        task_id=task_id,
        sql=f"""
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
            FROM '@{schema}.raw_data_stage/reddit/api/posts/{YMD}/{post_type}_reddit_posts.csv'
            FILE_FORMAT = (
                SKIP_HEADER = 1
            );

            -- 중복된 레코드를 삭제
            DELETE FROM {schema}.{table}
            WHERE id IN (
                SELECT id
                FROM reddit_posts_temp_table
                GROUP BY id
                HAVING COUNT(*) > 1
            );

            INSERT INTO {schema}.{table}
            SELECT t.*
            from reddit_posts_temp_table t;

            COMMIT;
        """,
        snowflake_conn_id='snowflake_conn_id',
        autocommit=False
    )


def create_upsert_comment_task(task_id, comment_type, schema, table):
    return SnowflakeOperator(
        task_id=task_id,
        sql=f"""
            BEGIN;
            
            -- 새로운 임시 테이블 생성
            CREATE TEMPORARY TABLE reddit_temp_table (
                type VARCHAR,
                submission_id VARCHAR,
                body VARCHAR,
                created TIMESTAMP_LTZ
            );
            
            COPY INTO reddit_temp_table
            FROM '@{schema}.raw_data_stage/reddit/api/comments/{YMD}/{comment_type}_reddit_comments.csv'
            FILE_FORMAT = (
                SKIP_HEADER = 1
            );
                        
            -- 중복된 레코드를 삭제
            DELETE FROM {schema}.{table}
            WHERE submission_id IN (
                SELECT submission_id
                FROM reddit_temp_table
                GROUP BY submission_id
                HAVING COUNT(*) > 1
            );
            
            INSERT INTO {schema}.{table}
            SELECT t.*
            from reddit_temp_table t;
            
            COMMIT;
        """,
        snowflake_conn_id='snowflake_conn_id',
        autocommit=False
    )


def cleanup_temp_files(task_instance, **kwargs):
    temp_files = glob.glob('/tmp/*.csv')
    for file in temp_files:
        os.remove(file)



with DAG(
    dag_id='scrape_reddit_data',
    start_date=days_ago(1), # datetime(2023, 5, 25)
    schedule_interval=None,  # timedelta(days=7)
    catchup=True,
    on_failure_callback=cleanup_temp_files
) as dag:
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )

    bucket_name = 'kpop-analysis'
    aws_conn_id = 'aws_conn_id'
    source_directory = '/tmp'
    posts_folder_name = f'raw_data/reddit/api/posts/{YMD}'
    comments_folder_name = f'raw_data/reddit/api/comments/{YMD}'
    schema = 'raw'
    posts_table = 'reddit_posts_data'
    comments_table = 'reddit_comments_data'



    start_task = EmptyOperator(task_id='start_task')

    end_task = EmptyOperator(task_id='end_task')

    cleanup_task = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_temp_files
    )

    with TaskGroup("scrape_reddit_data_tasks") as scrape_group:
        scrape_hot_task = PythonOperator(
            task_id='scrape_reddit_data_hot',
            python_callable=scrape_reddit_data,
            op_kwargs={'reddit': reddit, 'post_type': 'hot'}
            # op_kwargs={'reddit': reddit, 'post_type': 'hot', 'limit': 2}
        )

        scrape_new_task = PythonOperator(
            task_id='scrape_reddit_data_new',
            python_callable=scrape_reddit_data,
            op_kwargs={'reddit': reddit, 'post_type': 'new'}
        )

        scrape_top_task = PythonOperator(
            task_id='scrape_reddit_data_top',
            python_callable=scrape_reddit_data,
            op_kwargs={'reddit': reddit, 'post_type': 'top'}
        )

    with TaskGroup("scrape_reddit_comments_data_tasks") as scrape_comments_group:
        scrape_comments_hot_task = PythonOperator(
            task_id='scrape_reddit_comments_hot',
            python_callable=scrape_reddit_comments,
            op_kwargs={'reddit': reddit, 'post_type': 'hot'}
        )

        scrape_comments_new_task = PythonOperator(
            task_id='scrape_reddit_comments_new',
            python_callable=scrape_reddit_comments,
            op_kwargs={'reddit': reddit, 'post_type': 'new'}
        )

        scrape_comments_top_task = PythonOperator(
            task_id='scrape_reddit_comments_top',
            python_callable=scrape_reddit_comments,
            op_kwargs={'reddit': reddit, 'post_type': 'top'}
        )

    upload_post_files_to_s3 = UploadFilesToS3Operator(
        task_id='upload_post_files_to_s3',
        conn_id=aws_conn_id,
        file_directory=source_directory,
        file_pattern='*_reddit_posts.csv',
        bucket_name=bucket_name,
        folder_name=posts_folder_name
    )

    upload_comment_files_to_s3 = UploadFilesToS3Operator(
        task_id='upload_comment_files_to_s3',
        conn_id=aws_conn_id,
        file_directory=source_directory,
        file_pattern='*_reddit_comments.csv',
        bucket_name=bucket_name,
        folder_name=comments_folder_name
    )

    with TaskGroup("upsert_posts_group") as upsert_posts_group:
        for post_type in ['hot', 'new', 'top']:
            task_id = f'upsert_posts_{post_type}'
            task = create_upsert_post_task(task_id, post_type, schema, posts_table)
            cleanup_task >> task

    with TaskGroup("upsert_comments_group") as upsert_comments_group:
        for comment_type in ['hot', 'new', 'top']:
            task_id = f'upsert_comments_{comment_type}'
            task = create_upsert_comment_task(task_id, comment_type, schema, comments_table)
            cleanup_task >> task

    start_task >> scrape_group >> scrape_comments_group >> [upload_post_files_to_s3,
                                                            upload_comment_files_to_s3] >> cleanup_task
    [upsert_posts_group, upsert_comments_group] >> end_task