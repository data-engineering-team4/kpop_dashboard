import pandas as pd
import logging
from itertools import chain
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.exceptions import AirflowFailException

from utils.common_util import get_sql
from utils.spotify_util import get_data
from utils.spotify_util import set_access_tokens
from utils.aws_util import save_json_to_s3
from utils.aws_util import create_s3_client

def _set_tokens():
    client_ids = [
        Variable.get("client_id_1"),
        Variable.get("client_id_2"),
        Variable.get("client_id_3"),
    ]
    client_secrets = [
        Variable.get("client_secret_1"),
        Variable.get("client_secret_2"),
        Variable.get("client_secret_3"),
    ]
    set_access_tokens(client_ids, client_secrets)

def _get_global_famous_track_ids(**context):
    table_name = context['table_name']
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    sql = get_sql(table_name,'get_top_10')
    results = snowflake_hook.get_records(sql=sql)
    
    if len(results) == 0:
        logging.info(f'no results')
        raise AirflowFailException
    
    logging.info(f'got results : {results}')
    track_ids = list(chain.from_iterable(results))
    return track_ids

def _get_and_save_track_audio_features(**context):
    # 이전 태스크에서 반환된 결과 가져오기
    track_ids = context['ti'].xcom_pull(task_ids='get_global_famous_track_ids')
    logging.info(track_ids)
    date = context['ds']
    logging.info(f'date : {date}')
    ids = ','.join(track_ids)
    access_token = Variable.get('access_token_1')
    bucket_name = context['bucket_name']
    key = context['key']
    s3_client = create_s3_client('aws_conn_id')
    endpoint = f'https://api.spotify.com/v1/audio-features'
    headers = {
            "Authorization": f"Bearer {access_token}"
    }
    logging.info(f'{bucket_name}-{key}-{date}')
    
    # api에서 track 정보 가져오기
    status_code, data = get_data(endpoint, headers, params={"ids":ids})
    logging.info(f'status code : {status_code}')
    logging.info(f'data code : {data}')
    if status_code == 200:
        audio_features = data["audio_features"]
        for audio_feature in audio_features:
            logging.info(f"track id : {audio_feature['id']}")
            save_json_to_s3(audio_feature, bucket_name, key, date, str(audio_feature['id']), s3_client)
            # s3_key = f"raw_data/spotify/api/{key}/{ymd}/{id}.json"

default_args = {
        'owner': 'ohyujeong',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
        dag_id='global_famous_track_dag',
        start_date=datetime(2023, 7, 1) ,
        schedule_interval=timedelta(weeks=1),
        catchup=True,
        default_args=default_args
) as dag:
    table_name = 'global_weekly_chart'
    bucket_name = Variable.get('s3_bucket')
    date = '{{ ds }}'
    key = 'global_famous_audio_features'
    bucket_key = f"raw_data/spotify/api/{key}/{date}/*.json"
    load_sql = get_sql(table_name, 'load_top_10', key=key, date=date)

    # TODO ExternalTaskSensor 사용방법 알아보기
    # wait_for_spotify_global_weekly_chart_dag = ExternalTaskSensor(
    #     task_id='wait_for_spotify_global_weekly_chart_dag',
    #     external_dag_id='spotify_global_weekly_chart_dag',
    #     # external_task_id='load_s3_to_snowflake',
    #     start_date=datetime(2023, 7, 1),
    #     # execution_delta=timedelta(days=2),
    #     # execution_date_fn=lambda x: x - timedelta(days=2),
    #     timeout=3600

    
    set_tokens = PythonOperator(
        task_id='set_tokens',
        python_callable=_set_tokens,
    )
        
    get_global_famous_track_ids = PythonOperator(
        task_id='get_global_famous_track_ids',
        python_callable=_get_global_famous_track_ids,
        provide_context=True,
        op_kwargs={
            'table_name': table_name
        }
    )
    
    get_and_save_track_audio_features = PythonOperator(
        task_id='get_and_save_track_audio_features',
        python_callable=_get_and_save_track_audio_features,
        provide_context=True,
        op_kwargs={
            'bucket_name': bucket_name,
            'key': key
        }
    )
    
    check_file_exists = S3KeySensor(
        task_id='check_file_exists',
        bucket_name=bucket_name,
        bucket_key=bucket_key,
        aws_conn_id='aws_conn_id',
        wildcard_match=True,
        timeout=60 * 60,
        mode='reschedule',
        poke_interval=30,
    )
    
    load_s3_to_snowflake = SnowflakeOperator(
        task_id='load_s3_to_snowflake',
        sql=load_sql,
        snowflake_conn_id='snowflake_conn_id',
        autocommit=False,
    )

    # wait_for_spotify_global_weekly_chart_dag >> 
    set_tokens >> get_global_famous_track_ids >> get_and_save_track_audio_features >> check_file_exists >> load_s3_to_snowflake