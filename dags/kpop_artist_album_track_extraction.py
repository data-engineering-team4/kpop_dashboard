import json
import requests
import csv
import boto3
import logging
import io
import redis
import pendulum
import time
from math import ceil
from datetime import datetime

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from utils import set_access_tokens, get_data, save_json_to_s3

# timezone 설정
local_tz = pendulum.timezone("Asia/Seoul")
# 현재 시간 설정
now = datetime.now(tz=local_tz)
ymd = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2)
timestamp = now.strftime('%Y-%m-%d_%H:%M:%S')

# Task를 분리하여 병렬로 처리
num_partitions = 3
redis_conn = redis.Redis(host='redis', port=6379, db=0)

def start(**kwargs):
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

def scraping_kpop_artist(access_token):
    """
    spotify search API를 통해, k-pop 키워드로 조회되는 아티스트들의 리스트를 가져온다.
    """
    
    # aws_conn 정보 가져오기
    aws_conn = BaseHook.get_connection('aws_conn_id')
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password

    # boto3 클라이언트 생성
    s3 = boto3.client(
        's3',
        aws_access_key_id = aws_access_key,
        aws_secret_access_key = aws_secret_key
    )
    
    s3_bucket_name = 'kpop-analysis'
    # 로깅
    log = logging.getLogger(__name__)
    log.info(f"{timestamp} || search_kpop_artist start logging")

    artist_id_list = []
    search_url = "https://api.spotify.com/v1/search"

    # 초기화
    offset = 0
    limit = 50
    cnt = 0

    k_genre = ["k-pop", "k-pop girl group", "k-pop boy group", "k-rap", "korean r&b", "korean pop", "korean ost", "k-rap", "korean city pop", "classic k-pop", "korean singer-songwriter"]

    while True:
        
        params = {
            "q": "genre:K-pop",
            "type": "artist",
            "offset": offset,
            "limit": limit  # 페이지 당 아티스트 수
        }

        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        
        response = requests.get(search_url, headers = headers, params = params)
        data = response.json()

        if response.status_code == 200:
            # 정상 응답
            total_artist = data["artists"]["total"]  # API에서 retrun 해주는 전체 list 수
            artists = data["artists"]["items"]
            
            # TODO : Test로 5개만 하자 !!!
            for idx, artist in enumerate(artists[:5]):
                
                artist_id, artist_name = artist["id"], artist["name"]
                
                # search k-pop 조회 결과, 1000개가 조회되는데, k-pop에 해당하지 않는 artist 존재
                if len(list(set(artist["genres"]).intersection(k_genre))) == 0:
                    continue
                
                # search로 조회한 k-pop artist 데이터 리스트를 한개씩 s3에 업로드
                artist_data = json.dumps(artist)
                artist_id_list.append(artist_id)
                
                # 파일을 저장하지 않고, 유사한 객체를 메모리에서 직접 업로드
                s3.upload_fileobj(io.BytesIO(artist_data.encode("utf-8")), s3_bucket_name, f'raw_data/spotify/api/artist/{ymd}/{artist_id}.json')

                log.info(f"ARTIST [{offset + (idx+1)}/{total_artist}] || artist_id :: {artist_id} artist_name :: {artist_name} || S3_path :: {s3_bucket_name}/raw_data/spotify/api/artist/{ymd}/{artist_id}.json")
                cnt += 1
                
            offset += limit
            
            # TODO :: TEST 이므로, 한 depth만!!
            break
            # TODO :: TEST 끝나면 주석 제거
            # if offset >= total_artist:
            #     log.info("ARTIST [DONE] || 전체 Aritist 결과를 Scraping 해왔습니다.")
            #     break

        else:
            # response error
            log.info(f"ARTIST [ERROR] || Artist: status_code : {response.status_code} error_msg : {response.text}")
            break

    redis_conn.set('artist_id_list', json.dumps(artist_id_list))

artist_sql = f"""
USE SCHEMA raw;

CREATE or replace STAGE raw_data_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://kpop-analysis/raw_data/';

CREATE TABLE IF NOT EXISTS artist_data
(
    external_urls_spotify VARCHAR
    , followers_href VARCHAR
    , followers_total INT
    , genres ARRAY
    , href VARCHAR
    , id VARCHAR
    , images ARRAY
    , name VARCHAR
    , popularity INT
    , type VARCHAR
    , uri VARCHAR
);

COPY INTO artist_data
FROM (
    SELECT
        $1:external_urls:spotify,
        $1:followers.href,
        $1:followers:total,
        $1:genres,
        $1:href,
        $1:id,
        $1:images,
        $1:name,
        $1:popularity,
        $1:type,
        $1:uri
    FROM '@raw_data_stage/spotify/api/artist/{ymd}/'
    )
FILE_FORMAT = (TYPE = JSON);
""".format(ymd)

def scraping_album(ti, num_partitions, partition_index):
    """
    Artist의 앨범 정보를 가져온다.
    """

    # aws_conn 정보 가져오기
    aws_conn = BaseHook.get_connection('aws_conn_id')
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password

    # boto3 클라이언트 생성
    s3 = boto3.client(
        's3',
        aws_access_key_id = aws_access_key,
        aws_secret_access_key = aws_secret_key
    )
    
    s3_bucket_name = 'kpop-analysis'
    
    # 로깅
    log = logging.getLogger(__name__)
    log.info(f"{timestamp} || search_kpop_album start logging")
    
    # 직전 task에서 redis에 저장한, artist_id_list 전체 가져오기
    artist_list = json.loads(redis_conn.get('artist_id_list'))

    access_token = Variable.get(f"access_token_{partition_index + 1}")

    album_id_list = []
    error_artist_list = []

    num_artists = len(artist_list)
    group_size = ceil(num_artists / num_partitions)
    start_index = partition_index * group_size
    end_index = start_index + group_size
    # artist 기준으로 task를 분할
    for idx, artist_key in enumerate(artist_list[start_index:end_index]):
        
        offset = 0
        limit = 50
        # limit 가 계속 돌아야할 수 있으므로
        while True:
                
            album_url = f"https://api.spotify.com/v1/artists/{artist_key}/albums"
            
            params = {
                "offset": offset,
                "limit": limit
            }
            
            headers = {
                "Authorization": f"Bearer {access_token}"
            }
            
            response = requests.get(album_url, headers = headers, params = params)
            data = response.json()
            
            if response.status_code == 200:
                
                total_album = data["total"] # 전체 앨범 수
                
                for i, album in enumerate(data['items']):
                    
                    album_id, album_name = album['id'], album["name"]
                    album_data = json.dumps(album) # S3에 적재
                    album_id_list.append(album_id)
                    
                    # 파일을 저장하지 않고, 메모리에서 업로드
                    s3.upload_fileobj(io.BytesIO(album_data.encode("utf-8")), s3_bucket_name, f'raw_data/spotify/api/album/{ymd}/{album_id}.json')
                    log.info(f"ALBUM [{offset + (i+1)}/{total_album}] || album_id :: {album_id} album_name :: {album_name} || S3_path :: {s3_bucket_name}/raw_data/spotify/api/album/{ymd}/{album_id}.json")

                # TODO :: 50개만 받아지고 있는 이슈 있음 -> 수정해야함 : 방탄 데이터로 확인해볼것 176
                offset += limit
                log.info(f"{offset}") # 위 이슈 때문에 check 한 것 -> 찾아야 된다.
                
                if offset >= total_album:
                    log.info("ARTIST ALBUM [DONE] || 해당 {artist_key} 의 전체 Album 결과를 Scraping 해왔습니다.")
                    break
            
            # 429 Error
            elif response.status_code == 429:
                # TODO :: ERROR Queue에 넣든, Proxy로 IP를 우회하든, 해결방법 찾기
                wait_time = int(response.headers.get('Retry-After', 0))
                log.error(f"ALBUM 429 error || {response.text}")
                log.error(f"Rate limited Waiting for {wait_time} seconds...")
                time.sleep(wait_time)

            else:
                error_artist_list.append(artist_key)    # error가 있는 경우 - artist 기준이amfh
                log.info(f"ALBUM [ERROR] || Artist: status_code : {response.status_code} error_msg : {response.text}")
                break

        log.info(f"ALBUM {ti.task_id} [{idx+1}/{end_index - start_index + 1}] || artist_id :: {artist_key}의 앨범정보를 수집했습니다.")
    redis_conn.set(f'album_id_list_{partition_index}', json.dumps(album_id_list))

def merge_album(**context):
    # TODO :: 429 에러 발생할 시, -> failed 된 것들 queue에 넣어버리고, error 한번더 execute 실행
    # Task Group으로 분할하여 수집한 album 정보 저장
    album_id_list = []
    for i in range(num_partitions):
        album_id_list_partition = json.loads(redis_conn.get(f'album_id_list_{i}'))
        album_id_list.extend(album_id_list_partition)
    redis_conn.set('album_id_list', json.dumps(album_id_list)) # 전체 Album ID 리스트 생성 

album_sql = f"""
USE SCHEMA raw;

CREATE or replace STAGE raw_data_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://kpop-analysis/raw_data/';

CREATE OR REPLACE TABLE album_data (
    album_group VARCHAR
    , album_type VARCHAR
    , artists VARIANT
    , available_markets ARRAY
    , external_urls_spotify VARCHAR
    , href VARCHAR
    , id VARCHAR
    , images ARRAY
    , name VARCHAR
    , release_date DATE
    , release_date_precision VARCHAR
    , total_tracks INTEGER
    , type VARCHAR
    , uri VARCHAR
);

COPY INTO album_data
FROM (
    SELECT
        $1:album_group,
        $1:album_type,
        $1:artists,
        $1:available_markets,
        $1:external_urls.spotify,
        $1:href,
        $1:id,
        $1:images,
        $1:name,
        $1:release_date,
        $1:release_date_precision,
        $1:total_tracks,
        $1:type,
        $1:uri
    FROM '@raw_data_stage/spotify/api/album/{ymd}/'
    )
FILE_FORMAT = (TYPE = JSON);
"""


def scraping_track(ti, num_partitions, partition_index):
    # TODO :: Task Group을 더 생성해야하지 않을까?
    
    
    # aws_conn 정보 가져오기
    aws_conn = BaseHook.get_connection('aws_conn_id')
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password

    # boto3 클라이언트 생성
    s3 = boto3.client(
        's3',
        aws_access_key_id = aws_access_key,
        aws_secret_access_key = aws_secret_key
    )
    
    s3_bucket_name = 'kpop-analysis'
    
    # 로깅
    log = logging.getLogger(__name__)
    log.info(f"{timestamp} || search_kpop_track start logging")
    
    # 직전 Task에서 redis에 저장한, album_id_list 전체 가져오기
    album_list = json.loads(redis_conn.get('album_id_list'))

    access_token = Variable.get(f"access_token_{partition_index + 1}")

    track_id_list = []
    error_track_list = []

    num_albums = len(album_list)
    group_size = ceil(num_albums / num_partitions)
    start_index = partition_index * group_size
    end_index = start_index + group_size

    for idx, album_key in enumerate(album_list[start_index:end_index]):
        
        offset = 0
        limit = 50
        
        while True:
            
            track_url = f"https://api.spotify.com/v1/albums/{album_key}/tracks"
            
            params = {
                "offset": offset,
                "limit": limit
            }
            
            headers = {
                "Authorization": f"Bearer {access_token}"
            }
            
            response = requests.get(track_url, headers=headers, params=params)

            if response.status_code == 200:
                
                data = response.json()
                
                total_track = data["total"] # 한 앨범에 해당하는 전체 track의 리스트 
                
                # 앨범 안에 담겨있는 각각의 track
                for i, track in enumerate(data['items']):
                    track_id, track_name = track['id'], track['name']
                    track_id_list.append(track)
                    track_data = json.dumps(track)
                    
                    # 파일을 저장하지 않고, 메모리에서 업로드 
                    s3.upload_fileobj(io.BytesIO(track_data.encode("utf-8")), s3_bucket_name, f'raw_data/spotify/api/track/{ymd}/{track_id}.json')                   
                    log.info(f"ALBUM [{offset + (i+1)}/{total_track}] || track_id :: {track_id} track_name :: {track_name} || S3_path :: {s3_bucket_name}/raw_data/spotify/api/track/{ymd}/{track_id}.json")
                    

                # 50개 이상일 수 있음
                if offset >= total_track:
                    break
                offset += limit
                
            else:
                error_track_list.append(album_key)
                log.error(f'{response.status_code} - {response.text}')
                print("error")
                time.sleep(5)
                break
        log.info(f"ALBUM {ti.task_id} [{idx+1}/{end_index - start_index + 1}] || album_id :: {album_key}의 앨범 내 트랙 데이터를 수집했습니다.")
    redis_conn.set(f'track_list_{partition_index}', json.dumps(track_id_list))

def merge_track(**context):
    # TODO :: 429 에러 처리 어떻게 할 것인지 ? -> 추출 안된 error queue 여기서 다시 처리해줘도 될 듯용 
    merged_track_data = []

    for i in range(1, 4):
        track_partition = json.loads(redis_conn.get(f'track_list_{i}'))
        # TODO :: @유창님 :: ?? 어떤 내용이죵 ?/ 
        if track_partition is not None:
            merged_track_data.extend(track_partition)

    redis_conn.set('merged_track_data', json.dumps(merged_track_data))

track_sql = f"""
USE SCHEMA raw;

-- 파일 임시 저장 위치 (stage) 생성
CREATE or replace STAGE raw_data_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://kpop-analysis/raw_data/';

CREATE OR REPLACE TABLE track_data (
    artists VARIANT
    , available_markets ARRAY
    , disc_number INTEGER
    , duration_ms INTEGER
    , explicit BOOLEAN
    , external_urls_spotify VARCHAR
    , href VARCHAR
    , id VARCHAR
    , is_local BOOLEAN
    , name VARCHAR
    , preview_url VARCHAR
    , track_number INTEGER
    , type VARCHAR(16)
    , uri VARCHAR
);

COPY INTO track_data
FROM (
    SELECT
        $1:artists,
        $1:available_markets,
        $1:disc_number,
        $1:duration_ms,
        $1:explicit,
        $1:external_urls.spotify,
        $1:href,
        $1:id,
        $1:is_local,
        $1:name,
        $1:preview_url,
        $1:track_number,
        $1:type,
        $1:uri
    FROM '@raw_data_stage/spotify/api/track/{ymd}/'
    )
FILE_FORMAT = (TYPE = JSON);
"""


def load_track(**context):
    execution_date = context["execution_date"].strftime("%Y-%m-%d")
    merged_track_data = json.loads(redis_conn.get('merged_track_data'))
    if merged_track_data:
        track_list = merged_track_data
    else:
        track_list = []
    s3_bucket = Variable.get('s3_bucket')
    s3_key = f"tracks/{execution_date}_tracks.json"
    save_json_to_s3(track_list, s3_bucket, s3_key)

##########################################
#        Operator 함수 만들기
##########################################
def create_python_operator(task_id, python_callable, op_kwargs=None):
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        provide_context=True,
        dag=dag,
        do_xcom_push=True,
        op_kwargs=op_kwargs or {},
    )

def create_snowflake_operator(task_id, sql_query):
    return SnowflakeOperator(
        task_id=task_id,
        sql=sql_query,
        snowflake_conn_id = "snowflake_conn_id",
        autocommit = False,
        dag = dag
    )


##########################################
#        DAG 만들기
##########################################
with DAG(
    dag_id = 'kpop_artist_album_track_extraction',
    start_date = datetime(2023,6,26),
    catchup=False,
    tags=['example'],
    schedule_interval=None
) as dag:

    start_task = create_python_operator('start', start)

    scraping_kpop_artist_task = create_python_operator(
        'scrape_kpop_artist_task',
        scraping_kpop_artist,
        op_kwargs={'access_token': "{{ var.value.get('access_token_1') }}"}
    )

    load_artists_task = create_snowflake_operator('load_artists_task', artist_sql)
    
    with TaskGroup("scrape_album_group") as scrape_album_group:
        for i in range(num_partitions):
            scrape_album = create_python_operator(
                f'scrape_album_{i + 1}',
                scraping_album,
                op_kwargs={
                    'num_partitions': num_partitions,
                    'partition_index': i,
                }
            )

    merge_album_task = create_python_operator('merge_album_task', merge_album)
    
    load_album_task = create_snowflake_operator('load_album_task', album_sql)
    
    start_track_task = create_python_operator('start_track', start) # album 추출도 양이 많기 때문에 토큰 1회 발급 당 1시간이므로 재갱신

    with TaskGroup("scrape_track_group") as scrape_track_group:
        for i in range(num_partitions):
            scrape_track = create_python_operator(
                f'scrape_track_{i + 1}',
                scraping_track,
                op_kwargs={
                    'num_partitions': num_partitions,
                    'partition_index': i,
                }
            )

    merge_track_task = create_python_operator('merge_track_task', merge_track)

    load_track_task = create_snowflake_operator('load_track_task', track_sql)

    
    start_task >> scraping_kpop_artist_task >> load_artists_task # artist 데이터 추출
    load_artists_task >> scrape_album_group >> merge_album_task >> load_album_task # album 데이터 추출
    load_album_task >> start_track_task >> scrape_track_group >> merge_track_task >> load_track_task