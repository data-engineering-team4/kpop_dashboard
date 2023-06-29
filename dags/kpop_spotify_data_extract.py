import json
import requests
import datetime
import csv
import boto3
import logging
import io

import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from utils_ import spotify_util


dag = DAG(
    dag_id = "kpop_spotify_data_extract",
    start_date = datetime.datetime(2023, 6, 27),
    schedule_interval=None  # TODO :: 실행 간격 -> 아직 test -> weekly로 할 예정
)


DATA_PATH = "../data/"
now = datetime.datetime.now()
ymd = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2)
timestamp = now.strftime('%Y-%m-%d_%H:%M:%S')


def _search_kpop_artist():
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
    log = logging.getLogger(__name__)
    log.info(f"{timestamp} || search_kpop_artist start logging")
    
    url = "https://api.spotify.com/v1/search"

    # 초기화
    offset = 0
    limit = 50
    cnt = 0
    
    k_genre = ["k-pop", "k-pop girl group", "k-pop boy group", "k-rap", "korean r&b", "korean pop", "korean ost", "k-rap", "korean city pop","classic k-pop", "korean singer-songwriter"]

    while True: 
        
        params = {
            "q" : "genre:K-pop",
            "type" : "artist",
            "offset" : offset, 
            "limit": limit          # 페이지 당 아티스트 수 
        }
        
        response = requests.get(url, headers = spotify_util.get_access_token(), params = params)
        data = response.json()

        if response.status_code == 200 :
            # 정상 응답
            total_artist = data["artists"]["total"] # API에서 retrun 해주는 전체 list 수
            artists = data["artists"]["items"]
            
            for idx, artist in enumerate(artists) :
                
                artist_id, artist_name = artist["id"], artist["name"]
                
                # search k-pop 조회 결과, 1000개가 조회되는데, k-pop에 해당하지 않는 artist 존재
                if len(list(set(artist["genres"]).intersection(k_genre)))==0:
                    continue
                
                # search로 조회한 k-pop artist 데이터 리스트를 한개씩 s3에 업로드
                artist_data = json.dumps(artist)
                
                # 파일을 저장하지 않고, 유사한 객체를 메모리에서 직접 업로드
                s3.upload_fileobj(io.BytesIO(artist_data.encode("utf-8")), s3_bucket_name, f'raw_data/spotify/api/artist/{ymd}/{artist_id}.json')

                log.info(f"ARTIST [{offset + (idx+1)}/{total_artist}] || artist_id :: {artist_id} artist_name :: {artist_name}")
                cnt += 1
                    
            offset += limit
            
            # test이므로 한 depth만
            break 
            
            # if offset >= total_artist:
            #     log.info("ARTIST [DONE] || 전체 Aritist 결과를 Scraping 해왔습니다.")
            #     break
                
        else:
            # response error
            log.info(f"ARTIST [ERROR] || Artist: status_code : {response.status_code} error_msg : {response.text}")
            break
    
    log.info(f"ARTIST [SUCCESS] || scraping_kpop_artist 실행 완료 - 최종 추출 수량 :: {cnt}")

artist_sql = f"""
USE SCHEMA raw;

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

start = EmptyOperator(
    task_id = "start",
    dag = dag
)

search_kpop_artist = PythonOperator(
    task_id = "search_kpop_artist",
    python_callable=_search_kpop_artist,
    dag = dag    
)

load_s3_to_snowflake_artist = SnowflakeOperator(
    task_id = "load_s3_to_snowflake_artist",
    sql = artist_sql,
    snowflake_conn_id = "snowflake_conn_id",
    autocommit = False,
    dag= dag
)


end = EmptyOperator(
    task_id = "end",
    dag = dag
)


start >> search_kpop_artist >> load_s3_to_snowflake_artist >> end