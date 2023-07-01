from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.dummy_operator import DummyOperator
import json
from math import ceil
import time
import logging
import redis
from utils.slack_util import SlackAlert
from utils.spotify_util import set_access_tokens, get_data
from utils.aws_util import save_json_to_s3, create_s3_client
from utils.common_util import get_sql, get_sql_from_file
import pendulum

# timezone 설정
local_tz = pendulum.timezone("Asia/Seoul")
# 현재 시간 설정
now = datetime.now(tz=local_tz)
ymd = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day).zfill(2)
timestamp = now.strftime('%Y-%m-%d_%H:%M:%S')
num_partitions = 3
redis_conn = redis.Redis(host='redis', port=6379, db=0)
s3_bucket = Variable.get('s3_bucket')

def get_access_token(partition_index):
    access_token_key = f"access_token_{partition_index + 1}"
    access_token = Variable.get(access_token_key)
    return access_token

def process_data(start_index, end_index, process_func):
    for i in range(start_index, end_index):
        process_func(i)

def get_partition_indices(total_count, num_partitions, partition_index):
    group_size = ceil(total_count / num_partitions)
    start_index = partition_index * group_size
    end_index = start_index + group_size
    return start_index, end_index

def token(**kwargs):
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
    artist_id_list = []
    artist_list = []
    search_url = "https://api.spotify.com/v1/search"
    offset = 0
    limit = 50
    k_genre = ["k-pop", "k-pop girl group", "k-pop boy group", "k-rap", "korean r&b", "korean pop", "korean ost",
               "k-rap", "korean city pop", "classic k-pop", "korean singer-songwriter"]
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    s3_client = create_s3_client('aws_conn_id')

    while True:
        params = {
            "q": "genre:K-pop",
            "type": "artist",
            "offset": offset,
            "limit": limit
        }
        status_code, data = get_data(search_url, headers=headers, params=params)
        if status_code == 200:
            total_artist = data["artists"]["total"]
            artists = data["artists"]["items"]

            for idx, artist in enumerate(artists):
                if len(list(set(artist["genres"]).intersection(k_genre))) == 0:
                    continue
                artist_id_list.append(artist["id"])
                artist_list.append(artist)
                save_json_to_s3(artist, s3_bucket, "artists", ymd, str(artist['id']), s3_client)

            offset += limit
            if offset >= total_artist:
                break

    redis_conn.set('artist_list', json.dumps(artist_list))
    redis_conn.set('artist_id_list', json.dumps(artist_id_list))


def scraping_album(ti, num_partitions, partition_index):
    artist_list = json.loads(redis_conn.get('artist_id_list'))
    access_token = get_access_token(partition_index)
    album_id_list = []
    start_index, end_index = get_partition_indices(len(artist_list), num_partitions, partition_index)
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    s3_client = create_s3_client('aws_conn_id')

    for idx, artist_key in enumerate(artist_list[start_index:end_index]):
        offset = 0
        limit = 50

        while True:
            album_url = f"https://api.spotify.com/v1/artists/{artist_key}/albums"
            params = {
                "offset": offset,
                "limit": limit
            }
            status_code, data = get_data(album_url, headers=headers, params=params)
            if status_code == 200:
                total_album = data["total"]

                for i, album in enumerate(data['items']):
                    album_id_list.append(album["id"])
                    save_json_to_s3(album, s3_bucket, "albums", ymd, str(album['id']),s3_client)

                if offset >= total_album:
                    break
                offset += limit

    redis_conn.set(f'album_id_list_{partition_index}', json.dumps(album_id_list))

def merge_album(**context):
    album_id_list = []

    for i in range(num_partitions):
        album_id_list_partition = redis_conn.get(f'album_id_list_{i}')
        if album_id_list_partition is not None:
            album_id_list_partition = json.loads(redis_conn.get(f'album_id_list_{i}'))
            album_id_list.extend(album_id_list_partition)

    redis_conn.set('album_id_list', json.dumps(album_id_list))

def scraping_track(ti, num_partitions, partition_index):
    album_list = json.loads(redis_conn.get('album_id_list'))
    access_token = get_access_token(partition_index)
    track_id_list = []
    start_index, end_index = get_partition_indices(len(album_list), num_partitions, partition_index)
    several_albums_url = "https://api.spotify.com/v1/albums"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    s3_client = create_s3_client('aws_conn_id')

    for i in range(start_index, end_index, 20):
        params = {
            "offset": 0,
            "limit": 50,
            "ids": ','.join(album_list[i:i + 20])
        }
        status_code, data = get_data(several_albums_url, headers=headers, params=params)
        if status_code == 200:
            for album in data['albums']:
                for track in album['tracks']['items']:
                    save_json_to_s3(track, s3_bucket, "tracks", ymd, str(track['id']),s3_client)
                    track_id_list.append(track["id"])

    redis_conn.set(f'track_id_list_{partition_index}', json.dumps(track_id_list))

def merge_track(**context):
    track_id_list = []

    for i in range(num_partitions):
        track_id_partition = json.loads(redis_conn.get(f'track_id_list_{i}'))
        track_id_list.extend(track_id_partition)

    redis_conn.set('track_id_list', json.dumps(track_id_list))

def scraping_audio_features(ti, num_partitions, partition_index):
    track_id_list = json.loads(redis_conn.get('track_id_list'))
    access_token = get_access_token(partition_index)
    start_index, end_index = get_partition_indices(len(track_id_list), num_partitions, partition_index)
    several_audios_url = "https://api.spotify.com/v1/audio-features"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    s3_client = create_s3_client('aws_conn_id')

    for i in range(start_index, end_index, 100):
        track_ids = track_id_list[i:i + 100]
        params = {
            "ids": ','.join(track_ids)
        }
        status_code, data = get_data(several_audios_url, headers=headers, params=params)
        if status_code == 200:
            for audio in data['audio_features']:
                if audio is not None:
                    audio_data = {
                        "danceability": audio.get("danceability"),
                        "energy": audio.get("energy"),
                        "key": audio.get("key"),
                        "loudness": audio.get("loudness"),
                        "mode": audio.get("mode"),
                        "speechiness": audio.get("speechiness"),
                        "acousticness": audio.get("acousticness"),
                        "instrumentalness": audio.get("instrumentalness"),
                        "liveness": audio.get("liveness"),
                        "valence": audio.get("valence"),
                        "tempo": audio.get("tempo"),
                        "type": audio.get("type"),
                        "id": audio.get("id"),
                        "uri": audio.get("uri"),
                        "track_href": audio.get("track_href"),
                        "analysis_url": audio.get("analysis_url"),
                        "duration_ms": audio.get("duration_ms"),
                        "time_signature": audio.get("time_signature")
                    }
                    save_json_to_s3(audio, s3_bucket, "audio_features", ymd, str(audio['id']),s3_client)

def send_slack_message(ti, success):
    slack_token = Variable.get('slack_token')
    slack_alert = SlackAlert(channel="#alert", token=slack_token)
    if success:
        slack_alert.success_msg(ti)
    else:
        slack_alert.fail_msg(ti)


def create_python_operator(task_id, python_callable, op_kwargs=None):
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        provide_context=True,
        dag=dag,
        do_xcom_push=True,
        op_kwargs=op_kwargs or {},
        # on_success_callback=lambda ti: send_slack_message(ti, success=True),
        on_failure_callback=lambda ti: send_slack_message(ti, success=False)
    )

def create_snowflake_operator(task_id, file_path):
    sql_query = get_sql_from_file(file_path).format(ymd=ymd)
    return SnowflakeOperator(
        task_id=task_id,
        sql=sql_query,
        snowflake_conn_id = "snowflake_conn_id",
        autocommit = False,
        dag = dag,
        on_failure_callback=lambda ti: send_slack_message(ti, success=False)
    )

def create_dummy_operator(task_id):
    return DummyOperator(
        task_id=task_id,
        dag=dag,
        on_success_callback=lambda ti: send_slack_message(ti, success=True),
        on_failure_callback=lambda ti: send_slack_message(ti, success=False)
    )

with DAG(
    dag_id = 'kpop_artist_album_track_extraction',
    start_date = datetime(2023,6,26),
    catchup=False,
    tags=['example'],
    schedule_interval=None,
) as dag:

    start_task = create_dummy_operator('start')

    token_task = create_python_operator('token', token)

    scraping_kpop_artist_task = create_python_operator(
        'scrape_kpop_artist_task',
        scraping_kpop_artist,
        op_kwargs={'access_token': "{{ var.value.get('access_token_1') }}"}
    )

    load_artists_task = create_snowflake_operator('load_artists_task', 'dags/config/artist.sql')


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

    load_albums_task = create_snowflake_operator('load_albums_task', 'dags/config/album.sql')


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

    load_tracks_task = create_snowflake_operator('load_tracks_task', 'dags/config/track.sql')

    with TaskGroup("scrape_audio_features_group") as scrape_audio_features_group:
        for i in range(num_partitions):
            scrape_audio_features = create_python_operator(
                f'scrape_audio_features_{i + 1}',
                scraping_audio_features,
                op_kwargs={
                    'num_partitions': num_partitions,
                    'partition_index': i,
                }
            )

    load_audio_features_task = create_snowflake_operator('load_audio_features_task', 'dags/config/audio.sql')

    end_task = create_dummy_operator('end')

    start_task >> token_task >> scraping_kpop_artist_task
    scraping_kpop_artist_task >> [load_artists_task, scrape_album_group]
    scrape_album_group >> merge_album_task >> load_albums_task
    merge_album_task >> scrape_track_group >> merge_track_task >> load_tracks_task
    merge_track_task >> scrape_audio_features_group >> load_audio_features_task >> end_task
