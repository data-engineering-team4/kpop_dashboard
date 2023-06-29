from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import json
from math import ceil
import time
import logging
import redis
from utils import set_access_tokens, get_data, save_json_to_s3
from utils import SlackAlert

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
    artist_id_list = []
    artist_list = []
    search_url = "https://api.spotify.com/v1/search"

    offset = 0
    limit = 50

    k_genre = ["k-pop", "k-pop girl group", "k-pop boy group", "k-rap", "korean r&b", "korean pop", "korean ost",
               "k-rap", "korean city pop", "classic k-pop", "korean singer-songwriter"]

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
        status_code, data = get_data(search_url, headers=headers, params=params)

        if status_code == 200:
            total_artist = data["artists"]["total"]  # API에서 retrun 해주는 전체 list 수
            artists = data["artists"]["items"]

            for idx, artist in enumerate(artists):
                # search k-pop 조회 결과, 1000개가 조회되는데, k-pop에 해당하지 않는 artist 존재
                if len(list(set(artist["genres"]).intersection(k_genre))) == 0:
                    continue
                artist_id_list.append(artist["id"])
                artist_list.append(artist)

            offset += limit
            if offset >= total_artist:
                break

        else:
            # response error
            break

    redis_conn.set('artist_list', json.dumps(artist_list))
    redis_conn.set('artist_id_list', json.dumps(artist_id_list))

def load_kpop_artists(**context):
    execution_date = context["execution_date"].strftime("%Y-%m-%d")
    artist_list = json.loads(redis_conn.get('artist_list'))
    s3_bucket = Variable.get('s3_bucket')
    s3_key = f"artists/{execution_date}_artists.json"
    save_json_to_s3(artist_list, s3_bucket, s3_key)

def scraping_album(ti, num_partitions, partition_index):
    artist_list = json.loads(redis_conn.get('artist_id_list'))

    access_token_key = f"access_token_{partition_index + 1}"
    access_token = Variable.get(access_token_key)

    album_list = []
    album_id_list = []
    error_artist_list = []

    num_artists = len(artist_list)
    group_size = ceil(num_artists / num_partitions)
    start_index = partition_index * group_size
    end_index = start_index + group_size

    for idx, artist_key in enumerate(artist_list[start_index:end_index]):
        offset = 0
        limit = 50
        while True:
            album_url = f"https://api.spotify.com/v1/artists/{artist_key}/albums"
            params = {
                "offset": offset,
                "limit": limit
            }
            headers = {
                "Authorization": f"Bearer {access_token}"
            }
            status_code, data = get_data(album_url, headers=headers, params=params)
            print(status_code, data)
            if status_code == 200:
                total_album = data["total"]
                for i, album in enumerate(data['items']):
                    album_id_list.append(album["id"])
                    album_list.append(album)

                if offset >= total_album:
                    break
                offset += limit
            else:
                error_artist_list.append(artist_key)
                print("error") #todo 에러리스트에 넣어서 다시 하는 과정(429에러)
                time.sleep(5)
                break
    redis_conn.set(f'album_list_{partition_index}', json.dumps(album_list))
    redis_conn.set(f'album_id_list_{partition_index}', json.dumps(album_id_list))

def merge_album(**context):
    merged_album_data = []
    album_id_list = []
    for i in range(num_partitions):
        album_list_partition = json.loads(redis_conn.get(f'album_list_{i}'))
        album_id_list_partition = json.loads(redis_conn.get(f'album_id_list_{i}'))
        merged_album_data.extend(album_list_partition)
        album_id_list.extend(album_id_list_partition)
    redis_conn.set('merged_album_data', json.dumps(merged_album_data))
    redis_conn.set('album_id_list', json.dumps(album_id_list))

def load_album(**context):
    execution_date = context["execution_date"].strftime("%Y-%m-%d")
    merged_album_data = json.loads(redis_conn.get('merged_album_data'))
    if merged_album_data:
        album_list = merged_album_data
    else:
        album_list = []
    s3_bucket = Variable.get('s3_bucket')
    s3_key = f"albums/{execution_date}_albums.json"
    save_json_to_s3(album_list, s3_bucket, s3_key)

def scraping_track(ti, num_partitions, partition_index):
    album_list = json.loads(redis_conn.get('album_id_list'))

    access_token_key = f"access_token_{partition_index + 1}"
    access_token = Variable.get(access_token_key)

    track_list = []
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
            status_code, data = get_data(track_url, headers=headers, params=params)
            if status_code == 200:
                total_track = data["total"]
                for track in data['items']:
                    track_list.append(track)
                    track_id_list.append(track["id"])
                if offset >= total_track:
                    break
                offset += limit
            else:
                error_track_list.append(album_key)
                print("error")
                time.sleep(5)
                break

    redis_conn.set(f'track_list_{partition_index}', json.dumps(track_list))
    redis_conn.set(f'track_id_list_{partition_index}', json.dumps(track_id_list))

def merge_track(**context):
    merged_track_data = []
    track_id_list = []
    for i in range(1, 4):
        track_partition = json.loads(redis_conn.get(f'track_list_{i}'))
        track_id_partition = json.loads(redis_conn.get(f'track_id_list_{i}'))
        if track_partition is not None:
            merged_track_data.extend(track_partition)
            track_id_list.extend(track_id_partition)
    redis_conn.set('merged_track_data', json.dumps(merged_track_data))
    redis_conn.set('track_id_list', json.dumps(track_id_list))

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

def scraping_audio_features(ti, num_partitions, partition_index):
    track_id_list = json.loads(redis_conn.get('track_id_list'))

    access_token_key = f"access_token_{partition_index + 1}"
    access_token = Variable.get(access_token_key)

    audio_features_list = []
    error_track_list = []

    num_tracks = len(track_id_list)
    group_size = ceil(num_tracks / num_partitions)
    start_index = partition_index * group_size
    end_index = start_index + group_size

    for idx, track_key in enumerate(track_id_list[start_index:end_index]):
        audio_features_url = f"https://api.spotify.com/v1/audio-features/{track_key}"
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        status_code, data = get_data(audio_features_url, headers=headers)
        if status_code == 200:
            audio_features_list.append(data)
        else:
            error_track_list.append(track_key)
            logging.error(f"Error audio features {track_key}")
            time.sleep(5)

    redis_conn.set(f'audio_features_list_{partition_index}', json.dumps(audio_features_list))

def merge_audio_features(**context):
    merged_audio_features_data = []

    for i in range(1, num_partitions+1):
        audio_features_partition = json.loads(redis_conn.get(f'audio_features_list_{i}'))
        if audio_features_partition is not None:
            merged_audio_features_data.extend(audio_features_partition)

    redis_conn.set('merged_audio_features_data', json.dumps(merged_audio_features_data))

def load_audio_features(**context):
    execution_date = context["execution_date"].strftime("%Y-%m-%d")
    merged_audio_features_data = json.loads(redis_conn.get('merged_audio_features_data'))
    if merged_audio_features_data:
        audio_features_list = merged_audio_features_data
    else:
        audio_features_list = []
    s3_bucket = Variable.get('s3_bucket')
    s3_key = f"audio_features/{execution_date}_audio_features.json"
    save_json_to_s3(audio_features_list, s3_bucket, s3_key)

def send_slack_message(ti, success):
    slack_alert = SlackAlert(channel="#alert", token="") #각자 발급받은 token 집어넣기

    if success:
        slack_alert.success_msg(ti)
    else:
        slack_alert.fail_msg(ti)

def end(**context):
    pass

def create_python_operator(task_id, python_callable, op_kwargs=None):
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        provide_context=True,
        dag=dag,
        do_xcom_push=True,
        op_kwargs=op_kwargs or {},
        #on_success_callback=lambda ti: send_slack_message(ti, success=True),
        #on_failure_callback=lambda ti: send_slack_message(ti, success=False)
    )

with DAG(
    dag_id = 'kpop_artist_album_track_extraction',
    start_date = datetime(2023,6,26),
    catchup=False,
    tags=['example'],
    schedule_interval=None,
    default_args={
        'client_id_1': "{{ var.value.get('client_id_1') }}",
        'client_id_2': "{{ var.value.get('client_id_2') }}",
        'client_id_3': "{{ var.value.get('client_id_3') }}",
        'client_secret_1': "{{ var.value.get('client_secret_1') }}",
        'client_secret_2': "{{ var.value.get('client_secret_2') }}",
        'client_secret_3': "{{ var.value.get('client_secret_3') }}",
    }
) as dag:

    start_task = create_python_operator('start', start)

    scraping_kpop_artist_task = create_python_operator(
        'scrape_kpop_artist_task',
        scraping_kpop_artist,
        op_kwargs={'access_token': "{{ var.value.get('access_token_1') }}"}
    )

    load_artists_task = create_python_operator('load_artists_task', load_kpop_artists)

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

    load_album_task = create_python_operator('load_album_task', load_album)

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

    load_track_task = create_python_operator('load_track_task', load_track)

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

    merge_audio_features_task = create_python_operator('merge_audio_features_task', merge_audio_features)

    load_audio_features_task = create_python_operator('load_audio_features_task', load_audio_features)

    end_task = create_python_operator('end_task', end)

    start_task >> scraping_kpop_artist_task
    scraping_kpop_artist_task >> [load_artists_task, scrape_album_group]
    scrape_album_group >> merge_album_task >> load_album_task
    merge_album_task >> scrape_track_group >> merge_track_task >> load_track_task
    merge_track_task >> scrape_audio_features_group >> merge_audio_features_task >> load_audio_features_task