from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import json
from math import ceil

from utils import set_access_tokens, get_data, save_json_to_s3

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

    return artist_list, artist_id_list

def load_kpop_artists(**context):
    execution_date = context["execution_date"].strftime("%Y-%m-%d")
    artist_list = context['task_instance'].xcom_pull(task_ids='scrape_kpop_artist_task')[0]
    # s3_bucket = "{{ var.value.get('s3_bucket') }}"
    s3_bucket = Variable.get('s3_bucket')
    s3_key = f"artists/{execution_date}_artists.json"
    save_json_to_s3(artist_list, s3_bucket, s3_key)

def scraping_album(ti, num_partitions, partition_index):
    artist_list = ti.xcom_pull(task_ids="scrape_kpop_artist_task")[1]

    access_token_key = f"access_token_{partition_index + 1}"
    access_token = Variable.get(access_token_key)

    album_list = []
    album_id_list = []

    num_artists = len(artist_list)
    group_size = ceil(num_artists / num_partitions)
    start_index = partition_index * group_size
    end_index = start_index + group_size


    for idx, artist_key in enumerate(artist_list[start_index:end_index]):
        try:
            album_url = f"https://api.spotify.com/v1/artists/{artist_key}/albums"
            params = {
                "offset": 0,
                "limit": 50
            }
            headers = {
                "Authorization": f"Bearer {access_token}"
            }
            status_code, data = get_data(album_url, headers=headers, params=params)

            if status_code == 200:
                total_album = data["total"]
                for i, album in enumerate(data['items']):
                    album_id_list.append(album["id"])
                    album_list.append(album)

        except Exception as e:
            pass

    return album_list, album_id_list

def merge_album(**context):
    merged_album_data = []

    for i in range(1, 4):
        album = context['task_instance'].xcom_pull(task_ids=f'scrape_album_{i}')
        if album is not None:
            merged_album_data.extend(album)

    return merged_album_data

def load_album(**context):
    execution_date = context["execution_date"].strftime("%Y-%m-%d")
    album_list = context['task_instance'].xcom_pull(task_ids='merge_album_task')
    if album_list:
        album_list = album_list[0]
    else:
        album_list = []
    s3_bucket = Variable.get('s3_bucket')
    # s3_bucket = "{{ var.value.get('s3_bucket') }}"
    s3_key = f"albums/{execution_date}_albums.json"
    save_json_to_s3(album_list, s3_bucket, s3_key)

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

    start_task = PythonOperator(
        task_id='start',
        python_callable=start,
        provide_context=True,
        dag=dag
    )

    scraping_kpop_artist_task = PythonOperator(
        task_id='scrape_kpop_artist_task',
        python_callable=scraping_kpop_artist,
        dag=dag,
        op_kwargs={'access_token': "{{ var.value.get('access_token_1') }}"},
        do_xcom_push=True,
    )

    load_artists_task = PythonOperator(
        task_id='load_artists_task',
        python_callable=load_kpop_artists,
        dag=dag,
        provide_context=True,
    )

    num_partitions = 3
    with TaskGroup("scrape_album_group") as scrape_album_group:
        for i in range(num_partitions):
            scrape_album = PythonOperator(
                task_id=f'scrape_album_{i + 1}',
                python_callable=scraping_album,
                provide_context=True,
                op_kwargs={
                    'num_partitions': num_partitions,
                    'partition_index': i,
                },
                dag=dag,
            )

    merge_album_task = PythonOperator(
        task_id='merge_album_task',
        python_callable=merge_album,
        dag=dag,
        provide_context=True,
    )

    load_album_task = PythonOperator(
        task_id='load_album_task',
        python_callable=load_album,
        dag=dag,
        provide_context=True,
    )

    start_task >> scraping_kpop_artist_task
    scraping_kpop_artist_task >> [load_artists_task, scrape_album_group]
    scrape_album_group >> merge_album_task >> load_album_task