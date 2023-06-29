import base64
import requests
import json
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import time
from slack_sdk import WebClient
from datetime import datetime

def set_access_tokens(client_ids, client_secrets):
    """
    Spotify API를 사용할 때 필요한 Token 인증 방식
    - 1시간동안 유효한 토큰 발급
    - 따로 뺀 이유는 1시간이 넘는 etl작업시 예외처리로 토큰 재발급하기 위해
    """
    access_tokens = ["access_token_1", "access_token_2", "access_token_3"]

    for idx, client in enumerate(client_ids):
        client_id = client_ids[idx]
        client_secret = client_secrets[idx]

        auth_header = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode(
            'ascii')  # Base64로 인코딩된 인증 헤더 생성
        token_url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": f'Basic {auth_header}'
        }
        payload = {
            "grant_type": "client_credentials"
        }

        response = requests.post(token_url, data=payload, headers=headers)
        access_token = json.loads(response.text)["access_token"]
        Variable.set(access_tokens[idx], access_token)

def get_data(url, headers, params):
    response = requests.get(url, headers=headers, params=params)
    logging.info(f"Response object: {response}")
    status_code = response.status_code
    logging.info(f"Status code: {status_code}")

    if status_code == 429:
        logging.warning(f"429 error occurred: {response.text}")
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            logging.info(f"Retrying after {retry_after} seconds.")
            time.sleep(5)
            return get_data(url, headers, params)

    data = response.json()
    return status_code, data

def save_json_to_s3(data, s3_bucket, s3_key):
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")  # AWS 연결 설정

    s3_client = s3_hook.get_conn()
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
class SlackAlert:
    def __init__(self, channel, token):
        self.channel = channel
        self.client = WebClient(token=token)
    def success_msg(self, msg):
        text = f"""
            date : {datetime.today().strftime('%Y-%m-%d')}
            alert : 
                task 실행 성공!! 
                    task id : {msg.get('task_instance').task_id}, 
                    dag id : {msg.get('task_instance').dag_id}, 
                    log url : {msg.get('task_instance').log_url}
            """
        self.client.chat_postMessage(channel=self.channel, text=text)
    def fail_msg(self, msg):
        text = f"""
            date : {datetime.today().strftime('%Y-%m-%d')}  
            alert : 
                task 실행 실패 ㅠㅠ 
                    task id : {msg.get('task_instance').task_id}, 
                    dag id : {msg.get('task_instance').dag_id}, 
                    log url : {msg.get('task_instance').log_url}
        """
        self.client.chat_postMessage(channel=self.channel, text=text)