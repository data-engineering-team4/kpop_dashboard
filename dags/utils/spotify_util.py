import base64
import json
import requests
from airflow.models import Variable

def set_access_tokens(client_ids, client_secrets) :
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


def get_access_token():
    """
    Spotify API를 사용할 때, 1시간 동안 유효한 토큰 발급
    """
    
    client_id = "0fc23d50d5f04a22b922467e4b1e551c"
    client_secret = "76c849a25c194974a7b21c39ed7c3052"

    auth_header = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')    # Base64로 인코딩된 인증 헤더 생성
    token_url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization" : f'Basic {auth_header}'
    }
    payload = {
        "grant_type" : "client_credentials"
    }

    response = requests.post(token_url, data = payload, headers= headers)
    access_token = json.loads(response.text)["access_token"]
    
    return {"Authorization" : f"Bearer {access_token}"}


print(get_access_token())