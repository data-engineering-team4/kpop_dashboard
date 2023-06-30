import base64
import json
import requests
from airflow.models import Variable
import logging
from requests.exceptions import RequestException
from docker import DockerClient
import time

def set_access_tokens(client_ids, client_secrets):
    access_tokens = ["access_token_1", "access_token_2", "access_token_3"]

    for idx, client in enumerate(client_ids):
        client_id = client_ids[idx]
        client_secret = client_secrets[idx]
        auth_header = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode(
            'ascii')
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

def check_and_restart_selenium():
    client = DockerClient()
    hub_url = 'http://remote_chromedriver:4444/status'
    logging.info(hub_url)

    def is_node_running():
        try:
            response = requests.get(hub_url)
            status = response.json()
            logging.info(status)
            return status['value']['ready'] 
        except RequestException:
            return False

    def restart_selenium_node():
        selenium_container = client.containers.get('remote_chromedriver')
        selenium_container.restart()
        
    if not is_node_running():
        logging.info('Node is down or no available session. Restarting...')
        restart_selenium_node()

def get_data(url, headers, params):
    response = requests.get(url, headers=headers, params=params)
    status_code = response.status_code
    logging.info(f"Status code: {status_code}")

    if status_code == 429:
        logging.warning(f"429 error occurred: {response.text}")
        retry_after = int(response.headers.get('Retry-After'))
        if retry_after:
            logging.info(f"Retrying after {retry_after} seconds.")
            time.sleep(retry_after)
            return get_data(url, headers, params)
    elif status_code != 200:
        logging.info("200도 아니고 429도 아닌", status_code)

    data = response.json()
    return status_code, data

def get_sql_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

