import os
import glob
import requests
import logging
from requests.exceptions import RequestException
from docker import DockerClient

def exists(path):
    r = requests.head(path)
    return r.status_code == requests.codes.ok

def get_files(file_directory, file_pattern):
    return glob.glob(os.path.join(file_directory, file_pattern))

def delete_files(file_directory, file_pattern):
    for file in get_files(file_directory, file_pattern): 
        os.remove(file) 
        
def save_files(df, target_directory, target_file_pattern):
    new_file_path = os.path.join(target_directory, target_file_pattern)
    df.to_csv(new_file_path, index=False)
    
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