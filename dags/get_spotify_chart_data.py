from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from datetime import datetime, timedelta
import requests
import logging
import os
import boto3
import glob
import pandas as pd

def exists(path):
    r = requests.head(path)
    return r.status_code == requests.codes.ok

def get_files(file_directory, file_pattern):
    return glob.glob(os.path.join(file_directory, file_pattern))

def delete_files(file_directory, file_pattern):
    for file in get_files(file_directory, file_pattern): 
        os.remove(file)  
        
@task
def get_csv_files(date):
    countries = ["ar", "au", "at", "by", "be", "bo", "br", "bg", "ca", "cl", 
             "co", "cr", "cy", "cz", "dk", "do", "ec", "ch", "cl", "de", 
             "dk", "do", "ee", "eg", "es", "fi", "FR", "gb", "gr", "gt", 
             "hk", "hn", "hu", "id", "ie", "il", "in", "is", "it", "jp", 
             "kr", "kz", "lt", "lu", "lv", "ma", "mx", "my", "ng", "ni", 
             "nl", "no", "nz", "pa", "pe", "ph", "pk", "pl", "pt", "py", 
             "ro", "sa", "se", "sg", "sk", "sv", "th", "tr", "tw", "ua", 
             "us", "uy", "ve", "vn", "za"]

    logging.info(f'date : {date}')
    
    logging.info('delete before saving new files')
    file_directory = '/opt/downloads'
    file_pattern = f"regional-*-weekly-{date}.csv"
    delete_files(file_directory, file_pattern)
    
    # Set webriver options
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors')
    user_agent = 'userMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    options.add_argument(f'user-agent={user_agent}')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('-headless')
    prefs = {"download.default_directory" : file_directory}
    options.add_experimental_option("prefs", prefs)
    remote_webdriver = 'remote_chromedriver'
    
    with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
        logging.info("scraping start")
        
        driver.get(f"https://charts.spotify.com/charts/view/regional-global-daily/{date}")
        time.sleep(2)
        
        logging.info("click log in")
        login_button = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/main/div[2]/div/header/div/div[2]/a[3]/div[1]')
        login_button.click()
        
        logging.info("input id/password and log in")
        spotify_id = Variable.get('spotify_id')
        spotify_password = Variable.get('spotify_password')
        driver.find_element(By.XPATH, '//*[@id="login-username"]').send_keys(spotify_id)
        driver.find_element(By.XPATH, '//*[@id="login-password"]').send_keys(spotify_password)
        login_button = driver.find_element(By.XPATH, '//*[@id="login-button"]')
        login_button.click()
        
        time.sleep(2)
    
        logging.info("csv file download start")
        count = 0
        for country in countries:
            url = f"https://charts.spotify.com/charts/view/regional-{country}-weekly/{date}"
            if exists(url):
                driver.get(url)
                logging.info(f'url : {url}')
                time.sleep(3)
                csv_download_button = None
                try:
                    csv_download_button = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/main/div[2]/div[3]/div/div/a/button')                                
                except Exception as e:
                    logging.info(e)
                    continue
                logging.info(f"{country}-{date} csv file download")
                csv_download_button.click()
                count += 1
        logging.info(f'total {count} files downloaded')  

@task
def transform_and_combine_csv_files(source_directory, target_directory, file_pattern):
    
    logging.info("create dataframe for integration")
    header = ['rank','track_id','artist_names','track_name','peak_rank','previous_rank','weeks_on_chart','streams', 'country_code','chart_date']
    combined_df = pd.DataFrame(columns=header)
    files = get_files(source_directory, file_pattern)
    fn = os.path.basename(files[0])
    _, _, _, year, month, day = fn.split('-')
    date = f'{year}-{month}-{day}'

    logging.info("loop files start")
    # 파일명을 기반으로 국가명과 일자 정보를 추출하여 데이터에 추가
    for file in files:
        file_name = os.path.basename(file)
        logging.info(f"file name : {file_name}")
        file_path = os.path.join(source_directory, file_name)
        _, country, _, year, month, day = file_name.split('-')
        
        # 기존 데이터 로드
        df = pd.read_csv(file_path)
        
        # 필요없는 컬럼 삭제
        df = df.drop(['source'], axis=1)

        # 데이터 변환
        df['uri'] = df['uri'].str.split(':').str[-1]
        df.rename(columns={'uri': 'track_id'}, inplace=True)
        df['track_name'] = df['track_name'].str.strip("'")
        df['track_name'] = df['track_name'].str.replace('"', '')
        df['artist_names'] = df['artist_names'].str.strip("'")
        df['artist_names'] = df['artist_names'].str.replace('"', '')
        
        # 국가명과 일자 정보 추가 
        df['country_code'] = country.upper()
        df['chart_date'] = datetime(int(year), int(month), int(day.split('.')[0]))
        
        # DataFrame을 통합
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    logging.info("loop files end")
    
    logging.info("save combined csv file")
    new_filename = f"countries_weekly_top200_{date}"
    new_file_path = os.path.join(target_directory, new_filename)
    combined_df.to_csv(new_file_path, index=False)
    
    return new_file_path
    
@task
def upload_files_to_s3(file_directory, file_pattern, bucket_name, folder_name):
    
    logging.info("get aws connection")
    aws_conn = BaseHook.get_connection('aws_conn_id')
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password

    logging.info("create s3 client")
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    
    logging.info("get files from path")
    files = get_files(file_directory, file_pattern)
    
    logging.info(f"path : {os.path.join(file_directory, file_pattern)}")
    logging.info(f"files count : {len(files)}")
    logging.info("loop files start")
    
    for file in files:
        file_name = os.path.basename(file)
        s3_key = os.path.join(folder_name, file_name)
    
        logging.info(f"upload file : {s3_key}")
        with open(file, 'rb') as data:
            s3.upload_fileobj(data, bucket_name, s3_key)

        logging.info(f"remove {file_name}")  
        os.remove(file)  
        
    logging.info("loop files end")

      

with DAG(
    dag_id='get_spotify_chart_data',  
    start_date=datetime(2023, 5, 25),
    schedule_interval=timedelta(days=7),  # 실행 간격
    catchup=True
) as dag:
    # date = '2023-06-15'
    date = '{{ ds }}'
    bucket_name = 'kpop-analysis'
    source_folder_name = f'weekly_chart/countries/{date}'
    result_folder_name = f'weekly_chart/result/{date}'
    source_directory = os.getenv('AIRFLOW_HOME') + "/downloads"
    target_directory = os.getenv('AIRFLOW_HOME') + "/data"
    source_file_pattern = f"regional-*-weekly-{date}.csv"
    target_file_pattern = f"countries_weekly_top200_{date}.csv"
    
    start = EmptyOperator(
        task_id="start"
    )
    
    # check_combined_file = FileSensor(
    #     task_id='check_combined_file', 
    #     filepath=os.getenv('AIRFLOW_HOME') + "/data"
    # )
    
    end = EmptyOperator(
        task_id="end"
    )
    
    start >> get_csv_files(date) >> transform_and_combine_csv_files(source_directory, target_directory, source_file_pattern) >> [upload_files_to_s3(source_directory, source_file_pattern, bucket_name, source_folder_name)] >> upload_files_to_s3(target_directory, target_file_pattern, bucket_name, result_folder_name) >> end
    
             