from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
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
        
def _get_csv_files(date, file_directory, file_pattern):
    # countries = ["ar", "au", "at", "by", "be", "bo", "br", "bg", "ca", "cl", 
    #          "co", "cr", "cy", "cz", "dk", "do", "ec", "ch", "cl", "de", 
    #          "dk", "do", "ee", "eg", "es", "fi", "FR", "gb", "gr", "gt", 
    #          "hk", "hn", "hu", "id", "ie", "il", "in", "is", "it", "jp", 
    #          "kr", "kz", "lt", "lu", "lv", "ma", "mx", "my", "ng", "ni", 
    #          "nl", "no", "nz", "pa", "pe", "ph", "pk", "pl", "pt", "py", 
    #          "ro", "sa", "se", "sg", "sk", "sv", "th", "tr", "tw", "ua", 
    #          "us", "uy", "ve", "vn", "za"]
    countries = ['us','kr']

    logging.info(f'date : {date}')
    
    logging.info('delete before saving new files')
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

def _transform_and_combine_csv_files(source_directory, target_directory, source_file_pattern, target_file_pattern):
    
    logging.info("create dataframe for integration")
    header = ['rank','track_id','artist_names','track_name','peak_rank','previous_rank','weeks_on_chart','streams', 'country_code','chart_date']
    combined_df = pd.DataFrame(columns=header)
    files = get_files(source_directory, source_file_pattern)

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
    
    logging.info('delete before saving new files')
    delete_files(target_directory, target_file_pattern)
    
    logging.info("save combined csv file")
    new_file_path = os.path.join(target_directory, target_file_pattern)
    combined_df.to_csv(new_file_path, index=False)
    
def _upload_files_to_s3(file_directory, file_pattern, bucket_name, folder_name):
    
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
        
    logging.info("loop files end")  

def _delete_uploaded_files(file_directory, file_pattern):
    delete_files(file_directory, file_pattern)

with DAG(
    dag_id='spotify_regional_weekly_chart',  
    start_date=datetime(2023, 5, 25),
    schedule_interval=timedelta(days=7),  # 실행 간격
    catchup=True
) as dag:
    date = '{{ ds }}'
    bucket_name = 'kpop-analysis'
    source_folder_name = f'raw_data/spotify/chart/{date}'
    target_folder_name = f'transformed_data/spotify/chart/{date}'
    source_directory = os.getenv('AIRFLOW_HOME') + "/downloads"
    target_directory = os.getenv('AIRFLOW_HOME') + "/data"
    source_file_pattern = f"regional-*-weekly-{date}.csv"
    target_file_pattern = f"regional-weekly-{date}.csv"
    schema = 'raw'
    table = 'regional_weekly_chart'

    start = EmptyOperator(
        task_id="start"
    )
    
    get_csv_files = PythonOperator(
        task_id='get_csv_files',
        python_callable=_get_csv_files,
        op_args=[date, source_directory, source_file_pattern],  
    )
    
    upload_raw_files_to_s3 = PythonOperator(
        task_id='upload_raw_files_to_s3',
        python_callable=_upload_files_to_s3,
        op_args=[source_directory, source_file_pattern, bucket_name, source_folder_name],  
    )
    
    with TaskGroup("process_raw_files", tooltip="process raw files") as process_raw_files:
        transform_and_combine_csv_files = PythonOperator(
            task_id='transform_and_combine_csv_files',
            python_callable=_transform_and_combine_csv_files,
            op_args=[source_directory, target_directory, source_file_pattern, target_file_pattern],  
        )
        
        upload_transformed_files_to_s3 = PythonOperator(
            task_id='upload_transformed_files_to_s3',
            python_callable=_upload_files_to_s3,
            op_args=[target_directory, target_file_pattern, bucket_name, target_folder_name],  
        )
        
        check_transformed_file_exists = S3KeySensor(
            task_id='check_transformed_file_exists',
            bucket_key=f's3://{bucket_name}/{target_folder_name}/{target_file_pattern}',
            aws_conn_id='aws_conn_id',
            timeout=18 * 60 * 60,
            poke_interval=10 * 60,
        )
        
        transform_and_combine_csv_files >> upload_transformed_files_to_s3 >> check_transformed_file_exists 
        
    
    load_s3_to_snowflake = SnowflakeOperator(
        task_id='load_s3_to_snowflake',
        sql=f"""
            BEGIN;

            CREATE TEMPORARY TABLE temp_table AS 
            SELECT 
                $1 AS rank, 
                $2 AS track_id, 
                $3 AS artist_names, 
                $4 AS track_name, 
                $5 AS peak_rank, 
                $6 AS previous_rank, 
                $7 AS weeks_on_chart, 
                $8 AS streams, 
                $9 AS country_code, 
                $10 AS chart_date
            FROM @{schema}.transformed_data_stage_csv/spotify/chart/{date}/{target_file_pattern};
            
            DELETE FROM {schema}.{table} 
            WHERE DATE(chart_date) = '{date}'::DATE;

            INSERT INTO {schema}.{table} 
            SELECT t.* 
            FROM temp_table t;

            COMMIT;
            """,
            
        snowflake_conn_id='snowflake_conn_id',
        autocommit=False,
    )
    
    with TaskGroup("delete_files_from_container", tooltip="Delete the files from the container after they have been successfully loaded into S3") as delete_files_from_container:
        delete_uploaded_raw_files = PythonOperator(
            task_id='delete_uploaded_files',
            python_callable=_delete_uploaded_files,
            op_args=[source_directory, source_file_pattern],  
        )
        
        delete_uploaded_transformed_files = PythonOperator(
            task_id='delete_uploaded_transformed_files',
            python_callable=_delete_uploaded_files,
            op_args=[target_directory, target_file_pattern],  
        )
        [delete_uploaded_raw_files, delete_uploaded_transformed_files]
        
    
    end = EmptyOperator(
        task_id="end"
    )
    
    start >> get_csv_files >> [upload_raw_files_to_s3, process_raw_files] >> load_s3_to_snowflake >> delete_files_from_container >> end
