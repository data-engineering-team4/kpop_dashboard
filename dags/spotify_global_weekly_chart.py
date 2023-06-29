from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from operators.upload_files_to_s3_operator import UploadFilesToS3Operator
from operators.delete_files_operator import DeleteFilesOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from datetime import datetime, timedelta
import logging
import os
import pandas as pd
from utils import get_files
from utils import delete_files
from utils import save_files
from utils import check_and_restart_selenium
        
def _get_csv_files(date, file_directory, file_pattern):
    time.sleep(15)
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
    
    with webdriver.Remote('remote_chromedriver:4444/wd/hub', options=options) as driver:
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
        url = f"https://charts.spotify.com/charts/view/regional-global-weekly/{date}"
        driver.get(url)
        time.sleep(2)
        
        csv_download_button = None
        try:
            csv_download_button = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/main/div[2]/div[3]/div/div/a/button')                                
        except Exception as e:
            logging.info(e)
            raise AirflowFailException("No such element. DAG failed")
        
        csv_download_button.click()
        logging.info(f'files download complete')  

def _transform_and_combine_csv_files(source_directory, target_directory, source_file_pattern, target_file_pattern):
    
    logging.info("create dataframe for integration")
    header = ['rank','track_id','artist_names','track_name','peak_rank','previous_rank','weeks_on_chart','streams', 'country_code','chart_date']
    df = pd.DataFrame(columns=header)
    files = get_files(source_directory, source_file_pattern)

    # 파일명을 기반으로 국가명과 일자 정보를 추출하여 데이터에 추가
    file_name = os.path.basename(files[0])
    file_path = os.path.join(source_directory, file_name)
    logging.info(f"file path : {file_path}")
    _, _, _, year, month, day = file_name.split('-')
    
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

    # 일자 정보 추가        
    df['chart_date'] = datetime(int(year), int(month), int(day.split('.')[0]))
        
    logging.info('delete before saving new files')
    delete_files(target_directory, target_file_pattern)
    
    logging.info("save combined csv file")
    save_files(df, target_directory, target_file_pattern)

with DAG(
    dag_id='spotify_global_weekly_chart',  
    start_date=datetime(2023, 5, 25),
    schedule_interval=timedelta(days=7),  # 실행 간격
    catchup=True
) as dag:
    date = '{{ ds }}'
    airflow_home = os.getenv('AIRFLOW_HOME')
    aws_conn_id = 'aws_conn_id'
    bucket_name = 'kpop-analysis'
    source_directory = f"{airflow_home}/downloads"
    target_directory = f"{airflow_home}/data"
    source_folder_name = f'raw_data/spotify/chart/{date}'
    target_folder_name = f'transformed_data/spotify/chart/{date}'
    source_file_pattern = f"regional-global-weekly-{date}.csv"
    target_file_pattern = f"global-weekly-{date}.csv"
    schema = 'raw'
    table = 'global_weekly_chart'
    sql = f"""
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
                $9 AS chart_date
            FROM @{schema}.transformed_data_stage_csv/spotify/chart/{date}/{target_file_pattern};
            
            DELETE FROM {schema}.{table} 
            WHERE chart_date = '{date}';

            INSERT INTO {schema}.{table} 
            SELECT t.* 
            FROM temp_table t;

            COMMIT;
            """

    start = EmptyOperator(
        task_id="start"
    )

    check_and_restart_selenium_node = PythonOperator(
        task_id='check_and_restart_selenium_node',
        python_callable=check_and_restart_selenium
    )
    
    get_csv_files = PythonOperator(
        task_id='get_csv_files',
        python_callable=_get_csv_files,
        op_args=[date, source_directory, source_file_pattern],  
    )
    
    upload_raw_files_to_s3 = UploadFilesToS3Operator(
        task_id='upload_raw_files_to_s3',
        conn_id=aws_conn_id,
        file_directory=source_directory,
        file_pattern=source_file_pattern,
        bucket_name=bucket_name,
        folder_name=source_folder_name
    )
    
    with TaskGroup("process_raw_files", tooltip="process raw files") as process_raw_files:
        transform_and_combine_csv_files = PythonOperator(
            task_id='transform_and_combine_csv_files',
            python_callable=_transform_and_combine_csv_files,
            op_args=[source_directory, target_directory, source_file_pattern, target_file_pattern],  
        )
        
        upload_transformed_files_to_s3 = UploadFilesToS3Operator(
            task_id='upload_transformed_files_to_s3',
            conn_id=aws_conn_id,
            file_directory=target_directory,
            file_pattern=target_file_pattern,
            bucket_name=bucket_name,
            folder_name=target_folder_name
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
        sql=sql,
        snowflake_conn_id='snowflake_conn_id',
        autocommit=False,
    )
    
    with TaskGroup("delete_files_from_container", tooltip="Delete the files from the container after they have been successfully loaded into S3") as delete_files_from_container:
        delete_uploaded_raw_files = DeleteFilesOperator(
            task_id='delete_uploaded_raw_files',
            file_directory=source_directory,
            file_pattern=source_file_pattern,
        )

        delete_uploaded_transformed_files = DeleteFilesOperator(
            task_id='delete_uploaded_transformed_files',
            file_directory=target_directory,
            file_pattern=target_file_pattern,
        )
        
        [delete_uploaded_raw_files, delete_uploaded_transformed_files]
        
    end = EmptyOperator(
        task_id="end"
    )
    
    start >> check_and_restart_selenium_node >> get_csv_files >> [upload_raw_files_to_s3, process_raw_files] >> load_s3_to_snowflake >> delete_files_from_container >> end
