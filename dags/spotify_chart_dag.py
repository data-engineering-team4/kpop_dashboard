import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from selenium import webdriver
from selenium.webdriver.common.by import By

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from operators.upload_files_to_s3_operator import UploadFilesToS3Operator
from operators.delete_files_operator import DeleteFilesOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup

from utils.common_util import get_files, delete_files, save_files, get_sql, exists, get_table_info
from utils.spotify_util import check_and_restart_selenium

def _get_csv_files(table_name, date, file_directory, file_pattern):
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
    
        if table_name == 'global_weekly_chart':
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
            
        elif table_name == 'country_weekly_chart':
            countries = ["ar", "au", "at", "by", "be", "bo", "br", "bg", "ca", "cl", 
             "co", "cr", "cy", "cz", "dk", "do", "ec", "ch", "cl", "de", 
             "dk", "do", "ee", "eg", "es", "fi", "FR", "gb", "gr", "gt", 
             "hk", "hn", "hu", "id", "ie", "il", "in", "is", "it", "jp", 
             "kr", "kz", "lt", "lu", "lv", "ma", "mx", "my", "ng", "ni", 
             "nl", "no", "nz", "pa", "pe", "ph", "pk", "pl", "pt", "py", 
             "ro", "sa", "se", "sg", "sk", "sv", "th", "tr", "tw", "ua", 
             "us", "uy", "ve", "vn", "za"]
            
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
                        raise
                    logging.info(f"{country}-{date} csv file download")
                    csv_download_button.click()
                    count += 1
            if count == 0:
                raise AirflowFailException("File not found. DAG failed.")
            logging.info(f'total {count} files downloaded') 
        
        elif table_name == 'global_yearly_chart':
            url = f"https://charts.spotify.com/charts/view/regional-global-daily/{date}"
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

def _transform_and_combine_csv_files(table_name, source_directory, target_directory, source_file_pattern, target_file_pattern):
    
    logging.info("create dataframe for integration")
    header = ['rank','track_id','artist_names','track_name','peak_rank','previous_rank','weeks_on_chart','streams', 'country_code','chart_date']
    files = get_files(source_directory, source_file_pattern)
    
    if table_name == 'global_weekly_chart' or table_name == 'global_yearly_chart':
        df = pd.DataFrame(columns=header)
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
        
    elif table_name == 'country_weekly_chart':
        combined_df = pd.DataFrame(columns=header)
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
        save_files(combined_df, target_directory, target_file_pattern)
    
# 파라미터화된 DAG 생성 함수
def create_dag(start_date, schedule_interval, **params):
    date = '{{ macros.ds_add(ds, -2) }}'
    airflow_home = os.getenv('AIRFLOW_HOME')
    aws_conn_id = 'aws_conn_id'
    bucket_name = 'kpop-analysis'
    source_directory = f"{airflow_home}/downloads"
    target_directory = f"{airflow_home}/data"
    source_folder_name = f'raw_data/spotify/chart/{date}'
    target_folder_name = f'transformed_data/spotify/chart/{date}'
    source_file_pattern = params.get('source_file_pattern').format(date=date)
    target_file_pattern = params.get('target_file_pattern').format(date=date)
    table_name = params.get('table_name')
    sql = get_sql(table_name, 'load_sql', date=date, target_file_pattern=target_file_pattern)
    
    default_args = {
        'owner': 'ohyujeong',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
    
    with DAG(
        dag_id=f'spotify_{table_name}_dag',  
        start_date=start_date,
        schedule_interval=schedule_interval,  # 실행 간격
        catchup=True,
        default_args=default_args
    ) as dag:
        
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
            op_args=[table_name, date, source_directory, source_file_pattern],  
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
                op_args=[table_name, source_directory, target_directory, source_file_pattern, target_file_pattern],  
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


# 테이블에 따른 파라미터화된 DAG 생성
tables = ['country_weekly_chart', 'global_weekly_chart', 'global_yearly_chart']

for table in tables:
    dag_params = get_table_info(table, 'dag_params')
    
    if table in ['global_weekly_chart', 'country_weekly_chart']:
        start_date = datetime(2023, 7, 1)
        schedule_interval = timedelta(weeks=1)
    elif table == 'global_yearly_chart':
        start_date = datetime(2023, 5, 3)
        schedule_interval = relativedelta(months=1)        
    dag = create_dag(start_date, schedule_interval, **dag_params)

