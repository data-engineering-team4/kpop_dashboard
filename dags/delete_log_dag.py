"""30일 지난 Airflow 로그를 삭제함
    
    사용 명령어 : find [airflow log dir] -type f -mtime +30 -delete
        : ex. find /opt/airflow/logs -type f -mtime +30 -delete
        : 위에 명령어를 BashOperator로 실행

    옵션 설명 : 
        : -type f : 일반 파일만 검색
            * 추가 설명 :  d: 디렉토리, l: 링크 파일, b: 블록디바이스, c: 캐릭터 디바이스 ...
        : -mtime +30 : 생성한지 720시간 이상 지난 파일만 검색
        : -delete : 삭제
"""
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator # Import BashOperator

"""Set DAG"""
KST_TZ = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'kpop',
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='delete_logs_mtime_30', 
    default_args=default_args,
    start_date=datetime(2023, 7, 1, tzinfo=KST_TZ), 
    catchup=False, 
    schedule_interval='* 2 * * *',  # Crontime : min hour day month week / 매일 02시에 삭제
    max_active_runs=3,
    tags=['operation']
) as dag:

    delete_logs = BashOperator(
        task_id='delete_logs',
        bash_command='find /opt/airflow/logs -type f -mtime +30 -delete'
    )

    delete_logs