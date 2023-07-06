from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator # Import BashOperator

default_args = {
    'owner': 'kpop',
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='delete_logs_past_7days', 
    default_args=default_args,
    start_date=datetime(2023, 7, 1), 
    catchup=False, 
    schedule_interval='* 2 * * *',  # Crontime : min hour day month week / 매일 02시에 삭제
    tags=['operation']
) as dag:

    # 10일 이상 이전인 로그 파일 찾아서 삭제
    delete_logs = BashOperator(
        task_id='delete_logs',
        bash_command='find /opt/airflow/logs -type f -mtime +10 -delete'
    )

    delete_logs
