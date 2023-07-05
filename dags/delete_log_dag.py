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