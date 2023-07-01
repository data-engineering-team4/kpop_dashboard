from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.log_operator import LogOperator
from utils.common_util import get_sql
import logging

default_args = {
    'start_date': datetime(2023, 6, 30),
    'catchup': False
}

with DAG('sql_test_dag', schedule_interval='@once', default_args=default_args, tags=['example']) as dag:
    sql = get_sql('country_weekly_chart', 'test_sql', date='{{ds}}', target_file_pattern='country-weekly-2023-06-30.csv')

    log_test = LogOperator(
        task_id='run_main',
        param=sql,
    )

    log_test
