from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.common_utils import get_sql
import logging

default_args = {
    'start_date': datetime(2023, 6, 30),
    'catchup': False
}

with DAG('sql_test_dag', schedule_interval='@once', default_args=default_args, tags=['example']) as dag:
    def main():
        sql = get_sql('country_weekly_chart', 'test_sql', date='2023-06-22', target_file_pattern='country-weekly-2023-06-30.csv')
        logging.info(sql)

    run_task = PythonOperator(
        task_id='run_main',
        python_callable=main,
    )

    run_task
