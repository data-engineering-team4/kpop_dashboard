import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.log_operator import LogOperator
from airflow.operators.bash import BashOperator

from utils.common_util import get_sql


default_args = {
    'start_date': datetime(2023, 6, 30),
    'catchup': False
}

with DAG('dbt_test_dag', schedule_interval='@once', default_args=default_args, tags=['example']) as dag:

    task_1 = BashOperator(
        task_id='daily_transform',
        bash_command='cd /dbt && dbt run --profiles-dir .',
        env={
            'DBT_ENV_SECRET_USER': '{{ var.value.dbt_user }}',
            'DBT_ENV_SECRET_PASSWORD': '{{ var.value.dbt_password }}',
            **os.environ
        },
        dag=dag
    )