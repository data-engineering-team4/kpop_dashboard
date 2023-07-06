import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from utils.common_util import KST_TZ


default_args = {
    'start_date': datetime(2023, 6, 30,tzinfo=KST_TZ),
    'catchup': False
}

with DAG(
    dag_id='dbt_run_dag', 
    schedule_interval='0 15 * * 0', 
    default_args=default_args, 
    tags=['dbt']
) as dag:
    
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /dbt && dbt run --profiles-dir .',
        env={
            'DBT_ENV_SECRET_USER': '{{ var.value.dbt_user }}',
            'DBT_ENV_SECRET_PASSWORD': '{{ var.value.dbt_password }}',
            **os.environ
        }
    )
    
    run_dbt
