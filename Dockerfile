FROM apache/airflow:2.6.2
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
COPY --chown=airflow:root dags /opt/airflow/dags
COPY dbt /dbt


