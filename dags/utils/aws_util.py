import json
import boto3

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook

def save_json_to_s3(data, s3_bucket, key, ymd, id, s3_client):
    s3_key = f"raw_data/spotify/api/{key}/{ymd}/{id}.json"
    
    if data is None:
        data = []

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

def create_s3_client(aws_conn_id):
    aws_conn = BaseHook.get_connection(aws_conn_id)
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    return s3_client