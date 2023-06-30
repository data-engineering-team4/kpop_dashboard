import json
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def save_json_to_s3(data, s3_bucket, s3_key):
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")  # AWS 연결 설정

    s3_client = s3_hook.get_conn()
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )