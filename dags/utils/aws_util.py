import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def save_json_to_s3(data, s3_bucket, key, ymd, context):
    s3_key = f"raw_data/spotify/api/{key}/{ymd}/{key}.json"
    if data is None:
        data = []
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    s3_client = s3_hook.get_conn()
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )