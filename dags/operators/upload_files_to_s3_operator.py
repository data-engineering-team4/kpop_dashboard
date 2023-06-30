from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
import logging
import boto3
import glob
import os

class UploadFilesToS3Operator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            conn_id,
            file_directory,
            file_pattern,
            bucket_name,
            folder_name,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.file_directory = file_directory
        self.file_pattern = file_pattern
        self.bucket_name = bucket_name
        self.folder_name = folder_name

    def execute(self, context):
        logging.info("get aws connection")
        aws_conn = BaseHook.get_connection(self.conn_id)
        aws_access_key = aws_conn.login
        aws_secret_key = aws_conn.password

        logging.info("create s3 client")
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        logging.info("get files from path")
        files = glob.glob(os.path.join(self.file_directory, self.file_pattern))

        logging.info(f"path : {os.path.join(self.file_directory, self.file_pattern)}")
        logging.info(f"files count : {len(files)}")
        logging.info("loop files start")

        for file in files:
            file_name = os.path.basename(file)
            s3_key = os.path.join(self.folder_name, file_name)

            logging.info(f"upload file : {s3_key}")
            with open(file, 'rb') as data:
                s3.upload_fileobj(data, self.bucket_name, s3_key)

        logging.info("loop files end")
