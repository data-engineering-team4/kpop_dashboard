from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
import logging
import boto3
import glob
import os

class UploadFilesToS3Operator(BaseOperator):
    """
    UploadFilesToS3Operator는 로컬 디렉토리에 있는 파일들을 Amazon S3 버킷의 지정된 위치로 업로드합니다.

    :param conn_id: AWS 인증 정보에 대한 연결 ID.
                    AWS access key와 secret key는 이 연결에 저장되어야 합니다.
    :type conn_id: str

    :param file_directory: 업로드할 파일들이 있는 로컬 디렉토리.
    :type file_directory: str

    :param file_pattern: 디렉토리 내에서 일치해야 하는 파일 패턴. 
                         이 패턴에 일치하는 파일만 '업로드됩니다.
    :type file_pattern: str

    :param bucket_name: 파일들을 업로드할 Amazon S3 버킷의 이름.
    :type bucket_name: str

    :param folder_name: 버킷 내에서 파일들이 업로드될 위치.
    :type folder_name: str
    """
    template_fields = ['file_directory','file_pattern','bucket_name','folder_name']

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
