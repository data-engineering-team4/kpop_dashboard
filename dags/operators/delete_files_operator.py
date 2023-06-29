import glob
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DeleteFilesOperator(BaseOperator):
    """
    지정된 패턴의 파일들을 삭제하는 Airflow Operator입니다. 주어진 디렉토리에서 특정 패턴에 매칭되는 파일들을 찾아 삭제합니다.
    
    :param file_directory: 파일들이 있는 디렉토리 경로
    :type file_directory: str
    :param file_pattern: 삭제할 파일들의 패턴
    :type file_pattern: str
    """

    @apply_defaults
    def __init__(
        self,
        file_directory: str,
        file_pattern: str,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.file_directory = file_directory
        self.file_pattern = file_pattern

    def execute(self, context):
        files = glob.glob(os.path.join(self.file_directory, self.file_pattern))
        for file in files:
            os.remove(file)
        self.log.info(f"{len(files)} files removed from {self.file_directory} with pattern {self.file_pattern}")
