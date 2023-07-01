from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LogOperator(BaseOperator):
    """
    특정 값을 로깅하는 Airflow Operator입니다.
    
    :param param: 로깅할 값
    :type param: Any
    """
    template_fields = ['param']

    @apply_defaults
    def __init__(self, param, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.param = param

    def execute(self, context):
        logging.info(self.param)
