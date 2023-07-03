import os
import glob
import requests
import logging
import pendulum

def exists(path):
    """
    주어진 경로의 파일 또는 디렉토리가 존재하는지 확인합니다.

    Args:
        path (str): 확인할 파일 또는 디렉토리의 경로

    Returns:
        bool: 주어진 경로의 파일 또는 디렉토리가 존재하는 경우 True, 그렇지 않은 경우 False

    """
    r = requests.head(path)
    return r.status_code == requests.codes.ok

def get_files(file_directory, file_pattern):
    """
    주어진 디렉토리에서 주어진 파일 패턴과 일치하는 파일들의 리스트를 반환합니다.

    Args:
        file_directory (str): 파일들이 위치한 디렉토리 경로
        file_pattern (str): 파일 패턴

    Returns:
        list: 주어진 파일 패턴과 일치하는 파일들의 리스트

    """
    return glob.glob(os.path.join(file_directory, file_pattern))

def delete_files(file_directory, file_pattern):
    """
    주어진 디렉토리에서 주어진 파일 패턴과 일치하는 파일들을 삭제합니다.

    Args:
        file_directory (str): 파일들이 위치한 디렉토리 경로
        file_pattern (str): 파일 패턴

    """
    for file in get_files(file_directory, file_pattern): 
        os.remove(file) 
        
def save_files(df, target_directory, target_file_pattern):
    """
    DataFrame을 주어진 디렉토리에 주어진 파일 패턴으로 저장합니다.

    Args:
        df (pandas.DataFrame): 저장할 DataFrame
        target_directory (str): 저장할 디렉토리 경로
        target_file_pattern (str): 저장할 파일 패턴

    """
    new_file_path = os.path.join(target_directory, target_file_pattern)
    df.to_csv(new_file_path, index=False)
  
def load_all_jsons_into_list(path_to_json):
    """
    주어진 경로에서 모든 JSON 파일을 읽어 리스트에 저장합니다.

    Args:
        path_to_json (str): JSON 파일들이 위치한 경로

    Returns:
        list: JSON 파일들을 읽어서 저장한 리스트

    Raises:
        Exception: JSON 파일 읽기 중 오류가 발생한 경우

    """
    configs = []
    logging.info(path_to_json+ '/*.py')
    for f_name in glob.glob(path_to_json+ '/*.py'):
        with open(f_name) as f:
            dict_text = f.read()
            try:
                dict = eval(dict_text)
            except Exception as e:
                logging.info(str(e))
                raise
            else:
                configs.append(dict)
    return configs

def find(table_name, table_confs):
    """
    주어진 테이블 이름과 일치하는 테이블 설정을 찾습니다.

    Args:
        table_name (str): 찾을 테이블 이름
        table_confs (list): 테이블 설정이 저장된 리스트

    Returns:
        dict or None: 일치하는 테이블 설정을 담고 있는 딕셔너리, 일치하는 테이블이 없을 경우 None

    """
    for table in table_confs:
        if table.get("table") == table_name:
            return table
    return None

def get_sql(table_name, sql_name, **params):
    """
    주어진 테이블과 SQL 이름에 해당하는 SQL 쿼리를 가져옵니다.

    Args:
        table_name (str): 테이블 이름
        sql_name (str): SQL 이름
        **params: SQL 쿼리에 사용될 동적 변수들

    Returns:
        str: 포맷팅된 SQL 쿼리

    """
    table_confs = load_all_jsons_into_list("/opt/airflow/dags/config")
    table = find(table_name, table_confs)
    sql = table["sqls"][sql_name].format(**params)

    return sql


def get_sql_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()  

def get_table_info(table_name, key):
    """
    주어진 테이블의 key에 해당하는 값을 가져옵니다.

    Args:
        table_name (str): 테이블 이름
        key (str): key 이름

    Returns:
        any: key에 해당하는 값

    """
    table_confs = load_all_jsons_into_list("/opt/airflow/dags/config")
    table = find(table_name, table_confs)
    return table[key]


def get_current_datetime():
    local_tz = pendulum.timezone("Asia/Seoul")
    now = pendulum.now(tz=local_tz)
    return now

def get_formatted_date(now):
    ymd = now.strftime('%Y-%m-%d')
    return ymd

def get_formatted_timestamp(now):
    timestamp = now.strftime('%Y-%m-%d_%H:%M:%S')
    return timestamp