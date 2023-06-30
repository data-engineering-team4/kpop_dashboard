import os
import glob
import requests

def exists(path):
    r = requests.head(path)
    return r.status_code == requests.codes.ok

def get_files(file_directory, file_pattern):
    return glob.glob(os.path.join(file_directory, file_pattern))

def delete_files(file_directory, file_pattern):
    for file in get_files(file_directory, file_pattern): 
        os.remove(file) 
        
def save_files(df, target_directory, target_file_pattern):
    new_file_path = os.path.join(target_directory, target_file_pattern)
    df.to_csv(new_file_path, index=False)
    
def get_sql(schema, table, **params):
    sql = ''
    return sql