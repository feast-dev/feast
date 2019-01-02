import io
import time
import os
import re
import pandas as pd
from google.cloud import storage

_GCS_PATH_REGEX = r'^gs:\/\/[a-z0-9\.\-_\/]*$'

def gs_to_df(path):
    '''Reads a file from gs to pandas
    
    Args:
        path (str): full gcs path to the file

    Returns:
        pandas.DataFrame: dataframe 
    '''
    bucket_name, blob_name = split_gs_path(path)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    temp_file_path = 'temp{}.csv'.format(int(round(time.time() * 1000)))
    with open(temp_file_path, 'wb') as temp_file:
        blob.download_to_file(temp_file)
    df = pd.read_csv(temp_file_path)
    os.remove(temp_file_path)
    return df

def df_to_gs(df, path):
    '''Writes the given df to the path specified. Will fail if the bucket does 
    not exist.
    
    Args:
        df (pandas.DataFrame): dataframe
        path (str): path in gcs to write to
    '''
    bucket_name, blob_name = split_gs_path(path)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    s = io.StringIO()
    df.to_csv(s)
    blob.upload_from_string(s.getvalue())

def split_gs_path(path):
    path = path.replace("gs://", "", 1)
    return path.split('/', 1)

def is_gs_path(path):
    '''Check if path is a gcs path
    
    Args:
        path (str): path to file
    
    Returns:
        bool: is a valid gcs path
    '''
    return re.match(_GCS_PATH_REGEX, path) != None 