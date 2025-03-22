import pandas as pd 
from datetime import datetime
import logging
import requests
import zipfile
import os

logger = logging.getLogger(__name__)

def user_convert_csv_to_parquet(ings_path, trgt_path, schema):
    '''
        :param ings_path: local ingestion path
        :param trgt_path: local target path
        :param schema: to define column types 
    '''
    df = pd.read_csv(ings_path)

    df = df.replace('\\$', '', regex= True)
    dtype_mapping = schema
    df = df.astype(dtype_mapping)

    df.to_parquet(trgt_path, engine="pyarrow",index=False)
    logger.info(f'Saved file to {trgt_path}')

def cards_convert_csv_to_parquet(ings_path, trgt_path, schema):
    '''
        :param ings_path: local ingestion path
        :param trgt_path: local target path
        :param schema: to define column types 
    '''
    df = pd.read_csv(ings_path)

    df = df.replace('\\$', '', regex= True)
    df['expires'] = pd.to_datetime(df['expires'], format="%m/%Y").dt.strftime("%Y-%m-%d")
    df['acct_open_date'] = pd.to_datetime(df['acct_open_date'], format="%m/%Y").dt.strftime("%Y-%m-%d")

    dtype_mapping = schema
    df = df.astype(dtype_mapping)

    df.to_parquet(trgt_path, engine="pyarrow",index=False)
    logger.info(f'Saved file to {trgt_path} with {len(df)} rows')


def download_dataset(url, path, table_name):
    # url = "https://example.com/dataset.zip"  # Change this to your dataset URL
    # zip_path = "/tmp/dataset.zip"
    zip_path = f'{path}/{table_name}.zip'

    response = requests.get(url)
    with open(zip_path, "wb") as f:
        f.write(response.content)

    logger.info(f"✅ Dataset downloaded to {zip_path}")

    # Extract ZIP file
    extract_path = f'{path}/{table_name}.csv'
    os.makedirs(path, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(path)

    logger.info(f"✅ Dataset extracted to {path}")