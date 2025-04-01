from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import yaml

import pytest

# Get the directory of the current file (file_config.py)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to config.yaml
CONFIG_PATH = os.path.join(CURRENT_DIR, "config.yaml")

# with open('common/config.yaml', 'r') as f:
#     config = yaml.safe_load(f)

with open(CONFIG_PATH, 'r') as f:
     config = yaml.safe_load(f)

# Data ingest setting
user_url = config['user']['url']
cards_url = config['cards']['url']
trsction_url = config['trnsction']['url']
mcc_url = config['mcc']['url']

ings_file_type = config['ings_file_type']
trgt_file_type = config['trgt_file_type']

local_path = config['local_path']
dimension_path = config['dimension_path']

# mcc 
mcc_ings_file_type = config['mcc']['ings_file_type']

mcc_local_ings_name = f'mcc.{mcc_ings_file_type}'
mcc_local_trgt_name = f'mcc.{trgt_file_type}'
mcc_local_path = f'/opt/airflow/{local_path}/{dimension_path}/mcc'

mcc_gcs_prefix = f'{dimension_path}/mcc'
mcc_local_ings = f'{mcc_local_path}/{mcc_local_ings_name}'
mcc_local_trgt = f'{mcc_local_path}/{mcc_local_trgt_name}'
mcc_gcs_trgt = f'{mcc_gcs_prefix}/{mcc_local_trgt_name}'

bq_external_mcc = 'external_mcc'
bq_mcc = 'mcc'

# user
user_local_ings_name = f'user.{ings_file_type}'
user_local_trgt_name = f'user.{trgt_file_type}'
user_local_path = f'/opt/airflow/{local_path}/{dimension_path}/user'

user_gcs_prefix = f'{dimension_path}/user'
user_local_ings = f'{user_local_path}/{user_local_ings_name}'
user_local_trgt = f'{user_local_path}/{user_local_trgt_name}'
user_gcs_trgt = f'{user_gcs_prefix}/{user_local_trgt_name}'

bq_external_user = 'external_user'
bq_user = 'user'

# cards
cards_local_ings_name = f'cards.{ings_file_type}'
cards_local_trgt_name = f'cards.{trgt_file_type}'
cards_local_path = f'/opt/airflow/{local_path}/{dimension_path}/cards'

cards_gcs_prefix = f'{dimension_path}/cards'
cards_local_ings = f'{cards_local_path}/{cards_local_ings_name}'
cards_local_trgt = f'{cards_local_path}/{cards_local_trgt_name}'
cards_gcs_trgt = f'{dimension_path}/cards/{cards_local_trgt_name}'

bq_external_cards = 'external_cards'
bq_cards = 'cards'

# transaction
trnsction_table_name = config['trnsction']['igst_table_name']
trsction_local_ings_name = f'{trnsction_table_name}.{ings_file_type}'

trsction_local_trgt_name = f'trnsction'
trsction_local_path = f'/opt/airflow/{local_path}/trnsction'

trnsction_gcs_prefix = f'trnsction/trnsction'
trsction_local_ings = f'{trsction_local_path}/{trsction_local_ings_name}'
trsction_local_trgt = f'{trsction_local_path}/{trsction_local_trgt_name}'
trsction_gcs_trgt = f'trnsction/{trsction_local_trgt_name}'

bq_external_trsnction = 'external_trnsction'
bq_trsnction = 'trnsction'

# BigQuery file setting 
