from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import yaml

import pytest

with open('common/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Data ingest setting
user_url = config['user']['url']
cards_url = config['cards']['url']
trsction_url = config['trnsction']['url']

ings_file_type = config['ings_file_type']
trgt_file_type = config['trgt_file_type']

local_path = config['local_path']
dimension_path = config['dimension_path']

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
