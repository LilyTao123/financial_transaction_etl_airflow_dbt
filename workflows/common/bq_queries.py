from common.file_config import *
import os 

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("GCP_BIGQUERY_DATASET")

create_partitioned_transaction_table_query = f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_trsnction} 
    PARTITION BY DATE(transaction_date) 
    AS
    SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{bq_external_trsnction};
""" 

create_user_table_query = f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_user} 
    AS
    SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{bq_external_user};
""" 

create_cards_table_query = f""" 
    CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.{bq_cards} 
    AS
    SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{bq_external_cards};
""" 
