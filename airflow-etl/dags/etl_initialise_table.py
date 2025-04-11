
## because the table is too big, so don't recommend. Just save code here 

import os
import re
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateExternalTableOperator, BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.file_config import *
from common.bq_queries import *
from gcp_operations import *
from ingest_dimension import download_dataset

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET_ID = os.environ.get("GCP_BIGQUERY_DATASET")

default_args = {
    "owner": "airflow",
    "start_date": datetime.today(),
    "depends_on_past": False,
    "retries": 0,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="initialise_tables",
    schedule="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    bq_create_main_trnsction_table = BigQueryInsertJobOperator(
        task_id="bq_create_main_trnsction_table",
        configuration={
            'query': {
                'query': create_main_trnsction_table,
                'useLegacySql': False,
            }
        },
    )

    bq_create_main_user_table = BigQueryInsertJobOperator(
        task_id="bq_create_main_user_table",
        configuration={
            'query': {
                'query': create_main_user_table,
                'useLegacySql': False,
            }
        },
    )


    bq_create_main_cards_table = BigQueryInsertJobOperator(
        task_id="bq_create_main_cards_table",
        configuration={
            'query': {
                'query': create_main_cards_table,
                'useLegacySql': False,
            }
        },
    )

    bq_create_main_mcc_table = BigQueryInsertJobOperator(
        task_id="bq_create_main_mcc_table",
        configuration={
            'query': {
                'query': create_main_mcc_table,
                'useLegacySql': False,
            }
        },
    )
