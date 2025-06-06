import os
import re
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateExternalTableOperator, BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)


from common.file_config import *
from common.schema import *
from common.bq_queries import *
from ingest_dimension import *
from gcp_operations import *

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
    dag_id="dimension_ingestion_gcs_dag",
    schedule="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    create_temp_folder = BashOperator(
        task_id="create_temp_folder",
        bash_command=f" mkdir -p {user_local_path} && mkdir -p {cards_local_path} && mkdir -p {mcc_local_path}"
    )

    download_mcc_dataset = BashOperator(
        task_id="download_mcc_dataset",
        bash_command=f"curl -sSL {mcc_url} > {mcc_local_path}/{mcc_local_ings_name}"
    )

    mcc_convert_to_parquet = PythonOperator(
        task_id="mcc_convert_to_parquet",
        python_callable=mcc_convert_json_to_parquet,
        op_kwargs={
            "ings_path": mcc_local_ings,
            "trgt_path": mcc_local_trgt,
            "schema": mcc_dtype_mapping
        },
    )

    mcc_load_to_gcp = PythonOperator(
        task_id = 'mcc_load_to_gcp',
        python_callable = upload_single_file_to_gcs,
        op_kwargs = {
            'bucket': BUCKET, 
            'object_name': mcc_gcs_trgt, 
            'local_file': mcc_local_trgt}

    )

    bq_create_mcc_external_table = BigQueryCreateExternalTableOperator(
        task_id='bq_create_mcc_external_table',
        table_resource={
            'tableReference': {
                'projectId': PROJECT_ID,
                'datasetId': DATASET_ID,
                'tableId': bq_external_mcc,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{BUCKET}/{mcc_gcs_prefix}/*.parquet'],
            },
        },
    )

    bq_merge_to_mcc_table = BigQueryInsertJobOperator(
        task_id='merge_to_mcc_table',
        configuration={
            'query': {
                'query': merge_to_mcc_table,
                'useLegacySql': False,
            }
        },
    )

    download_user_dataset = BashOperator(
        task_id="download_user_dataset",
        bash_command=f"curl -sSL {user_url} > {user_local_path}/{user_local_ings_name}"
    )

    user_convert_to_parquet = PythonOperator(
        task_id="user_convert_to_parquet",
        python_callable=user_convert_csv_to_parquet,
        op_kwargs={
            "ings_path": user_local_ings,
            "trgt_path": user_local_trgt,
            "schema": user_dtype_mapping
        },
    )


    user_load_to_gcp = PythonOperator(
        task_id = 'user_load_to_gcp',
        python_callable = upload_single_file_to_gcs,
        op_kwargs = {
            'bucket': BUCKET, 
            'object_name': user_gcs_trgt, 
            'local_file': user_local_trgt}

    )

    bq_create_user_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_external_user_table',
        table_resource={
            'tableReference': {
                'projectId': PROJECT_ID,
                'datasetId': DATASET_ID,
                'tableId': bq_external_user,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{BUCKET}/{user_gcs_prefix}/*.parquet'],
            },
        },
    )


    bq_merge_user_table = BigQueryInsertJobOperator(
        task_id='merge_user_table_query',
        configuration={
            'query': {
                'query': merge_user_table_query,
                'useLegacySql': False,
            }
        },
    )


    download_cards_dataset = BashOperator(
        task_id="download_cards_dataset",
        bash_command=f"curl -sSL {cards_url} > {cards_local_path}/{cards_local_ings_name}"
    )

    cards_convert_to_parquet = PythonOperator(
        task_id="cards_convert_to_parquet",
        python_callable=cards_convert_csv_to_parquet,
        op_kwargs={
            "ings_path": cards_local_ings,
            "trgt_path": cards_local_trgt,
            "schema": cards_dtype_mapping
        },
    )


    bq_create_cards_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_external_cards_table',
        table_resource={
            'tableReference': {
                'projectId': PROJECT_ID,
                'datasetId': DATASET_ID,
                'tableId': bq_external_cards,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{BUCKET}/{cards_gcs_prefix}/*.parquet'],
            },
        },
    )

    cards_load_to_gcp = PythonOperator(
        task_id = 'cards_load_to_gcp',
        python_callable = upload_single_file_to_gcs,
        op_kwargs = {
            'bucket': BUCKET, 
            'object_name': cards_gcs_trgt, 
            'local_file': cards_local_trgt}

    )

    bq_merge_cards_table = BigQueryInsertJobOperator(
        task_id='bq_merge_cards_table',
        configuration={
            'query': {
                'query': merge_cards_table_query,
                'useLegacySql': False,
            }
        },
    )


    delete_cards_external = BigQueryDeleteTableOperator(
        task_id="delete_cards_external",
        deletion_dataset_table= f'{PROJECT_ID}.{DATASET_ID}.{bq_external_cards}',
        ignore_if_missing=True,  # Set to True to avoid failure if the table doesn't exist
    )

    delete_user_external = BigQueryDeleteTableOperator(
        task_id="delete_user_external",
        deletion_dataset_table= f'{PROJECT_ID}.{DATASET_ID}.{bq_external_user}',
        ignore_if_missing=True,  # Set to True to avoid failure if the table doesn't exist
    )

    delete_mcc_external = BigQueryDeleteTableOperator(
        task_id="delete_mcc_external",
        deletion_dataset_table= f'{PROJECT_ID}.{DATASET_ID}.{bq_external_mcc}',
        ignore_if_missing=True,  # Set to True to avoid failure if the table doesn't exist
    )

    # job_success = BashOperator(
    #     task_id="job_success",
    #     bash_command=f" echo 'job success' "
    # )

# Ensure create_temp_folder runs before all downloads
create_temp_folder >> download_user_dataset
create_temp_folder >> download_cards_dataset
create_temp_folder >> download_mcc_dataset

# Dependencies for user dataset processing
download_user_dataset >> user_convert_to_parquet >> user_load_to_gcp >> bq_create_user_external_table >> bq_merge_user_table >> delete_user_external

# Dependencies for cards dataset processing
download_cards_dataset >> cards_convert_to_parquet >> cards_load_to_gcp >> bq_create_cards_external_table >> bq_merge_cards_table >> delete_cards_external

# Dependencies for mcc dataset processing
download_mcc_dataset >> mcc_convert_to_parquet >> mcc_load_to_gcp >> bq_create_mcc_external_table >> bq_merge_to_mcc_table >> delete_mcc_external

