import os
import re
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.file_config import *
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
    dag_id="trnsaction_ingestion_gcs_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    create_temp_folder = BashOperator(
        task_id="create_temp_folder",
        bash_command=f" mkdir -p {trsction_local_path}"
    )

    # download_trsction_dataset = BashOperator(
    #     task_id="download_trsction_dataset",
    #     bash_command=f"curl -sSL {trsction_url} > {trsction_local_path}/{trnsction_ingst_as}"
    # )

    # unzip_trsction_dataset = BashOperator(
    #     task_id="unzip_trsction_dataset",
    #     bash_command=f"unzip {trsction_local_path}/{trnsction_ingst_as}"
    # )

    download_trsction_dataset = PythonOperator(
        task_id = 'download_trsction_dataset',
        python_callable = download_dataset,
        op_kwargs={
            "url": trsction_url,
            "path": trsction_local_path,
            'table_name': trnsction_table_name
        },
    )

    trsction_convert_to_parquet = SparkSubmitOperator(
        task_id="spark_convert_to_parquet",
        application="/opt/airflow/jobs/pyspark_convert_to_parquet.py", # Spark application path created in airflow and spark cluster
        name="spark-transaction-rename",
        conn_id="spark-conn"
    )
    
    trsction_load_to_gcp = PythonOperator(
        task_id = 'trsction_load_to_gcp',
        python_callable=upload_mult_file_from_directory,
        op_kwargs={
            "directory_path": trsction_local_trgt,
            "dest_bucket_name": BUCKET
        },
    )

    # gcs_object_exists = GCSObjectExistenceSensor(
    #     bucket=BUCKET,
    #     object=trsction_local_trgt,
    #     task_id="gcs_object_exists_task",
    # )

    bq_create_trsnction_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_external_transaction_table',
        table_resource={
            'tableReference': {
                'projectId': PROJECT_ID,
                'datasetId': DATASET_ID,
                'tableId': bq_external_trsnction,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{BUCKET}/{trnsction_gcs_prefix}/*.parquet'],
            },
        },
    )

create_temp_folder >> download_trsction_dataset >> trsction_convert_to_parquet >> trsction_load_to_gcp >> bq_create_trsnction_external_table