from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime

import os 
import sys

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

dbt_job_id = os.environ.get("DBT_JOB_ID")

default_args = {"owner": "airflow", "start_date": datetime(2025, 3, 28)}

dag = DAG("dbt_cloud_workflow", default_args=default_args, schedule_interval="@once")

dbt_cloud_run = DbtCloudRunJobOperator(
    task_id="dbt_cloud_run",
    dbt_cloud_conn_id="dbt_cloud_conn",
    job_id=70471823435386,  # Replace with actual dbt Cloud job ID
    dag=dag,
)

dbt_cloud_run
