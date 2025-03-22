from airflow import DAG
from airflow_dbt.operators.dbt import DbtRunOperator, DbtTestOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 22),
    "retries": 1,
}

with DAG(
    dag_id="dbt_airflow_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        dir="/opt/airflow/dbt",
    )

    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        dir="/opt/airflow/dbt",
    )

    dbt_run >> dbt_test  # Ensure dbt tests run after transformations
