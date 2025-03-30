from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG
with DAG(
    'dbt_run_example',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',  # You can set your desired schedule
    catchup=False
) as dag:
    
    # DBT install packages
    install_dbt_packages = BashOperator(
        task_id="install_dbt_packages",
        bash_command=f"cd /opt/airflow/dbt && dbt deps",
    )

    # dbt seeds 
    load_dbt_seeds = BashOperator(
        task_id="load_dbt_seeds",
        bash_command=f"cd /opt/airflow/dbt && dbt seed --profiles-dir /opt/airflow/dbt",
    )

    # DBT Run task
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt"  # Path to your profiles.yml
    )

    # DBT Test task (optional)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt"  # Path to your profiles.yml
    )

    # Task sequence: First run, then test
install_dbt_packages >> load_dbt_seeds >> dbt_run >> dbt_test
