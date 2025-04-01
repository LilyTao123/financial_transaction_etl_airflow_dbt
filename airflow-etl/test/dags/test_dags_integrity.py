from airflow.models import DagBag
from collections import Counter
import os, sys

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

dag_bag = DagBag(include_examples=False)

# if has import errors
def test_dagbag():
    
    assert not dag_bag.import_errors



# if missing tags
def test_dagbag_is_empty():
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags  #Assert dag.tags is not empty



# if has duplicated dag id 
def test_no_duplicate_dag_ids():
    
    dag_ids = [dag_id for dag_id in dag_bag.dags.keys()]
    duplicate_dags = [dag_id for dag_id, count in Counter(dag_ids).items() if count > 1]

    assert not duplicate_dags, f"Duplicate DAG IDs found: {duplicate_dags}"



# if has duplicated task id 
def test_no_duplicate_task_ids():
    
    duplicate_task_dags = {}

    for dag_id, dag in dag_bag.dags.items():
        task_ids = [task.task_id for task in dag.tasks]
        duplicates = [task_id for task_id, count in Counter(task_ids).items() if count > 1]

        if duplicates:
            duplicate_task_dags[dag_id] = duplicates
    
    assert not duplicate_task_dags, f"DAGs with duplicate task IDs: {duplicate_task_dags}"



# if missing task id 
def test_no_missing_task_ids():

    missing_task_dags = []
    
    for dag_id, dag in dag_bag.dags.items():
        task_ids = [task.task_id for task in dag.tasks]
        if not task_ids:  # If task_ids list is empty
            missing_task_dags.append(dag_id)
    
    assert not missing_task_dags, f"The following DAGs have no tasks: {missing_task_dags}"