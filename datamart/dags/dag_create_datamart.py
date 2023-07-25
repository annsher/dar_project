import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


hide_sensitive_var_conn_fields = True

with DAG(
    "dag_create_datamart",
    start_date = datetime.datetime(2023, 7, 1),
    schedule = None, catchup = False
) as dag:

    dag_create_datamart = BashOperator(
    task_id='dag_create_datamart',
    bash_command='python  /opt/airflow/data/datamart.py')

    dag_create_datamart 

