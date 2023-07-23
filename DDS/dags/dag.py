import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

hide_sensitive_var_conn_fields = True

with DAG(
    "import_processing",
    start_date = datetime.datetime(2023, 7, 1),
    schedule = "@once", catchup = False
) as dag:

    import_tables = BashOperator(
    task_id='import_tables',
    bash_command='python  /opt/airflow/data/data_preprocessing.py')

    import_tables

