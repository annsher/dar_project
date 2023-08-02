import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

hide_sensitive_var_conn_fields = True

with DAG(
    "import_processing",
    start_date = datetime.datetime(2023, 7, 1),
    schedule = "0 3 * * *", catchup = False
) as dag:

    import_tables = BashOperator(
    task_id='import_tables',
    bash_command='python  /opt/airflow/data/data_preprocessing.py')



    import_tables