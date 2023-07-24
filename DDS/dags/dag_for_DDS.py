import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

hide_sensitive_var_conn_fields = True

with DAG(
    "import_processing",
    start_date = datetime.datetime(2023, 7, 1),
    schedule = "@daily", catchup = False
) as dag:

    import_tables = BashOperator(
    task_id='import_tables',
    bash_command='python  /opt/airflow/data/data_preprocessing.py')

    trigger_dag_create_datamart = TriggerDagRunOperator(
        task_id='trigger_dag_create_datamart',
        trigger_dag_id='dag_create_datamart',
    )

    import_tables >> trigger_dag_create_datamart
