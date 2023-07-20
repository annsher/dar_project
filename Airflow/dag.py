import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from DDS import data_preprocessing

def upload_tables(conn_i):
    try:
        pg_hook1 = PostgresHook(postgres_conn_id=conn_i)
        conn_a = pg_hook1.get_conn()
        print('Postgres connect success')
    except:
        print('Postgres connect not successful')
    try:
        data_preprocessing.create_table_intern(conn_a)
    except Exception as error:
        raise AirflowException('ERROR: Select error {}'.format(error))

def import_brand(conn_z, conn_i):
    try:
        pg_hook1 = PostgresHook(postgres_conn_id=conn_z)
        conn_a = pg_hook1.get_conn()
        pg_hook2 = PostgresHook(postgres_conn_id=conn_i)
        conn_b = pg_hook2.get_conn()
        print('Postgres connect success')
    except:
        print('Postgres connect not successful')
    try:
        data_preprocessing.brand_table_processing(conn_a, conn_b)
    except Exception as error:
        raise AirflowException('ERROR: Select error {}'.format(error))


def import_category(conn_z, conn_i):
    try:
        pg_hook1 = PostgresHook(postgres_conn_id=conn_z)
        conn_a = pg_hook1.get_conn()
        pg_hook2 = PostgresHook(postgres_conn_id=conn_i)
        conn_b = pg_hook2.get_conn()
        print('Postgres connect success')
    except:
        print('Postgres connect not successful')
    try:
        data_preprocessing.category_table_processing(conn_a, conn_b)
    except Exception as error:
        raise AirflowException('ERROR: Select error {}'.format(error))

def import_product(conn_z, conn_i):
    try:
        pg_hook1 = PostgresHook(postgres_conn_id=conn_z)
        conn_a = pg_hook1.get_conn()
        pg_hook2 = PostgresHook(postgres_conn_id=conn_i)
        conn_b = pg_hook2.get_conn()
        print('Postgres connect success')
    except:
        print('Postgres connect not successful')
    try:
        data_preprocessing.product_table_processing(conn_a, conn_b)
    except Exception as error:
        raise AirflowException('ERROR: Select error {}'.format(error))


def import_stock(conn_z, conn_i):
    try:
        pg_hook1 = PostgresHook(postgres_conn_id=conn_z)
        conn_a = pg_hook1.get_conn()
        pg_hook2 = PostgresHook(postgres_conn_id=conn_i)
        conn_b = pg_hook2.get_conn()
        print('Postgres connect success')
    except:
        print('Postgres connect not successful')
    try:
        data_preprocessing.stock_table_processing(conn_a, conn_b)
    except Exception as error:
        raise AirflowException('ERROR: Select error {}'.format(error))


def import_transaction(conn_z, conn_i):
    try:
        pg_hook1 = PostgresHook(postgres_conn_id=conn_z)
        conn_a = pg_hook1.get_conn()
        pg_hook2 = PostgresHook(postgres_conn_id=conn_i)
        conn_b = pg_hook2.get_conn()
        print('Postgres connect success')
    except:
        print('Postgres connect not successful')
    try:
        data_preprocessing.transaction_table_processing(conn_a, conn_b)
    except Exception as error:
        raise AirflowException('ERROR: Select error {}'.format(error))

with DAG(
    "import_processing",
    start_date = datetime.datetime(2023, 7, 1),
    schedule = "@once", catchup = False
) as dag:
    start_step = EmptyOperator(task_id = 'start_step')

    create_tables = PythonOperator(task_id='create_tables', python_callable=upload_tables,
                                  op_args=['internship_4_db'])
    import_brand = PythonOperator(task_id='import_brand', python_callable=import_brand,
                                    op_args=["internship_sources", 'internship_4_db'])
    import_category = PythonOperator(task_id='import_category', python_callable=import_category,
                                  op_args=["internship_sources", 'internship_4_db'])
    import_product = PythonOperator(task_id='import_product', python_callable=import_product,
                                     op_args=["internship_sources", 'internship_4_db'])
    import_stock = PythonOperator(task_id='import_stock', python_callable=import_stock,
                                    op_args=["internship_sources", 'internship_4_db'])
    import_transaction = PythonOperator(task_id='import_transaction', python_callable=import_transaction,
                                  op_args=["internship_sources", 'internship_4_db'])

    end_step = EmptyOperator(task_id='end_step')

    start_step >> create_tables >> import_brand  >> import_category >> import_product >> import_stock  >> import_transaction >>  end_step

