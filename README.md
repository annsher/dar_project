# dar_project
Папка DDS:
- dags - папка дагов для airflow:
  - dag_for_DDS.py содержит скрипт дага для загрузки данных из бд заказчика, а также запускает даг для создания витрин с помощью TriggerDagRunOperator
- data - папка с скриптами и файлами для дага:
  - create_tables_fos_DDS.sql содержит текст sql запросов для создания всех таблиц 
  - data_preprocessing.py содержит python скрипт для обработки данных и их загрузки
  - pos.csv таблица с магазинами, переданная заказчиком
  - transaction_pos.csv таблица связи транзакций и магазинов, переданная заказчиком

Папка datamart:
- dags - папка дагов для airflow:
  - dag_create_datamart.py содержит скрипт дага для загрузки данных из слоя DDS в слой витрины datamart
- data - папка с скриптами и файлами для дага:
  - create_tables_for_datamart.sql содержит текст sql запросов для создания всех таблиц в datamart
  - datamart.py скрипт для обработки данных, рассчета показателей и загрузки из DDS в datamart
    
