# dar_project
Папка dags:
- dag_for_DDS.py содержит скрипт дага для загрузки данных из бд заказчика
- dag_create_datamart.py содержит скрипт дага для загрузки данных из слоя DDS в слой витрины datamart

Папка data:
- create_tables_for_datamart.sql содержит текст sql запросов для создания всех таблиц в datamart
- datamart.py скрипт для обработки данных, рассчета показателей и загрузки из DDS в datamart
- transaction_pos.csv, pos.csv таблицы, переданные заказчиком
- data_preprocessing.py скрипт для обработки и загрузки данных в слой DDS

Папка dashboard: содержит текст запросов к power bi
