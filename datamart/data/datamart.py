import pandas as pd
import datetime

def truncate_table_intern(table, conn_i):
    cursor = conn_i.cursor()
    truncate = f'TRUNCATE TABLE "datamart".{table} CASCADE;'
    cursor.execute(truncate)
    conn_i.commit()
    cursor.close()

def create_table_intern(conn_i):
    cursor = conn_i.raw_connection().cursor()
    f = open('create_tables_for_datamart.sql', 'r')
    create = ''.join(f.readlines())
    cursor.execute(create)
    for tables in ['avg_purchase', 'avg_purchase_month', 'revenue_month', 'sold_item', 'sold_item_month', 'to_order', 'top_5', 'pos', 'category', 'updated_on', 'months', 'years']:
        truncate_table_intern(f'{tables}', conn_i.raw_connection())
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    pos.to_sql('pos', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    category = pd.read_sql_table('category', conn_i.connect(), 'DDS', coerce_float=False)
    category.to_sql('category', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    months = pd.DataFrame(['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'], columns = ['month'])
    months.to_sql('months', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    records = []
    for i in range(2020, datetime.datetime.now().year + 1):
        records.append(i)
    years = pd.DataFrame(records, columns=['year'])
    years.to_sql('years', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    conn_i.raw_connection().commit()
    cursor.close()
    print('Таблицы загружены')

def fill_avg_purchase(conn_i):
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction_pos = pd.read_sql_table('transaction_pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction = pd.read_sql_table('transaction', conn_i.connect(), 'DDS', coerce_float=False)
    merged = transaction_pos.merge(pos, how='left', on='pos')
    merged = transaction.merge(merged, how='left', on='transaction_id')
    merged.insert(loc=len(merged.columns), column='total', value=merged['quantity']*merged['price'])
    merged = merged.groupby(['recorded_on', 'pos_name'], as_index=False)['total'].agg(['sum', 'mean'])
    merged.columns = ['recorded_on', 'pos_name', 'revenue_daily', 'avg_amount']
    merged.to_sql('avg_purchase', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    print('avg_purchase uploaded')

def fill_avg_purchase(conn_i):
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction_pos = pd.read_sql_table('transaction_pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction = pd.read_sql_table('transaction', conn_i.connect(), 'DDS', coerce_float=False)
    merged = transaction_pos.merge(pos, how='left', on='pos')
    merged = transaction.merge(merged, how='left', on='transaction_id')
    merged.insert(loc=len(merged.columns), column='total', value=merged['quantity']*merged['price'])
    merged = merged.groupby(['recorded_on', 'pos_name'], as_index=False)['total'].agg(['sum', 'mean'])
    merged.columns = ['recorded_on', 'pos_name', 'revenue_daily', 'avg_amount']
    merged.to_sql('avg_purchase', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    print('avg_purchase uploaded')

def fill_avg_purchase_month(conn_i):
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction_pos = pd.read_sql_table('transaction_pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction = pd.read_sql_table('transaction', conn_i.connect(), 'DDS', coerce_float=False)
    merged = transaction_pos.merge(pos, how='left', on='pos')
    merged = transaction.merge(merged, how='left', on='transaction_id')
    merged.insert(loc=len(merged.columns), column='total', value=merged['quantity']*merged['price'])
    merged.insert(loc=len(merged.columns), column='month', value=merged['recorded_on'].dt.month)
    merged.insert(loc=len(merged.columns), column='year', value=merged['recorded_on'].dt.year)
    merged['month'] = merged['month'].astype(str)
    merged['month'] = merged['month'].replace(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'], ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'])
    merged = merged.groupby(['month', 'year', 'pos_name'], as_index=False)['total'].agg(['mean'])
    merged.columns = ['month', 'year', 'pos_name', 'avg_amount_month']
    merged.to_sql('avg_purchase_month', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    print('avg_purchase_month uploaded')

def fill_revenue_month(conn_i):
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    product = pd.read_sql_table('product', conn_i.connect(), 'DDS', coerce_float=False)
    category = pd.read_sql_table('category', conn_i.connect(), 'DDS', coerce_float=False)
    transaction_pos = pd.read_sql_table('transaction_pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction = pd.read_sql_table('transaction', conn_i.connect(), 'DDS', coerce_float=False)
    merged = product.merge(category, how='left', on='category_id')
    merged = transaction.merge(merged, how='left', on='product_id')
    merged = merged.merge(transaction_pos, how='left', on='transaction_id')
    merged = merged.merge(pos, how='left', on='pos')
    merged.insert(loc=len(merged.columns), column='total', value=merged['quantity'] * merged['price'])
    merged.insert(loc=len(merged.columns), column='month', value=merged['recorded_on'].dt.month)
    merged['month'] = merged['month'].astype(str)
    merged['month'] = merged['month'].replace(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'],
                                              ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август',
                                               'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'])
    merged.insert(loc=len(merged.columns), column='year', value=merged['recorded_on'].dt.year)
    merged = merged.groupby(['month', 'year', 'category_name', 'pos_name'], as_index=False)['total'].agg(['sum'])
    merged.columns = ['month', 'year', 'category_name', 'pos_name', 'revenue']
    merged.to_sql('revenue_month', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    print('revenue_month uploaded')

def fill_sold_item(conn_i):
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    product = pd.read_sql_table('product', conn_i.connect(), 'DDS', coerce_float=False)
    category = pd.read_sql_table('category', conn_i.connect(), 'DDS', coerce_float=False)
    transaction_pos = pd.read_sql_table('transaction_pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction = pd.read_sql_table('transaction', conn_i.connect(), 'DDS', coerce_float=False)
    merged = product.merge(category, how='left', on='category_id')
    merged = transaction.merge(merged, how='left', on='product_id')
    merged = merged.merge(transaction_pos, how='left', on='transaction_id')
    merged = merged.merge(pos, how='left', on='pos')
    merged = merged.groupby(['recorded_on', 'category_name', 'pos_name'], as_index=False)['quantity'].agg(['sum'])
    merged.columns = ['recorded_on', 'category_name', 'pos_name', 'qnt']
    merged.to_sql('sold_item', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    print('sold_item uploaded')

def fill_sold_item_month(conn_i):
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    product = pd.read_sql_table('product', conn_i.connect(), 'DDS', coerce_float=False)
    category = pd.read_sql_table('category', conn_i.connect(), 'DDS', coerce_float=False)
    transaction_pos = pd.read_sql_table('transaction_pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction = pd.read_sql_table('transaction', conn_i.connect(), 'DDS', coerce_float=False)
    merged = product.merge(category, how='left', on='category_id')
    merged = transaction.merge(merged, how='left', on='product_id')
    merged = merged.merge(transaction_pos, how='left', on='transaction_id')
    merged = merged.merge(pos, how='left', on='pos')
    merged.insert(loc=len(merged.columns), column='month', value=merged['recorded_on'].dt.month)
    merged.insert(loc=len(merged.columns), column='year', value=merged['recorded_on'].dt.year)
    merged['month'] = merged['month'].astype(str)
    merged['month'] = merged['month'].replace(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'],
                                              ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август',
                                               'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'])
    merged = merged.groupby(['month', 'year', 'category_name', 'pos_name'], as_index=False)['quantity'].agg(['sum'])
    merged.columns = ['month', 'year', 'category_name', 'pos_name', 'qnt_month']
    merged.to_sql('sold_item_month', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    print('sold_item_month uploaded')

def fill_top_5(conn_i):
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    product = pd.read_sql_table('product', conn_i.connect(), 'DDS', coerce_float=False)
    category = pd.read_sql_table('category', conn_i.connect(), 'DDS', coerce_float=False)
    transaction_pos = pd.read_sql_table('transaction_pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction = pd.read_sql_table('transaction', conn_i.connect(), 'DDS', coerce_float=False)
    merged = product.merge(category, how='left', on='category_id')
    merged = transaction.merge(merged, how='left', on='product_id')
    merged = merged.merge(transaction_pos, how='left', on='transaction_id')
    merged = merged.merge(pos, how='left', on='pos')
    merged.insert(loc=len(merged.columns), column='month', value=merged['recorded_on'].dt.month)
    merged['month'] = merged['month'].astype(str)
    merged['month'] = merged['month'].replace(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'],
                                              ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август',
                                               'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'])
    merged.insert(loc=len(merged.columns), column='year', value=merged['recorded_on'].dt.year)
    merged = merged.groupby(['name_short', 'category_name', 'month', 'year', 'pos_name'], as_index=False)['quantity'].agg(['sum']).sort_values(by=['sum'], ascending=False)
    merged.columns = ['name_short', 'category_name', 'month', 'year', 'pos_name', 'qnt_sum']
    merged.to_sql('top_5', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    print('top_5 uploaded')

def fill_to_order(conn_i):
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    stock = pd.read_sql_table('stock', conn_i.connect(), 'DDS', coerce_float=False)
    product = pd.read_sql_table('product', conn_i.connect(), 'DDS', coerce_float=False)
    category = pd.read_sql_table('category', conn_i.connect(), 'DDS', coerce_float=False)
    merged = stock.merge(pos, how='left', on='pos')
    merged = merged.merge(product, how='left', on='product_id')
    merged = merged.merge(category, how='left', on='category_id')
    merged.insert(loc=len(merged.columns), column='limit', value=2)
    merged.insert(loc=len(merged.columns), column='order', value=merged['available_quantity'] <= merged['limit'])
    #так как данные за июнь 2021 года, то при выполненни постановки аналитика где AVAILABLE_ON = CURRENT_DATE, витрина будет пустая
    #после заполнения бд актуальными данными расскомментировать
    #merged = merged[merged['available_on'] == datetime.datetime.now().isoformat()]
    #эту строку удалить
    merged = merged[merged['available_on'] == '2021-06-30']
    merged = merged[merged['order'] == True]
    merged['order'] = 'Да'
    result = merged[['name_short', 'category_name', 'available_quantity', 'limit', 'pos_name', 'order']]
    result.to_sql('to_order', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    print('to_order uploaded')

def fill_updated_on(conn_i):
    upd = pd.DataFrame([datetime.datetime.now()], columns =['date'])
    upd.to_sql('updated_on', conn_i.connect(), 'datamart', 'append', index=False, method='multi')
    print('updated_on uploaded')

from sqlalchemy import create_engine

conn_i = create_engine("postgresql+psycopg2://interns_4:dlf6y?@10.1.108.29:5432/internship_4_db")
create_table_intern(conn_i)
fill_avg_purchase(conn_i)
fill_avg_purchase_month(conn_i)
fill_revenue_month(conn_i)
fill_sold_item(conn_i)
fill_sold_item_month(conn_i)
fill_top_5(conn_i)
fill_to_order(conn_i)
fill_updated_on(conn_i)