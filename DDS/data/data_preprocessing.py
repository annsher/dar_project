import pandas as pd
from datetime import date, timedelta
from fnmatch import fnmatch

def truncate_table_intern(table, conn_i):
    cursor = conn_i.cursor()
    truncate = f'TRUNCATE TABLE "DDS".{table} CASCADE;'
    cursor.execute(truncate)
    conn_i.commit()
    cursor.close()

def csv_to_db(table, columns, conn):
    cursor = conn.cursor()
    f = open(f"{table}.csv", 'r', encoding="utf-8")
    cursor.copy_expert(f'COPY "DDS".{table}({columns}) FROM STDIN  DELIMITER \',\' CSV HEADER;', f)
    conn.commit()
    cursor.close()

def create_table_intern(conn_i):
    cursor = conn_i.raw_connection().cursor()
    f = open('create_tables_for_DDS.sql', 'r')
    create = ''.join(f.readlines())
    cursor.execute(create)
    truncate_table_intern('pos', conn_i.raw_connection())
    truncate_table_intern('transaction_pos', conn_i.raw_connection())
    truncate_table_intern('error', conn_i.raw_connection())
    #загрузка таблиц, которую предоставил заказчик
    csv_to_db('pos', 'pos, pos_name', conn_i.raw_connection())
    csv_to_db('transaction_pos', 'transaction_id, pos', conn_i.raw_connection())
    conn_i.raw_connection().commit()
    cursor.close()
    print('Таблицы загружены')

def record_errors(records, table, error_name, conn):
    error = records
    records = []
    if len(error) != 0:
        for i in range(len(error)):
            records.append([' '.join(list(error.iloc[[i]].values)[0]), error_name, table])
        error = pd.DataFrame(list(records), columns=['data', 'error', 'table_name'])
        error.to_sql('error', conn, 'DDS', 'append', index = False, method='multi')
        print(f'error table updated: {error_name} {table}')

def brand_table_processing(conn_z, conn_i):
    truncate_table_intern('brand', conn_i.raw_connection())
    brand = pd.read_sql_table('brand', conn_z.connect(), 'sources', coerce_float = False)
    brand = brand.drop_duplicates()
    for i in range(len(brand)):
        if not brand['brand_id'].iloc[i].isdigit() and brand['brand'].iloc[i].isdigit():
            brand['brand_id'].iloc[i], brand['brand'].iloc[i] = brand['brand'].iloc[i], brand['brand_id'].iloc[i]
    record_errors(brand[brand.duplicated(subset=['brand_id'])], 'brand', 'дубликат id', conn_i.connect())
    brand = brand.drop_duplicates(subset=['brand_id'])
    record_errors(brand[(pd.to_numeric(brand['brand_id'], errors='coerce') < 0) | (
                str(pd.to_numeric(brand['brand_id'], errors='coerce')) == 'nan')], 'brand', 'неверный формат id', conn_i.connect())
    brand = brand[pd.to_numeric(brand['brand_id'], errors='coerce') >= 0]
    brand['brand_id'] = brand['brand_id'].astype('int')
    brand['brand'] = brand['brand'].replace(['', 'NULL'], ["Не определено", "Не определено"])
    brand.to_sql('brand', conn_i.connect(), 'DDS', 'append', index = False, method='multi')
    print('brand table uploaded')

def category_table_processing(conn_z, conn_i):
    truncate_table_intern('category', conn_i.raw_connection())
    category = pd.read_sql_table('category', conn_z.connect(), 'sources', coerce_float=False)
    category = category.drop_duplicates()
    record_errors(category[category.duplicated(subset=['category_id'])], 'category', 'дубликат id', conn_i.connect())
    category = category.drop_duplicates(subset=['category_id'])
    category = category.drop_duplicates(subset=['category_name'])
    for name in ['НЕ ИСПОЛЬЗУМЕ', 'ПАРТНЕРЫ', 'Нераспределенные категории', 'Нераспределенные  товары', 'Пусто', 'Трубы', 'НЕ ИСПОЛЬЗУЕМ', 'Нераспределенные товары']:
        category = category[category['category_name'] != name]
    record_errors(category[(category['category_id'] == '') | (category['category_id'] == 'NULL')| (category['category_id'] == 'nan')], 'category', 'неверный формат id', conn_i.connect())
    category = category[(category['category_id'] != '') & (category['category_id'] != 'NULL') & (category['category_id'] != 'nan')]
    category['category_name'] = category['category_name'].replace(['', 'NULL'], ["Не определено", "Не определено"])
    category.to_sql('category', conn_i.connect(), 'DDS', 'append', index=False, method='multi')
    print('category table uploaded')

def product_table_processing(conn_z, conn_i):
    truncate_table_intern('product', conn_i.raw_connection())
    product = pd.read_sql_table('product', conn_z.connect(), 'sources', coerce_float=False)
    category = pd.read_sql_table('category', conn_i.connect(), 'DDS', coerce_float=False)
    merged = product.merge(category, how='left', on='category_id')
    merged['category_name'] = merged['category_name'].astype('str')
    error = merged[merged['category_name'] == 'nan']
    records = []
    for i in range(len(error)):
        records.append([str(error['product_id'].iloc[i]) + ' ' + str(error['name_short'].iloc[i]) + ' ' +
                        str(error['category_id'].iloc[i]) + ' ' + str(error['pricing_line_id'].iloc[i]) + ' ' +
                        str(error['brand_id'].iloc[i]), 'категории не существует', 'product'])
    merged = merged[merged['category_name'] != 'nan']
    product = merged[list(product)]
    error = pd.DataFrame(records, columns=['data', 'error', 'table_name'])
    error.to_sql('error', conn_i.connect(), 'DDS', 'append', index = False, method='multi')
    print('error table updated: категории не существует')
    product = product.drop_duplicates()
    record_errors(product[product.duplicated(subset=['product_id', 'brand_id'])], 'product', 'дубликат product_id, brand_id', conn_i.connect())
    product = product.drop_duplicates(subset=['product_id', 'brand_id'])
    record_errors(product[product.duplicated(subset=['name_short', 'brand_id'])], 'product', 'дубликат name_short, brand_id', conn_i.connect())
    product = product.drop_duplicates(subset=['name_short', 'brand_id'])
    for i in range(len(product)):
        if not product['product_id'].iloc[i].isdigit() and product['category'].iloc[i].isdigit():
            product['product_id'].iloc[i], product['name_short'].iloc[i] = product['name_short'].iloc[i], \
                                                                           product['product_id'].iloc[i]
    record_errors(product[product.duplicated(subset=['product_id'])], 'product','дубликат id', conn_i.connect())
    product = product.drop_duplicates(subset=['product_id'])
    record_errors(product[(pd.to_numeric(product['product_id'], errors='coerce') < 0) | (
                str(pd.to_numeric(product['product_id'], errors='coerce')) == 'nan')], 'product', 'неверный формат product_id', conn_i.connect())
    product = product[(product['category_id'] == '') | (product['category_id'] == 'NULL') | (product['category_id'] != 'nan')]
    record_errors(product[(pd.to_numeric(product['category_id'], errors='coerce') < 0) | (str(pd.to_numeric(product['category_id'], errors='coerce')) == 'nan')], 'product', 'неверный формат category_id', conn_i.connect())
    product = product[(product['category_id'] != '') & (product['category_id'] != 'NULL') & (product['category_id'] != 'nan')]
    record_errors(product[(pd.to_numeric(product['pricing_line_id'], errors='coerce') < 0) | (str(pd.to_numeric(product['pricing_line_id'], errors='coerce')) == 'nan')], 'product', 'неверный формат pricing_line_id', conn_i.connect())
    product = product[pd.to_numeric(product['pricing_line_id'], errors='coerce') >= 0]
    record_errors(product[(pd.to_numeric(product['brand_id'], errors='coerce') < 0) | (str(pd.to_numeric(product['brand_id'], errors='coerce')) == 'nan')], 'product', 'неверный формат brand_id', conn_i.connect())
    product = product[pd.to_numeric(product['brand_id'], errors='coerce') >= 0]
    product['product_id'] = product['product_id'].astype('int')
    product['pricing_line_id'] = product['pricing_line_id'].astype('int')
    product['brand_id'] = product['brand_id'].astype('int')
    record_errors(product[pd.to_numeric(product['name_short'], errors='coerce') >= 0].astype(str), 'product', 'неверный формат name_short', conn_i.connect())
    product = product[~(pd.to_numeric(product['name_short'], errors='coerce') >= 0)]
    product['name_short'] = product['name_short'].replace(['', 'NULL'], ["Не определено", "Не определено"])
    product.to_sql('product', conn_i.connect(), 'DDS', 'append', index=False, method='multi')
    print('links in product checked')
    print('product table uploaded')

def stock_table_processing(conn_z, conn_i):
    truncate_table_intern('stock', conn_i.raw_connection())
    product = pd.read_sql_table('product', conn_i.connect(), 'DDS', coerce_float=False)
    stock = pd.read_sql_table('stock', conn_z.connect(), 'sources', coerce_float=False)
    stock = stock.drop_duplicates()
    record_errors(stock[(pd.to_numeric(stock['available_on'], errors='coerce') < 0) & (~(fnmatch(str(stock['available_on']), '????-??-??*')))], 'stock', 'неверный формат available_on', conn_i.connect())
    stock = stock[((pd.to_numeric(stock['available_on'], errors='coerce') >= 0)) | (fnmatch(str(stock['available_on']), '????-??-??*'))]
    for i in stock.index:
        stock.at[i, 'available_on'] = date.fromisoformat('1900-01-01') + timedelta(days=int(stock.at[i, 'available_on']) - 2)
    stock['available_on'] = stock['available_on'].astype('datetime64[ns]')
    stock = stock.drop_duplicates(subset=['available_on','product_id', 'pos'])
    record_errors(product[product.duplicated(subset=['product_id', 'brand_id'])], 'stock', 'неверный формат product_id', conn_i.connect())
    stock = stock[pd.to_numeric(stock['product_id'], errors='coerce') >= 0]
    stock['product_id'] = stock['product_id'].astype('int')
    stock = stock[pd.to_numeric(stock['available_quantity'], errors='coerce') >= 0]
    stock['available_quantity'] = stock['available_quantity'].astype('float')
    stock['available_quantity'] = round(stock['available_quantity'])
    stock['available_quantity'] = stock['available_quantity'].astype('int')
    record_errors(stock[(pd.to_numeric(stock['cost_per_item'], errors='coerce') <= 0) | (str(pd.to_numeric(stock['cost_per_item'], errors='coerce')) == 'nan')], 'stock',
                  'неверный формат cost_per_item', conn_i.connect())
    stock = stock[pd.to_numeric(stock['cost_per_item'], errors='coerce') > 0]
    stock['cost_per_item'] = stock['cost_per_item'].astype('float64')
    merged = stock.merge(product, how='left', on='product_id')
    merged['name_short'] = merged['name_short'].astype('str')
    error = merged[merged['name_short'] == 'nan']
    records = []
    for i in range(len(error)):
        records.append([str(error['available_on'].iloc[i]) + ' ' + str(error['product_id'].iloc[i]) + ' ' +
                        str(error['pos'].iloc[i]) + ' ' + str(error['available_quantity'].iloc[i]) + ' ' +
                        str(error['cost_per_item'].iloc[i]), 'продукта не существует', 'stock'])
    merged = merged[merged['name_short'] != 'nan']
    stock = merged[list(stock)]
    error = pd.DataFrame(records, columns=['data', 'error', 'table_name'])
    error.to_sql('error', conn_i.connect(), 'DDS', 'append', index = False, method='multi')
    print('error table updated: продукта не существует stock')
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    merged = stock.merge(pos, how='left', on='pos')
    merged['pos_name'] = merged['pos_name'].astype('str')
    error = merged[merged['pos_name'] == 'nan']
    records = []
    for i in range(len(error)):
        records.append([str(error['available_on'].iloc[i]) + ' ' + str(error['product_id'].iloc[i]) + ' ' +
                        str(error['pos'].iloc[i]) + ' ' + str(error['available_quantity'].iloc[i]) + ' ' +
                        str(error['cost_per_item'].iloc[i]), 'магазина не существует', 'stock'])
    merged = merged[merged['pos_name'] != 'nan']
    stock = merged[list(stock)]
    error = pd.DataFrame(records, columns=['data', 'error', 'table_name'])
    error.to_sql('error', conn_i.connect(), 'DDS', 'append', index = False, method='multi')
    print('error table updated: магазина не существует stock')
    stock.to_sql('stock', conn_i.connect(), 'DDS', 'append', index = False, method='multi')
    print('links in stock checked')
    print('stock table uploaded')

def transaction_table_processing(conn_z, conn_i):
    truncate_table_intern('transaction', conn_i.raw_connection())
    product = pd.read_sql_table('product', conn_i.connect(), 'DDS', coerce_float=False)
    transaction = pd.read_sql_table('transaction', conn_z.connect(), 'sources', coerce_float=False)
    transaction = transaction.drop_duplicates()
    record_errors(transaction[(transaction['transaction_id'] == '') | (transaction['transaction_id'] == 'NULL')], 'transaction', 'пустое значение transaction_id', conn_i.connect())
    transaction = transaction[(transaction['transaction_id'] != '') & (transaction['transaction_id'] != 'NULL')]
    record_errors(transaction[(pd.to_numeric(transaction['product_id'], errors='coerce') < 0) | (str(
        pd.to_numeric(transaction['product_id'], errors='coerce')) == 'nan')], 'transaction', 'неверный формат product_id', conn_i.connect())
    transaction = transaction[pd.to_numeric(transaction['product_id'], errors='coerce') >= 0]
    transaction['product_id'] = transaction['product_id'].astype('int')
    transaction['recorded_on'] = pd.to_datetime(transaction['recorded_on'])
    record_errors(transaction[pd.to_numeric(transaction['quantity'], errors='coerce') < 0], 'transaction', 'неверный формат quantity', conn_i.connect())
    transaction = transaction[pd.to_numeric(transaction['quantity'], errors='coerce') >= 0]
    transaction['quantity'] = transaction['quantity'].astype('int')
    record_errors(transaction[pd.to_numeric(transaction['price'], errors='coerce') <= 0], 'transaction',
                  'неверный формат price', conn_i.connect())
    transaction = transaction[pd.to_numeric(transaction['price'], errors='coerce') > 0]
    transaction['price'] = transaction['price'].astype('float64')
    record_errors(transaction[pd.to_numeric(transaction['price_full'], errors='coerce') <= 0], 'transaction',
                  'неверный формат price_full', conn_i.connect())
    transaction = transaction[pd.to_numeric(transaction['price_full'], errors='coerce') > 0]
    transaction['price_full'] = transaction['price_full'].astype('float64')
    merged = transaction.merge(product, how='left', on='product_id')
    merged['name_short'] = merged['name_short'].astype('str')
    error = merged[merged['name_short'] == 'nan']
    records = []
    for i in range(len(error)):
        records.append([str(error['transaction_id'].iloc[i]) + ' ' + str(error['product_id'].iloc[i]) + ' ' +
                        str(error['recorded_on'].iloc[i]) + ' ' + str(error['quantity'].iloc[i]) + ' ' +
                        str(error['price'].iloc[i]) + ' ' + str(error['price_full'].iloc[i]) + ' ' + str(error['order_type_id'].iloc[i]), 'продукта не существует', 'transaction'])
    merged = merged[merged['name_short'] != 'nan']
    transaction = merged[list(transaction)]
    error = pd.DataFrame(records, columns=['data', 'error', 'table_name'])
    error.to_sql('error', conn_i.connect(), 'DDS', 'append', index = False, method='multi')
    print('error table updated: продукта не существует transaction')
    transaction = transaction.drop_duplicates(subset=['transaction_id', 'product_id'])
    truncate_table_intern('transaction', conn_i.raw_connection())
    transaction.to_sql('transaction', conn_i.connect(), 'DDS', 'append', index=False, method='multi')
    print('links in transaction checked')
    print('transaction table uploaded')

def transaction_pos_table_processing(conn_i):
    transaction_pos = pd.read_sql_table('transaction_pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction_pos.drop_duplicates()
    transaction_pos['pos'] = transaction_pos['pos'].astype(str)
    transaction_pos = transaction_pos[transaction_pos['pos'] != 'None']
    pos = pd.read_sql_table('pos', conn_i.connect(), 'DDS', coerce_float=False)
    transaction = pd.read_sql_table('transaction', conn_i.connect(), 'DDS', coerce_float=False)
    merged = transaction.merge(transaction_pos, how='left', on='transaction_id')
    merged = merged.merge(pos, how='left', on='pos')
    merged['pos'] = merged['pos'].astype(str)
    merged = merged[merged['pos'] != 'nan']
    merged['pos_name'] = merged['pos_name'].astype(str)
    merged = merged[merged['pos_name'] != 'nan']
    transaction_pos = merged[['transaction_id', 'pos']]
    transaction = merged[list(transaction)]
    truncate_table_intern('transaction_pos', conn_i.raw_connection())
    truncate_table_intern('transaction', conn_i.raw_connection())
    transaction.to_sql('transaction', conn_i.connect(), 'DDS', 'append', index=False, method='multi')
    transaction_pos.to_sql('transaction_pos', conn_i.connect(), 'DDS', 'append', index=False, method='multi')
    print('links in transaction_pos checked')
    print('link transaction to transaction_pos checked')

from sqlalchemy import create_engine

conn_i = create_engine("postgresql+psycopg2://interns_4:dlf6y?@10.1.108.29:5432/internship_4_db")
conn_z = create_engine("postgresql+psycopg2://interns_4:dlf6y?@10.1.108.29:5432/internship_sources")
create_table_intern(conn_i)
brand_table_processing(conn_z, conn_i)
category_table_processing(conn_z, conn_i)
product_table_processing(conn_z, conn_i)
stock_table_processing(conn_z, conn_i)
transaction_table_processing(conn_z, conn_i)
transaction_pos_table_processing(conn_i)

