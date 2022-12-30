#!/usr/bin/python3

import psycopg2
from psycopg2 import Error
#from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import database, user, password, host, port
from create_table_sql import *

try:
	conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
	cursor = conn.cursor()

	create_tables = [create_schema, stg_vedomosti, stg_lenta, stg_kommersant, stg_news, stg_categories, stg_authors, stg_date_of_downloads, stg_public_date, tgt_categories, tgt_authors, tgt_date_of_downloads, tgt_date_split_week, tgt_date_split, tgt_public_date, meta_sources, insert_meta_sources, tgt_news, view_count_new_of_date, meta_date_max_count_news, view_test_kommersant, view_test_vedomosti, view_test_lenta]

	for i in create_tables:
		cursor.execute(i)
		conn.commit()
		print("Таблица успешно создана в PostgreSQL")
except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if conn:
        cursor.close()
        conn.close()
        print("Соединение с PostgreSQL закрыто")