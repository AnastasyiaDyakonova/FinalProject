#!/usr/bin/python3
import psycopg2
from psycopg2 import Error
from config import database, user, password, host, port
from etl_sql import *

try:
    # Подкдючение к базе данных. Переменные в модуле config
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()

    # Списки, содержащие переменные из модулей trancate_table_sql, insert_stg_sql, insert_tgt_sql содержащие SQL-скрипты.
    list_truncate_sql = [tt_stg_news, tt_stg_categories, tt_stg_authors, tt_stg_date_of_downloads, tt_stg_public_date, tt_stg_vedomosti, tt_stg_lenta, tt_stg_kommersant, tt_meta_date_max_count_news]
    
    list_insert_stg_sql = [insert_stg_news, insert_stg_categories, insert_stg_authors, insert_stg_date_of_downloads, insert_stg_public_date]
    
    list_insert_tgt_sql = [insert_tgt_categories, insert_tgt_authors, insert_tgt_date_of_downloads, insert_tgt_date_split_week, insert_tgt_date_split, insert_tgt_public_date, insert_tgt_news, insert_meta_date_max_count_news]

    # Удаление содержимого таблиц в слое сырых данных
    for elem in list_truncate_sql:
        cursor.execute(elem)
        print("Trancate table", elem)
    # Копирование данных из источников
    with open ('/mnt/c/Users/Second/Desktop/project/lenta_ru.csv', encoding='utf-8', newline='') as file:
        sql = "COPY anadya.stg_lenta(title, links, public_date, author, category, description) FROM STDIN ENCODING 'utf8' DELIMITER ',' CSV HEADER"
        cursor.copy_expert(sql, file)
        print("COPY anadya.stg_lenta")
    with open ('/mnt/c/Users/Second/Desktop/project/kommersant_ru.csv', encoding='utf-8', newline='') as file:
        sql = "COPY anadya.stg_kommersant(title, links, public_date, category, description)  FROM STDIN ENCODING 'utf8' DELIMITER ',' CSV HEADER"
        cursor.copy_expert(sql, file)
        print("COPY anadya.stg_kommersant")
    with open ('/mnt/c/Users/Second/Desktop/project/vedomosti_ru.csv', encoding='utf-8', newline='') as file:
        sql = "COPY anadya.stg_vedomosti(title, links, public_date, author, category) FROM STDIN ENCODING 'utf8' DELIMITER ',' CSV HEADER"
        cursor.copy_expert(sql, file)
        print("COPY anadya.stg_vedomosti")

    conn.commit()
    # загрузка в слой сырых данных
    for elem in list_insert_stg_sql:
        cursor.execute(elem)
        conn.commit()
        print("Insert stg")
    # загрузка в хранилище данных
    for elem in list_insert_tgt_sql:
        cursor.execute(elem)
        conn.commit()
        print("Insert tgt")

   

    conn.commit()

except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if conn:
        cursor.close()
        conn.close()
        print("Соединение с PostgreSQL закрыто")