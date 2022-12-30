#!/usr/bin/python3
import psycopg2
from psycopg2 import Error
from config import database, user, password, host, port

try:
    # Подкдючение к базе данных. Переменные в модуле config
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port, client_encoding='utf8')
    cursor = conn.cursor()
    query = """
            SELECT DISTINCT t1.id_category"Номер категории",
                t2.category "Категория", 
                COUNT(*) OVER ( PARTITION BY t1.id_category ) "Общее количество новостей",
                COUNT(*) OVER ( PARTITION BY t1.id_category, t1.id_source) AS "Общее количество по источникам",
                COUNT(*) FILTER (WHERE t3.public_date BETWEEN current_date - interval '1 days' AND current_date) OVER ( PARTITION BY t1.id_category) AS "Общее количество за вчера",
                COUNT(*) FILTER (WHERE t3.public_date >=current_date - interval '1 days' AND t3.public_date < current_date) OVER ( PARTITION BY t1.id_category, t1.id_source) AS "Общее за вчера по источникам",
                COUNT(*) OVER ( PARTITION BY t1.id_category ) / (SELECT COUNT(*) FROM anadya.tgt_date_of_downloads)  AS "Среднее количество в сутки",
                t4.date_max  "День с максимумом новостей",
                COUNT(*) FILTER (WHERE t5.day_of_week = 1)OVER ( PARTITION BY t1.id_category ) "Понедельник",
                COUNT(*) FILTER (WHERE t5.day_of_week = 2)OVER ( PARTITION BY t1.id_category ) "Вторник",
                COUNT(*) FILTER (WHERE t5.day_of_week = 3)OVER ( PARTITION BY t1.id_category ) "Среда",
                COUNT(*) FILTER (WHERE t5.day_of_week = 4)OVER ( PARTITION BY t1.id_category ) "Четверг",
                COUNT(*) FILTER (WHERE t5.day_of_week = 5)OVER ( PARTITION BY t1.id_category ) "Пятница",
                COUNT(*) FILTER (WHERE t5.day_of_week = 6)OVER ( PARTITION BY t1.id_category ) "Суббота",
                COUNT(*) FILTER (WHERE t5.day_of_week = 7)OVER ( PARTITION BY t1.id_category ) "Воскресение"
            FROM anadya.tgt_news t1
            LEFT JOIN anadya.tgt_categories t2
            ON t2.id_category = t1.id_category
            LEFT JOIN anadya.tgt_public_date t3
            ON t3.id_public_date = t1.id_public_date
            LEFT JOIN anadya.meta_date_max_count_news t4
            on t4.id_category = t1.id_category
			LEFT JOIN anadya.tgt_date_split_week t5
			on t5.id_date_split_week = t3.id_date_split_week
            ORDER BY t1.id_category
            """
    query1 = """
            SELECT t1.test, count_to_lenta, count_to_kommersant, count_to_vedomosti
            FROM anadya.test_lenta t1
            LEFT JOIN anadya.test_kommersant t2
            ON t2.test = t1.test
            LEFT JOIN anadya.test_vedomosti t3
            ON t3.test = t1.test
            """
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER encoding 'UTF8'".format(query)
    outputquery1 = "COPY ({0}) TO STDOUT WITH CSV HEADER encoding 'UTF8'".format(query1)

    with open('/mnt/c/Users/Second/Desktop/project/reports/resultsfile.csv', 'w', encoding='utf-8') as f:
        cursor.copy_expert(outputquery, f)
    with open('/mnt/c/Users/Second/Desktop/project/reports/resultsfile_test.csv', 'w', encoding='utf-8') as f:
        cursor.copy_expert(outputquery1, f)
        
except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if conn:
        cursor.close()
        conn.close()
        print("Соединение с PostgreSQL закрыто")