**Индивидуальный проект: "Анализ публикуемых новостей"**
=============================================
**Общая задача:**
создать ETL-процесс формирования витрин данных для анализа публикаций новостей.

**Источники данных:**
1) https://lenta.ru/rss/
2) https://www.vedomosti.ru/rss/news
3) https://www.kommersant.ru/RSS/news.xml

Ежедневно запускается ETL-процесс, строится витрина данных и отчет по заполнению данными слоя сырых данных, которые сохраняются в локальную файловую систему.

**Содержимое витрины данных:**
-Суррогатный ключ категории
-Название категории
-Общее количество новостей из всех источников по данной категории за все время
-Количество новостей данной категории для каждого из источников за все время
-Общее количество новостей из всех источников по данной категории за последние сутки
-Количество новостей данной категории для каждого из источников за последние сутки
-Среднее количество публикаций по данной категории в сутки
-День, в который было сделано максимальное количество публикаций по данной категории
-Количество публикаций новостей данной категории по дням недели

**Отчет о загруженных в слой сырых данных включает в себя обобщенную информацию по количеству записей из 3 источников за прошедшие сутки по параметрам:**
- общее количество загруженных записей;
- количество IS NOT NULL уникальных загруженных записей;
- количество IS NOT NULL по столбцу заголовков;
- количество IS NOT NULL по столбцу адресов страниц;
- количество IS NOT NULL по столбцу дат опубликованных новостей;
- количество IS NOT NULL по столбцу авторов;
- количество IS NOT NULL по столбцу категорий;
- количество IS NOT NULL по столбцу текста новостей;
- количество IS NOT NULL по столбцу источников;

**Реализация:**
==========
Данный проект реализован на локальной машине с операционной системой Windows 10. Для автоматизации процессов перешла на Ubuntu.
Используемые технологии:
- Проект построен на Python3
- Использовала библиотеку psycopg2 для подключения и работы с базой данных
- Для оркестрации выбрала crontab, так как он решит все потребности проекта. 

**Этапы установки и работы:**
1) скачать Ubuntu 20.04 через WSL2;
2) установить postgresql в Ubuntu 20.04, запустить сервис POSTGRESQL, создать базу данных, пользователя, пароль из командной строки;
3) запустить скрипт create_table.py(конфиг для скрипта - config.py, нужно настроить под локальный компьютер) и модуль с переменными(create_table_sql.py - лежат в этой же директории), который создает схему, таблицы слоя сырых данных, хранения данных, таблицы с метаданными и представления;
4) запустить сервис crontab
5) запустить из командной строки crontab -e, скопировать в него задачи из cron.cron, сохранить и выйти.

**Сrontab ежедневно запускает 7 задач:**
1)первые три задачи на парсинг данных из источников. Файлы помещаются в директорию project.

2)четвертая задача на загрузку данных в хранилище.
Скрипт etl_linux.py выполняет:
- очистку слоя сырых данных и таблицы метаданных(anadya.meta_date_max_count_news);
- копирует из спарсенных файлов информацию в слой сырых данных;
- из полученных данных формируется отношение anadya.stg_news, где объединенные данные из трех источников берутся за прошедшие сутки и обработаны от Null значений, приводятся к единому формату категорий; 
в отношение anadya.stg_categories, anadya.stg_authors, anadya.stg_date_of_downloads, anadya.stg_public_date записываются категории, авторы, дата загрузки новостей, дата публикации новостей соответственно. 
- далее формируется слой хранения данных: постаралась привести таблицы ко второй нормальной форме, реализовать схему "Снежинка". Связи можно посмотреть на ER-диаграмме, ссылка ниже. В этот слой загружаются новые данные, которые до этого не приходили из слоя сырых данных. На этом этапе заполняется таблица с метаданными anadya.meta_date_max_count_news, которая в дальнейшем будет нужна для построения отчета. В этой таблице будут лежать данные о дате максимального количества новостей в разрезе категорий. 
Модуль с переменными - etl_sql.py;

3)Пятая задача на построение отчета и сохранение его в папку с отчетами.
Скрипт date_end.py строит витрину данных(resultsfile.csv) и отчет о загруженных в слой сырых данных(resultsfile_test.csv), которые сохраняются в локальную файловую систему в директорию reports.

4)6 и 7 задачи направлены на перенос отработанных файлов в папки хранения. В дальнейшем можно поставить задачи на удаление архивных данных.
С помощью скрипта data_transfer_reports.sh формирует в директории reports папку с именем текущей даты-в нее переносится файл resultsfile.csv и resultsfile_test.csv, далее переименовывается в resultsfile+текущая дата.csv(например resultsfile2022-12-29.csv) и resultsfile_test+текущая дата.csv(например resultsfile_test2022-12-29.csv)
С помощью скрипта data_transfer_pars.py спарсенные файлы перемещаются в папку data_end/текущая дата/имя+текущая дата.csv по принципу, указанному выше.

**Выводы и результаты:**
===================
В проекте мне удалось создать ETL-процесс и сформировать витрину данных для анализа публикаций новостей.
В результате ежедневно запускается ETL-процесс, строится витрина данных и отчет по заполнению слоя сырых данных, которые сохраняются в локальную файловую систему.

**Ссылки:**
==========
[Ссылка на презентацию](https://docs.google.com/presentation/d/1EtyiUmGRYCFY-NJpmgWNaDNBnJimHAbqEmfo-G-jI5k/edit?usp=sharing)
[Ссылка на ER-диаграмму](https://drive.google.com/file/d/1UNEqktfKiPllFagRxwrwJVBIujiqfTyO/view?usp=sharing)