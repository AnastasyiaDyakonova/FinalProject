# Очистка слоя сырых данных

tt_stg_news = """TRUNCATE TABLE anadya.stg_news"""

tt_stg_categories = """TRUNCATE TABLE anadya.stg_categories"""

tt_stg_authors = """TRUNCATE TABLE anadya.stg_authors"""

tt_stg_date_of_downloads = """TRUNCATE TABLE anadya.stg_date_of_downloads"""

tt_stg_public_date = """TRUNCATE TABLE anadya.stg_public_date"""

tt_stg_vedomosti = """TRUNCATE TABLE anadya.stg_vedomosti"""

tt_stg_lenta = """TRUNCATE TABLE anadya.stg_lenta"""

tt_stg_kommersant = """TRUNCATE TABLE anadya.stg_kommersant"""

tt_meta_date_max_count_news = """TRUNCATE TABLE anadya.meta_date_max_count_news"""

# Загрузка новых данных в слой сырых данных

insert_stg_news = """INSERT INTO anadya.stg_news(title, links, public_date, author, category, description, source, date_of_download)
SELECT title, 
	   links, 
	   public_date, 
	   coalesce(author, 'Неизвестно'), 
	   CASE 
		WHEN category = 'Финансы' THEN 'Экономика' 
		ELSE category 
		END category, 
	  'Нет данных', 
	  source,
	  to_date('2022-12-29', 'YYYY-MM-DD')
FROM anadya.stg_vedomosti
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
	UNION ALL 
SELECT title, 
		links, 
		public_date, 
		coalesce(author,'Неизвестно'), 
		CASE 
			WHEN category IN ('Из жизни', 'Россия', 'Силовые структуры', 'Среда обитания', 'Ценности', '69-я параллель') THEN 'Общество'
			WHEN category = 'Интернет и СМИ' THEN 'Медиа'
			WHEN category IN ('Бывший СССР', 'Мир') THEN 'Политика'
			WHEN category = 'Наука и техника' THEN 'Технологии'
			ELSE category
			END category,
		description,
		source,
		CURRENT_DATE
FROM anadya.stg_lenta
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
	UNION ALL 
SELECT title,
		links, 
		public_date,
		'Неизвестно', 
		CASE 
			WHEN category IN ('Происшествия', 'Топ главной') THEN 'Общество'
			WHEN category = 'Телекоммуникации' THEN 'Медиа'
			WHEN category = 'Мир' THEN 'Политика'
			WHEN category = 'Hi-Tech' THEN 'Технологии'
			ELSE category
			END category,
		description,
		source, 
		CURRENT_DATE
FROM anadya.stg_kommersant
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE"""

insert_stg_categories = """INSERT INTO anadya.stg_categories(category)
							SELECT DISTINCT category
							FROM anadya.stg_news"""

insert_stg_authors = """INSERT INTO anadya.stg_authors(author)
						SELECT DISTINCT author
						FROM anadya.stg_news"""

insert_stg_date_of_downloads = """INSERT INTO anadya.stg_date_of_downloads(date_of_download)
									SELECT DISTINCT date_of_download
									FROM anadya.stg_news"""

insert_stg_public_date = """INSERT INTO anadya.stg_public_date(public_date, date_split_week, date_split)
							SELECT DISTINCT public_date, EXTRACT(ISODOW FROM public_date), to_date(public_date::varchar, 'YYYY-MM-DD')
							FROM anadya.stg_news"""


# Загрузка в слой хранения данных

insert_tgt_categories = """INSERT INTO anadya.tgt_categories(category)
SELECT t1.category 
FROM anadya.stg_categories t1
LEFT JOIN anadya.tgt_categories t2
ON t2.category = t1.category
WHERE t2.category IS NULL"""

insert_tgt_authors = """INSERT INTO anadya.tgt_authors(author)
SELECT t1.author
FROM anadya.stg_authors t1
LEFT JOIN anadya.tgt_authors t2
ON t2.author = t1.author
WHERE t2.author IS NULL"""

insert_tgt_date_of_downloads = """INSERT INTO anadya.tgt_date_of_downloads(date_of_download)
SELECT t1.date_of_download
FROM anadya.stg_date_of_downloads t1
LEFT JOIN anadya.tgt_date_of_downloads t2
ON t2.date_of_download = t1.date_of_download
WHERE t2.date_of_download IS NULL"""


insert_tgt_date_split_week="""INSERT INTO anadya.tgt_date_split_week(day_of_week)
SELECT DISTINCT EXTRACT(ISODOW FROM t1.public_date)
FROM anadya.stg_public_date t1
LEFT JOIN anadya.tgt_public_date t2
ON t2.public_date = t1.public_date
WHERE t2.public_date IS NULL"""

insert_tgt_date_split="""INSERT INTO anadya.tgt_date_split(date_day)
SELECT DISTINCT to_date(t1.public_date::varchar, 'YYYY-MM-DD')
FROM anadya.stg_public_date t1
LEFT JOIN anadya.tgt_public_date t2
ON t2.public_date = t1.public_date
WHERE t2.public_date IS NULL"""

insert_tgt_public_date= """INSERT INTO anadya.tgt_public_date(public_date, id_date_split_week, id_date_split)
SELECT t1.public_date, t3.id_date_split_week, t4.id_date_split
FROM anadya.stg_public_date t1
LEFT JOIN anadya.tgt_public_date t2
ON t2.public_date = t1.public_date
LEFT JOIN anadya.tgt_date_split_week t3
ON t1.date_split_week=t3.day_of_week
LEFT JOIN anadya.tgt_date_split t4
ON t1.date_split=t4.date_day
WHERE t2.public_date IS NULL"""

insert_tgt_news = """INSERT INTO anadya.tgt_news(title, links, id_public_date, id_author, id_category, description, id_source, id_date_of_download)
SELECT t1.title, t1.links, t5.id_public_date, t2.id_authors, t3.id_category, t1.description, t6.id_source, t4.id_date_of_download
FROM anadya.stg_news t1
LEFT JOIN anadya.tgt_authors t2
ON t2.author = t1.author
LEFT JOIN anadya.tgt_categories t3 
ON t1.category=t3.category
LEFT JOIN anadya.tgt_date_of_downloads t4
ON t4.date_of_download = t1.date_of_download
LEFT JOIN anadya.tgt_public_date t5 
ON t5.public_date=t1.public_date
LEFT JOIN anadya.meta_sources t6
ON t6.source = t1.source
LEFT JOIN anadya.tgt_news t7 
ON t1.title = t7.title AND t7.links = t1.links
WHERE t7.title IS NULL"""

insert_meta_date_max_count_news="""INSERT INTO anadya.meta_date_max_count_news(id_category, date_max)
SELECT t1.id_category, t2.date_day
FROM (SELECT id_category, max(kol_vo) kol_vo
      FROM anadya.count_new_of_date
      group by id_category) t1
LEFT JOIN anadya.count_new_of_date t2
ON t1.kol_vo = t2.kol_vo AND t1.id_category=t2.id_category
ORDER BY t1.id_category"""


