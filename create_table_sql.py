create_schema = """CREATE SCHEMA anadya"""
stg_vedomosti = """CREATE TABLE anadya.stg_vedomosti (
    id_new SERIAL,
    title VARCHAR(300),
    links VARCHAR(200),
    public_date TIMESTAMPTZ,
    author VARCHAR(30),
    category VARCHAR(20),
    source VARCHAR(20) DEFAULT 'Vedomosti'
)"""

stg_lenta="""CREATE TABLE anadya.stg_lenta (
    id_new SERIAL,
    title VARCHAR(300),
    links VARCHAR(200),
    public_date TIMESTAMPTZ,
    author VARCHAR(30),
    category VARCHAR(20),
    description VARCHAR(400),
    source VARCHAR(20) DEFAULT 'Lenta'
)"""

stg_kommersant="""CREATE TABLE anadya.stg_kommersant (
    id_new SERIAL,
    title VARCHAR(300),
    links VARCHAR(200),
    public_date TIMESTAMPTZ,
    category VARCHAR(20),
    description VARCHAR(400),
    source VARCHAR(20) DEFAULT 'Kommersant'
)"""


stg_news="""CREATE TABLE anadya.stg_news (
    id_new SERIAL,
    title VARCHAR(300),
    links VARCHAR(200),
    public_date TIMESTAMPTZ,
    author VARCHAR(30),
    category VARCHAR(20),
    description VARCHAR(400),
    source VARCHAR(20),
    date_of_download DATE
)"""

stg_categories="""CREATE TABLE anadya.stg_categories(id_category SERIAL, category VARCHAR(20))"""

stg_authors="""CREATE TABLE anadya.stg_authors( id_authors SERIAL, author VARCHAR(30))"""

stg_date_of_downloads="""CREATE TABLE anadya.stg_date_of_downloads( id_date_of_download SERIAL, date_of_download DATE)"""

stg_public_date="""CREATE TABLE anadya.stg_public_date( id_public_date SERIAL, public_date TIMESTAMPTZ, date_split_week INT, date_split DATE)"""

tgt_categories="""CREATE TABLE anadya.tgt_categories(id_category SERIAL PRIMARY KEY, category VARCHAR(20))"""

tgt_authors="""CREATE TABLE anadya.tgt_authors( id_authors SERIAL PRIMARY KEY, author VARCHAR(30))"""

tgt_date_of_downloads="""CREATE TABLE anadya.tgt_date_of_downloads( id_date_of_download SERIAL PRIMARY KEY, date_of_download DATE)"""

tgt_date_split_week="""CREATE TABLE anadya.tgt_date_split_week( id_date_split_week SERIAL PRIMARY KEY, day_of_week INT)"""

tgt_date_split="""CREATE TABLE anadya.tgt_date_split( id_date_split SERIAL PRIMARY KEY, date_day DATE)"""

tgt_public_date="""CREATE TABLE anadya.tgt_public_date( id_public_date SERIAL PRIMARY KEY, 
                public_date TIMESTAMPTZ, 
                id_date_split_week INT, 
                id_date_split INT,
                FOREIGN KEY (id_date_split_week) REFERENCES anadya.tgt_date_split_week(id_date_split_week),
                FOREIGN KEY (id_date_split) REFERENCES anadya.tgt_date_split(id_date_split))"""

meta_sources="""CREATE TABLE anadya.meta_sources( id_source SERIAL PRIMARY KEY, source VARCHAR(20))"""
insert_meta_sources="""INSERT INTO anadya.meta_sources(source)
VALUES ('Kommersant'),
        ('Lenta'),
        ('Vedomosti')"""

tgt_news="""CREATE TABLE anadya.tgt_news(
    id_new SERIAL PRIMARY KEY,
    title VARCHAR(300),
    links VARCHAR(200),
    id_public_date INT,
    id_author INT,
    id_category INT,
    description VARCHAR(400),
    id_source INT,
    id_date_of_download INT,
    FOREIGN KEY (id_public_date) REFERENCES anadya.tgt_public_date(id_public_date),
    FOREIGN KEY (id_author) REFERENCES anadya.tgt_authors(id_authors),
    FOREIGN KEY (id_category) REFERENCES anadya.tgt_categories(id_category),
    FOREIGN KEY (id_source) REFERENCES anadya.meta_sources(id_source),
    FOREIGN KEY (id_date_of_download) REFERENCES anadya.tgt_date_of_downloads(id_date_of_download)
    )"""

view_count_new_of_date="""CREATE VIEW anadya.count_new_of_date AS (
SELECT DISTINCT t1.id_category,
	t3.date_day,
	COUNT(t3.date_day) OVER (PARTITION BY t1.id_category, t3.date_day) AS kol_vo
FROM anadya.tgt_news t1
LEFT JOIN anadya.tgt_public_date t2
ON t2.id_public_date = t1.id_public_date
left join anadya.tgt_date_split t3
on t3.id_date_split = t2.id_date_split)"""
    
meta_date_max_count_news="""CREATE TABLE anadya.meta_date_max_count_news(id_category INT PRIMARY KEY, date_max DATE)"""

view_test_kommersant="""CREATE VIEW anadya.test_kommersant AS (
SELECT 'total' as test,  COUNT(*) as count_to_kommersant
FROM anadya.stg_kommersant
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'total unique', COUNT(*)
FROM(SELECT distinct title, public_date
FROM anadya.stg_kommersant
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE) t
UNION ALL
SELECT 'title is not null', COUNT(title)
FROM anadya.stg_kommersant
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'links is not null', COUNT(links)
FROM anadya.stg_kommersant
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'public date is not null', COUNT(public_date)
FROM anadya.stg_kommersant
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'category is not null', COUNT(category)
FROM anadya.stg_kommersant
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'description is not null', COUNT(description)
FROM anadya.stg_kommersant
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'source is not null', COUNT(source)
FROM anadya.stg_kommersant
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
"""



view_test_vedomosti="""CREATE VIEW anadya.test_vedomosti AS (
SELECT 'total' as test, COUNT(*) as count_to_vedomosti
FROM anadya.stg_vedomosti
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL 
SELECT 'total unique', COUNT(*)
FROM(SELECT distinct title, public_date
FROM anadya.stg_vedomosti
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE) t
UNION ALL
SELECT 'title is not null', COUNT(title)
FROM anadya.stg_vedomosti
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'links is not null', COUNT(links)
FROM anadya.stg_vedomosti
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'public date is not null', COUNT(public_date)
FROM anadya.stg_vedomosti
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'author is not null', COUNT(author)
FROM anadya.stg_vedomosti
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'category is not null', COUNT(category) 
FROM anadya.stg_vedomosti
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'source is not null', COUNT(source)
FROM anadya.stg_vedomosti
WHERE public_date >= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
"""


view_test_lenta="""CREATE VIEW anadya.test_lenta AS (
SELECT 'total' as test,  COUNT(*) as count_to_lenta
FROM anadya.stg_lenta
WHERE public_date>= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'total unique', COUNT(*)
FROM(SELECT distinct title, public_date
FROM anadya.stg_lenta
WHERE public_date>= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE) t
UNION ALL
SELECT 'title is not null', COUNT(title)
FROM anadya.stg_lenta
WHERE public_date>= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'links is not null', COUNT(links)
FROM anadya.stg_lenta
WHERE public_date>= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'public date is not null', COUNT(public_date)
FROM anadya.stg_lenta
WHERE public_date>= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'author is not null', COUNT(author)
FROM anadya.stg_lenta
WHERE public_date>= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'category is not null', COUNT(category)
FROM anadya.stg_lenta
WHERE public_date>= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'description is not null', COUNT(description)
FROM anadya.stg_lenta
WHERE public_date>= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
UNION ALL
SELECT 'source is not null', COUNT(source)
FROM anadya.stg_lenta
WHERE public_date>= CURRENT_DATE - INTERVAL '1 days' AND public_date < CURRENT_DATE
"""