#!/usr/bin/python

import feedparser
import csv
import pandas as pd
import re

newsurls = {'vedomosti.ru':'https://www.vedomosti.ru/rss/news'} 

f_all_news = '/mnt/c/Users/Second/Desktop/project/vedomosti_ru.csv'

def parseRSS( rss_url ): #функция получает линк на рсс ленту, возвращает распаршенную ленту с помощью feedpaeser
    return feedparser.parse( rss_url )  
    
def getHeadlines( rss_url ): #функция для получения заголовков новости
    headlines = []
    feed = parseRSS( rss_url )
    for newsitem in feed['items']:
        headlines.append(newsitem['title'])
    return headlines


def getLinks( rss_url ): #функция для получения ссылки на источник новости
    links = []
    feed = parseRSS( rss_url )
    for newsitem in feed['items']:
        links.append(newsitem['link'])
    return links

def getDates( rss_url ): #функция для получения даты публикации новости
    dates = []
    feed = parseRSS( rss_url )
    for newsitem in feed['items']:
        dates.append(newsitem['published'])
    return dates
    
def getAuthor( rss_url ): #функция для получения автора
    authors = []
    feed = parseRSS( rss_url )
    for newsitem in feed['items']:
        authors.append(newsitem['author'])
    return authors

def getCategory( rss_url ): #функция для получения даты публикации новости
    category = []
    feed = parseRSS( rss_url )
    for newsitem in feed['items']:
        category.append(newsitem['category'])
    return category
    
allheadlines, alllinks, alldates, allauthor,allcategory = [],[],[],[],[]
# Прогоняем нашии URL и добавляем их в наши пустые списки
for key,url in newsurls.items():
    allheadlines.extend( getHeadlines( url ) )
    
    
for key,url in newsurls.items():
    alllinks.extend( getLinks( url ) )
    
for key,url in newsurls.items():
    alldates.extend( getDates( url ) )

for key,url in newsurls.items():
    allauthor.extend( getAuthor( url ) )

for key,url in newsurls.items():
    allcategory.extend( getCategory( url ) )

def write_all_news(all_news_filepath): #функция для записи всех новостей в .csv, возвращает нам этот датасет
    header = ['Title','Links','Publication Date', 'Author', 'Category'] 

    with open(all_news_filepath, 'w', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')

        writer.writerow(i for i in header) 

        for a,b,c,d,e in zip(allheadlines,
                            alllinks, alldates, allauthor, allcategory):
            writer.writerow((a,b,c,d,e))


        df = pd.read_csv(all_news_filepath)
        
            
    return df

write_all_news(f_all_news)