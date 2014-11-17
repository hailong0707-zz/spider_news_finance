# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import threading
import MySQLdb
from scrapy import log

class SpiderNewsFinancePipeline(object):

    SELECT_NEWS_FINANCE_BY_TITLE = "SELECT * FROM news_finance WHERE type1='%s' AND type2='%s' AND title='%s'"
    INSERT_NEWS_FINANCE = ("INSERT INTO news_finance (web, type1, type2, day, time, title, tags, article) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

    lock = threading.RLock()
    conn=MySQLdb.connect(user='root', passwd='123123', db='news', autocommit=True)
    cursor = conn.cursor()

    def insert(self, web, type1, type2, title, day, _time, article, tags):
        self.lock.acquire()
        rows = self.cursor.execute(self.SELECT_NEWS_FINANCE_BY_TITLE % (type1, type2, title))
        if rows > 0:
            log.msg(web + "::" + type1 + "::" + type2 + " '" + title + "' has already saved !", level=log.INFO)
            return
            self.lock.release()
        else:
            news = (web, type1, type2, day, _time, title, tags, article)
            try:
                self.cursor.execute(self.INSERT_NEWS_FINANCE, news)
                log.msg(web + "::" + type1 + "::" + type2 + " '" + title + "' saved successfully", level=log.INFO)
            except:
                log.msg("MySQL exception !!!", level=log.ERROR)
            self.lock.release()

    def process_item(self, item, spider):
        web = item['web']
        type1 = item['type1']
        type2 = item['type2']
        title = item['title']
        day = item['day']
        time = item['time']
        article = item['article']
        tags = item['tags']
        self.insert(web, type1, type2, title, day, time, article, tags)
