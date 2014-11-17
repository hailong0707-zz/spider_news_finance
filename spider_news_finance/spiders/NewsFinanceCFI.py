# -*- coding: utf-8 -*-
import scrapy
from bs4 import BeautifulSoup
from spider_news_finance.items import SpiderNewsFinanceItem
from scrapy import log
import threading
import MySQLdb

class NewsfinancecfiSpider(scrapy.Spider):
    name = "NewsFinanceCFI"
    allowed_domains = ["industry.cfi.cn"]
    start_urls = (
        'http://industry.cfi.cn/BCA0A4127A4128A4132.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4138.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4133.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4134.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4135.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4136.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4137.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4139.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4140.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4141.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4142.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4143.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4144.html',
        'http://industry.cfi.cn/BCA0A4127A4128A4145.html',
        'http://industry.cfi.cn/BCA0A4127A4128A5063.html',
    )

    WEB_NEWS_FINANCE_CFI = u"中财网"
    TYPE1_NEWS_FINANCE_CFI_INDUSTRY = u"产经"
    ROOT_CFI_INDUSTRY = "http://industry.cfi.cn/"
    FLAG_INTERRUPT = True
    SELECT_NEWS_FINANCE_BY_TITLE = "SELECT * FROM news_finance WHERE type1='%s' AND type2='%s' AND title='%s'"

    lock = threading.RLock()
    conn=MySQLdb.connect(user='root', passwd='123123', db='news', autocommit=True)
    cursor = conn.cursor()

    def is_news_not_saved(self, type1, type2, title):
        if self.FLAG_INTERRUPT:
            self.lock.acquire()
            rows = self.cursor.execute(self.SELECT_NEWS_FINANCE_BY_TITLE % (type1, type2, title))
            if rows > 0:
                log.msg(self.WEB_NEWS_FINANCE_CFI + "::" + type1 + "::" + type2 + " saved all finished !", level=log.INFO)
                return False
            else:
                return True
            self.lock.release()
        else:
            return True

    def parse_news_finance_cfi(self, response):
        url = response.url
        items = []
        item = SpiderNewsFinanceItem()
        type2 = response.meta['type2']
        title = day = _time = article = tags = ""
        try:
            response = response.body.decode('utf-8')
            soup = BeautifulSoup(response)
            title = soup.find("h1").text.strip()
            day = soup.find(width="40%").text.split("&nbsp")[0].split(u"：")[1].split(" ")[0]
            time = soup.find(width="40%").text.split("&nbsp")[0].split(u"：")[1].split(" ")[1]
            article = soup.find(id="tdcontent").text.replace("\u3000", " ")
        except:
            log.msg("News " + url + " parse ERROR !!! ", level=log.ERROR)
            return
        item['web'] = self.WEB_NEWS_FINANCE_CFI
        item['type1'] = self.TYPE1_NEWS_FINANCE_CFI_INDUSTRY
        item['type2'] = type2
        item['title'] = title
        item['day'] = day
        item['time'] = time
        item['article'] = article
        item['tags'] = tags
        return item

    def get_type2_from_url(self, url):
        type2 = url.split("/")[3]
        if not type2.endswith("html"):
            type2 = "BC" + url.split("/")[3].split("=")[1].split("&")[0] + ".html"
        if type2 == 'BCA0A4127A4128A4132.html':
            return u'经济'
        elif type2 == 'BCA0A4127A4128A4138.html':
            return u'地产'
        elif type2 == 'BCA0A4127A4128A4133.html':
            return u'经济评论'
        elif type2 == 'BCA0A4127A4128A4134.html':
            return u'经济数据'
        elif type2 == 'BCA0A4127A4128A4135.html':
            return u'金融'
        elif type2 == 'BCA0A4127A4128A4136.html':
            return u'证券'
        elif type2 == 'BCA0A4127A4128A4137.html':
            return u'贸易'
        elif type2 == 'BCA0A4127A4128A4139.html':
            return u'能源'
        elif type2 == 'BCA0A4127A4128A4140.html':
            return u'原材料'
        elif type2 == 'BCA0A4127A4128A4141.html':
            return u'工业'
        elif type2 == 'BCA0A4127A4128A4142.html':
            return u'消费'
        elif type2 == 'BCA0A4127A4128A4143.html':
            return u'IT'
        elif type2 == 'BCA0A4127A4128A4144.html':
            return u'行业聚焦'
        elif type2 == 'BCA0A4127A4128A4145.html':
            return u'国际'
        elif type2 == 'BCA0A4127A4128A5063.html':
            return u'沪港'
        else:
            return u'无'

    def parse(self, response):
        url = response.url
        type2 = self.get_type2_from_url(url)
        items = []
        try:
            response = response.body.decode("utf-8")
            soup = BeautifulSoup(response)
            links = soup.find(class_="zidiv2").find_all("a")
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 2:
            r = range(0, len(links)-2)
            if not url.endswith("html"):
                r = range(0, len(links)-3)
            for i in r:
                link = self.ROOT_CFI_INDUSTRY + links[i]["href"]
                title = links[i].text.strip()
                need_parse_next_page = self.is_news_not_saved(self.TYPE1_NEWS_FINANCE_CFI_INDUSTRY, type2, title)
                if not need_parse_next_page:
                    break
                items.append(self.make_requests_from_url(link).replace(callback=self.parse_news_finance_cfi, meta={'type2': type2}))
            page_next = self.ROOT_CFI_INDUSTRY + links[len(links)-1]["href"]
            if need_parse_next_page:
                if not page_next.endswith("curid="):
                    items.append(self.make_requests_from_url(page_next))
            return items
