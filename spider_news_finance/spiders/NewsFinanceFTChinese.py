# -*- coding: utf-8 -*-
import scrapy
from bs4 import BeautifulSoup
from spider_news_finance.items import SpiderNewsFinanceItem
from scrapy import log
import threading
import MySQLdb

class NewsfinanceftchineseSpider(scrapy.Spider):
    name = "NewsFinanceFTChinese"
    allowed_domains = ["www.ftchinese.com"]
    start_urls = (
        "http://www.ftchinese.com/channel/chinareport.html?page=1",
        "http://www.ftchinese.com/channel/chinabusiness.html?page=1",
        "http://www.ftchinese.com/channel/chinamarkets.html?page=1",
        "http://www.ftchinese.com/channel/chinastock.html?page=1",
        "http://www.ftchinese.com/channel/chinaproperty.html?page=1",
        "http://www.ftchinese.com/channel/culture.html?page=1",
        "http://www.ftchinese.com/channel/chinaopinion.html?page=1",
    )

    URL_ROOT_FTCHINESE = "http://www.ftchinese.com"
    URL_ROOT_FTCHINESE_CHANNEL = "http://www.ftchinese.com/channel/"
    WEB_NEWS_FINANCE_FTCHINESE = u"FT中文网"
    TYPE1_NEWS_FINANCE_FTCHINESE_CHINA = u"中国"
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
                log.msg(self.WEB_NEWS_FINANCE_FTCHINESE + "::" + type1 + "::" + type2 + " saved all finished !", level=log.INFO)
                return False
            else:
                return True
            self.lock.release()
        else:
            return True

    def parse_news_finance_ftchinese(self, response):
        url = response.url
        items = []
        item = SpiderNewsFinanceItem()
        type2 = response.meta['type2']
        title = day = _time = article = tags = ""
        try:
            response = response.body.decode('utf-8')
            soup = BeautifulSoup(response)
            title = soup.find(id="topictitle").text.strip()
            day_time = soup.find(class_="storytime").text.strip().split(" ")
            day = day_time[0]
            _time = day_time[1] + " "+ day_time[2]
            article_array = soup.find(id="bodytext").find_all("p")
            for i in range(0, len(article_array)):
                article += article_array[i].text.strip() + "\n\r"
        except:
            log.msg("News " + url + " parse ERROR !!!", level=log.ERROR)
            return
        item['web'] = self.WEB_NEWS_FINANCE_FTCHINESE
        item['type1'] = self.TYPE1_NEWS_FINANCE_FTCHINESE_CHINA
        item['type2'] = type2
        item['title'] = title
        item['day'] = day
        item['time'] = _time
        item['article'] = article
        item['tags'] = tags
        return item
        
    def get_type2_from_url(self, url):
        type2 = url.split("?")[0].split("/")[4]
        if type2 == 'chinareport.html':
            return u'政经'
        elif type2 == 'chinabusiness.html':
            return u'商业'
        elif type2 == 'chinamarkets.html':
            return u'金融市场'
        elif type2 == 'chinastock.html':
            return u'股市'
        elif type2 == 'chinaproperty.html':
            return u'房地产'
        elif type2 == 'culture.html':
            return u'社会与文化'
        elif type2 == 'chinaopinion.html':
            return u'观点'
        else:
            return u'无'

    def parse(self, response):
        url = response.url
        type2 = self.get_type2_from_url(url)
        items = []
        try:
            response = response.body.decode('utf-8')
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        soup = BeautifulSoup(response)
        links = soup.find_all(class_="coverlink") + soup.find_all(class_="thl")
        if len(links) == 0:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " is NULL, try again.", level=log.ERROR)
            return items
        need_parse_next_page = True
        for i in range(0, len(links)):
            link = self.URL_ROOT_FTCHINESE +  links[i]["href"].split("#")[0] + "?full=y"
            title = links[i].text.strip()
            need_parse_next_page = self.is_news_not_saved(self.TYPE1_NEWS_FINANCE_FTCHINESE_CHINA, type2, title)
            if not need_parse_next_page:
                break
            items.append(self.make_requests_from_url(link).replace(callback=self.parse_news_finance_ftchinese, meta={'type2': type2}))
        if need_parse_next_page:
            pagination = soup.find_all(class_='pagination')[0].find_all("a")
            pagination = pagination[len(pagination) :: -1]
            for i in range(0, len(pagination)):
                if pagination[i].text == u'下一页':
                    link = self.URL_ROOT_FTCHINESE_CHANNEL + pagination[i]['href']
                    items.append(self.make_requests_from_url(link))
        return items