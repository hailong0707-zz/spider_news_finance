# -*- coding: utf-8 -*-
import scrapy
from spider_news_finance.items import SpiderNewsFinanceItem
from bs4 import BeautifulSoup
from scrapy import log
import threading
import MySQLdb


class NewsfinancesinaSpider(scrapy.Spider):
    name = "NewsFinanceSina"
    allowed_domains = ["sina.com.cn"]
    start_urls = (
        'http://roll.finance.sina.com.cn/finance/cj4/cj_cyxw/index_1.shtml',
        'http://roll.finance.sina.com.cn/finance/cj4/cj_gsxw/index_1.shtml',
        'http://roll.finance.sina.com.cn/finance/cj4/sdbd/index_1.shtml',
        'http://roll.finance.sina.com.cn/finance/cj4/rsbd/index_1.shtml',
        'http://roll.finance.sina.com.cn/finance/pl1/cjgc/index_1.shtml',
        'http://roll.finance.sina.com.cn/finance/gl/sypl/index_1.shtml',
    )

    WEB_NEWS_FINANCE_SINA = u"新浪财经"
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
                log.msg(self.WEB_NEWS_FINANCE_SINA + "::" + type1 + "::" + type2 + " saved all finished !", level=log.INFO)
                return False
            else:
                return True
            self.lock.release()
        else:
            return True

    def parse_news_finance_sina(self, response):
        url = response.url
        items = []
        item = SpiderNewsFinanceItem()
        type1 = response.meta['type1']
        type2 = response.meta['type2']
        title = day = _time = article = tags = ""
        try:
            response = response.body.decode('GB18030')
            soup = BeautifulSoup(response)
            title = soup.find(id="artibodyTitle").text.strip()
            day_time = soup.find(id="pub_date").text.strip()
            # day = day_time.split(" ")[0].strip()
            # _time = day_time.split(" ")[1].strip()
            day = day_time[0:10]
            _time = day_time[11:len(day_time)]
            article_array = soup.find(id="artibody").find_all("p")
            for i in range(0, len(article_array)):
                str = article_array[i].text
                if not (str.startswith(u"分享到") or str.startswith(u"转发此文")):
                    article += article_array[i].text + "\n\r"
        except:
            log.msg("News " + url + " parse ERROR !!!", level=log.ERROR)
            return
        try:
            #maybe don't has tags
            soup_tags = soup.find(class_="art_keywords").find_all("a")
            for i in range(0,len(soup_tags)):
                tags += " " + soup_tags[i].text
            tags = tags.strip()
        except:
            log.msg("News don't has tags")
        item['web'] = self.WEB_NEWS_FINANCE_SINA
        item['type1'] = type1
        item['type2'] = type2
        item['title'] = title
        item['day'] = day
        item['time'] = _time
        item['article'] = article
        item['tags'] = tags
        return item

    def get_type1_from_url(self, url):
        type1 = url.split("/")[4]
        if type1 == 'cj4':
            return u'产经'
        elif type1 == 'pl1':
            return u'评论'
        elif type1 == 'gl':
            return u'管理'
        else:
            return u'无'

    def get_type2_from_url(self, url):
        type2 = url.split("/")[5]
        if type2 == 'cj_cyxw':
            return u'产业新闻'
        elif type2 == 'cj_gsxw':
            return u'公司新闻'
        elif type2 == 'sdbd':
            return u'深度报道'
        elif type2 == 'rsbd':
            return u'人事变动'
        elif type2 == 'cjgc':
            return u'产经观察'
        elif type2 == 'sypl':
            return u'商业评论'
        else:
            return u'无'

    def get_url_next_page(self, url_current, suf_next):
        return url_current.split('index')[0] + suf_next.split('/')[1]

    def parse(self, response):
        url = response.url
        type1 = self.get_type1_from_url(url)
        type2 = self.get_type2_from_url(url)
        items = []
        try:
            response = response.body.decode('GB18030')
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        soup = BeautifulSoup(response)
        links = soup.find_all("li")
        need_parse_next_page = True
        if(len(links) > 2):  
            for i in range(2, len(links)):
                link = links[i].a['href']
                title = links[i].a.text.strip()
                need_parse_next_page = self.is_news_not_saved(type1, type2, title)
                if not need_parse_next_page:
                    break
                items.append(self.make_requests_from_url(link).replace(callback=self.parse_news_finance_sina, meta={'type1': type1, 'type2': type2}))
            if need_parse_next_page:
                try:
                    suf_next_page = soup.find(class_='pagebox_next').a['href']
                    link_next_page = self.get_url_next_page(url, suf_next_page)
                    items.append(self.make_requests_from_url(link_next_page))
                except:
                    log.msg("Page " + url + " is the last page of " + type2, level=log.INFO)
            return items
