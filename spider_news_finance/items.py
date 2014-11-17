# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SpiderNewsFinanceItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    web = scrapy.Field()
    type1 = scrapy.Field()
    type2 = scrapy.Field()
    day = scrapy.Field()
    time = scrapy.Field()
    title = scrapy.Field()
    tags = scrapy.Field()
    article = scrapy.Field()
    # pass
