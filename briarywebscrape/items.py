# -*- coding: utf-8 -*-

# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class BriarywebscrapeItem(scrapy.Item):
    # Note, this field is required to format absolute URL for each individual item processed by pipelines.py
    image_hostname = scrapy.Field()
    product_dimension = scrapy.Field()
    wine_vintage = scrapy.Field()
    product_price = scrapy.Field()
    store_domain = scrapy.Field()
    product_image_url = scrapy.Field()
    product_title = scrapy.Field()
    product_producer = scrapy.Field()
    wine_type = scrapy.Field()
    wine_varietal = scrapy.Field()
    wine_country = scrapy.Field()
    wine_region = scrapy.Field()
    wine_sub_region = scrapy.Field()
    

# constraint
# product_dimension, wine_vintage, store_domain, product_title, wine_type, wine_varietal, wine_country

# wine product
# product_title, product_producer, wine_type, wine_varietal, wine_country, wine_region, wine_sub_region 

# Replace Null with empty string
# Constraint: product_dimension, wine_vintage, store_domain, product_title, wine_type
# Not constraint: product_image_url, product_producer, wine_region, wine_sub_region 

# DropItem if Null
# product_price, wine_varietal, wine_country