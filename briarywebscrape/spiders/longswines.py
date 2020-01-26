# -*- coding: utf-8 -*-
import scrapy
import re
from ..items import BriarywebscrapeItem


class LongswinesSpider(scrapy.Spider):
    name = 'longswines'
    allowed_domains = ['www.longswines.com']
    start_urls = ['https://www.longswines.com/wines/']
    # additional class attributes
    image_hostname = 'www.longswines.com'
    next_page_text = 'next >>'
    producer_name_split = ' - '
    wine_year_regex = r'20\d{2}|19\d{2}'

    def parse(self, response):
        # Instantiate dictionary object from items module
        items = BriarywebscrapeItem()

        # Scrape all products on inventory page
        for product in response.xpath("//div[@class='productlistitem']"):
            
            # Extract fields and assign to dictionary keys
            items['image_hostname'] = self.image_hostname
            items['store_domain'] = self.allowed_domains[0]
            items['product_price'] = product.xpath(".//div[@class='productprice']/h2[last()]/text()").get()
            try:
                items['product_image_url'] = self.image_hostname + product.xpath(".//div[@class='productimage']/a/img/@src").get()
            except TypeError:
                items['product_image_url'] = None            
            items['product_dimension'] = product.xpath(".//div[@class='producttitle']/h2/a/span/text()").get()
            items['product_title'] = product.xpath("normalize-space(.//div[@class='producttitle']/h2/a/text())").get()
            # Extract fields from product title
            items['product_producer'] = items['product_title'].split(self.producer_name_split)[0]
            try:
                items['wine_vintage'] = re.search(self.wine_year_regex, items['product_title']).group()
            except AttributeError:
                items['wine_vintage'] = None
            # Extract fields from product summary
            product_summary = product.xpath("normalize-space(.//div[@class='producttitle']/p/text())").get()
            items['wine_type'] = product_summary.split(',')[0]
            items['wine_varietal'] = product_summary.split(',')[1].split('|')[0].strip()
            if len(product_summary.split('|')) > 2:
                items['wine_country'] = product_summary.split('|')[1].strip()
            else:
                items['wine_country'] = None
            if len(product_summary.split('|')) > 3:
                items['wine_region'] = product_summary.split('|')[2].strip()
            else:
                items['wine_region'] = None
            if len(product_summary.split('|')) > 4:
                items['wine_sub_region'] = product_summary.split('|')[3].strip()
            else:
                items['wine_sub_region'] = None

            yield items

        # Follow next page link on inventory page until link is exhausted
        if response.xpath("(//div[@class='pagebox'])[1]/a[last()]/text()").get() == self.next_page_text:
            yield scrapy.Request(url=response.urljoin(response.xpath("(//div[@class='pagebox'])[1]/a[last()]/@href").get()), callback=self.parse)

        # # Scrape product page URLs on inventory page
        # for inventory_title in response.xpath("//h2/a"):
        #     product_page_url = inventory_title.xpath(".//@href").get()
        #     yield response.follow(url=product_page_url, callback=self.parse_product)


    # def parse_product(self, response):
        
    #     # Scrape product page
    #     items['product_price'] = None
    #     items['product_image_src'] = None
    #     items['image_hostname'] = None
    #     items['product_dimension'] = None
    #     items['product_title'] = None
    #     items['product_producer'] = None
    #     items['wine_vintage'] = None
    #     items['wine_type'] = None
    #     items['wine_varietal'] = None
    #     items['wine_country'] = None
    #     items['wine_region'] = None

    #     yield items