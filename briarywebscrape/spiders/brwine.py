# -*- coding: utf-8 -*-
import scrapy
import re
from ..items import BriarywebscrapeItem


class BrwineSpider(scrapy.Spider):
    name = 'brwine'
    allowed_domains = ['www.brwine.com']
    start_urls = ['https://www.brwine.com/wines/']
    # additional class attributes
    image_hostname = 'www.brwine.com'
    next_page_text = '>'
    producer_name_split = ' - '
    wine_year_regex = r'20\d{2}|19\d{2}'

    def parse(self, response):
        # Instantiate dictionary object from items module
        items = BriarywebscrapeItem()

        # Scrape all products on inventory page
        for product in response.xpath("//table[@class='wf_content prow']"):

            # Extract fields and assign to dictionary keys
            items['image_hostname'] = self.image_hostname
            items['store_domain'] = self.allowed_domains[0]
            items['product_price'] = product.xpath(".//td[@class='rpwrap']//span[@class='rd14']/b[last()]/text()").get()
            try:
                items['product_image_url'] = self.image_hostname + product.xpath(".//td[@class='resimg']//img/@src").get()
            except TypeError:
                items['product_image_url'] = None
            items['product_dimension'] = product.xpath(".//td[@class='srmid']//tr[1]/td/a/span/text()").get()
            items['product_title'] = product.xpath("normalize-space(.//td[@class='srmid']//tr[1]/td/a/text())").get()
            # Extract fields from product title
            items['product_producer'] = items['product_title'].split(self.producer_name_split)[0]
            try:
                items['wine_vintage'] = re.search(self.wine_year_regex, items['product_title']).group()
            except AttributeError:
                items['wine_vintage'] = None
            # Extract field from first product summary
            items['wine_type'] = product.xpath(".//td[@class='srmid']//tr[2]//tr[1]//td//td//td[1]/img/@title").get()
            # Extract fields from second product summary
            # product_summary = product.xpath("//td[@class='srmid']//tr[2]//tr[2]//td/child::node()/text()").getall()
            for attribute in product.xpath("//td[@class='srmid']//tr[2]//tr[2]//td/child::node()/text()").getall():
                split_attribute = attribute.split(": ")
                if split_attribute[0].lower() == "country":
                    items['wine_country'] = split_attribute[1]
                if split_attribute[0].lower() == "region":
                    items['wine_region'] = split_attribute[1]
                if split_attribute[0].lower() == "subregion":
                    items['wine_sub_region'] = split_attribute[1]
                if split_attribute[0].lower() == "varietal":
                    items['wine_varietal'] = split_attribute[1]

            yield items

        # Follow next page link on inventory page until link is exhausted
        if response.xpath("//div[@class='sorting']/ul/li[last()]/a/text()").get() == self.next_page_text:
            yield scrapy.Request(url=response.urljoin(response.xpath("//div[@class='sorting']/ul/li[last()]/a/@href").get()), callback=self.parse)
            