# Note, any changes to the Sql database connection must 
# be conformed in project_runner.py and pipeline.py

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import sql_queries
from briarywebscrape.spiders.longswines import LongswinesSpider
from briarywebscrape.spiders.brwine import BrwineSpider


# Run spiders and pipelines
process = CrawlerProcess(settings=get_project_settings())

process.crawl(LongswinesSpider)
# process.crawl(BrwineSpider)

process.start()


# Sync inventory foreign key with product primary key
sync_items_products = sql_queries.SqlQueries()
sync_items_products.open_sql_connection()
sync_items_products.insert_product_update_inventory()
sync_items_products.insert_item_update_product()
sync_items_products.close_sql_connection()
