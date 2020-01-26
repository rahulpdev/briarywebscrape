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
update_inventory_product_id = sql_queries.SqlQueries()
update_inventory_product_id.open_sql_connection(
    "localhost","root","Tutti792!@#$","briary"
    )
update_inventory_product_id.sync_tables()
update_inventory_product_id.close_sql_connection()
