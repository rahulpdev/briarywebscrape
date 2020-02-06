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
