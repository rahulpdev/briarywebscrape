# -*- coding: utf-8 -*-

# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item[pipeline.html
import re
from unidecode import unidecode
import mysql.connector
import sql_queries
from scrapy.exceptions import DropItem
from spellchecker import SpellChecker


class BriarywebscrapePipeline(object):
    product_image_src_separator = "src="
    product_image_thumbnail_separator = "&w="
    currency_regex = r"\$"
    punctuation_regex = r"\(|\)|-"
    whitespace_regex = r"\s+"
    wine_regex = r" (?i)wine\b"
    usa_regex = r"\b(?i)usa*|\b(?i)u.s.a*"
    ny_regex = r"\b(?i)ny|\b(?i)n.y."
    wine_vintage_regex = r"20\d{2}|19\d{2}"
    wine_dimension_regex = r"\d{3}(?i)ml|\d%s\.%s\d{1,3}(?i)l"
    wine_varietal_regex = r"\\|\.| ,|/"
    wine_varietal_dict = {
        'Grenache':['Garnacha','Grenache Blanc','Garnacha Blanc'],
        'Bordeaux':['Bordeaux Blanc'],
        'Pinot Grigio':['Pinot Gris'],
        'Syrah':['Shiraz'],
        'Moscato':['Muscat']
        }
    wine_country_region_dict = {
        'Italy' : [], 'France' : ['Champagne', 'Bordeaux', 'Burgundy'], 'Spain' : [], 'United States' : ['Amsterdam','California', 'Long Island', 'Napa', 'Napa Valley', 'New York', 'NY', 'Oregon', 'Paso Robles', 'Santa Barbara', 'Sonoma', 'Texas', 'Washington', 'Willamette Valley'], 'China' : [], 'Argentina' : ['Mendoza'], 'Chile' : [], 'Australia' : [], 'South Africa' : [], 'Germany' : [], 'Portugal' : [], 'Romania' : [], 'Greece' : [], 'Russia' : [], 'New Zealand' : [], 'Brazil' : [], 'Hungary' : [], 'Austria' : [], 'Serbia' : [], 'Moldova' : [], 'Bulgaria' : [], 'Georgia' : [], 'Switzerland' : [], 'Ukraine' : [], 'Japan' : [], 'Peru' : [], 'Uruguay' : [], 'Canada' : [], 'Algeria' : [], 'Czech Republic' : [], 'North Macedonia' : [], 'Croatia' : [], 'Turkey' : [], 'Mexico' : [], 'Turkmenistan' : [], 'Morocco' : [], 'Uzbekistan' : [], 'Slovakia' : [], 'Belarus' : [], 'Kazakhstan' : [], 'Tunisia' : [], 'Albania' : [], 'Montenegro' : [], 'Lebanon' : [], 'Slovenia' : [], 'Colombia' : [], 'Luxembourg' : [], 'Cuba' : [], 'Estonia' : [], 'Cyprus' : [], 'Azerbaijan' : [], 'Bolivia' : [], 'Madagascar' : [], 'Bosnia and Herzegovina' : [], 'Armenia' : [], 'Lithuania' : [], 'Egypt' : [], 'Israel' : [], 'Belgium' : [], 'Latvia' : [], 'Malta' : [], 'Zimbabwe' : [], 'Kyrgyzstan' : [], 'Paraguay' : [], 'Ethiopia' : [], 'Jordan' : [], 'United Kingdom' : [], 'Panama' : [], 'Tajikistan' : [], 'Liechtenstein' : [], 'Syria' : [], 'Poland' : [], 'Reunion' : []
        }
    champagne_regex = r"\b(?i)champagne"

    def __init__(self):
        self.spell = SpellChecker(language=None, case_sensitive=False)
        self.spell.word_frequency.load_text_file('./varietal_dictionary.txt')

    def spell_check(self,phrase,word_separator):
        word_split = phrase.split(word_separator)
        if "," in word_separator:
            word_split = sorted(word_split)
        new_phrase = []
        # Spell check each word in phrase and standardise each wine varietal in blend
        for i in word_split:
            i = self.spell.correction(i)
            i = i.title()
            for key in self.wine_varietal_dict:
                if i in self.wine_varietal_dict[key]:
                    i = key
            new_phrase.append(i)
        new_phrase = word_separator.join(new_phrase)
        # Standardise each wine varietal
        for key in self.wine_varietal_dict:
            if new_phrase in self.wine_varietal_dict[key]:
                new_phrase = key

        return new_phrase

    # Note item is a dictionary object from the items module with each scraped attribute a key
    def process_item(self, item, spider):
        # Convert to integer, or replace non numeric and Null with 0 for unique record in Sql database 
        try:
            item['wine_vintage'] = int(item['wine_vintage'])
        except ValueError:
            item['wine_vintage'] = 0
        except TypeError:
            item['wine_vintage'] = 0
        # Remove currency symbol and convert to float, or replace Null with 0.0 for unique record in Sql database
        try:
            item['product_price'] = float(re.sub(self.currency_regex,"", item['product_price']))
        except ValueError:
            item['product_price'] = 0.0
        except TypeError:
            item['product_price'] = 0.0
        # Strip whitespace and construct product image absolute url
        if item['product_image_url'] is not None:
            item['product_image_url'] = item['product_image_url'][0:item['product_image_url'].find(item['image_hostname']) + len(item['image_hostname'])] + item['product_image_url'][item['product_image_url'].find(self.product_image_src_separator) + len(self.product_image_src_separator):item['product_image_url'].find(self.product_image_thumbnail_separator)]
            item['product_image_url'] = item['product_image_url'].strip()
        # Remove accents, standardise United States and New York nomenclature, remove phrase wine, eliminate whitespace and apply title case
        for i in ('product_dimension','product_title','wine_type','wine_varietal','wine_country','wine_region','wine_sub_region'):
            if item[i] is not None:
                item[i] = unidecode(item[i])
                item[i] = re.sub(self.usa_regex,"United States", item[i])
                item[i] = re.sub(self.ny_regex,"New York", item[i])
                item[i] = re.sub(self.wine_regex," ", item[i])
                item[i] = re.sub(self.punctuation_regex," ",item[i])
                item[i] = re.sub(self.whitespace_regex," ",item[i])
                item[i] = item[i].strip().title()
        # Remove vintage and dimension from product title
        if item['product_title'] is not None:
            item['product_title'] = re.sub(self.wine_vintage_regex,"", item['product_title'])
            item['product_title'] = re.sub(self.wine_dimension_regex,"", item['product_title'])
        # Standardise wine varietal names
        if item['wine_varietal'] is not None:
            item['wine_varietal'] = re.sub(self.wine_varietal_regex,", ", item['wine_varietal'])
            item['wine_varietal'] = re.sub(self.whitespace_regex," ", item['wine_varietal'])
            item['wine_varietal'] = item['wine_varietal'].strip()
            if "," in item['wine_varietal']:            
                item['wine_varietal'] = self.spell_check(item['wine_varietal'],", ")
            else:
                item['wine_varietal'] = self.spell_check(item['wine_varietal']," ")
        # Fix incorrectly categorised wine country and wine region
        if item['wine_country'] is not None:
            if item['wine_country'] not in self.wine_country_region_dict.keys():
                item['wine_sub_region'] = item['wine_region']
                item['wine_region'] = item['wine_country']
                item['wine_country'] = None
        # Assign Champagne wine varietal to wine region if null or empty
        if item['wine_region'] is None or item['wine_region'] is '':
            if re.search(self.champagne_regex,item['wine_varietal']) is not None:
                item['wine_region'] = item['wine_varietal']
        # Reverse lookup wine country from country-region dictionary if null or empty
        if item['wine_country'] is None or item['wine_country'] is '':
            for key in self.wine_country_region_dict:
                if item['wine_region'] in self.wine_country_region_dict[key]:
                    item['wine_country'] = key
        # Replace Null with "" for unique record in Sql database
        for i in ('product_dimension','store_domain','product_title','product_producer','wine_type','wine_varietal','wine_country','wine_region','wine_sub_region'):
            if item[i] is None:
                item[i] = ""

        return item


# Drop all non wine products
class FilterNonWinesPipeline(object):
    wine_type_regex = r"\b(?i)red|\b(?i)rose|\b(?i)white|\b(?i)sparkling"
    # Note non_wine list needs to be regularly refreshed!
    non_wine_regex = r"\b(?i)sangria|\b(?i)cider|\b(?i)madeira|\b(?i)madiera|\b(?i)wother|\b(?i)other|\b(?i)sherry|\b(?i)port|\b(?i)honey"

    def process_item(self, item, spider):
        # Drop item if zero price
        if item['product_price'] is 0.0:
            raise DropItem("This item does not have a product price value")
        # Drop item if empty wine country
        elif item['wine_country'] is "":
            raise DropItem("This item does not have a wine country value")
        # Drop item if Blend and empty wine region
        elif 'Blend' in item['wine_varietal'] and item['wine_region'] is None:
            raise DropItem("This item is a Blend and does not have a wine region value")
        # Drop item if empty wine varietal
        elif item['wine_varietal'] is "":
            raise DropItem("This item does not have a wine varietal value")
        # Drop item if not wine product
        elif re.search(self.non_wine_regex,item['wine_varietal']) is not None or re.search(self.wine_type_regex,item['wine_varietal']) is not None:
            raise DropItem("This item is not wine")
        # Drop item if empty wine type
        elif item['wine_type'] is "":
            raise DropItem("This item does not have a wine varietal value")
        # Drop item if not wine type
        elif re.search(self.wine_type_regex,item['wine_type']) is None:
            raise DropItem("This item is not wine")
        else:
            return item


class MysqlInventoryPipeline(object):

    def __init__(self):
        self.create_product_inventory_tables = sql_queries.SqlQueries()


    def open_spider(self, spider):
        self.create_product_inventory_tables.open_sql_connection(
            "localhost","root","Tutti792!@#$","briary"
            )
        self.create_product_inventory_tables.create_tables()


    def close_spider(self, spider):
        self.create_product_inventory_tables.close_sql_connection()


    def process_item(self, item, spider):
        self.create_product_inventory_tables.update_tables(
            item['product_dimension'],
            item['wine_vintage'],
            item['product_price'],
            item['store_domain'],
            item['product_image_url'],
            item['product_title'],
            item['product_producer'],
            item['wine_type'],
            item['wine_varietal'],
            item['wine_country'],
            item['wine_region'],
            item['wine_sub_region']
        )

        return item


class MysqlInventoryPipeline2(object):

    def open_spider(self, spider):
        self.db_connection = mysql.connector.connect(
            host="localhost", user="root", passwd="Tutti792!@#$", database="briary"
        )
        self.my_cursor = self.db_connection.cursor()
        try:
            self.my_cursor.execute('''
                CREATE TABLE products2 (
                    id INT NOT NULL AUTO_INCREMENT,
                    product_producer VARCHAR(100),
                    product_title VARCHAR(100) NOT NULL,
                    wine_country VARCHAR(100),
                    wine_region VARCHAR(100),
                    wine_sub_region VARCHAR(100),
                    wine_type VARCHAR(100) NOT NULL,
                    wine_varietal VARCHAR(100),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT wine_product UNIQUE (product_title, wine_country, wine_type, wine_varietal),
                    PRIMARY KEY (id)
                )
            ''')
        except mysql.connector.ProgrammingError:
            pass
        else:
            self.db_connection.commit()
        try:
            self.my_cursor.execute('''
                CREATE TABLE inventory2 (
                    product_dimension VARCHAR(100) NOT NULL,
                    product_id INT,
                    product_image_url VARCHAR(200),
                    product_price DECIMAL(10, 2) NOT NULL,
                    product_producer VARCHAR(100),
                    product_title VARCHAR(100) NOT NULL,
                    store_domain VARCHAR(100) NOT NULL,
                    wine_country VARCHAR(100),
                    wine_region VARCHAR(100),
                    wine_sub_region VARCHAR(100),
                    wine_type VARCHAR(100) NOT NULL,
                    wine_varietal VARCHAR(100),
                    wine_vintage INT(4),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT wine_inventory UNIQUE (product_dimension, product_title, store_domain, wine_country, wine_type, wine_varietal, wine_vintage),
                    FOREIGN KEY (product_id) REFERENCES products2(id)
                )
            ''')
        except mysql.connector.ProgrammingError:
            pass
        else:
            self.db_connection.commit()


    def close_spider(self, spider):
        self.db_connection.close()

    def process_item(self, item, spider):
        self.my_cursor.execute('''
            INSERT INTO inventory2 (
                product_dimension,
                product_image_url,
                product_price,
                product_producer,
                product_title,
                store_domain,
                wine_country,
                wine_region,
                wine_sub_region,
                wine_type,
                wine_varietal,
                wine_vintage
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            product_price = VALUES (product_price),
            last_updated = NOW()            
        ''', (
            item['product_dimension'],
            item['product_image_url'],
            item['product_price'],
            item['product_producer'],
            item['product_title'],
            item['store_domain'],
            item['wine_country'],
            item['wine_region'],
            item['wine_sub_region'],
            item['wine_type'],
            item['wine_varietal'],
            item['wine_vintage']
        ))
        self.db_connection.commit()
        return item


# Drop duplicate products that vary only by dimension and vintage
class FilterDuplicatesPipeline(object):

    def __init__(self):
        self.unique_products = set()

    def process_item(self, item, spider):
        wine_product = (item['product_title'], item['wine_country'], item['wine_region'], item['wine_sub_region'], item['wine_type'],item['wine_varietal'])
        if wine_product in self.unique_products:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.unique_products.add(wine_product)
            return item


class MysqlProductPipeline2(object):

    def open_spider(self, spider):
        self.db_connection = mysql.connector.connect(
            host="localhost", user="root", passwd="Tutti792!@#$", database="briary"
        )
        self.my_cursor = self.db_connection.cursor()
        try:
            self.my_cursor.execute('''
                CREATE TABLE products2 (
                    id INT NOT NULL AUTO_INCREMENT,
                    product_producer VARCHAR(100),
                    product_title VARCHAR(100) NOT NULL,
                    wine_country VARCHAR(100),
                    wine_region VARCHAR(100),
                    wine_sub_region VARCHAR(100),
                    wine_type VARCHAR(100) NOT NULL,
                    wine_varietal VARCHAR(100),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT wine_product UNIQUE (product_title, wine_country, wine_type, wine_varietal),
                    PRIMARY KEY (id)
                )
            ''')
        except mysql.connector.ProgrammingError:
            pass
        else:
            self.db_connection.commit()


    def close_spider(self, spider):
        self.db_connection.close()
    
    # Note an alternative is to use INSERT IGNORE Sql query
    def process_item(self, item, spider):
        try:
            self.my_cursor.execute('''
                INSERT INTO products2 (
                    product_producer,
                    product_title,
                    wine_country,
                    wine_region,
                    wine_sub_region,
                    wine_type,
                    wine_varietal
                ) VALUES(%s,%s,%s,%s,%s,%s,%s)
            ''', (
                item['product_producer'],
                item['product_title'],
                item['wine_country'],
                item['wine_region'],
                item['wine_sub_region'],
                item['wine_type'],
                item['wine_varietal']
            ))
        except mysql.connector.errors.IntegrityError:
            pass
        else:
            self.db_connection.commit()
        return item
