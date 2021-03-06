# -*- coding: utf-8 -*-

# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item[pipeline.html
import re
from unidecode import unidecode
import mysql.connector
from scrapy.exceptions import DropItem
from spellchecker import SpellChecker
import sql_queries


class ReadMysqlPipeline(object):
    read_table = sql_queries.SqlQueries()

    def __init__(self):
        ReadMysqlPipeline.read_table.open_sql_connection()
        # self.db_varietals = read_db_tables.read_table('varietal')
        self.db_varietals = ReadMysqlPipeline.read_table.read_varietal()
        self.wine_country_dict = {}
        # db_wine_countries = read_db_tables.read_table('terroir')
        self.db_wine_countries = ReadMysqlPipeline.read_table.read_terroir()
        for i in self.db_wine_countries:
            if i[2] is not None:
                if i[1] not in self.wine_country_dict.keys():
                    self.wine_country_dict[i[1]] = [i[2]]
                else:
                    self.wine_country_dict[i[1]].append(i[2])
        del self.db_wine_countries
        ReadMysqlPipeline.read_table.close_sql_connection()


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
    regex = {
        'currency' : r"\$",
        'punctuation' : r"\(|\)|-",
        'whitespace' : r"\s+",
        'wine' : r" (?i)wine\b",
        'usa' : r"\b(?i)usa*|\b(?i)u.s.a*",
        'ny' : r"\b(?i)ny|\b(?i)n.y.",
        'wine_vintage' : r"20\d{2}|19\d{2}",
        'wine_dimension' : r"\d{3}(?i)ml|\d%s\.%s\d{1,3}(?i)l",
        'wine_varietal' : r"\\|\.| ,|/",
        'champagne' : r"\b(?i)champagne"
    }
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


    def spell_check(self,phrase,phrase_separator):
        phrase_list = phrase.split(phrase_separator)
        new_phrase_list = []
        if "," in phrase_separator:
            phrase_list = sorted(phrase_list)
        # Spell check each word in phrase and standardise each wine varietal in blend
        for i in phrase_list:
            i = self.spell.correction(i)
            i = i.title()
            for key in self.wine_varietal_dict:
                if i in self.wine_varietal_dict[key]:
                    i = key
            # for varietal in self.db_varietals:
            #     if i == varietal[2]:
            #         i = varietal[1]
            new_phrase_list.append(i)
        new_phrase = phrase_separator.join(new_phrase_list)
        del new_phrase_list
        # Standardise each wine varietal
        for key in self.wine_varietal_dict:
            if new_phrase in self.wine_varietal_dict[key]:
                new_phrase = key
        # for varietal in self.db_varietals:
        #     if new_phrase == varietal[2]:
        #         new_phrase = varietal[1]

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
        self.item_product_inventory = sql_queries.SqlQueries()


    def open_spider(self, spider):
        self.item_product_inventory.open_sql_connection()
        self.item_product_inventory.create_tables()


    def close_spider(self, spider):
        self.item_product_inventory.insert_product_update_inventory()
        self.item_product_inventory.insert_item_update_product()
        self.item_product_inventory.close_sql_connection()


    def process_item(self, item, spider):
        self.item_product_inventory.insert_inventory(
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

