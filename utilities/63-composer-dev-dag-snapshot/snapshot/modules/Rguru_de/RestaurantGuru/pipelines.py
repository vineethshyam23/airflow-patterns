# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime
from furl import furl
import psycopg2
import json
from itemadapter import ItemAdapter
import scrapy

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class RestaurantguruPipeline:
    def process_item(self, item, spider):
        return item

class TimestampPipeline:

    def process_item(self, item, spider):
        """Processes every item."""
        item["_spider"] = spider.name
        item["_timestamp"] = datetime.now().timestamp()
        if "_from_url" in item and item["_from_url"].__class__ == str:
            item["_from_url"] = furl(item["_from_url"]).tostr()
        return item

class PostgresPipeline:
    timezone = 'Europe/Berlin'
    connection = None
    pguser='REDACTED' 
    pgpassword='REDACTED' 
    pghost='REDACTED' 
    pgport='' 
    pgdatabase=''

    pgtable_name = ''
    pgspiderfield_name = 'spider'
    pgjsonfield_name = ''

    def __init__(self, pghost, pguser, pgpassword, pgdatabase, pgport, pgtable_name, pgspiderfield_name, pgjsonfield_name):
        self.pghost = pghost
        self.pguser = pguser
        self.pgpassword = pgpassword
        self.pgdatabase = pgdatabase
        self.pgport = pgport
        self.pgtable_name = pgtable_name
        self.pgspiderfield_name = pgspiderfield_name
        self.pgjsonfield_name = pgjsonfield_name

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            pghost=crawler.settings.get('POSTGRES_HOST'),
            pguser=crawler.settings.get('POSTGRES_USER'),
            pgpassword=crawler.settings.get('POSTGRES_PASSWORD'),
            pgdatabase=crawler.settings.get('POSTGRES_DATABASE'),
            pgport=crawler.settings.get('POSTGRES_PORT'),
            pgtable_name=crawler.settings.get('POSTGRES_TABLE_NAME'),
            pgspiderfield_name=crawler.settings.get('POSTGRES_SPIDERFIELD_NAME'),
            pgjsonfield_name=crawler.settings.get('POSTGRES_JSONFIELD_NAME', 'items')
        )
    
    def open_spider(self, spider):
        self.connection = psycopg2.connect(user=self.pguser,
                password=self.pgpassword,
                host=self.pghost,
                port=self.pgport,
                database=self.pgdatabase)
        query = "set timezone='"+self.timezone+"';"
        with self.connection.cursor() as curs:
            curs.execute(query)

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        # Prüfung, ob der Datensatz schon in der Tabelle vorhanden ist erfolgt durch den unique index auf spider, md5(json)
        #
        if hasattr(spider, 'country'):
            spider_name = f"{spider.name}_{spider.country}"
        else:
            spider_name = f"{spider.name}"
        # iquery = "insert into "+self.pgtable_name+" ("+self.pgspiderfield_name+", "+self.pgjsonfield_name+") values ('"+spider.name+"', '"+json.dumps(ItemAdapter(item).asdict()).replace("'", "''")+"')"
        iquery = "insert into "+self.pgtable_name+" ("+self.pgspiderfield_name+", "+self.pgjsonfield_name+") values ('"+spider_name+"', '"+json.dumps(ItemAdapter(item).asdict()).replace("'", "''")+"')"
        iquery += " ON CONFLICT DO NOTHING"
        try:
            with self.connection.cursor() as curs:
                curs.execute(iquery)
                self.connection.commit()
        except Exception as e:
            raise scrapy.exceptions.DropItem('Error in pipeline.py: ' + str(e))
        return item