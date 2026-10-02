# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime
from furl import furl
import psycopg2
import json
from itemadapter.adapter import ItemAdapter
import scrapy
import logging
from scrapy.exceptions import DropItem

# useful for handling different item types with a single interface


class JusteatPipeline:
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
    connection_timeout = ''
    keepalive_idle = ''
    keepalive_interval = ''
    keepalive_count = ''

    def __init__(self, pghost, pguser, pgpassword, pgdatabase, pgport, pgtable_name, pgspiderfield_name, pgjsonfield_name, connection_timeout, keepalive_idle, keepalive_interval, keepalive_count):
        self.pghost = pghost
        self.pguser = pguser
        self.pgpassword = pgpassword
        self.pgdatabase = pgdatabase
        self.pgport = pgport
        self.pgtable_name = pgtable_name
        self.pgspiderfield_name = pgspiderfield_name
        self.pgjsonfield_name = pgjsonfield_name
        self.connection_timeout = connection_timeout
        self.keepalive_idle = keepalive_idle
        self.keepalive_interval = keepalive_interval
        self.keepalive_count = keepalive_count
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        # Get database connection settings
        pghost = crawler.settings.get('POSTGRES_HOST')
        pguser = crawler.settings.get('POSTGRES_USER')
        pgpassword = crawler.settings.get('POSTGRES_PASSWORD')
        pgdatabase = crawler.settings.get('POSTGRES_DATABASE')
        pgport = crawler.settings.get('POSTGRES_PORT')
        connection_timeout = crawler.settings.get('POSTGRES_CONNECTION_TIMEOUT')
        keepalive_idle = crawler.settings.get('POSTGRES_KEEPALIVE_IDLE')
        keepalive_interval = crawler.settings.get('POSTGRES_KEEPALIVE_INTERVAL')
        keepalive_count = crawler.settings.get('POSTGRES_KEEPALIVE_COUNT')
        
        # Validate that required environment variables are set
        if not all([pghost, pguser, pgpassword, pgdatabase]):
            raise ValueError("Database connection environment variables are not set. Please ensure POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DATABASE are set.")
        
        return cls(
            pghost=pghost,
            pguser=pguser,
            pgpassword=pgpassword,
            pgdatabase=pgdatabase,
            pgport=pgport,
            pgtable_name=crawler.settings.get('POSTGRES_TABLE_NAME'),
            pgspiderfield_name=crawler.settings.get('POSTGRES_SPIDERFIELD_NAME'),
            pgjsonfield_name=crawler.settings.get('POSTGRES_JSONFIELD_NAME', 'items'),
            connection_timeout=connection_timeout,
            keepalive_idle=keepalive_idle,
            keepalive_interval=keepalive_interval,
            keepalive_count=keepalive_count
        )
    
    def open_spider(self, spider):
        self.logger.info(f"🔌 Opening PostgreSQL connection to {self.pghost}:{self.pgport}/{self.pgdatabase}")
        try:
            # Add connection timeout and keepalive settings
            self.connection = psycopg2.connect(
                user=self.pguser,
                password=self.pgpassword,
                host=self.pghost,
                port=self.pgport,
                database=self.pgdatabase,
                connect_timeout=self.connection_timeout,
                keepalives_idle=self.keepalive_idle,
                keepalives_interval=self.keepalive_interval,
                keepalives_count=self.keepalive_count
            )
            query = "set timezone='"+self.timezone+"';"
            with self.connection.cursor() as curs:
                curs.execute(query)
            self.logger.info(f"✅ Successfully connected to PostgreSQL database")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            self.logger.error(f"❌ Connection details - Host: {self.pghost}, Port: {self.pgport}, Database: {self.pgdatabase}, User: {self.pguser}")
            raise

    def process_item(self, item, spider):
        #Checking whether the dataset already exists in the table is done through the unique index on spider, md5(json)
        self.logger.info(f"🔄 Processing item for spider: {spider.name}")
        
        item_dict = ItemAdapter(item).asdict()
        # self.logger.info(f"📋 Item keys: {list(item_dict.keys())}")
        # self.logger.info(f"📋 Item type: {type(item)}")
        # self.logger.info(f"📋 Item content preview: {str(item_dict)[:100]}...")
        
        # Check if connection is still alive
        if not self.connection or self.connection.closed:
            self.logger.error(f"❌ Database connection is closed or None")
            raise DropItem('Database connection is closed')
        
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        iquery = f"""
        INSERT INTO {self.pgtable_name} 
        (spider, jsondata, eingefuegtam, eingefuegtvon, geaendertam, geaendertvon, zipcode, latitude, longitude, rest_id, scraping_status) 
        VALUES 
        ('{spider.name}', '{json.dumps(item_dict).replace("'", "''")}', 
         '{current_timestamp}', '{spider.name}', '{current_timestamp}', '{spider.name}', '{item_dict['zipcode']}', '{item_dict['latitude']}', '{item_dict['longitude']}', '{item_dict['rest_id']}', '{scraping_status}')
        ON CONFLICT DO NOTHING
        """

        if item_dict['scraping_status'] == 'rest_discovery_extracted' and item_dict['scraping_status'] == 'menu_url_extracted': 
            scraping_status='Success'
        else:
            scraping_status='Failed'

        iquery = iquery + f"""
        Update {self.pgtable_name}
        SET scraping_status = '{scraping_status}'
        WHERE spider = '{spider.name}' and zipcode = '{item_dict['zipcode']}' and latitude = '{item_dict['latitude']}' and longitude = '{item_dict['longitude']}' and rest_id = '{item_dict['rest_id']}'
        """
        #self.logger.info(f"🔍 Inserting item into {self.pgtable_name} for {spider.name}")
        #self.logger.info(f"🔍 Executing query: {iquery[:100]}...")
        
        try:
            with self.connection.cursor() as curs:
                curs.execute(iquery)
                self.connection.commit()
                self.logger.info(f"✅ Successfully inserted item for {spider.name}")
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"❌ Error inserting item: {e}")
            self.logger.error(f"❌ Query: {iquery}")
            self.logger.error(f"❌ Item data: {item_dict}")
            raise DropItem('Error in pipeline.py: ' + str(e))
        return item 
    
    def close_spider(self, spider):
        if self.connection:
            self.connection.close()
            self.logger.info(f"🔌 Closed PostgreSQL connection")