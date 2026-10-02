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
        try:
            self.connection = psycopg2.connect(user=self.pguser,
                    password=self.pgpassword,
                    host=self.pghost,
                    port=self.pgport,
                    database=self.pgdatabase)
            query = "set timezone='"+self.timezone+"';"
            with self.connection.cursor() as curs:
                curs.execute(query)
            spider.logger.info(f"PostgresPipeline connected to database at {self.pghost}")
        except Exception as e:
            spider.logger.error(f"Failed to connect to database: {str(e)}")
            self.connection = None

    def close_spider(self, spider):
        if self.connection:
            try:
                self.connection.close()
                spider.logger.info("PostgresPipeline database connection closed")
            except Exception as e:
                spider.logger.error(f"Error closing database connection: {str(e)}")
        else:
            spider.logger.info("PostgresPipeline: No connection to close")

    def process_item(self, item, spider):
        # Handle different table structures based on table name
        if 'restaurant_guru_raw_germany' in self.pgtable_name:
            # Special handling for restaurant_guru_raw_germany table
            return self._process_restaurant_guru_item(item, spider)
        else:
            # Original logic for other tables
            return self._process_original_item(item, spider)
    
    def _process_restaurant_guru_item(self, item, spider):
        """Process item for restaurant_guru_raw_germany table structure."""
        if not self.connection:
            spider.logger.warning("No database connection - skipping item save")
            return item
            
        if hasattr(spider, 'country'):
            spider_name = f"{spider.name}_{spider.country}"
        else:
            spider_name = f"{spider.name}"
        
        # ðŸ”§ Generate SIMPLE sequential job_id (6-7 digits)
        try:
            with self.connection.cursor() as curs:
                # Get next sequential job_id from database
                curs.execute("""
                    SELECT COALESCE(MAX(job_id), 100000) + 1 as next_job_id
                    FROM smartdata_analyticdb.restaurant_guru_raw_germany
                    WHERE job_id BETWEEN 100000 AND 9999999  -- 6-7 digit range
                """)
                result = curs.fetchone()
                job_id = result[0] if result else 100001  # Start from 100001 if no data
                
                spider.logger.info(f"ðŸ”§ Generated sequential job_id: {job_id} for restaurant: {item.get('title', 'unknown')}")
                
        except Exception as e:
            # Fallback: use simple timestamp-based ID
            import time
            job_id = int(str(int(time.time()))[-6:])  # Last 6 digits of timestamp
            spider.logger.warning(f"ðŸ”§ Using fallback job_id: {job_id} due to DB error: {e}")
        
        # Extract city_name from item - use current_city (processing city) instead of city (address city)
        city_name = item.get('current_city', item.get('city', 'unknown_city'))
        if isinstance(city_name, list):
            city_name = city_name[0] if city_name else 'unknown_city'
        
        # Extract restaurant URL (unique identifier for each restaurant)
        restaurant_url = item.get('_from_url', item.get('url', ''))
        if restaurant_url:
            # Clean up proxy URLs to get actual Restaurant Guru URL
            import urllib.parse
            if 'scrapeops.io' in restaurant_url:
                parsed_url = urllib.parse.parse_qs(urllib.parse.urlparse(restaurant_url).query)
                restaurant_url = parsed_url.get('url', [''])[0]
                restaurant_url = urllib.parse.unquote(restaurant_url)
        
        # Extract restaurant slug from URL for backup identification
        restaurant_slug = ''
        if restaurant_url and 'restaurantguru.com/' in restaurant_url:
            restaurant_slug = restaurant_url.split('restaurantguru.com/')[-1].split('?')[0]
        
        # Fallback unique identifier if no URL available
        if not restaurant_url:
            title = item.get('title', 'unknown')
            phone = item.get('phone', 'unknown')
            restaurant_url = f"unknown-{job_id}-{title}-{phone}"  # ðŸ”§ Use job_id
            restaurant_slug = f"unknown-{title}-{phone}"
            spider.logger.warning(f"No URL found for restaurant, using fallback: {restaurant_url}")
        
        # Sanitize for SQL
        city_name = str(city_name).replace("'", "''")
        restaurant_url = str(restaurant_url).replace("'", "''")
        restaurant_slug = str(restaurant_slug).replace("'", "''")
        
        # Count items (assuming each item represents one restaurant)
        items_count = 1
        
        # Prepare JSON data
        json_data = json.dumps(ItemAdapter(item).asdict()).replace("'", "''")
        
        # ðŸ”§ Create INSERT query with job_id instead of execution_id
        iquery = f"""
            INSERT INTO {self.pgtable_name} 
            (job_id, city_name, spider_name, raw_json, items_count, restaurant_url, restaurant_slug, created_at)
            VALUES ({job_id}, '{city_name}', '{spider_name}', '{json_data}', {items_count}, '{restaurant_url}', '{restaurant_slug}', NOW())
            ON CONFLICT (job_id, restaurant_url) DO UPDATE SET 
                raw_json = EXCLUDED.raw_json,
                items_count = EXCLUDED.items_count,
                city_name = EXCLUDED.city_name,
                restaurant_slug = EXCLUDED.restaurant_slug,
                created_at = CURRENT_TIMESTAMP
        """
        
        try:
            with self.connection.cursor() as curs:
                curs.execute(iquery)
                self.connection.commit()
                spider.logger.info(f"ðŸ”§ Saved restaurant '{restaurant_slug}' for city: {city_name} (job_id: {job_id})")
        except Exception as e:
            spider.logger.error(f'Error saving restaurant {restaurant_slug} to raw table: {str(e)}')
            spider.logger.error(f'Restaurant URL: {restaurant_url}')
            # Don't drop item on database error - continue processing
        return item
    
    def _process_original_item(self, item, spider):
        """Original processing logic for legacy tables."""
        if not self.connection:
            spider.logger.warning("No database connection - skipping item save")
            return item
            
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
            spider.logger.error(f'Error in pipeline.py: {str(e)}')
            # Don't drop item on database error - continue processing
        return item