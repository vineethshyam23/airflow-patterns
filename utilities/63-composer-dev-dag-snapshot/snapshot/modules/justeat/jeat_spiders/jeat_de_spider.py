#print(" Loading jeat_nl.py module")

import logging
import traceback
import os
import scrapy
from scrapy.exceptions import CloseSpider
import json
import sys
import psycopg2
import time

from json.decoder import JSONDecodeError
from urllib.parse import urlencode
from scrapy.utils.project import get_project_settings
from datetime import datetime
from scrapy import signals

# Global error tracking lists
CRITICAL_ERRORS = []
NON_CRITICAL_ERRORS = []

def handle_error(error_message, is_critical=False):
    """
    Centralized error handling function with 2 levels: critical and non-critical
    
    Args:
        error_message (str): Main error message
        is_critical (bool): Whether this is a critical error that should stop execution
    
    Behavior:
        - Critical errors: Stop execution and return exit code 1 (failure)
        - Non-critical errors: Continue execution and return exit code 0 (success)
    """
    if is_critical:
        CRITICAL_ERRORS.append(error_message)
        logging.error(f"[CRITICAL ERROR] {error_message}")
    else:
        NON_CRITICAL_ERRORS.append(error_message)
        logging.warning(f"[NON-CRITICAL ERROR] {error_message}")
    
    return error_message

def get_error_summary():
    """
    Get a summary of all errors encountered during execution
    
    Returns:
        dict: Summary of critical and non-critical errors
    """
    return {
        'critical_errors': len(CRITICAL_ERRORS),
        'non_critical_errors': len(NON_CRITICAL_ERRORS),
        'critical_error_list': CRITICAL_ERRORS,
        'non_critical_error_list': NON_CRITICAL_ERRORS
    }

def clear_errors():
    """Clear all error tracking lists"""
    global CRITICAL_ERRORS, NON_CRITICAL_ERRORS
    CRITICAL_ERRORS.clear()
    NON_CRITICAL_ERRORS.clear()

# Add the parent directory to the Python path to enable imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)

# Import the JusteatItem class
try:
    from justeat.items import JusteatItem
except ImportError:
    # Fallback for command line execution
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from items import JusteatItem
#from justeat.pipelines import PostgresPipeline

# def get_db_connection(settings_dict=None, connect_timeout=30):
#     """
#     Create a database connection using settings or provided dictionary
#     Args:
#         settings_dict: Dictionary with database settings (host, database, user, password, port)
#         connect_timeout: Connection timeout in seconds
#     Returns:
#         psycopg2 connection object
#     """
#     import psycopg2
#     conn = psycopg2.connect(
#         host=settings_dict['host'],
#         database=settings_dict['database'],
#         user=settings_dict['user'],
#         password=settings_dict['password'],
#         port=settings_dict['port'],
#         connect_timeout=connect_timeout
#     )
#     conn.autocommit = True
#     return conn

class JeatDeSpider(scrapy.Spider):
    name = "jeat_de"
   
    custom_settings = {
        "CONCURRENT_REQUESTS": 1, #TODO: change to 6
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1, #TODO: change to 3
        "DOWNLOAD_TIMEOUT": 100.0,
        "DOWNLOAD_DELAY": 6, #TODO: change to 0
        "COOKIES_ENABLED": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 5,
        "AUTOTHROTTLE_MAX_DELAY": 60,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "AUTOTHROTTLE_DEBUG": True,
    }

    def __init__(self, *args, **kwargs):
        logging.info(f" JeatDeSpider initialized")
        super(JeatDeSpider, self).__init__(*args, **kwargs)

        try:
            settings = get_project_settings()
            self.api_key = settings.get('SCRAPEOPS_API_KEY')
            if not self.api_key:
                handle_error("Scrapeops API key not found", is_critical=True)
                # Exit immediately on critical error during initialization
                sys.exit(1)
            else:
                logging.info(f" Scrapeops API key is set")
        except Exception as e:
            handle_error(f"Failed to get project settings: {e}", is_critical=True)
            # Exit immediately on critical error during initialization
            sys.exit(1)

        # Initialize tracking for duplicate prevention
        self.processed_requests = set()
        self.processed_restaurants = set()  # Track processed restaurant IDs
        self.processed_menu_slugs = set()  # Track processed menu slugs
        
        # Get database connection using settings
        try:
            logging.info(f"Received Database config from settings - HOST: {settings.get('POSTGRES_HOST')}, DB: {settings.get('POSTGRES_DATABASE')}, USER: {settings.get('POSTGRES_USER')}, PORT: {settings.get('POSTGRES_PORT')}")
            
            self.conn = psycopg2.connect(
                host=settings.get('POSTGRES_HOST'),
                database=settings.get('POSTGRES_DATABASE'),
                user=settings.get('POSTGRES_USER'),
                password=settings.get('POSTGRES_PASSWORD'),
                port=settings.get('POSTGRES_PORT'),
                connect_timeout=settings.get('POSTGRES_CONNECTION_TIMEOUT'),
                keepalives_idle=settings.get('POSTGRES_KEEPALIVE_IDLE'),
                keepalives_interval=settings.get('POSTGRES_KEEPALIVE_INTERVAL'),
                keepalives_count=settings.get('POSTGRES_KEEPALIVE_COUNT')
            )
            logging.info(f"Database connection established Successfully using settings config")
        except Exception as e:
            handle_error(f"Failed to establish database connection: {e}", is_critical=True)
            sys.exit(1)
        self.conn.autocommit = True
        with self.conn.cursor() as curs:
            curs.execute("SELECT 1")
        logging.info(f"Database connection test successful while initializing spider")

        # if not self.db_connection:
        #     logging.error(f"Failed to connect to PostgreSQL in spider initialization")
        #     handle_error(f"Failed to connect to PostgreSQL in spider initialization", is_critical=True)
        #     sys.exit(1)
        
        # Check for any critical errors during initialization
        error_summary = get_error_summary()
        if error_summary['critical_errors'] > 0:
            logging.error(f"Critical errors detected during spider initialization - stopping execution {error_summary['critical_error_list']}")
            sys.exit(1)

    def handle_request_error(self, failure):
        """Handle request failures and categorize them as critical or non-critical"""
        # Get request information
        request_id = failure.request.meta.get('request_id', 'unknown')
        est_slug = failure.request.meta.get('est_slug_name', 'unknown')
        
        try:
            # Check if it's a response error
            if hasattr(failure.value, 'response') and failure.value.response:
                status_code = failure.value.response.status
                
                # Critical errors that should stop execution
                if status_code == 520:
                    handle_error("520 Error detected - likely Cloudflare protection", is_critical=True)
                    raise CloseSpider("520 Error detected - Cloudflare protection")
                    
                elif status_code == 401:
                    handle_error("401 Unauthorized - Authentication required", is_critical=True)
                    raise CloseSpider("401 Unauthorized - Authentication required")
                
                # elif status_code == 500:
                #     handle_error(f"Server error (500) for request", is_critical=False)
                
                # # ScrapeOps specific errors
                # elif 'scrapeops' in failure.request.url:
                #     handle_error("ScrapeOps API key issue detected", is_critical=True)
                #     raise CloseSpider("ScrapeOps API key issue detected")
                
                else:
                    # Other HTTP errors
                    handle_error(f"HTTP error {status_code} for request id {request_id} and request url {failure.request.url}", is_critical=False)
            
            else:
                # Non-HTTP errors (network, timeout, etc.)
                handle_error(f"Request failed: {failure.value} for request id {request_id} and request url {failure.request.url}", is_critical=False)
                
        except CloseSpider:
            # Re-raise CloseSpider exceptions
            raise
        except Exception as e:
            handle_error(f"Error in request error handler: {e} for request id {request_id} and request url {failure.request.url}", is_critical=True)
            # Critical error in error handler itself - should stop execution
            return sys.exit(1)

        # If this was a menu request that failed, yield the restaurant item without menu data
        if 'restaurant_item' in failure.request.meta:
            item = failure.request.meta.get('restaurant_item')
            item['menu_processing_status'] = 'menu_failed'
            item['menu_error_message'] = f"Menu request failed: {failure.value}"
            #logging.info(f" Yielding restaurant item without menu due to menu request failure: {est_slug}")
            yield item

    # Get zipcodes from PostgreSQL using NTILE approach
    def get_zipcodes_from_postgres(self, curr_tile, total_tiles):
        """
        Fetch zipcode data for a given tile using NTILE windowing.

        Args:
            tile_number (int): Which tile to process (1-based)
            total_tiles (int): Total number of tiles

        Returns:
            dict: {zipcode: (latitude, longitude)}, or {} if none found
        """
        from psycopg2.extras import RealDictCursor

        zipcode_coords = {}

        try:
            # Validate tile inputs
            if not isinstance(curr_tile, int) or not isinstance(total_tiles, int):
                handle_error(f"Invalid tile params: curr_tile={curr_tile}, total_tiles={total_tiles}", is_critical=True)
                return None

            # if curr_tile < 1 or curr_tile > total_tiles:
            #     handle_error(f"Tile number {curr_tile} is out of valid range 1..{total_tiles}", is_critical=True) 
            #     return None

            # Ensure DB connection is still alive
            if not self.conn or self.conn.closed != 0:
                handle_error("Database connection is closed or invalid", is_critical=True)
                return None

            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # #Count all DE zipcodes
                # cursor.execute("""
                #     SELECT COUNT(*) AS count
                #     FROM smartdatastagdb.zipcode_details
                #     WHERE country_code = 'DE'
                # """)
                # total_count = cursor.fetchone()['count'] or 0
                # logging.info(f" Total zipcodes for country Germany(DE) are: {total_count}")

                # if total_count == 0:
                #     handle_error("No zipcodes found for country Germany(DE)", is_critical=True)
                #     return None
                
                # Get zipcodes for this tile using NTILE
                query = f"""
                    SELECT zipcode, latitude, longitude 
                    FROM (
                        SELECT zipcode, latitude, longitude,
                                NTILE({total_tiles}) OVER (ORDER BY zipcode) AS tile_group
                        FROM smartdatastagdb.zipcode_details
                        WHERE country_code = 'DE'
                    ) t
                    WHERE tile_group = {curr_tile} and 
                    not exists 
                    (select 1 from smartdatastagdb.jsonimport_sample where spider = 'jeat_de' 
                    and zipcode = t.zipcode and t.latitude = latitude and t.longitude = longitude and scraping_status = 'Success')
                    ORDER BY zipcode, latitude, longitude
                """
                cursor.execute(query) 
                rows = cursor.fetchall()

                if not rows:
                    handle_error(f"No zipcodes returned for tile {curr_tile}/{total_tiles}", is_critical=False)
                    return None

                # Build dict safely
                for row in rows:
                    try:
                        if row['zipcode'] and row['latitude'] and row['longitude']:
                            zipcode_coords[row['zipcode']] = (
                                float(row['latitude']),
                                float(row['longitude'])
                            )
                        else:
                            handle_error(f"Skipping invalid row: {row}", is_critical=False)
                    except Exception as parse_err:
                        handle_error(f"Failed to parse row {row}: {parse_err}", is_critical=False)

                logging.info(f"Loaded {len(zipcode_coords)} zipcodes for tile {curr_tile}/{total_tiles}")
                return zipcode_coords

        except Exception as e:
            handle_error(f"Failed to fetch zipcodes (tile {curr_tile}/{total_tiles}): {e}", is_critical=True)
            return None

    #proxy url generation for scrapeops 
    def get_url(self, url):
        payload = {
            'api_key': self.api_key,
            'url': url,
            'bypass': "cloudflare",
            'keep_headers': 'true',  
            'render_js': 'true',      # to allow js rendering
            'residential': 'true',    # Use residential proxies for better success
            'country': 'de'           # Use German residential proxies
        }
        proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
        #logging.info(f" using Scrapeops with residential proxies")
        return proxy_url
    
    #Creates API requests to Just-Eat's discovery endpoint
    def start_requests(self):
        logging.info(f"Starting requests for zipcode processing")

        # Get tile number from spider arguments or use default
        curr_tile = int(getattr(self, 'tile_number', args.tile_number))
        total_tiles = int(getattr(self, 'total_tiles', args.total_tiles))
        # tile_number = 1
        # total_tiles = 50                   

        #for tile in range(tile_number, total_tiles + 1):
        logging.info(f" Processing tile {curr_tile} of {total_tiles}")
        #time.sleep(10)
    
        # Get zipcode data from PostgreSQL using NTILE approach
        zipcode_coords = self.get_zipcodes_from_postgres(curr_tile, total_tiles)

        # if not zipcode_coords:
        #     handle_error(f"No zipcodes found for tile {tile_number}", is_critical=False)
        #     return
        
        # For testing, you can limit to a few zipcodes
        # zipcode_coords = {'07318': (50.6086, 11.3163)}  # Uncomment for testing
        
        for zipcode, (latitude, longitude) in zipcode_coords.items():
            logging.info(f" Processing zipcode: {zipcode} at coordinates ({latitude}, {longitude})")
            discovery_url = f"https://rest.api.eu-central-1.production.jet-external.com/discovery/de/restaurants/enriched?latitude={latitude}&longitude={longitude}&serviceType=delivery&ratingsOutOfFive=true&je-tgl-ops_include_closed=true&je-tgl-tmp_banners=true"
            proxy_url = self.get_url(discovery_url)
            #logging.info(f" Discovery URL: {discovery_url}")
            #logging.info(f" Proxy URL: {proxy_url}")
            headers = {
                'authority': 'rest.api.eu-central-1.production.jet-external.com',
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en',
                'origin':  'https://www.lieferando.de/', 
                'referer': 'https://www.lieferando.de/en', 
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                'x-country-code': 'de',
                'x-language-code': 'en'
            }
            request = scrapy.Request(
                url=proxy_url,
                #url=discovery_url,
                headers=headers,
                callback=self.parse_discovery,
                errback=self.handle_request_error,
                meta={'zipcode': zipcode, 'latitude': latitude, 'longitude': longitude}
            )
            #logging.info(f" Created request for zipcode {zipcode}") 
            yield request
            
        logging.info(f"All requests created for tile {curr_tile}")
    
    def start_rest_menu(self, est_slug, item):
        if est_slug:
            request_id=f"{est_slug}_{id(item)}"
             # Check if this request has already been processed
            if request_id in self.processed_requests:
                #logging.info(f" Skipping duplicate request for restaurant: {est_slug} (ID: {request_id})")
                return None
            self.processed_requests.add(request_id)
            rest_menu_url = f"https://www.lieferando.de/en/menu/{est_slug}"
            proxy_url = self.get_url(rest_menu_url)  # This should return the full ScrapeOps URL

            headers = {
                'authority': 'www.lieferando.de',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'origin': 'https://www.lieferando.de', 
                'referer': 'https://www.lieferando.de/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            }

            #logging.info(f" Using ScrapeOps headless browser for: {rest_menu_url}")

            rest_menu_request = scrapy.Request(
                url=proxy_url,  
                headers=headers,
                callback=self.parse_rest_menu,
                errback=self.handle_request_error,
                meta={
                    # Removed "proxy" since ScrapeOps is handled by URL
                    "dont_retry": False,             # Allow retry on timeout (optional)
                    "download_timeout": 90,          # Set proper timeout (in seconds)
                    "est_slug_name": est_slug,
                    "original_url": rest_menu_url,
                    "restaurant_item": item,
                    "request_id": request_id
                }
            )
            #logging.info(f" Returning menu request for restaurant: {est_slug}")
            return rest_menu_request
        else:
            #logging.info(f" No restaurant slug provided, skipping menu request")
            return None

    def parse_rest_menu(self, response):
        """Parse restaurant menu page using Crawlbase headless browser"""
        try:
            #logging.info(f" parse_rest_menu called for URL: {response.url}")
            #logging.info(f" Response status: {response.status}")
            
            # Get request tracking info
            request_id = response.meta.get('request_id', 'unknown')
            est_slug = response.meta.get('est_slug_name', 'unknown')
            #logging.info(f" Processing request ID: {request_id} for restaurant: {est_slug}")
            
            # Check if response is valid
           
            if response.status != 200:
                if response.status in (502,401):
                    self.handle_error(f"[ERROR] Bad response status: {response.status} for request url {response.url}", is_critical=True)
                    return
                raise self.handle_error(f"Bad response status: {response.status} for request url {response.url}",is_critical=False)

            # Parse Crawlbase JSON response
            try:
                response_data = response.body.decode('utf-8', errors='ignore')
                #response_data = json.loads(response.body)
                
                # Extract the actual HTML content from Crawlbase response
                if 'body' not in response_data:
                    handle_error(f"No 'body' key in menu response for request id {request_id} and request url {response.url}", is_critical=False)
                    #logging.info(f"[ERROR] Available keys: {list(response_data.keys())}")
                
                item = response.meta.get('restaurant_item')
                item['menu_url'] = response.meta.get('original_url')
                item['menu_source_url'] = response.url
                item['menu_url_extraction_status'] = 'menu_url_extracted'
                item['request_id'] = request_id  # Track which request processed this item
                
                #logging.info(f" Successfully processed menu for restaurant: {est_slug} (ID: {request_id})")
                yield item
            
            except json.JSONDecodeError as e:
                handle_error(f"Could not parse Menu request for restaurant: {est_slug} with error: {e}", is_critical=False)
        
        except Exception as e:
            handle_error(f"Exception in parsing menu request for restaurant: {est_slug} with error: {e}", is_critical=False)

    # Parses the discovery response and extracts restaurant data
    # Skips restaurants without IDs or test restaurants
    # Data Mapping: Maps API fields to JusteatItem fields
    def parse_discovery(self, response):
        try:
            #logging.info(f" parse_discovery called for URL: {response.url}")
            # Always extract the real JSON (from 'body' if present, else directly)
            try:
                #logging.info(f" Response status: {response.status}")
                #logging.info(f" Response body length: {len(response.body)}")
                #logging.info(f" Response body preview: {response.body[:200]}...")

                if response.status != 200:
                    if response.status in (502,401):
                        self.handle_error(f"[ERROR] Bad response status: {response.status} for request url {response.url}", is_critical=True)
                        return
                    raise self.handle_error(f"Bad response status: {response.status} for request url {response.url}",is_critical=False) 
                
                # Try to parse the response body directly
                data = json.loads(response.body)
                #logging.info(f" Top-level keys in response: {list(data.keys())}")
                
                # If the response has a 'body' field (wrapped response), extract it
                if 'body' in data:
                    data = json.loads(data['body'])
                    #logging.info(f" Extracted 'body' from response, new keys: {list(data.keys())}")
                    
            except Exception as e:
                handle_error(f"Could not parse restaurant discovery response body as JSON: {e} for request url {response.url}", is_critical=False)
                return
            if 'restaurants' not in data:
                handle_error(f"No 'restaurants' key in data for request url {response.url}", is_critical=False) 
                return
            
            restaurants = data['restaurants']
            #logging.info(f" Number of restaurants found: {len(restaurants)}")
            yielded = 0

            for restaurant in restaurants:
                if not restaurant.get('id'):
                    #logging.info(f" Skipping restaurant with missing id: {restaurant}")
                    continue
                if restaurant.get('isTestRestaurant', False):
                    #logging.info(f" Skipping test restaurant: {restaurant.get('name', '')}")
                    continue        
                
                # Check if this restaurant has already been processed
                restaurant_id = restaurant.get('id', '')
                if restaurant_id in self.processed_restaurants:
                    #logging.info(f" Skipping duplicate restaurant: {restaurant.get('name', '')} (ID: {restaurant_id})")
                    continue
                
                # Mark this restaurant as processed
                self.processed_restaurants.add(restaurant_id)
                
                item = JusteatItem()
                item['restaurant_id'] = restaurant.get('id', '')
                item['name'] = restaurant.get('name', '')
                item['slug'] = restaurant.get('uniqueName', '')
                address = restaurant.get('address', {})
                item['address_line1'] = address.get('firstLine', '')
                item['address_line2'] = address.get('secondLine', '')
                item['city'] = address.get('city', '')
                item['postcode'] = address.get('postalCode', '')
                item['country'] = 'Germany'
                location = address.get('location', {})
                coordinates = location.get('coordinates', [])
                item['longitude'] = coordinates[0] if len(coordinates) >= 2 else ''
                item['latitude'] = coordinates[1] if len(coordinates) >= 2 else ''
                item['phone'] = ''
                item['website'] = ''
                rating = restaurant.get('rating', {})
                item['rating'] = rating.get('starRating', '')
                item['rating_count'] = rating.get('count', '')
                item['rating_out_of_five'] = rating.get('starRating', '')
                cuisines = restaurant.get('cuisines', [])
                cuisine_types = [c.get('name', '') for c in cuisines if isinstance(c, dict) and c.get('name')]
                item['cuisines'] = cuisine_types
                item['categories'] = restaurant.get('tags', [])
                item['is_open'] = restaurant.get('isOpenNowForDelivery', False) or restaurant.get('isOpenNowForCollection', False)
                item['is_test_restaurant'] = restaurant.get('isTestRestaurant', False)
                item['is_brand'] = restaurant.get('isPremier', False)
                item['is_chain'] = restaurant.get('isPremier', False)
                eta = restaurant.get('deliveryEtaMinutes', {})
                item['delivery_time'] = f"{eta.get('rangeLower', '')}-{eta.get('rangeUpper', '')} minutes" if eta else ''
                item['minimum_order'] = restaurant.get('minimumDeliveryValue', '')
                item['delivery_fee'] = restaurant.get('deliveryCost', '')
                item['free_delivery_threshold'] = ''
                
                item['zipcode'] = response.meta.get('zipcode', '')
                item['rest_scraped_at'] = datetime.now().isoformat()
                item['rest_source_url'] = response.url
                item['rest_error_message'] = ''
                item['rest_processing_status'] = 'rest_discovery_extracted'

                #logging.info(f" Creating menu request for restaurant: {item['name']} (ID: {item['restaurant_id']})")
                
                # Don't yield the restaurant item here - only yield it after menu processing
                # yield item
                # yielded += 1

                # Then yield menu request for this restaurant
                restaurant_slug = restaurant.get('uniqueName','')
                #logging.info(f" Creating menu request for restaurant slug: {restaurant_slug}")
                if restaurant_slug:
                    # Check if this menu slug has already been processed
                    if restaurant_slug in self.processed_menu_slugs:
                        #logging.info(f" Skipping duplicate menu request for slug: {restaurant_slug}") 
                        # If menu was already processed, yield the restaurant item without menu
                        yield item
                        yielded += 1
                    else:
                        # Mark this menu slug as processed
                        self.processed_menu_slugs.add(restaurant_slug)
                        menu_req = self.start_rest_menu(restaurant_slug, item)
                        if menu_req:
                            yield menu_req
                        else:
                            #logging.info(f" No menu request created for slug: {restaurant_slug}")
                            # If menu request failed, yield the restaurant item without menu
                            yield item
                            yielded += 1
                else:
                    #logging.info(f" No restaurant slug provided, yielding restaurant without menu")
                    # If no slug, yield the restaurant item without menu
                    yield item
                    yielded += 1

            logging.info(f"Total restaurants processed: {yielded}")

            # if yielded == 0:
            #     handle_error("No restaurants yielded from discovery response", is_critical=True)
            #     raise CloseSpider("No restaurants yielded")
                
        except Exception as e:
            handle_error(f"Exception in parsing discovery response for request url {response.url} with error: {e}", is_critical=False)
    
    def spider_closed(spider, reason):
        stats = spider.crawler.stats.get_stats() or {}

        total_requests   = stats.get("downloader/request_count", 0)
        response_count   = stats.get("downloader/response_count", 0)
        success_200      = stats.get("downloader/response_status_count/200", 0)
        non_200_resps    = sum(v for k, v in stats.items()
                            if k.startswith("downloader/response_status_count/") and not k.endswith("/200"))
        exceptions       = stats.get("downloader/exception_count", 0)
        no_response      = max(0, total_requests - response_count)
        items_scraped    = stats.get("item_scraped_count", 0)

        logging.info(f"Scraping Summary - Requests: {total_requests}, "
                    f"Responses: {response_count}, 200s: {success_200}, "
                    f"Non-200s: {non_200_resps}, Exceptions: {exceptions}, "
                    f"No-response: {no_response}, Items scraped: {items_scraped}")

        # Print error summary at the end
        error_summary = get_error_summary()
        logging.info(f"Error Summary - Critical: {error_summary['critical_errors']}, "
                    f"Non-critical: {error_summary['non_critical_errors']}")

        if error_summary['critical_errors'] > 0:
            logging.error(f"Spider completed with {error_summary['critical_errors']} critical errors")
        elif error_summary['non_critical_errors'] > 0:
            logging.warning(f"Spider completed with {error_summary['non_critical_errors']} non-critical errors")
        else:
            logging.info(f"Spider completed successfully with no errors")

if __name__ == "__main__":
    try:
        logging.info("Starting JeatDeSpider script")

        # Clear any previous errors
        clear_errors()

        # Confirm project directory
        current_dir = os.getcwd()
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)

        # Test critical imports
        try:
            from justeat.items import JusteatItem
            logging.info("Critical imports successful")
        except ImportError as imp_err:
            handle_error(f"Failed to import modules: {imp_err}", is_critical=True)
            sys.exit(1)

        from scrapy.crawler import CrawlerProcess
        from scrapy.utils.project import get_project_settings
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--tile_number', type=int, default=1)
        parser.add_argument('-T', '--total_tiles', type=int, default=20)
        args=parser.parse_args()

        #total_tiles = 10  #adjust depending on how you want to split zipcodes

        # Run each tile sequentially
        #for tile in range(1, total_tiles + 1):
        #logging.info(f"Running spider for tile {tile}/{total_tiles}")

        process = CrawlerProcess(get_project_settings())
        crawler = process.create_crawler(JeatDeSpider)
        crawler.signals.connect(JeatDeSpider.spider_closed, signal=signals.spider_closed)

        process.crawl(crawler, tile_number=args.tile_number, total_tiles=args.total_tiles)
        process.start()  # blocks until this tile is finished

        # Print error summary after each tile
        error_summary = get_error_summary()
        # logging.info(
        #     f"Tile {tile} completed - Critical: {error_summary['critical_errors']}, "
        #     f"Non-critical: {error_summary['non_critical_errors']}"
        # )

        logging.info(
            f"Completed - Critical: {error_summary['critical_errors']}, "
            f"Non-critical: {error_summary['non_critical_errors']}"
        )

        if error_summary['critical_errors'] > 0:
            logging.error("Critical Error Details:")
            for i, error in enumerate(error_summary['critical_error_list'], 1):
                logging.error(f"  {i}. {error}")
            sys.exit(1)

        # reset errors before next tile run
        clear_errors()

        #logging.info("All tiles processed successfully")
        logging.info("Spider Completed successfully for country Germany(DE)")
        sys.exit(0)
    
    except Exception as main_err:
        handle_error(f"Fatal error in main execution: {main_err} of spider for country Germany(DE)", is_critical=True)
        sys.exit(1)