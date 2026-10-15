#print("[DEBUG] Loading jeat_nl.py module")

import logging
import traceback
import os
import scrapy
from scrapy.exceptions import CloseSpider
import json
import sys
import psycopg2

from json.decoder import JSONDecodeError
from urllib.parse import urlencode
from scrapy.utils.project import get_project_settings
from datetime import datetime

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

def get_db_connection(settings_dict=None, connect_timeout=30):
    """
    Create a database connection using settings or provided dictionary
    Args:
        settings_dict: Dictionary with database settings (host, database, user, password, port)
        connect_timeout: Connection timeout in seconds
    Returns:
        psycopg2 connection object
    """
    import psycopg2
    
    if settings_dict:
        # Use provided settings dictionary
        conn = psycopg2.connect(
            host=settings_dict['host'],
            database=settings_dict['database'],
            user=settings_dict['user'],
            password=settings_dict['password'],
            port=settings_dict['port'],
            connect_timeout=connect_timeout
        )
    else:
        # Use settings from justeat.settings
        from justeat.settings import POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT
        
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT,
            connect_timeout=connect_timeout
        )
    
    return conn

class JeatNlSpider(scrapy.Spider):
    name = "jeat_nl"
   
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
        logging.info("[DEBUG] JeatNlSpider initialized")
        super(JeatNlSpider, self).__init__(*args, **kwargs)

        try:
            settings = get_project_settings()
            self.api_key = settings.get('SCRAPEOPS_API_KEY')
            if not self.api_key:
                handle_error("Scrapeops API key not found", is_critical=True)
                # Exit immediately on critical error during initialization
                sys.exit(1)
            else:
                logging.info(f"[DEBUG] Scrapeops API key is set")
        except Exception as e:
            handle_error(f"Failed to get project settings: {e}", is_critical=True)
            # Exit immediately on critical error during initialization
            sys.exit(1)

        # Initialize tracking for duplicate prevention
        self.processed_requests = set()
        self.processed_restaurants = set()  # Track processed restaurant IDs
        self.processed_menu_slugs = set()  # Track processed menu slugs
        
        # Get database settings from scrapy settings
        self.db_settings = {
            'host': settings.get('POSTGRES_HOST'),
            'database': settings.get('POSTGRES_DATABASE'),
            'user': settings.get('POSTGRES_USER'),
            'password': settings.get('POSTGRES_PASSWORD'),
            'port': settings.get('POSTGRES_PORT')
        }
        
        # Check for any critical errors during initialization
        error_summary = get_error_summary()
        if error_summary['critical_errors'] > 0:
            logging.error("❌ Critical errors detected during spider initialization - stopping execution")
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
                
                elif status_code == 500:
                    handle_error(f"Server error (500) for request", is_critical=False)
                
                # ScrapeOps specific errors
                elif 'scrapeops' in failure.request.url:
                    handle_error("ScrapeOps API key issue detected", is_critical=True)
                    raise CloseSpider("ScrapeOps API key issue detected")
                
                else:
                    # Other HTTP errors
                    handle_error(f"HTTP error {status_code} for request", is_critical=False)
            
            else:
                # Non-HTTP errors (network, timeout, etc.)
                handle_error(f"Request failed: {failure.value}", is_critical=False)
                
        except CloseSpider:
            # Re-raise CloseSpider exceptions
            raise
        except Exception as e:
            handle_error(f"Error in request error handler: {e}", is_critical=True)
            # Critical error in error handler itself - should stop execution
            return sys.exit(1)

        # If this was a menu request that failed, yield the restaurant item without menu data
        if 'restaurant_item' in failure.request.meta:
            item = failure.request.meta.get('restaurant_item')
            item['menu_processing_status'] = 'menu_failed'
            item['menu_error_message'] = f"Menu request failed: {failure.value}"
            logging.info(f"[DEBUG] Yielding restaurant item without menu due to menu request failure: {est_slug}")
            yield item

    # Get zipcodes from PostgreSQL using NTILE approach
    def get_zipcodes_from_postgres(self, tile_number=1, total_tiles=10):
        """
        Read zipcode data from PostgreSQL table using NTILE for distributed processing
        Args:
            tile_number: Which tile to process (1-based)
            total_tiles: Total number of tiles to split the data into
        """
        try:
            # Import psycopg2 here to avoid import issues
            from psycopg2.extras import RealDictCursor
            
            # Connect to PostgreSQL using the helper function
            conn = get_db_connection(self.db_settings)

            # Create a cursor with RealDictCursor for better performance
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get the total number of tiles
                cursor.execute(f"SELECT COUNT(*) FROM smartdatastagdb.zipcode_details_sample WHERE country_code = 'NL'")      
                total_count = cursor.fetchone()['count']
                logging.info(f"[DEBUG] Total zipcodes for Netherlands are: {total_count}")
                
                # Check if there are any zipcodes to process
                if total_count > 0:
                    # Get the zipcodes for the current tile
                    # Calculate the number of zipcodes per tile
                    zipcodes_per_tile = total_count // total_tiles
                    query = f"""
                    SELECT zipcode, latitude, longitude 
                    FROM (
                        SELECT zipcode, latitude, longitude,
                            NTILE({total_tiles}) OVER (ORDER BY zipcode) as tile_group
                        FROM smartdatastagdb.zipcode_details_sample 
                        WHERE country_code = 'NL'
                    ) tiled_data
                    WHERE tile_group = {tile_number}
                    ORDER BY zipcode
                    """ #TODO: change to zipcode_details
                    #logging.info(f"[DEBUG] Query: {query}")
                    cursor.execute(query)
                    
                    # Convert to dictionary format
                    zipcode_coords = {}
                    for row in cursor:
                        zipcode_coords[row['zipcode']] = (float(row['latitude']), float(row['longitude']))  

                    cursor.close()
                    conn.close()

                    # Check if we got any zipcodes for this tile
                    if len(zipcode_coords) == 0:
                        logging.info(f"[DEBUG] No zipcodes found for tile {tile_number}")
                        return None

                    #logging.info(f"[DEBUG] Loaded {len(zipcode_coords)} zipcodes from PostgreSQL for tile {tile_number}")
                    return zipcode_coords

                else:
                    logging.info(f"[DEBUG] No zipcodes found for country NL")
                    return None
                
        except Exception as e:
            handle_error(f"Failed to get zipcode data from PostgreSQL: {e}", is_critical=True)
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
            'country': 'nl'           # Use German residential proxies
        }
        proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
        #logging.info(f"[DEBUG] using Scrapeops with residential proxies")
        return proxy_url
    
    #Creates API requests to Just-Eat's discovery endpoint
    def start_requests(self):
        logging.info("[DEBUG] start_requests called")

        # Get tile number from spider arguments or use default
        tile_number = int(getattr(self, 'tile_number', 1))
        total_tiles = int(getattr(self, 'total_tiles', 10))
        
        #logging.info(f"[DEBUG] Processing tile {tile_number} of {total_tiles}")
        
        # Process only the specified tile, not a range of tiles
        # Get zipcode data from PostgreSQL using NTILE approach
        zipcode_coords = self.get_zipcodes_from_postgres(tile_number, total_tiles)

        if not zipcode_coords:
            handle_error(f"No zipcodes found for tile {tile_number}", is_critical=False)
            return
        
        # For testing, you can limit to a few zipcodes
        # zipcode_coords = {'07318': (50.6086, 11.3163)}  # Uncomment for testing
        
        for zipcode, (latitude, longitude) in zipcode_coords.items():
            logging.info(f"[DEBUG] Processing zipcode: {zipcode} at coordinates ({latitude}, {longitude})")
            discovery_url = f"https://rest.api.eu-central-1.production.jet-external.com/discovery/nl/restaurants/enriched?latitude={latitude}&longitude={longitude}&serviceType=delivery&ratingsOutOfFive=true&je-tgl-ops_include_closed=true&je-tgl-tmp_banners=true"
            proxy_url = self.get_url(discovery_url)
            #logging.info(f"[DEBUG] Discovery URL: {discovery_url}")
            logging.info(f"[DEBUG] Proxy URL: {proxy_url}")
            headers = {
                    'authority': 'rest.api.eu-central-1.production.jet-external.com',
                    'accept': 'application/json, text/plain, */*',
                    'accept-language': 'en',
                    'origin':  'https://www.thuisbezorgd.nl/', 
                    'referer': 'https://www.thuisbezorgd.nl/en', 
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                    'x-country-code': 'nl',
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
            logging.info(f"[DEBUG] Created request for zipcode {zipcode}") 
            yield request
        
        logging.info("[DEBUG] start_requests completed")
    
    def get_spider_error_summary(self):
        """
        Get error summary for this spider instance
        
        Returns:
            dict: Summary of critical and non-critical errors
        """
        return get_error_summary()

    def start_rest_menu(self, est_slug, item):
        if est_slug:
            request_id=f"{est_slug}_{id(item)}"
             # Check if this request has already been processed
            if request_id in self.processed_requests:
                logging.info(f"[DEBUG] Skipping duplicate request for restaurant: {est_slug} (ID: {request_id})")
                return None
            self.processed_requests.add(request_id)
            rest_menu_url = f"https://www.thuisbezorgd.nl/en/menu/{est_slug}"
            proxy_url = self.get_url(rest_menu_url)  # This should return the full ScrapeOps URL

            headers = {
                'authority': 'www.thuisbezorgd.nl',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'origin': 'https://www.thuisbezorgd.nl',
                'referer': 'https://www.thuisbezorgd.nl/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            }

            #logging.info(f"[DEBUG] Using ScrapeOps headless browser for: {rest_menu_url}")

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
            logging.info(f"[DEBUG] Returning menu request for restaurant: {est_slug}")
            return rest_menu_request
        else:
            logging.info(f"[DEBUG] No restaurant slug provided, skipping menu request")
            return None

    def parse_rest_menu(self, response):
        """Parse restaurant menu page using Crawlbase headless browser"""
        try:
            logging.info(f"[DEBUG] parse_rest_menu called for URL: {response.url}")
            logging.info(f"[DEBUG] Response status: {response.status}")
            
            # Get request tracking info
            request_id = response.meta.get('request_id', 'unknown')
            est_slug = response.meta.get('est_slug_name', 'unknown')
            logging.info(f"[DEBUG] Processing request ID: {request_id} for restaurant: {est_slug}")
            
            # Check if response is valid
           
            # if response.status != 200:
            #     logging.error(f"[ERROR] Bad response status: {response.status}")
            #     return

            # Parse Crawlbase JSON response
            try:
                response_data = response.body.decode('utf-8', errors='ignore')
                #response_data = json.loads(response.body)
                
                # Extract the actual HTML content from Crawlbase response
                if 'body' not in response_data:
                    logging.info(f"[ERROR] No 'body' key in Crawlbase response")
                    #logging.info(f"[ERROR] Available keys: {list(response_data.keys())}")

                
                item = response.meta.get('restaurant_item')
                item['menu_url'] = response.meta.get('original_url')
                item['menu_source_url'] = response.url
                item['menu_url_extraction_status'] = 'menu_url_extracted'
                item['request_id'] = request_id  # Track which request processed this item
                
                logging.info(f"[DEBUG] Successfully processed menu for restaurant: {est_slug} (ID: {request_id})")
                yield item
            
            except json.JSONDecodeError as e:
                handle_error(f"Failed to parse Crawlbase JSON response: {e}", is_critical=False)
        
        
        except Exception as e:
            handle_error(f"Exception in parse_restaurant: {e}", is_critical=False)

    # Parses the discovery response and extracts restaurant data
    # Skips restaurants without IDs or test restaurants
    # Data Mapping: Maps API fields to JusteatItem fields
    def parse_discovery(self, response):
        try:
            #logging.info(f"[DEBUG] parse_discovery called for URL: {response.url}")
            # Always extract the real JSON (from 'body' if present, else directly)
            try:
                logging.info(f"[DEBUG] Response status: {response.status}")
                #logging.info(f"[DEBUG] Response body length: {len(response.body)}")
                #logging.info(f"[DEBUG] Response body preview: {response.body[:200]}...")

                if response.status in (500,401):
                    # import traceback
                    # traceback.print_exc()
                    raise self.handle_error(response)
                
                # Try to parse the response body directly
                data = json.loads(response.body)
                logging.info(f"[DEBUG] Top-level keys in response: {list(data.keys())}")
                
                # If the response has a 'body' field (wrapped response), extract it
                if 'body' in data:
                    data = json.loads(data['body'])
                    logging.info(f"[DEBUG] Extracted 'body' from response, new keys: {list(data.keys())}")
                    
            except Exception as e:
                handle_error(f"Could not parse response body as JSON: {e}", is_critical=False)
                return
            if 'restaurants' not in data:
                handle_error("No 'restaurants' key in data", is_critical=False)
                return
            
            restaurants = data['restaurants']
            logging.info(f"[DEBUG] Number of restaurants found: {len(restaurants)}")
            yielded = 0

            for restaurant in restaurants:
                if not restaurant.get('id'):
                    logging.info(f"[DEBUG] Skipping restaurant with missing id: {restaurant}")
                    continue
                if restaurant.get('isTestRestaurant', False):
                    logging.info(f"[DEBUG] Skipping test restaurant: {restaurant.get('name', '')}")
                    continue        
                
                # Check if this restaurant has already been processed
                restaurant_id = restaurant.get('id', '')
                if restaurant_id in self.processed_restaurants:
                    logging.info(f"[DEBUG] Skipping duplicate restaurant: {restaurant.get('name', '')} (ID: {restaurant_id})")
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
                item['country'] = 'Netherlands'
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

                #logging.info(f"[DEBUG] Creating menu request for restaurant: {item['name']} (ID: {item['restaurant_id']})")
                
                # Don't yield the restaurant item here - only yield it after menu processing
                # yield item
                # yielded += 1

                # Then yield menu request for this restaurant
                restaurant_slug = restaurant.get('uniqueName','')
                logging.info(f"[DEBUG] Creating menu request for restaurant slug: {restaurant_slug}")
                if restaurant_slug:
                    # Check if this menu slug has already been processed
                    if restaurant_slug in self.processed_menu_slugs:
                        logging.info(f"[DEBUG] Skipping duplicate menu request for slug: {restaurant_slug}") 
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
                            logging.info(f"[DEBUG] No menu request created for slug: {restaurant_slug}")
                            # If menu request failed, yield the restaurant item without menu
                            yield item
                            yielded += 1
                else:
                    logging.info(f"[DEBUG] No restaurant slug provided, yielding restaurant without menu")
                    # If no slug, yield the restaurant item without menu
                    yield item
                    yielded += 1

            logging.info(f"[DEBUG] Total restaurant yielded: {yielded}")

            if yielded == 0:
                handle_error("No restaurants yielded from discovery response", is_critical=True)
                raise CloseSpider("No restaurants yielded")
                
        except Exception as e:
            handle_error(f"Exception in parse_discovery: {e}", is_critical=False)

if __name__ == "__main__":
    try:
        logging.info("🚀 Starting JeatNlSpider script")
        
        # Clear any previous errors
        clear_errors()
        
        # Confirm project directory
        current_dir = os.getcwd()
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
        
        # Test critical imports
        try:
            from justeat.items import JusteatItem
            from justeat.settings import POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT
            logging.info("✅ Critical imports successful")
        except ImportError as imp_err:
            handle_error(f"Failed to import modules: {imp_err}", is_critical=True)
            sys.exit(1)
        
        # Test database connection
        try:
            conn = get_db_connection()
            with conn.cursor() as curs:
                curs.execute("SELECT 1")
            conn.close()
            logging.info("✅ Database connection test successful")
        except Exception as db_err:
            handle_error(f"Database connection failed: {db_err}", is_critical=True)
            sys.exit(1)
        
        # Start Scrapy process
        try:
            from scrapy.crawler import CrawlerProcess
            from scrapy.utils.project import get_project_settings

            settings_obj = get_project_settings()
            process = CrawlerProcess(settings_obj)
            process.crawl(JeatNlSpider)
            logging.info("🕷️ Starting spider...")
            process.start()
            
            # Print error summary at the end
            error_summary = get_error_summary()
            logging.info(f"📊 Error Summary - Critical: {error_summary['critical_errors']}, Non-critical: {error_summary['non_critical_errors']}")
            
            # If there are critical errors, return failure (exit code 1)
            if error_summary['critical_errors'] > 0:
                logging.error("❌ Spider completed with critical errors - returning failure")
                sys.exit(1)
            else:
                # If only non-critical errors or no errors, return success (exit code 0)
                if error_summary['non_critical_errors'] > 0:
                    logging.warning(f"⚠️ Spider completed with {error_summary['non_critical_errors']} non-critical errors - continuing execution")
                else:
                    logging.info("✅ Spider completed successfully with no errors")
                sys.exit(0)
        
        except Exception as crawl_err:
            handle_error(f"Spider run failed: {crawl_err}", is_critical=True)
            sys.exit(1)

    except Exception as main_err:
        handle_error(f"Fatal error in main execution: {main_err}", is_critical=True)
        sys.exit(1)
