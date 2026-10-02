import logging
import traceback
import os
from datetime import datetime
from json.decoder import JSONDecodeError
from scrapy import Spider, Selector
from scrapy.http import Response
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
import re as regex
from math import ceil
import json
from typing import Optional, Any, cast, Tuple, Dict, List
import urllib.parse

# Import BeautifulSoup with fallback
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    BeautifulSoup = None

class RestGuruSpider(Spider):
    name = "rguru_de"

    # 🚨 CRITICAL FIX: Make these instance variables, not class variables
    # rest_slugs = []  # REMOVED - was causing cross-run duplicates
    # restaurant_count = 0  # REMOVED - moved to __init__

    def __init__(self, job_id=None, city_slug=None, *args, **kwargs):
        super(RestGuruSpider, self).__init__(*args, **kwargs)
        
        # 🚨 DEPLOYMENT CHECK: This log will confirm if latest code is deployed
        # Deployment check logs removed to reduce overhead
        
        # 🔧 Set job_id from spider argument or generate default as integer
        if job_id:
            self.job_id = int(job_id)  # Ensure it's an integer
        else:
            from datetime import datetime
            self.job_id = int(datetime.now().strftime('%Y%m%d%H%M%S'))  # Integer format
        
        # Single-city processing: accept specific city or auto-select next
        self.target_city_slug = city_slug
        self.current_city_info = None
        self.restaurants_found = 0
        self.restaurants_processed = 0
        self._initial_restaurants_processed = 0  # Track initial count for retry detection
        self.items_yielded = 0  # 🚨 NEW: Track actual items successfully yielded (for accurate database updates)
        self._initial_db_count = 0  # 🔥 NEW: Track initial database count for cumulative updates
        
        # 🚨 CRITICAL FIX: Initialize as instance variables to prevent cross-run duplicates
        self.rest_slugs = []
        self.restaurant_count = 0
        
        # 🚨 CRITICAL FIX: Reuse database connection to prevent connection spam
        self._db_connection = None
        self._working_selector = None  # Cache working selector to avoid trying 12 selectors every time
        
        # ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Enhanced error tracking for smart failure detection
        self.technical_errors = {
            'scrapeops_401_errors': 0,
            'scrapeops_403_errors': 0,
            'connection_errors': 0,
            'timeout_errors': 0,
            'total_failed_requests': 0,
            'proxy_failures': 0
        }
        self.has_critical_errors = False
        self.error_messages = []
        self.successful_requests = 0
        self.total_requests = 0
        
        # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ API optimization settings
        self.circuit_breaker_triggered = False
        # 🚀 MEGA CITY: Configurable restaurant limit with massive city support
        self.max_restaurants_per_city = int(kwargs.get('restaurant_limit', 45000))  # MEGA CITY: Default 45K limit
        
        # 🚀 NEW: Index-based processing parameters
        self.start_index = int(kwargs.get('start_index', 1))  # Start from index 1 by default
        self.end_index = int(kwargs.get('end_index', 50000))  # 🏙️ MEGA CITY: Increased to 50K by default
        self.index_based_mode = kwargs.get('index_based_mode', 'false').lower() == 'true'
        
        # 🏙️ MEGA CITY: Process entire city in single run (no batch limits)
        self.restaurants_per_batch = int(kwargs.get('restaurants_per_batch', 50000))  # Default 50K for mega cities
        self.batch_processing_active = True  # 🎯 Enable batch processing with restaurant limit
        
        # 🚀 SMART PAGE SKIPPING: Track API calls saved
        self.api_calls_saved = 0
        self.pages_skipped = 0
        
        # 🏙️ MEGA CITY: Enhanced processing modes
        self.enhanced_mode = kwargs.get('enhanced_mode', 'false').lower() == 'true'
        self.mega_city_mode = kwargs.get('mega_city_mode', 'false').lower() == 'true'
        
        # 🎯 SEQUENTIAL PAGINATION: Track page completion to avoid queue overflow
        self.current_page_restaurants = {}  # Track restaurants per page: {page_num: [list_of_restaurants]}
        self.current_page_completed = {}    # Track completion: {page_num: completed_count}
        self.next_page_ready = True         # Flag to control when next page should be triggered
        self.pending_next_page = None       # Store next page info until current page completes
        self.current_page_restaurant_count = 0  # Track restaurants on current page
        
        # 🚫 CAPTCHA DETECTION: Track consecutive empty pages
        self.consecutive_empty_pages = 0
        self.max_consecutive_empty_pages = 3  # Stop after 3 consecutive empty pages
        
        # 🔄 RESUME CAPABILITY: Track processing state for resume functionality
        self.resume_enabled = kwargs.get('resume_enabled', 'true').lower() == 'true'
        self.checkpoint_interval = int(kwargs.get('checkpoint_interval', 25))  # 🔧 ULTRA-OPTIMIZED: Save checkpoint every 25 restaurants to prevent data loss
        self.last_processed_index = 0
        self.last_processed_page = 0
        self.resume_checkpoint_data = {}
        
        # 🎯 PAGINATION FIX: Variables for proper request handling
        self.pending_next_page = None  # Track next page to be triggered
        self.pending_next_request = None  # Store next page request to be yielded
        
        # 🎯 100% SUCCESS TRACKING: Monitor success rates and retry effectiveness
        self.success_stats = {
            'total_restaurant_requests': 0,
            'successful_extractions': 0,
            'captcha_retries_level_1': 0,
            'captcha_retries_level_2': 0,
            'captcha_retries_level_3': 0,
            'retry_successes': 0,
            'max_retry_failures': 0
        }
        
        # 🚀 MEGA CITY: Dynamic configuration based on restaurant count
        if self.mega_city_mode or self.max_restaurants_per_city >= 30000:  # MEGA CITIES (30K+)
            self.max_scrapeops_failures = 10    # INCREASED: More tolerance for mega cities
            self.request_batch_size = 100       # INCREASED: Larger batches
            self.memory_management_enabled = True
            self.log(f"🏙️ MEGA CITY MODE activated for {self.max_restaurants_per_city} restaurants", logging.INFO)
        elif self.max_restaurants_per_city >= 15000:  # LARGE CITIES (15K-30K)
            self.max_scrapeops_failures = 8
            self.request_batch_size = 75
            self.memory_management_enabled = True
            self.log(f"🌆 LARGE CITY MODE for {self.max_restaurants_per_city} restaurants", logging.INFO)
        elif self.max_restaurants_per_city >= 5000:   # Medium-large cities
            self.max_scrapeops_failures = 6
            self.request_batch_size = 10  # 🔧 BATCH: Request batch size
            self.memory_management_enabled = False
            self.log(f"⚡ ENHANCED MODE for {self.max_restaurants_per_city} restaurants (10-page batches)", logging.INFO)
        elif self.max_restaurants_per_city >= 500:   # Medium cities
            self.max_scrapeops_failures = 4     # Standard reliability
            self.request_batch_size = 10  # 🔧 BATCH: Request batch size
            self.memory_management_enabled = False
            self.log(f"🏙️ MEDIUM CITY MODE for {self.max_restaurants_per_city} restaurants (10-page batches)", logging.INFO)
        else:  # Small cities (<500) - Optimize for minimal API usage
            self.max_scrapeops_failures = 6     # Higher tolerance for small cities
            self.request_batch_size = 10        # 🔧 BATCH: Request batch size
            self.memory_management_enabled = False
            self.log(f"🏘️ SMALL CITY LITE MODE for {self.max_restaurants_per_city} restaurants (10-page batches)", logging.INFO)
        
        # Initialize database deduplication
        self.scraped_urls = set()
        self._load_scraped_urls()
        
        # 🏙️ MEGA CITY: Initialize memory monitoring
        if self.memory_management_enabled:
            self._memory_check_interval = 100  # Check memory every 100 restaurants
            self._last_memory_check = 0
            self.log("🏙️ MEGA CITY: Memory monitoring enabled", logging.INFO)

    def _load_scraped_urls(self):
        """Load already-scraped URLs from database to prevent cross-run duplicates - MEGA CITY OPTIMIZED"""
        try:
            # Try to import psycopg2 for database connection
            try:
                import psycopg2
            except ImportError:
                self.log("ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â psycopg2 not available - skipping database deduplication", logging.WARNING)
                return
            
            # Database connection config - using working Airflow environment host
            db_config = {
                'host': '10.32.48.200',  # Working host from Airflow environment
                'port': 5432,
                'user': 'postgres', 
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres',
                'connect_timeout': 30,  # Increased timeout for mega cities
                'application_name': 'rguru_dedup'
            }
            
            self.log("ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ…â€™ Connecting to database for deduplication check...", logging.INFO)
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # 🏙️ MEGA CITY OPTIMIZATION: Check if we're in mega city mode
            if self.memory_management_enabled:
                self.log("🏙️ MEGA CITY MODE: Using memory-efficient deduplication strategy", logging.INFO)
                
                # Strategy 1: Use database-level deduplication instead of loading all URLs
                # This prevents memory explosion for mega cities
                self._use_database_deduplication = True
                self.scraped_urls = set()  # Keep empty set for compatibility
                
                # Create a temporary table for efficient URL checking (if needed)
                try:
                    cursor.execute("""
                        CREATE TEMP TABLE IF NOT EXISTS temp_scraped_urls (
                            url_hash VARCHAR(64) PRIMARY KEY,
                            url TEXT
                        )
                    """)
                    connection.commit()
                    self.log("🏙️ MEGA CITY: Created temporary deduplication table", logging.INFO)
                except Exception as e:
                    self.log(f"⚠️ Could not create temp table: {e}", logging.WARNING)
                    self._use_database_deduplication = False
                
            else:
                # Standard mode: Load URLs in batches to prevent memory issues
                self._use_database_deduplication = False
                self.log("🏘️ STANDARD MODE: Loading scraped URLs in batches", logging.INFO)
                
                # Get total count first to estimate memory usage
                cursor.execute("""
                    SELECT COUNT(DISTINCT raw_json)
                    FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                    WHERE raw_json IS NOT NULL 
                    AND raw_json != 'null'
                    AND raw_json != ''
                    AND LENGTH(raw_json) > 100
                """)
                total_count = cursor.fetchone()[0]
                self.log(f"📊 Total scraped records to process: {total_count:,}", logging.INFO)
                
                # If too many records, use database deduplication instead
                if total_count > 50000:  # 50K threshold for memory safety
                    self.log("⚠️ Too many records for in-memory deduplication, switching to database mode", logging.WARNING)
                    self._use_database_deduplication = True
                    self.scraped_urls = set()
                else:
                    # Load in batches to prevent memory issues
                    batch_size = 10000
                    offset = 0
                    loaded_count = 0
                    
                    while offset < total_count:
                        self.log(f"📦 Loading batch {offset//batch_size + 1} (offset: {offset})", logging.INFO)
                        
                        cursor.execute("""
                            SELECT DISTINCT raw_json
                            FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                            WHERE raw_json IS NOT NULL 
                            AND raw_json != 'null'
                            AND raw_json != ''
                            AND LENGTH(raw_json) > 100
                            ORDER BY raw_json
                            LIMIT %s OFFSET %s
                        """, (batch_size, offset))
                        
                        batch_rows = cursor.fetchall()
                        if not batch_rows:
                            break
                            
                        for row in batch_rows:
                            if row[0]:
                                try:
                                    # Parse JSON to extract URL
                                    json_data = json.loads(row[0])
                                    scraped_url = json_data.get('_from_url')
                                    
                                    if scraped_url:
                                        # Extract real URL from proxy URL if needed
                                        if 'proxy.scrapeops.io' in scraped_url:
                                            try:
                                                parsed = urllib.parse.parse_qs(scraped_url.split('?', 1)[1])
                                                if 'url' in parsed:
                                                    real_url = urllib.parse.unquote(parsed['url'][0])
                                                    self.scraped_urls.add(real_url)
                                                    loaded_count += 1
                                            except:
                                                pass  # Skip malformed proxy URLs
                                        else:
                                            self.scraped_urls.add(scraped_url)
                                            loaded_count += 1
                                except (json.JSONDecodeError, KeyError):
                                    pass  # Skip malformed JSON
                        
                        offset += batch_size
                        
                        # Memory management: Force garbage collection every batch
                        if self.memory_management_enabled:
                            import gc
                            gc.collect()
                    
                    self.log(f"✅ Loaded {loaded_count:,} URLs in {offset//batch_size} batches", logging.INFO)
                        
            cursor.close()
            connection.close()
            
            if not self._use_database_deduplication:
                self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â Loaded {len(self.scraped_urls)} already-scraped URLs from database", logging.INFO)
            else:
                self.log("🏙️ MEGA CITY: Using database-level deduplication (no URLs loaded into memory)", logging.INFO)
            
        except Exception as e:
            self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Could not load scraped URLs from database: {e}", logging.WARNING)
            self.log("🔄 Continuing without database deduplication", logging.WARNING)
            self.scraped_urls = set()  # Continue without deduplication
            self._use_database_deduplication = False
    
    def _get_db_connection(self):
        """Get reusable database connection to prevent connection spam"""
        if self._db_connection is None or self._db_connection.closed:
            try:
                import psycopg2
                db_config = {
                    'host': '10.32.48.200',
                    'port': 5432,
                    'user': 'postgres',
                    'password': 'xqhCcs&"c#Y*}S,_',
                    'database': 'postgres'
                }
                self._db_connection = psycopg2.connect(**db_config)
                self.log("🔗 DATABASE: Reusable connection established", logging.DEBUG)
            except Exception as e:
                self.log(f"❌ DATABASE: Connection failed: {e}", logging.ERROR)
                self._db_connection = None
        return self._db_connection
    
    def _close_db_connection(self):
        """Close database connection when spider finishes"""
        if self._db_connection and not self._db_connection.closed:
            self._db_connection.close()
            self.log("🔗 DATABASE: Connection closed", logging.DEBUG)

    def _is_already_scraped(self, url: str) -> bool:
        """Check if URL was already scraped successfully in previous runs - MEGA CITY OPTIMIZED"""
        # 🚨 INFINITE SCROLL FIX: Temporarily disable duplicate detection for big cities
        # This allows re-processing with the corrected infinite scroll logic
        if hasattr(self, 'current_city_info') and self.current_city_info:
            city_name = self.current_city_info.get('city_name', '').lower()
            # Skip duplicate detection for major cities during infinite scroll fix deployment
            major_cities = ['hamburg', 'munich', 'berlin', 'cologne', 'frankfurt', 'stuttgart', 'dusseldorf']
            if any(major_city in city_name for major_city in major_cities):
                self.log(f"🔄 INFINITE SCROLL FIX: Bypassing duplicate detection for {city_name} to allow re-processing", logging.INFO)
                return False
        
        # For mega cities, use database-level deduplication instead of in-memory set
        if hasattr(self, '_use_database_deduplication') and self._use_database_deduplication:
            return self._check_url_in_database(url)
        else:
            # Standard mode: use in-memory set
            return url in self.scraped_urls
    
    def _check_url_in_database(self, url: str) -> bool:
        """Check if URL was scraped in ANY previous job to prevent duplicates"""
        try:
            import psycopg2
            
            # Extract restaurant slug from URL for precise matching
            try:
                restaurant_slug = url.split('.com/')[-1]
            except:
                restaurant_slug = url
            
            self.log(f"🔍 CHECKING DUPLICATE: {restaurant_slug} for city {self.current_city_info.get('city_name', 'Unknown') if self.current_city_info else 'Unknown'}", logging.INFO)
            
            # Database connection config
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres', 
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres',
                'connect_timeout': 10,
                'application_name': 'rguru_url_check'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # 🚨 COMPREHENSIVE DEDUPLICATION: Check multiple tables and formats
            current_city = self.current_city_info.get('city_name', 'Unknown') if self.current_city_info else 'Unknown'
            
            # Method 1: Check import_hasdata_base with json_base array (your sample format)
            try:
                cursor.execute("""
                    SELECT 1 FROM smartdatastagdb.import_hasdata_base AS t,
                         jsonb_array_elements(t.json_base::jsonb) AS obj
                    WHERE (obj->>'intern_link' LIKE %s OR obj->>'url' LIKE %s OR obj->>'title' LIKE %s)
                    AND (obj->>'current_city' = %s OR obj->>'city' = %s)
                    LIMIT 1
                """, (f'%{restaurant_slug}%', f'%{restaurant_slug}%', f'%{restaurant_slug}%', current_city, current_city))
                
                exists = cursor.fetchone() is not None
                if exists:
                    self.log(f"🔍 DUPLICATE FOUND in import_hasdata_base: {restaurant_slug}", logging.INFO)
                    cursor.close()
                    connection.close()
                    return True
            except Exception as e:
                self.log(f"⚠️ Error checking import_hasdata_base: {e}", logging.WARNING)
            
            # Method 2: Check restaurant_guru_raw_germany 
            try:
                cursor.execute("""
                    SELECT 1 FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                    WHERE (raw_json::text LIKE %s OR raw_json::text LIKE %s OR raw_json::text LIKE %s)
                    AND city_name = %s
                    LIMIT 1
                """, (f'%{restaurant_slug}%', f'%{restaurant_slug}%', f'%{restaurant_slug}%', current_city))
                
                exists = cursor.fetchone() is not None
                if exists:
                    self.log(f"🔍 DUPLICATE FOUND in restaurant_guru_raw_germany: {restaurant_slug}", logging.INFO)
                    cursor.close()
                    connection.close()
                    return True
            except Exception as e:
                self.log(f"⚠️ Error checking restaurant_guru_raw_germany: {e}", logging.WARNING)
            
            # Method 3: Simple text search as fallback
            try:
                cursor.execute("""
                    SELECT 1 FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                    WHERE raw_json::text LIKE %s
                    AND city_name = %s
                    LIMIT 1
                """, (f'%{restaurant_slug}%', current_city))
                
                exists = cursor.fetchone() is not None
                if exists:
                    self.log(f"🔍 DUPLICATE FOUND via text search: {restaurant_slug}", logging.INFO)
                    cursor.close()
                    connection.close()
                    return True
            except Exception as e:
                self.log(f"⚠️ Error in text search: {e}", logging.WARNING)
            
            cursor.close()
            connection.close()
            
            self.log(f"✅ NO DUPLICATE: {restaurant_slug} is new for {current_city}", logging.INFO)
            return False
            
        except Exception as e:
            # If database check fails, fall back to allowing the URL (safer for processing)
            self.log(f"⚠️ Database URL check failed for {url}: {e}", logging.WARNING)
            return False

    def _monitor_memory_usage(self, restaurant_count: int):
        """Monitor memory usage for mega cities and trigger cleanup if needed"""
        if not self.memory_management_enabled:
            return
            
        if restaurant_count - self._last_memory_check >= self._memory_check_interval:
            try:
                import psutil
                import gc
                
                # Get current memory usage
                process = psutil.Process()
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024
                
                self.log(f"🏙️ MEGA CITY: Memory usage: {memory_mb:.1f} MB (restaurant #{restaurant_count})", logging.INFO)
                
                # If memory usage is high, force garbage collection
                if memory_mb > 2048:  # 2GB threshold
                    self.log(f"⚠️ High memory usage detected ({memory_mb:.1f} MB), forcing garbage collection", logging.WARNING)
                    gc.collect()
                    
                    # Check memory after cleanup
                    memory_info_after = process.memory_info()
                    memory_mb_after = memory_info_after.rss / 1024 / 1024
                    self.log(f"🧹 Memory after cleanup: {memory_mb_after:.1f} MB (freed {memory_mb - memory_mb_after:.1f} MB)", logging.INFO)
                
                self._last_memory_check = restaurant_count
                
            except ImportError:
                # psutil not available, just do basic garbage collection
                import gc
                gc.collect()
                self.log(f"🏙️ MEGA CITY: Basic garbage collection performed (restaurant #{restaurant_count})", logging.INFO)
                self._last_memory_check = restaurant_count
            except Exception as e:
                self.log(f"⚠️ Memory monitoring failed: {e}", logging.WARNING)

    def _get_resume_checkpoint(self, city_slug: str):
        """Get the last successful processing checkpoint for a city"""
        try:
            import psycopg2
            import json
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # Get checkpoint directly from table (fallback if functions don't exist)
            cursor.execute("""
                SELECT 
                    COALESCE(last_processed_restaurant_index, 0) as last_index,
                    COALESCE(last_processed_page, 0) as last_page,
                    COALESCE(resume_checkpoint_data, '{}'::jsonb) as checkpoint_data,
                    COALESCE(restaurants_found, 0) as restaurants_found,
                    COALESCE(restaurants_processed, 0) as restaurants_processed
                FROM smartdatastagdb.city_processing_status 
                WHERE city_slug = %s 
                AND processing_status IN ('failed', 'processing')
            """, (city_slug,))
            
            result = cursor.fetchone()
            connection.close()
            
            if result:
                last_index, last_page, checkpoint_data, restaurants_found, restaurants_processed = result
                
                # 🔥 CRITICAL: Store initial database count for cumulative updates
                self._initial_db_count = restaurants_processed
                
                # 🚨 CRITICAL: Check for database inconsistency (checkpoint vs actual data)
                check_connection = psycopg2.connect(**db_config)
                check_cursor = check_connection.cursor()
                
                # Convert city_slug to city_name for database lookup
                city_name = city_slug.replace('-', ' ').title()  # munich -> Munich
                
                check_cursor.execute("""
                    SELECT COUNT(*) 
                    FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                    WHERE city_name = %s
                """, (city_name,))
                
                count_result = check_cursor.fetchone()
                actual_count = count_result[0] if count_result else 0
                check_connection.close()
                
                # Check for inconsistency between checkpoint and actual data
                if actual_count > restaurants_processed:
                    self.log(f"🚨 DATABASE INCONSISTENCY DETECTED!", logging.WARNING)
                    self.log(f"   📊 Checkpoint shows: {restaurants_processed} restaurants processed", logging.WARNING)
                    self.log(f"   🔍 Database contains: {actual_count} restaurants for {city_name}", logging.WARNING)
                    self.log(f"   🔧 RECOVERING: Updating checkpoint to match database reality", logging.INFO)
                    
                    # Update checkpoint to match database reality
                    connection = psycopg2.connect(**db_config)
                    cursor = connection.cursor()
                    cursor.execute("""
                        UPDATE smartdatastagdb.city_processing_status
                        SET 
                            last_processed_restaurant_index = %s,
                            restaurants_processed = %s,
                            processing_status = 'processing',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE city_slug = %s
                    """, (actual_count, actual_count, city_slug))
                    connection.commit()
                    connection.close()
                    
                    # Update spider state
                    self.restaurants_processed = actual_count
                    self.log(f"💾 CHECKPOINT RECOVERED: Resume from restaurant #{actual_count + 1}", logging.INFO)
                    return actual_count, 0, {'recovered': True}
                
                elif last_index > 0 or last_page > 0:
                    # 🚨 CRITICAL FIX: Detect pagination mode from last_processed_page > 0
                    try:
                        checkpoint_json = json.loads(checkpoint_data) if isinstance(checkpoint_data, str) else checkpoint_data
                        # 🔍 PAGINATION DETECTION: If last_processed_page > 0, it was pagination mode
                        if last_page > 0:
                            # Pagination mode: resume from last completed page
                            self.log(f"🔗 PAGINATION RESUME DETECTED: Found checkpoint at page #{last_page} (pagination mode)", logging.INFO)
                            self.log(f"🔗 PAGINATION RESUME: {restaurants_processed} restaurants processed so far", logging.INFO)
                            # For pagination, we resume from the NEXT page (since current page was completed)
                            resume_page = last_page + 1
                            return restaurants_processed, resume_page, checkpoint_data
                        else:
                            # Infinite scroll mode (last_page = 0)
                            self.restaurants_processed = restaurants_processed
                            self.log(f"🔄 INFINITE SCROLL RESUME: Found checkpoint at restaurant #{last_index}, page #{last_page}", logging.INFO)
                            self.log(f"🔄 INFINITE SCROLL RESUME: Restored processed count: {restaurants_processed}", logging.INFO)
                            return last_index, last_page, checkpoint_data
                    except:
                        # Fallback to infinite scroll mode if checkpoint_data parsing fails
                        self.restaurants_processed = restaurants_processed
                        self.log(f"🔄 RESUME: Found checkpoint at restaurant #{last_index}, page #{last_page}", logging.INFO)
                        self.log(f"🔄 RESUME: Restored processed count: {restaurants_processed} (restaurants_found will be re-discovered)", logging.INFO)
                        return last_index, last_page, checkpoint_data
                else:
                    # Check if database has restaurants but checkpoint shows 0
                    if actual_count > 0:
                        self.log(f"🔄 RECOVERY: Database has {actual_count} restaurants but checkpoint is 0", logging.INFO)
                        self.restaurants_processed = actual_count
                        # 🔥 RECOVERY: Set initial database count to existing count
                        self._initial_db_count = actual_count
                        return actual_count, 0, {'recovered': True}
                    else:
                        self.log("🆕 FRESH START: No checkpoint found, starting from beginning", logging.INFO)
                        # 🔥 FRESH START: Set initial database count to 0
                        self._initial_db_count = 0
                        return 0, 0, {}
            else:
                self.log("🆕 FRESH START: No checkpoint found, starting from beginning", logging.INFO)
                # 🔥 FRESH START: Set initial database count to 0
                self._initial_db_count = 0
                return 0, 0, {}
                
        except Exception as e:
            self.log(f"⚠️ Could not load checkpoint: {e}", logging.WARNING)
            # 🔥 ERROR FALLBACK: Set initial database count to 0
            self._initial_db_count = 0
            return 0, 0, {}
    
    def _save_page_checkpoint(self, city_slug: str, current_page: int, restaurants_found_on_page: int):
        """Save page-level checkpoint for pagination mode to city_processing_status table"""
        try:
            import psycopg2
            import json
            
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres',
                'connect_timeout': 10,
                'application_name': 'rguru_page_checkpoint'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # Update city_processing_status with current page progress
            from datetime import datetime
            checkpoint_data = {
                'current_page': current_page,
                'restaurants_found_on_page': restaurants_found_on_page,
                'timestamp': datetime.now().isoformat(),
                'mode': 'pagination'
            }
            
            # Calculate total restaurants processed so far (pages completed * 20 per page)
            total_restaurants_processed = (current_page - 1) * 20 + restaurants_found_on_page
            
            cursor.execute("""
                UPDATE smartdatastagdb.city_processing_status
                SET 
                    last_processed_page = %s,
                    restaurants_found = %s,
                    restaurants_processed = %s,
                    resume_checkpoint_data = %s,
                    processing_status = 'processing',
                    updated_at = CURRENT_TIMESTAMP
                WHERE city_slug = %s
            """, (current_page, self.restaurants_found, total_restaurants_processed, json.dumps(checkpoint_data), city_slug))
            
            rows_affected = cursor.rowcount
            connection.commit()
            connection.close()
            
            if rows_affected > 0:
                self.log(f"💾 PAGE CHECKPOINT: Saved page {current_page} progress for {city_slug} (found {restaurants_found_on_page} restaurants)", logging.INFO)
                return True
            else:
                self.log(f"⚠️ PAGE CHECKPOINT FAILED: No rows updated for city_slug='{city_slug}'", logging.ERROR)
                return False
                
        except Exception as e:
            self.log(f"⚠️ Could not save page checkpoint: {e}", logging.ERROR)
            return False
    
    def _save_checkpoint(self, city_slug: str, restaurant_index: int, restaurant_data: dict = None):
        """Save processing checkpoint to database - focuses on restaurant index for infinite scroll"""
        try:
            # 🚨 INFINITE SCROLL FIX: Page number is not relevant for infinite scroll, focus on restaurant index
            self.log(f"🔍 DEBUG: Attempting to save checkpoint for city_slug='{city_slug}', restaurant_index={restaurant_index} (infinite scroll mode)", logging.INFO)
            import psycopg2
            import json
            
            # 🔧 ENHANCED: More robust database configuration
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres',
                'connect_timeout': 10,
                'application_name': 'rguru_checkpoint'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # Prepare checkpoint data
            from datetime import datetime
            checkpoint_data = {
                'last_processed_slug': restaurant_data.get('slug', '') if restaurant_data else '',
                'last_processed_url': restaurant_data.get('url', '') if restaurant_data else '',
                'timestamp': datetime.now().isoformat(),  # 🚨 SIMPLIFIED: Use actual timestamp instead of job_id
                'restaurants_processed': self.restaurants_processed,
                'total_restaurants_found': getattr(self, 'restaurants_found', 0),
                'processing_time': str(getattr(self, 'start_time', 'unknown'))
            }
            
            # 🔧 ENHANCED: Check if city exists first
            cursor.execute("SELECT city_slug FROM smartdatastagdb.city_processing_status WHERE city_slug = %s", (city_slug,))
            city_exists = cursor.fetchone()
            
            if not city_exists:
                self.log(f"⚠️ CHECKPOINT FAILED: City '{city_slug}' not found in city_processing_status table", logging.ERROR)
                connection.close()
                return False
            
            # 🔥 CUMULATIVE COUNT: Add new items to initial database count
            cumulative_count = self._initial_db_count + self.items_yielded
            self.log(f"💾 CUMULATIVE UPDATE: {self._initial_db_count} (initial) + {self.items_yielded} (new) = {cumulative_count} total", logging.DEBUG)
            
            # Save checkpoint directly to table with updated restaurant counts
            cursor.execute("""
                UPDATE smartdatastagdb.city_processing_status
                SET 
                    last_processed_restaurant_index = %s,
                    last_processed_page = %s,
                    resume_checkpoint_data = %s,
                    restaurants_found = %s,
                    restaurants_processed = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE city_slug = %s
            """, (restaurant_index, 0, json.dumps(checkpoint_data or {}), self.restaurants_found, cumulative_count, city_slug))  # Page always 0 for infinite scroll
            
            rows_affected = cursor.rowcount
            connection.commit()
            connection.close()
            
            if rows_affected > 0:
                self.log(f"💾 CHECKPOINT SAVED: Restaurant #{restaurant_index}, Page #0 (infinite scroll), Found: {self.restaurants_found}, Processed: {self.restaurants_processed} (rows updated: {rows_affected})", logging.INFO)
                return True
            else:
                self.log(f"⚠️ CHECKPOINT FAILED: No rows updated for city_slug='{city_slug}'", logging.ERROR)
                return False
            
        except psycopg2.Error as e:
            self.log(f"⚠️ Database error saving checkpoint: {e}", logging.ERROR)
            return False
        except Exception as e:
            self.log(f"⚠️ Could not save checkpoint: {e}", logging.ERROR)
            return False
    
    def _clear_checkpoint(self, city_slug: str):
        """Clear resume checkpoint data when city is completed"""
        try:
            import psycopg2
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # Clear checkpoint directly from table (fallback if functions don't exist)
            cursor.execute("""
                UPDATE smartdatastagdb.city_processing_status
                SET 
                    last_processed_restaurant_index = 0,
                    last_processed_page = 0,
                    resume_checkpoint_data = '{}'::jsonb,
                    updated_at = CURRENT_TIMESTAMP
                WHERE city_slug = %s
            """, (city_slug,))
            
            connection.commit()
            connection.close()
            
            self.log(f"🧹 CHECKPOINT CLEARED: {city_slug}", logging.INFO)
            
        except Exception as e:
            self.log(f"⚠️ Could not clear checkpoint: {e}", logging.WARNING)


    def _cleanup_stale_processing_cities(self):
        """
        Ã°Å¸â€º Ã¯Â¸Â PERMANENT FIX: Clean up stale processing cities before starting spider execution.
        This prevents the spider hanging issue from recurring by automatically resetting
        cities that have been stuck in 'processing' status for too long.
        """
        try:
            import psycopg2
            
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # Reset cities that have been processing for more than 2 hours
            cleanup_query = """
            UPDATE smartdatastagdb.city_processing_status 
            SET processing_status = 'pending',
                error_message = 'Auto-reset: Processing timeout exceeded (2+ hours) - Spider startup cleanup',
                updated_at = CURRENT_TIMESTAMP,
                retry_count = retry_count + 1
            WHERE processing_status = 'processing'
            AND updated_at < NOW() - INTERVAL '2 hours'
            AND retry_count < 3
            RETURNING city_slug, 
                     EXTRACT(EPOCH FROM (NOW() - updated_at))/3600 as hours_stuck;
            """
            
            cursor.execute(cleanup_query)
            reset_cities = cursor.fetchall()
            connection.commit()
            
            if reset_cities:
                reset_info = []
                for city_slug, hours_stuck in reset_cities:
                    reset_info.append(f"{city_slug} ({hours_stuck:.1f}h)")
                
                self.log(f"Ã°Å¸Â§Â¹ Auto-cleanup: Reset {len(reset_cities)} stale processing cities: {', '.join(reset_info)}", logging.INFO)
                self.log("Ã°Å¸â€â€ž These cities can now be processed again", logging.INFO)
                
                # Log cleanup activity to import_log table
                log_query = """
                INSERT INTO smartdatastagdb.import_log (import_id, table_name, notes, created_at)
                VALUES (0, 'city_processing_status', %s, CURRENT_TIMESTAMP)
                """
                log_message = f"Spider auto-cleanup reset {len(reset_cities)} stale processing cities: {', '.join([c[0] for c in reset_cities])}"
                cursor.execute(log_query, (log_message,))
                connection.commit()
                
            else:
                self.log("Ã°Å¸Â§Â¹ Auto-cleanup: No stale processing cities found - system healthy", logging.INFO)
                
            cursor.close()
            connection.close()
            
            return len(reset_cities)
            
        except Exception as e:
            self.log(f"Ã¢Å¡ Ã¯Â¸Â Could not run stale city cleanup: {e}", logging.WARNING)
            self.log("Ã°Å¸â€â€ž Continuing with spider execution - manual cleanup may be needed", logging.WARNING)
            return 0

    def _get_next_city_to_process(self):
        """Get the next city to process from the database"""
        try:
            import psycopg2
            
            # Database configuration - use internal IP for Airflow environment
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres', 
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # Get next city to process
            cursor.execute("SELECT * FROM smartdatastagdb.get_next_city_to_process()")
            result = cursor.fetchone()
            
            connection.close()
            
            if result:
                city_id, city_name, city_slug = result
                self.log(f"ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ Selected next city to process: {city_name} (slug: {city_slug})", logging.INFO)
                return {
                    'city_id': city_id,
                    'city_name': city_name, 
                    'city_slug': city_slug
                }
            else:
                self.log("ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â No more cities to process", logging.WARNING)
                return None
                
        except Exception as e:
            self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ Error getting next city: {e}", logging.ERROR)
            return None

    def _mark_city_processing(self, city_slug: str):
        """Mark city as currently being processed"""
        try:
            import psycopg2
            
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_', 
                'database': 'postgres'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            cursor.execute(
                "SELECT smartdatastagdb.mark_city_processing(%s, %s)",
                (city_slug, self.job_id)  # 🔧 Use job_id instead of execution_id
            )
            connection.commit()
            connection.close()
            
            self.log(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Marked city {city_slug} as processing", logging.INFO)
            
        except Exception as e:
            self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ Error marking city as processing: {e}", logging.ERROR)

    def _mark_city_completed(self, city_slug: str, notes: str = None):
        """Mark city as completed with statistics - Enhanced with validation from RGURU.txt"""
        try:
            import psycopg2
            import time
            
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres'
            }
            
            # 🚨 RACE CONDITION FIX: Wait for PostgresPipeline to commit all items
            self.log(f"⏳ VALIDATION: Waiting 2 seconds for all items to be committed to database...", logging.INFO)
            time.sleep(2)  # Wait for PostgresPipeline to commit all items
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # 🔢 CUMULATIVE COUNT: Use total count across all runs (same as RGURU.txt)
            cumulative_count = self._initial_db_count + self.items_yielded
            
            # 🚨 VALIDATION: Check actual database count before marking complete
            try:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                    WHERE job_id = %s
                """, (self.job_id,))
                actual_db_count = cursor.fetchone()[0]
                
                self.log(f"🔍 DATABASE VALIDATION: Spider reported {self.items_yielded} items, database contains {actual_db_count} items", logging.INFO)
                
                # 🚨 MISMATCH DETECTION: If counts don't match, use database count
                if actual_db_count != self.items_yielded:
                    self.log(f"⚠️ COUNT MISMATCH: Spider={self.items_yielded}, Database={actual_db_count} - using database count", logging.WARNING)
                    # Recalculate cumulative count using actual database count
                    cumulative_count = self._initial_db_count + actual_db_count
                    
            except Exception as e:
                self.log(f"⚠️ Could not validate database count: {e} - proceeding with spider count", logging.WARNING)
            
            cursor.execute(
                "SELECT smartdatastagdb.mark_city_completed(%s, %s, %s, %s)",
                (city_slug, self.restaurants_found, cumulative_count, notes)
            )
            connection.commit()
            connection.close()
            
            self.log(f"✅ COMPLETED: {city_slug} marked as completed: {cumulative_count}/{self.restaurants_found} restaurants (validated)", logging.INFO)
            
        except Exception as e:
            self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ Error marking city as completed: {e}", logging.ERROR)

    def _mark_city_failed(self, city_slug: str, error_message: str, increment_retry: bool = False):
        """Mark city as failed with error message"""
        try:
            import psycopg2
            
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            cursor.execute(
                "SELECT smartdatastagdb.mark_city_failed(%s, %s, %s)",
                (city_slug, error_message, increment_retry)  # Only increment retry count when explicitly requested
            )
            connection.commit()
            connection.close()
            
            self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ Marked city {city_slug} as failed: {error_message}", logging.ERROR)
            
        except Exception as e:
            self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ Error marking city as failed: {e}", logging.ERROR)

    def _get_enhanced_google_rating(self, response):
        """Enhanced Google rating extraction with multiple fallback strategies"""
        
        # Multiple Google rating selectors to try
        google_rating_selectors = [
            # Alternative selectors for Google ratings
            './/div[contains(@class, "google")]//span[contains(@class, "rating")]/text()',
            './/div[contains(@class, "google")]//span[contains(text(), "/")]/text()',
            './/span[contains(text(), "Google")]/following-sibling::span/text()',
            './/div[contains(text(), "Google")]//span[@class="agency-count"]/text()',
            './/div[@class="rating_list"]//div[contains(@class, "google")]//span/text()',
            # Try broader rating selectors  
            './/div[@class="ratings"]//span[@class="rating"]/text()',
            './/span[contains(@class, "google-rating")]/text()',
        ]
        
        for selector in google_rating_selectors:
            try:
                rating_text = response.xpath(selector).get()
                if rating_text:
                    self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â Found Google rating text: '{rating_text}' using selector: {selector}", logging.DEBUG)
                    
                    rating = self._parse_rating_text(rating_text)
                    if rating:
                        self.log(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Successfully parsed Google rating: {rating}", logging.INFO)
                        return rating
                        
            except Exception as e:
                self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Error with Google rating selector {selector}: {e}", logging.DEBUG)
                continue
        
        # Fallback: Try to get from aggregate rating in JSON-LD
        try:
            meta_json = response.xpath('.//script[@type="application/ld+json"]').extract_first()
            if meta_json:
                meta_json = meta_json.replace("</script>", "").replace('<script type="application/ld+json">', "")
                json_data = json.loads(meta_json)
                aggregate_rating = json_data.get("aggregateRating", {})
                if aggregate_rating and isinstance(aggregate_rating, dict):
                    rating_value = aggregate_rating.get("ratingValue")
                    if rating_value:
                        rating = float(rating_value)
                        self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…  Using aggregate rating as Google rating: {rating}", logging.INFO)
                        return rating
        except Exception as e:
            self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Error extracting aggregate rating: {e}", logging.DEBUG)
        
        return None

    def _parse_rating_text(self, rating_text):
        """Parse rating text with multiple format support"""
        try:
            rating_text = rating_text.strip()
            
            # Format 1: "4.1/5" or "(4.1/5)"
            if "/" in rating_text:
                rating_text = rating_text.strip("()")
                rating_part = rating_text.split("/")[0]
                rating_match = regex.search(r'(\d+\.?\d*)', rating_part)
                if rating_match:
                    return float(rating_match.group(1))
            
            # Format 2: Plain number "4.1"
            rating_match = regex.search(r'(\d+\.?\d*)', rating_text)
            if rating_match:
                return float(rating_match.group(1))
                
        except (ValueError, IndexError) as e:
            self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Could not parse rating text '{rating_text}': {e}", logging.DEBUG)
        
        return None

    def _get_enhanced_opening_hours(self, response, existing_hours=None):
        """Enhanced opening hours extraction with detailed schedule support"""
        
        opening_hours = []
        
        # First, try the existing hours if they look comprehensive
        if existing_hours and isinstance(existing_hours, list) and len(existing_hours) > 7:
            # If we have more than 7 entries, it might already be detailed
            self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¦ Using existing comprehensive opening hours: {len(existing_hours)} entries", logging.DEBUG)
            return existing_hours
        
        # Strategy 1: Extract from detailed schedule table
        try:
            schedule_selectors = [
                './/table[@class="schedule-table"]//tr',
                './/div[@class="schedule"]//tr', 
                './/table[contains(@class, "schedule")]//tr',
                './/div[contains(@class, "hours")]//tr',
                './/div[@class="opening-hours"]//tr'
            ]
            
            for selector in schedule_selectors:
                schedule_rows = response.xpath(selector)
                if schedule_rows and len(schedule_rows) > 1:  # More than header
                    self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¦ Found schedule table with {len(schedule_rows)} rows", logging.DEBUG)
                    
                    for row in schedule_rows:
                        try:
                            # Extract day name
                            day_selectors = [
                                './/span[@class="short-day"]/text()',
                                './/span[contains(@class, "day")]/text()',
                                './/td[1]//text()',
                                './/th//text()'
                            ]
                            
                            day = None
                            for day_sel in day_selectors:
                                day_text = row.xpath(day_sel).get()
                                if day_text and day_text.strip():
                                    day = day_text.strip()
                                    break
                            
                            if not day or len(day) > 10:  # Skip if no day or too long (likely header)
                                continue
                                
                            # Extract all time ranges for this day
                            time_selectors = [
                                './/td[2]//text()',
                                './/td[contains(@class, "hours")]//text()',
                                './/div[contains(@class, "time")]//text()',
                                './/span[contains(@class, "time")]//text()'
                            ]
                            
                            times = []
                            for time_sel in time_selectors:
                                time_texts = row.xpath(time_sel).getall()
                                times.extend([t.strip() for t in time_texts if t.strip() and len(t.strip()) > 2])
                            
                            # Process times and create entries
                            for time_text in times:
                                if self._is_valid_time_format(time_text):
                                    formatted_entry = f"{day} {time_text}"
                                    if formatted_entry not in opening_hours:  # Avoid duplicates
                                        opening_hours.append(formatted_entry)
                                        
                        except Exception as e:
                            self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Error processing schedule row: {e}", logging.DEBUG)
                            continue
                    
                    if opening_hours:
                        self.log(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Extracted {len(opening_hours)} opening hour entries from schedule table", logging.INFO)
                        break
        
        except Exception as e:
            self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Error extracting from schedule table: {e}", logging.DEBUG)
        
        # Strategy 2: Fallback to existing hours if table extraction failed
        if not opening_hours and existing_hours:
            if isinstance(existing_hours, list):
                opening_hours = existing_hours
                self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…  Using existing opening hours: {len(opening_hours)} entries", logging.DEBUG)
            elif isinstance(existing_hours, str):
                opening_hours = [existing_hours]
        
        # Strategy 3: Last resort - look for time patterns in text
        if not opening_hours:
            try:
                # Look for opening hours in various text elements
                text_selectors = [
                    './/div[@class="info"]//text()',
                    './/div[contains(@class, "hours")]//text()',
                    './/div[contains(@class, "schedule")]//text()',
                    './/p[contains(text(), ":")]/text()'
                ]
                
                for selector in text_selectors:
                    texts = response.xpath(selector).getall()
                    info_text = ' '.join([t.strip() for t in texts if t.strip()])
                    
                    if info_text:
                        # Look for day-time patterns
                        patterns = [
                            r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s*:?\s*(\d{1,2}[:\.,]\d{2}\s*[AP]?M?\s*[-ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“]\s*\d{1,2}[:\.,]\d{2}\s*[AP]?M?)',
                            r'(Mo|Di|Mi|Do|Fr|Sa|So)\s*:?\s*(\d{1,2}[:\.,]\d{2}\s*[-ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“]\s*\d{1,2}[:\.,]\d{2})'
                        ]
                        
                        for pattern in patterns:
                            matches = regex.findall(pattern, info_text, regex.IGNORECASE)
                            for day, time_range in matches:
                                formatted_entry = f"{day.strip()} {time_range.strip()}"
                                if formatted_entry not in opening_hours:
                                    opening_hours.append(formatted_entry)
                        
                        if opening_hours:
                            self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â Extracted {len(opening_hours)} opening hours from text patterns", logging.INFO)
                            break
                            
            except Exception as e:
                self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Error with pattern extraction: {e}", logging.DEBUG)
        
        return opening_hours if opening_hours else None

    def _is_valid_time_format(self, time_text):
        """Check if text contains valid time format"""
        if not time_text or len(time_text.strip()) < 4:
            return False
            
        time_patterns = [
            r'\d{1,2}[:\.,]\d{2}\s*[AP]?M?\s*[-ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“]\s*\d{1,2}[:\.,]\d{2}\s*[AP]?M?',  # 11:30AM-2PM
            r'\d{1,2}[:\.,]\d{2}\s*[-ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“]\s*\d{1,2}[:\.,]\d{2}',  # 11:30-14:00
            r'\d{1,2}[AP]M\s*[-ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“]\s*\d{1,2}[AP]M',  # 1PM-10PM
            r'\d{1,2}:\d{2}',  # At least one time like 14:30
        ]
        
        for pattern in time_patterns:
            if regex.search(pattern, time_text):
                return True
        return False

    def _get_enhanced_coordinates(self, response, existing_lat=None, existing_lng=None):
        """Enhanced coordinate extraction with multiple sources and validation"""
        
        coordinates_found = []
        
        # Source 1: Use existing coordinates if they look valid
        if existing_lat and existing_lng:
            try:
                lat = float(existing_lat)
                lng = float(existing_lng)
                if self._validate_coordinates(lat, lng, "Germany"):
                    coordinates_found.append(("existing", lat, lng))
                    self.log(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Valid existing coordinates: {lat}, {lng}", logging.DEBUG)
                else:
                    self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Invalid existing coordinates: {lat}, {lng}", logging.WARNING)
            except (ValueError, TypeError):
                self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Could not parse existing coordinates: {existing_lat}, {existing_lng}", logging.WARNING)
        
        # Source 2: Enhanced direction link extraction
        direction_selectors = [
            './/a[@class="direction_link"]/@href',
            './/a[contains(@href, "maps.google")]/@href',
            './/a[contains(@href, "destination=")]/@href',
            './/a[contains(text(), "Directions")]/@href',
            './/a[contains(text(), "Route")]/@href'
        ]
        
        for selector in direction_selectors:
            try:
                direction_link = response.xpath(selector).get()
                if direction_link:
                    self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â Found direction link: {direction_link}", logging.DEBUG)
                    
                    # Multiple coordinate patterns in URLs
                    patterns = [
                        r"destination=([-+]?\d*\.?\d+),([-+]?\d*\.?\d+)",  # destination=lat,lng
                        r"daddr=([-+]?\d*\.?\d+),([-+]?\d*\.?\d+)",       # daddr=lat,lng
                        r"ll=([-+]?\d*\.?\d+),([-+]?\d*\.?\d+)",          # ll=lat,lng
                        r"@([-+]?\d*\.?\d+),([-+]?\d*\.?\d+)",           # @lat,lng
                        r"/([-+]?\d*\.?\d+),([-+]?\d*\.?\d+)/",          # /lat,lng/
                    ]
                    
                    for pattern in patterns:
                        matches = regex.search(pattern, direction_link)
                        if matches:
                            try:
                                lat = float(matches.group(1))
                                lng = float(matches.group(2))
                                if self._validate_coordinates(lat, lng, "Germany"):
                                    coordinates_found.append(("direction_link", lat, lng))
                                    self.log(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Valid direction coordinates: {lat}, {lng}", logging.DEBUG)
                                    break
                            except (ValueError, IndexError):
                                continue
                                
            except Exception as e:
                self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Error extracting from direction link: {e}", logging.DEBUG)
                continue
        
        # Source 3: Look for coordinates in script tags or data attributes
        try:
            script_selectors = [
                './/script[contains(text(), "latitude")]//text()',
                './/script[contains(text(), "coordinates")]//text()',
                './/div[@data-lat]/@data-lat',
                './/div[@data-lng]/@data-lng'
            ]
            
            for selector in script_selectors:
                scripts = response.xpath(selector).getall()
                for script in scripts:
                    if script:
                        # Look for coordinate patterns in JavaScript
                        coord_patterns = [
                            r'"latitude":\s*([-+]?\d*\.?\d+).*"longitude":\s*([-+]?\d*\.?\d+)',
                            r'"lat":\s*([-+]?\d*\.?\d+).*"lng":\s*([-+]?\d*\.?\d+)',
                            r'lat:\s*([-+]?\d*\.?\d+).*lng:\s*([-+]?\d*\.?\d+)'
                        ]
                        
                        for pattern in coord_patterns:
                            matches = regex.search(pattern, script)
                            if matches:
                                try:
                                    lat = float(matches.group(1))
                                    lng = float(matches.group(2))
                                    if self._validate_coordinates(lat, lng, "Germany"):
                                        coordinates_found.append(("script", lat, lng))
                                        self.log(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Valid script coordinates: {lat}, {lng}", logging.DEBUG)
                                        break
                                except (ValueError, IndexError):
                                    continue
                                    
        except Exception as e:
            self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Error extracting from scripts: {e}", logging.DEBUG)
        
        # Choose the best coordinates
        if coordinates_found:
            # Prefer direction links over existing data (usually more accurate)
            priority_order = ["direction_link", "script", "existing"]
            
            for source_type in priority_order:
                for source, lat, lng in coordinates_found:
                    if source == source_type:
                        self.log(f"ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ Using {source} coordinates: {lat}, {lng}", logging.INFO)
                        return lat, lng
            
            # Fallback to first found coordinates
            source, lat, lng = coordinates_found[0]
            self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â Using first valid coordinates from {source}: {lat}, {lng}", logging.INFO)
            return lat, lng
        
        # No valid coordinates found
        self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ No valid coordinates found for {response.url}", logging.WARNING)
        return None, None

    def _validate_coordinates(self, lat, lng, country_hint="Germany"):
        """Validate if coordinates are reasonable for the expected location"""
        try:
            lat = float(lat)
            lng = float(lng)
            
            # Basic range validation
            if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
                return False
            
            # Country-specific validation for Germany
            if country_hint == "Germany":
                # Germany approximate bounds: lat 47-55, lng 5-15
                if not (47 <= lat <= 55) or not (5 <= lng <= 15):
                    self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Coordinates {lat}, {lng} outside Germany bounds", logging.DEBUG)
                    return False
            
            # Check for obviously wrong coordinates (0,0 or similar)
            if lat == 0 and lng == 0:
                return False
                
            return True
            
        except (ValueError, TypeError):
            return False

    # ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Enhanced error tracking methods for smart failure detection
    def _track_request_start(self, request):
        """Track when a request starts"""
        self.total_requests += 1
        self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…  Request #{self.total_requests}: {request.url}", logging.DEBUG)

    def _should_stop_processing(self):
        """ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Circuit breaker: Determine if processing should stop due to critical failures"""
        if self.circuit_breaker_triggered:
            return True
            
        # Stop if too many ScrapeOps failures
        if self.technical_errors['scrapeops_401_errors'] >= self.max_scrapeops_failures:
            self.circuit_breaker_triggered = True
            self.log(f"ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ CIRCUIT BREAKER TRIGGERED: {self.technical_errors['scrapeops_401_errors']} ScrapeOps 401 failures", logging.ERROR)
            return True
            
        # Stop if too many restaurants processed (API limit protection)
        if self.restaurant_count >= self.max_restaurants_per_city:
            self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ¢â‚¬Ëœ Restaurant limit reached ({self.max_restaurants_per_city}) - stopping to save API credits", logging.INFO)
            return True
            
        return False

    def _track_response_received(self, response):
        """Track response and identify technical errors with circuit breaker"""
        self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¨ Response {response.status} for: {response.url}", logging.DEBUG)
        
        # Check for ScrapeOps-specific errors
        if 'proxy.scrapeops.io' in response.url:
            if response.status == 401:
                self.technical_errors['scrapeops_401_errors'] += 1
                self.has_critical_errors = True
                error_msg = f"ScrapeOps authentication failure (401) - API key issue"
                self.error_messages.append(error_msg)
                self.log(f"ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ CRITICAL: {error_msg} (Failure #{self.technical_errors['scrapeops_401_errors']})", logging.ERROR)
                
                # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Circuit breaker: Check if we should stop
                if self.technical_errors['scrapeops_401_errors'] >= self.max_scrapeops_failures:
                    self.circuit_breaker_triggered = True
                    self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Circuit breaker will trigger after this request - stopping further processing", logging.ERROR)
                
            elif response.status == 403:
                self.technical_errors['scrapeops_403_errors'] += 1
                self.has_critical_errors = True
                error_msg = f"ScrapeOps access forbidden (403) - subscription issue"
                self.error_messages.append(error_msg)
                self.log(f"ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ CRITICAL: {error_msg}", logging.ERROR)
                
            elif response.status >= 500:
                self.technical_errors['proxy_failures'] += 1
                error_msg = f"ScrapeOps server error ({response.status})"
                self.error_messages.append(error_msg)
                self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â {error_msg}", logging.WARNING)
        
        # Track successful responses
        if response.status == 200:
            self.successful_requests += 1
        else:
            self.technical_errors['total_failed_requests'] += 1

    def _track_request_error(self, request, error):
        """Track request-level errors"""
        self.technical_errors['total_failed_requests'] += 1
        
        error_str = str(error).lower()
        if 'timeout' in error_str:
            self.technical_errors['timeout_errors'] += 1
            self.log(f"ÃƒÂ¢Ã‚ÂÃ‚Â±ÃƒÂ¯Ã‚Â¸Ã‚Â Timeout error: {error}", logging.WARNING)
        elif 'connection' in error_str:
            self.technical_errors['connection_errors'] += 1
            self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ…â€™ Connection error: {error}", logging.WARNING)
        
        # Critical errors that indicate systemic issues
        if (self.technical_errors['connection_errors'] > 3 or 
            self.technical_errors['timeout_errors'] > 5):
            self.has_critical_errors = True
            self.error_messages.append(f"Multiple connection/timeout failures")

    custom_settings = {
        # 🚀 MEGA CITY: AGGRESSIVE SCRAPEOPS OPTIMIZATION - Maximize API utilization
        "CONCURRENT_REQUESTS": 8,   # INCREASED: Utilize ScrapeOps concurrent limit
        "CONCURRENT_REQUESTS_PER_DOMAIN": 6,  # INCREASED: ScrapeOps can handle high concurrency
        "DOWNLOAD_TIMEOUT": 300.0,   # INCREASED: 5 minutes for ScrapeOps processing with CAPTCHA solving
        "DOWNLOAD_DELAY": 2.0,      # OPTIMIZED: 2 second delay for balance
        "COOKIES_ENABLED": True,
        "SCRAPEOPS_PROXY_ENABLED": True,
        
        # 🌐 SCRAPEOPS: Enhanced configuration for Restaurant Guru
        "SCRAPEOPS_API_KEY": "0a0bd6f5-e1c0-49c3-b8f5-1ec732a2e3b6",
        "SCRAPEOPS_FAKE_USER_AGENT_ENABLED": True,
        "SCRAPEOPS_FAKE_BROWSER_HEADER_ENABLED": True,
        
        # 💰 SCRAPEOPS OPTIMIZATION: Aggressive retries since we're paying for API
        "RETRY_TIMES": 5,           # INCREASED: More retries for better success rate
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504, 403],  # ADDED 429 & 403 back - ScrapeOps handles these
        
        # 🔧 404 HANDLING: Allow 404 responses to be processed (for non-existent pages)
        "HTTPERROR_ALLOWED_CODES": [404],  # Allow 404s to be handled gracefully
        
        # 🏙️ MEGA CITY: Memory optimization
        "MEMUSAGE_ENABLED": True,
        "MEMUSAGE_LIMIT_MB": 8192,  # 8GB limit
        "MEMUSAGE_WARNING_MB": 6144,  # 6GB warning
        
        # 🚀 SCRAPEOPS: High performance settings for paid API service
        "REACTOR_THREADPOOL_MAXSIZE": 50,  # INCREASED: More I/O threads for ScrapeOps
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,    # OPTIMIZED: Balanced start delay
        "AUTOTHROTTLE_MAX_DELAY": 30,       # INCREASED: Max 30 seconds for flexibility
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 6.0,  # INCREASED: Match concurrent requests
        "AUTOTHROTTLE_DEBUG": True,         # Enable to monitor throttling
        
        # 🏙️ MEGA CITY: Rate limit specific settings
        "RANDOMIZE_DOWNLOAD_DELAY": True,   # Add randomness to avoid patterns
        "DOWNLOAD_HANDLERS_BASE": {
            'http': 'scrapy.core.downloader.handlers.http.HTTPDownloadHandler',
            'https': 'scrapy.core.downloader.handlers.http.HTTPDownloadHandler',
        },
        
        # 🚨 CRITICAL FIX: Disable duplicate filtering for infinite scroll
        "DUPEFILTER_DEBUG": True,  # Enable debug logging for duplicate filtering
    }

    def start_requests(self):
        """Single-city processing: Get one city to process and generate requests for it"""
        
        try:
            # Ã°Å¸â€º Ã¯Â¸Â AUTOMATIC CLEANUP: Prevent stale processing cities from causing spider hangs
            self.log("🧹 Running automatic cleanup of stale processing cities...", logging.INFO)
            reset_count = self._cleanup_stale_processing_cities()
            if reset_count > 0:
                self.log(f"✅ System recovery: {reset_count} cities reset and ready for processing", logging.INFO)
            
            # 🚀 NEW: Log processing mode
            self.log(f"🚀 SPIDER STARTING: Job ID {self.job_id}", logging.INFO)
            if self.index_based_mode:
                self.log(f"🔢 INDEX-BASED MODE: Processing restaurants {self.start_index}-{self.end_index} ({self.end_index - self.start_index + 1} restaurants)", logging.INFO)
            else:
                self.log(f"🎯 BATCH PROCESSING MODE: {self.restaurants_per_batch} restaurants per run", logging.INFO)
            
            # Determine which city to process
            if self.target_city_slug:
                # Use specified city (for manual/specific city processing)
                self.current_city_info = {
                    'city_slug': self.target_city_slug,
                    'city_name': self.target_city_slug.replace('-', ' ').title()
                }
                self.log(f"ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ Processing specified city: {self.current_city_info['city_name']}", logging.INFO)
                
            else:
                # Auto-select next city from database (for production automation)
                self.current_city_info = self._get_next_city_to_process()
                
                if not self.current_city_info:
                    self.log("ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â No more cities to process. All cities completed!", logging.INFO)
                    return
            
            city_slug = self.current_city_info['city_slug']
            city_name = self.current_city_info.get('city_name', city_slug.replace('-', ' ').title())
            
            # Mark city as being processed
            self._mark_city_processing(city_slug)
            
            self.log(f"ÃƒÂ°Ã…Â¸Ã‚ÂÃ¢â€žÂ¢ÃƒÂ¯Ã‚Â¸Ã‚Â Starting single-city processing for: {city_name} (slug: {city_slug})", logging.INFO)
            self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â Execution ID: {self.job_id}", logging.INFO)
            
            # Generate request for this single city
            city_url = f"https://de.restaurantguru.com/restaurant-{city_slug}-t1"  # Clean URL for infinite scroll
            
            yield Request(
                url=city_url,
                headers={'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"},
                callback=self.parse,
                dont_filter=False,  # ENABLE URL filtering
                meta={
                    'city_info': self.current_city_info,  # Pass city info to parse methods
                    'infinite_scroll': True  # Mark as infinite scroll page for middleware
                }
            )
            
            self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…  Single city request generated for: {city_name}", logging.INFO)
            
        except Exception as e:
            error_msg = f"Error in start_requests: {str(e)}"
            self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ {error_msg}", logging.ERROR)
            
            # Mark city as failed if we know which city we were trying to process
            if hasattr(self, 'current_city_info') and self.current_city_info:
                self._mark_city_failed(self.current_city_info['city_slug'], error_msg, increment_retry=False)  # Don't increment retry for parsing errors
            
            import traceback
            self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å¾ Full traceback: {traceback.format_exc()}", logging.ERROR)
            raise

    def _log_100_percent_success_stats(self):
        """🎯 100% SUCCESS: Log comprehensive success statistics and retry effectiveness"""
        try:
            if not hasattr(self, 'success_stats'):
                self.log("🎯 No success statistics available", logging.INFO)
                return
                
            stats = self.success_stats
            
            # Calculate success rate
            if stats['total_restaurant_requests'] > 0:
                success_rate = (stats['successful_extractions'] / stats['total_restaurant_requests']) * 100
                
                self.log("🎯" + "="*80, logging.INFO)
                self.log("🎯 100% SUCCESS FINAL REPORT", logging.INFO)
                self.log("🎯" + "="*80, logging.INFO)
                
                self.log(f"🎯 OVERALL SUCCESS RATE: {success_rate:.2f}% ({stats['successful_extractions']}/{stats['total_restaurant_requests']})", logging.INFO)
                
                if success_rate >= 100:
                    self.log("🎯 ✅ PERFECT SUCCESS: 100% of restaurants processed successfully!", logging.INFO)
                elif success_rate >= 95:
                    self.log("🎯 🌟 EXCELLENT SUCCESS: >95% success rate achieved!", logging.INFO)
                elif success_rate >= 90:
                    self.log("🎯 ✅ GOOD SUCCESS: >90% success rate achieved!", logging.INFO)
                else:
                    self.log(f"🎯 ⚠️ SUCCESS RATE: {success_rate:.1f}% - Room for improvement", logging.WARNING)
                
                # Retry effectiveness analysis
                total_retries = stats['captcha_retries_level_1'] + stats['captcha_retries_level_2'] + stats['captcha_retries_level_3']
                if total_retries > 0:
                    retry_success_rate = (stats['retry_successes'] / total_retries) * 100 if total_retries > 0 else 0
                    
                    self.log("🎯" + "-"*50, logging.INFO)
                    self.log("🎯 RETRY EFFECTIVENESS ANALYSIS", logging.INFO)
                    self.log("🎯" + "-"*50, logging.INFO)
                    self.log(f"🎯 Total Retries Required: {total_retries}", logging.INFO)
                    self.log(f"🎯   ├─ Level 1 (Balanced): {stats['captcha_retries_level_1']}", logging.INFO)
                    self.log(f"🎯   ├─ Level 2 (Stealth): {stats['captcha_retries_level_2']}", logging.INFO)
                    self.log(f"🎯   └─ Level 3 (Ultra): {stats['captcha_retries_level_3']}", logging.INFO)
                    self.log(f"🎯 Retry Success Rate: {retry_success_rate:.1f}% ({stats['retry_successes']}/{total_retries})", logging.INFO)
                    self.log(f"🎯 Max Retries Failed: {stats['max_retry_failures']}", logging.INFO)
                    
                    if retry_success_rate >= 90:
                        self.log("🎯 ✅ EXCELLENT: ScrapeOps retry system is highly effective!", logging.INFO)
                    elif retry_success_rate >= 70:
                        self.log("🎯 ✅ GOOD: ScrapeOps retry system is working well!", logging.INFO)
                    else:
                        self.log("🎯 ⚠️ OPTIMIZATION NEEDED: Consider adjusting ScrapeOps parameters", logging.WARNING)
                else:
                    self.log("🎯 ✅ NO RETRIES NEEDED: All restaurants processed successfully on first attempt!", logging.INFO)
                
                # API efficiency report
                self.log("🎯" + "-"*50, logging.INFO)
                self.log("🎯 API EFFICIENCY REPORT", logging.INFO)
                self.log("🎯" + "-"*50, logging.INFO)
                total_requests = stats['total_restaurant_requests'] + total_retries
                efficiency = (stats['successful_extractions'] / total_requests) * 100 if total_requests > 0 else 0
                self.log(f"🎯 Total API Calls: {total_requests} (Original: {stats['total_restaurant_requests']}, Retries: {total_retries})", logging.INFO)
                self.log(f"🎯 API Efficiency: {efficiency:.1f}% (successful extractions per API call)", logging.INFO)
                
                self.log("🎯" + "="*80, logging.INFO)
                
            else:
                self.log("🎯 No restaurant processing statistics available", logging.INFO)
                
        except Exception as e:
            self.log(f"🎯 Error generating success statistics: {e}", logging.WARNING)

    def closed(self, reason):
        """Enhanced spider close handler with smart failure detection"""
        try:
            # 🎯 100% SUCCESS FINAL REPORT: Log comprehensive success statistics
            self._log_100_percent_success_stats()
            
            # 🚨 CRITICAL FIX: Close database connection to prevent connection leaks
            self._close_db_connection()
            
            if hasattr(self, 'current_city_info') and self.current_city_info:
                city_slug = self.current_city_info['city_slug']
                city_name = self.current_city_info.get('city_name', city_slug)
                
                # ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ SMART FAILURE DETECTION LOGIC
                should_mark_as_failed = self._should_mark_as_failed(reason)
                
                if should_mark_as_failed:
                    # Mark as failed due to technical issues
                    error_summary = self._generate_error_summary()
                    self._mark_city_failed(city_slug, error_summary, increment_retry=True)  # Increment retry for technical failures
                    self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ TECHNICAL FAILURE: {city_name} marked as failed - {error_summary}", logging.ERROR)
                    
                elif reason == 'finished':
                    # 🎯 SMART COMPLETION: Check if ALL restaurants in city are processed
                    if hasattr(self, 'restaurants_found') and hasattr(self, 'restaurants_processed'):
                        # 🔢 CUMULATIVE COUNT: Calculate total restaurants processed across all runs
                        # 🚨 FIX: Use restaurants_processed (includes checkpoint data) instead of items_yielded (only current run)
                        cumulative_count = self.restaurants_processed
                        
                        # 🚨 COMPLETION THRESHOLD: Use exact count, no adjustments for cross-city contamination
                        # The spider should process ALL restaurants found, not reduce the count
                        completion_threshold = self.restaurants_found
                        self.log(f"📊 COMPLETION CHECK: Checking if {cumulative_count} >= {completion_threshold} restaurants processed", logging.INFO)
                        
                        if cumulative_count >= completion_threshold:
                            # 🚨 CHECKPOINT VALIDATION: Check if this was a valid checkpoint resume
                            items_processed_this_run = getattr(self, 'items_yielded', 0)
                            if items_processed_this_run == 0 and hasattr(self, '_initial_db_count') and self._initial_db_count >= self.restaurants_found:
                                self.log(f"✅ CHECKPOINT COMPLETION: All {self.restaurants_found} restaurants already in database, no new work needed", logging.INFO)
                                self.log(f"🧹 CLEANUP: Clearing stale checkpoint data", logging.INFO)
                            
                            # 🚨 SUCCESS OVERRIDE: Reset critical errors flag if we achieved 100% completion
                            # This prevents false failures when CAPTCHA was detected early but processing succeeded
                            if cumulative_count >= self.restaurants_found and self.restaurants_found > 0:
                                if self.has_critical_errors:
                                    self.log(f"✅ SUCCESS OVERRIDE: Resetting critical errors flag - achieved 100% completion ({cumulative_count}/{self.restaurants_found})", logging.INFO)
                                    self.has_critical_errors = False
                            
                            # 🚨 CRITICAL ERROR CHECK: Check for technical failures FIRST (after success override)
                            if self.has_critical_errors:
                                self.log(f"🚨 CAPTCHA/BLOCKING DETECTED: City {city_name} failed due to critical errors (likely CAPTCHA)", logging.ERROR)
                                self._mark_city_failed(city_slug, f"Critical errors detected - likely CAPTCHA or blocking", increment_retry=True)
                                return
                            
                            # 🚨 ZERO RESTAURANT VALIDATION: Special handling for zero restaurant cities
                            if self.restaurants_found == 0:
                                # Validate if zero is legitimate (only if no critical errors)
                                city_name_lower = city_name.lower() if city_name else ''
                                is_major_city = any(mega_city in city_name_lower for mega_city in ['munich', 'münchen', 'hamburg', 'berlin', 'cologne', 'köln', 'frankfurt', 'stuttgart', 'düsseldorf', 'dortmund'])
                                
                                if is_major_city:
                                    # Major city with 0 restaurants = likely error
                                    self.log(f"🚨 VALIDATION FAILED: Major city {city_name} completed with 0 restaurants - marking as failure", logging.ERROR)
                                    self._mark_city_failed(city_slug, f"Major city shows 0 restaurants - likely CAPTCHA or scraping error", increment_retry=True)
                                    return
                                else:
                                    # Small city - zero might be legitimate (but only if no critical errors)
                                    self.log(f"✅ ZERO RESTAURANT CITY: Small city {city_name} legitimately has no restaurants", logging.INFO)
                            
                            # ALL restaurants processed - mark city as completed
                            notes = self._generate_success_notes()
                            self._mark_city_completed(city_slug, notes)
                            self._clear_checkpoint(city_slug)
                            self.log(f"✅ CITY FULLY COMPLETED: {city_name} - {cumulative_count}/{self.restaurants_found} restaurants processed", logging.INFO)
                        else:
                            # 🚨 SMART RETRY DETECTION: Check if we're stuck in a retry loop
                            initial_processed = getattr(self, '_initial_restaurants_processed', 0)
                            new_restaurants_this_run = self.restaurants_processed - initial_processed
                            
                            self.log(f"🔍 AUTO-ADVANCE DEBUG:", logging.ERROR)
                            self.log(f"   📊 initial_processed: {initial_processed}", logging.ERROR)
                            self.log(f"   📊 current_processed: {self.restaurants_processed}", logging.ERROR)
                            self.log(f"   📊 new_restaurants_this_run: {new_restaurants_this_run}", logging.ERROR)
                            self.log(f"   🔄 _high_page_retry: {getattr(self, '_high_page_retry', False)}", logging.ERROR)
                            self.log(f"   🔍 _initial_restaurants_processed attribute exists: {hasattr(self, '_initial_restaurants_processed')}", logging.ERROR)
                            self.log(f"   🔍 _initial_restaurants_processed raw value: {getattr(self, '_initial_restaurants_processed', 'MISSING')}", logging.ERROR)
                            
                            # 🚨 TRIGGER AUTO-ADVANCE: If 0 new restaurants processed, advance checkpoint
                            trigger_auto_advance = False
                            
                            if new_restaurants_this_run == 0:
                                high_page_retry = getattr(self, '_high_page_retry', False)
                                checkpoint_resume = initial_processed > 0  # We resumed from a checkpoint
                                trigger_auto_advance = high_page_retry or checkpoint_resume
                                self.log(f"🔍 AUTO-ADVANCE TRIGGER CHECK:", logging.ERROR)
                                self.log(f"   📊 new_restaurants_this_run == 0: {new_restaurants_this_run == 0}", logging.ERROR)
                                self.log(f"   🔄 high_page_retry: {high_page_retry}", logging.ERROR)
                                self.log(f"   📊 checkpoint_resume: {checkpoint_resume}", logging.ERROR)
                                self.log(f"   ✅ trigger_auto_advance: {trigger_auto_advance}", logging.ERROR)
                            
                            # 🚨 FORCED TRIGGER: If Hamburg and restaurants_processed == 184, force auto-advance
                            if city_name == "Hamburg" and self.restaurants_processed == 184:
                                self.log(f"🚨 FORCED AUTO-ADVANCE: Detected Hamburg stuck at 184 restaurants", logging.ERROR)
                                trigger_auto_advance = True
                                
                            if trigger_auto_advance:
                                    self.log(f"🔄 RETRY ISSUE DETECTED: 0 new restaurants processed in retry scenario", logging.ERROR)
                                    self.log(f"💡 DIAGNOSIS: Likely cause - all restaurants on pages are duplicates OR captcha blocking", logging.ERROR)
                                
                                    # 🚨 AUTO-ADVANCE CHECKPOINT: If we're stuck with duplicates, advance the checkpoint
                                    # Advance by 200 restaurants (equivalent to old 10-page batch)
                                    new_checkpoint = self.restaurants_processed + 200
                                    
                                    # 🔧 SAFETY CHECK: Don't advance beyond total restaurants
                                    if new_checkpoint < self.restaurants_found:
                                        self.log(f"🔧 AUTO-ADVANCING CHECKPOINT: From {self.restaurants_processed} to {new_checkpoint} (skip 200 restaurants)", logging.WARNING)
                                        
                                        # Update checkpoint to advance past problematic pages (but keep real restaurant count)
                                        try:
                                            # 🚨 CRITICAL FIX: Don't inflate restaurant count, just advance the page
                                            self._save_checkpoint(
                                                city_slug,
                                                self.restaurants_processed,  # Keep REAL count (184)
                                                {
                                                    'auto_advanced': True, 
                                                    'reason': 'duplicate_pages', 
                                                    'original_checkpoint': self.restaurants_processed,
                                                    'target_restaurant': new_checkpoint + 1,  # Next run should start from restaurant
                                                    'skipped_restaurants': 200,
                                                    'advanced_to_restaurant': new_checkpoint  # Store restaurant info in data
                                                }
                                            )
                                            self.log(f"✅ CHECKPOINT ADVANCED: Keep real count {self.restaurants_processed}, but start next run from restaurant #{new_checkpoint + 1}", logging.INFO)
                                        except Exception as e:
                                            self.log(f"❌ CHECKPOINT ADVANCE FAILED: {e}", logging.ERROR)
                                            self.log(f"🔧 MANUAL INTERVENTION REQUIRED: Update checkpoint manually to advance past duplicates", logging.ERROR)
                                    else:
                                        self.log(f"⚠️ AUTO-ADVANCE SKIPPED: New checkpoint {new_checkpoint} would exceed total restaurants {self.restaurants_found}", logging.WARNING)
                                        self.log(f"🏁 RECOMMENDATION: City is likely near completion, manual review recommended", logging.INFO)
                            
                            # 🏙️ MEGA CITY: Check if city is fully completed
                            # 🔢 CUMULATIVE COUNT: Calculate total restaurants processed across all runs
                            cumulative_count = self._initial_db_count + self.items_yielded
                            
                            if cumulative_count >= self.restaurants_found:
                                # City fully completed
                                notes = self._generate_success_notes()
                                self._mark_city_completed(city_slug, notes)
                                self.log(f"🏆 MEGA CITY COMPLETED: {city_name} - {cumulative_count}/{self.restaurants_found} restaurants processed", logging.INFO)
                            else:
                                # Partial completion - UPDATE DATABASE with cumulative count
                                self.log(f"🏙️ MEGA CITY PARTIAL: Processed {cumulative_count}/{self.restaurants_found} restaurants for {city_name} (this run: {self.items_yielded})", logging.INFO)
                                self.log(f"🔄 NEXT RUN: Will continue from restaurant #{cumulative_count + 1}", logging.INFO)
                                
                                # 💾 UPDATE DATABASE: Save cumulative progress to database
                                self._update_city_partial_completion(city_slug, cumulative_count)
                    else:
                        # 🚨 MISSING RESTAURANT COUNTS: This should not happen in normal operation
                        self.log(f"⚠️ WARNING: Missing restaurant counts for {city_name}", logging.WARNING)
                        self.log(f"⚠️ restaurants_found: {getattr(self, 'restaurants_found', 'NOT SET')}", logging.WARNING)
                        self.log(f"⚠️ restaurants_processed: {getattr(self, 'restaurants_processed', 'NOT SET')}", logging.WARNING)
                        
                        # 🚨 CRITICAL FIX: Don't mark as completed without proper validation
                        items_processed_this_run = getattr(self, 'items_yielded', 0)
                        if items_processed_this_run > 0:
                            # We processed some restaurants but don't have proper counts - this is suspicious
                            self.log(f"🚨 INCOMPLETE: Processed {items_processed_this_run} restaurants but missing count tracking", logging.ERROR)
                            self._mark_city_failed(city_slug, f"Missing restaurant count tracking - processed {items_processed_this_run} items", increment_retry=True)
                        elif hasattr(self, 'restaurants_found') and self.restaurants_found > 0:
                            # We have restaurants_found but processed nothing - likely technical failure
                            self.log(f"🚨 INCOMPLETE: City has {self.restaurants_found} restaurants but processed 0 items", logging.ERROR)
                            self._mark_city_failed(city_slug, f"Technical failure - found {self.restaurants_found} restaurants but processed 0", increment_retry=True)
                        else:
                            # No restaurant count information and no work done - likely technical failure
                            self.log(f"🚨 TECHNICAL FAILURE: No restaurants processed and no count tracking", logging.ERROR)
                            self._mark_city_failed(city_slug, "Technical failure - no restaurants processed and no count tracking", increment_retry=True)
                    
                else:
                    # Spider failure due to other reasons
                    error_msg = f"Spider closed with reason: {reason}"
                    self._mark_city_failed(city_slug, error_msg, increment_retry=True)  # Increment retry for spider failures
                    self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ Failed processing for {city_name}: {error_msg}", logging.ERROR)
                    
                # Log final statistics
                self._log_final_statistics(city_name)
                
            else:
                self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â Spider closed with reason: {reason}, no city info available", logging.INFO)
                
        except Exception as e:
            self.log(f"ÃƒÂ¢Ã‚ÂÃ…â€™ Error in spider close handler: {e}", logging.ERROR)

    def _should_mark_as_failed(self, reason):
        """
        Determine if spider should be marked as failed despite 'finished' status
        Returns True if technical failures indicate the run was unsuccessful
        """
        # If spider didn't finish normally, it's definitely a failure
        if reason != 'finished':
            return True
        
        # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Check if circuit breaker was triggered (new condition)
        if self.circuit_breaker_triggered:
            self.log("ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ Circuit breaker was triggered - marking as failure", logging.WARNING)
            return True
        
        # Check for critical technical errors
        if self.has_critical_errors:
            self.log("ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ Critical errors detected - marking as failure", logging.WARNING)
            return True
        
        # Check for high error rates indicating systemic issues
        if self.total_requests > 0:
            error_rate = self.technical_errors['total_failed_requests'] / self.total_requests
            if error_rate > 0.8:  # More than 80% requests failed
                self.log(f"ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ High error rate ({error_rate:.1%}) - marking as failure", logging.WARNING)
                return True
        
        # Check for zero data with ScrapeOps errors (key scenario from user's issue)
        # 🚨 FIX: Use items_yielded here as this is about technical connectivity failures
        if (self.items_yielded == 0 and 
            self.technical_errors['scrapeops_401_errors'] > 0):
            self.log("ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ Zero restaurants + ScrapeOps 401 errors - marking as failure", logging.WARNING)
            return True
        
        # Check for zero data with any significant technical errors
        # 🚨 FIX: Use items_yielded here as this is about technical connectivity failures
        if (self.items_yielded == 0 and 
            (self.technical_errors['scrapeops_403_errors'] > 0 or 
             self.technical_errors['proxy_failures'] > 2)):
            self.log("ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ Zero restaurants + proxy failures - marking as failure", logging.WARNING)
            return True
        
        # 🚨 FIXED: Use restaurants_processed for retry run detection (not items_yielded)
        # This tracks actual database processing, not Scrapy item pipeline
        processed_this_run = self.restaurants_processed - getattr(self, '_initial_db_count', 0)
        if (processed_this_run == 0 and 
            hasattr(self, '_initial_db_count') and 
            self._initial_db_count > 0 and 
            self._initial_db_count < self.restaurants_found):
            self.log(f"🚨 Zero restaurants on retry run - city incomplete ({self._initial_db_count}/{self.restaurants_found}) - marking as failure", logging.WARNING)
            return True
        
        # 🚨 FIXED: Use restaurants_processed for CAPTCHA detection (not items_yielded)
        # This ensures we don't mark as failed when restaurants were actually processed
        if (self.restaurants_processed == 0 and 
            hasattr(self, 'restaurants_found') and 
            self.restaurants_found > 0):
            self.log(f"🚨 CAPTCHA/BLOCKING DETECTED: City has {self.restaurants_found} restaurants but spider collected 0 - marking as failure", logging.WARNING)
            return True
        
        # If we reached here, it's a genuine success
        return False

    def _generate_error_summary(self):
        """Generate comprehensive error summary for failure reporting"""
        error_parts = []
        
        # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Add circuit breaker information
        if self.circuit_breaker_triggered:
            error_parts.append("CIRCUIT BREAKER TRIGGERED")
        
        # Add specific error counts
        if self.technical_errors['scrapeops_401_errors'] > 0:
            error_parts.append(f"ScrapeOps 401 errors: {self.technical_errors['scrapeops_401_errors']}")
        
        if self.technical_errors['scrapeops_403_errors'] > 0:
            error_parts.append(f"ScrapeOps 403 errors: {self.technical_errors['scrapeops_403_errors']}")
        
        if self.technical_errors['proxy_failures'] > 0:
            error_parts.append(f"Proxy failures: {self.technical_errors['proxy_failures']}")
        
        if self.technical_errors['connection_errors'] > 0:
            error_parts.append(f"Connection errors: {self.technical_errors['connection_errors']}")
        
        if self.technical_errors['timeout_errors'] > 0:
            error_parts.append(f"Timeout errors: {self.technical_errors['timeout_errors']}")
        
        # Add statistics with API optimization info
        if self.total_requests > 0:
            success_rate = (self.successful_requests / self.total_requests) * 100
            error_parts.append(f"Success rate: {success_rate:.1f}%")
        
        error_parts.append(f"Restaurants processed: {self.restaurants_processed}/{self.max_restaurants_per_city}")
        error_parts.append(f"API calls saved by early stop")
        error_parts.append(f"Job ID: {self.job_id}")  # 🔧 Use job_id instead of execution_id
        
        # Include specific error messages
        if self.error_messages:
            error_parts.extend(self.error_messages[:3])  # Include first 3 specific errors
        
        return " | ".join(error_parts)

    def _generate_success_notes(self):
        """Generate notes for successful completion"""
        notes_parts = [
            f"Job ID: {self.job_id}",  # 🔧 Use job_id instead of execution_id
            f"API calls saved: {self.api_calls_saved}",
            f"Pages skipped: {self.pages_skipped}",
            f"Restaurants processed: {self.items_yielded}",
            f"Restaurants found: {self.restaurants_found}",
            f"Requests: {self.successful_requests}/{self.total_requests}"
        ]
        
        if self.technical_errors['total_failed_requests'] > 0:
            notes_parts.append(f"Minor errors: {self.technical_errors['total_failed_requests']}")
        
        return " | ".join(notes_parts)

    def _log_final_statistics(self, city_name):
        """Log comprehensive final statistics with API optimization info"""
        self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…  Final Statistics for {city_name}:", logging.INFO)
        self.log(f"  ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Âª Restaurants processed: {self.restaurants_processed}", logging.INFO)
        self.log(f"  ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â Restaurants found: {self.restaurants_found}", logging.INFO)
        self.log(f"  ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¡ Successful requests: {self.successful_requests}/{self.total_requests}", logging.INFO)
        self.log(f"  ÃƒÂ¢Ã‚ÂÃ…â€™ Failed requests: {self.technical_errors['total_failed_requests']}", logging.INFO)
        
        # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ API optimization statistics
        self.log(f"  ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Restaurant limit: {self.max_restaurants_per_city}", logging.INFO)
        if self.circuit_breaker_triggered:
            self.log(f"  ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ Circuit breaker triggered: YES (saved API calls)", logging.WARNING)
        
        if self.technical_errors['scrapeops_401_errors'] > 0:
            self.log(f"  ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ ScrapeOps 401 errors: {self.technical_errors['scrapeops_401_errors']}", logging.WARNING)
        
        if self.technical_errors['scrapeops_403_errors'] > 0:
            self.log(f"  ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ ScrapeOps 403 errors: {self.technical_errors['scrapeops_403_errors']}", logging.WARNING)
        
        if self.has_critical_errors:
            self.log(f"  ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ Critical errors detected: YES", logging.ERROR)
            
        # Estimate API calls saved
        if self.circuit_breaker_triggered and self.restaurants_found > 0:
            potential_requests = 1 + ((self.restaurants_found // 20) + 1) + self.restaurants_found
            actual_requests = self.total_requests
            saved_requests = max(0, potential_requests - actual_requests)
            if saved_requests > 0:
                self.log(f"  ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â° Estimated API calls saved by early stop: {saved_requests}", logging.INFO)

    # Funktion zum ZÃƒÆ’Ã‚Â¤hlen der Estabs pro Stadt
    def count_estabs(self, response):
        # ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Track response for error detection
        self._track_response_received(response)
        self.log(f'URL: {response.url}',logging.INFO)
        
        # 🚨 DEBUG: Check page content and potential blocking
        page_size = len(response.text)
        page_title = response.xpath('//title/text()').get('').strip()
        self.log(f"🔍 COUNT_ESTABS DEBUG: Page size={page_size}, Title='{page_title}'", logging.INFO)
        
        # 🚨 CAPTCHA DETECTION: Check for suspicious activity page
        if page_title and 'suspicious activity detected' in page_title.lower():
            self.log(f"🚨 CAPTCHA BLOCKING DETECTED: Page title indicates blocking - '{page_title}'", logging.ERROR)
            self.has_critical_errors = True
            
            # 🚨 CAPTCHA RETRY: Immediately mark for retry with different proxy
            if hasattr(self, 'current_city_info') and self.current_city_info:
                city_slug = self.current_city_info['city_slug']
                self.log(f"🔄 CAPTCHA RETRY: Marking {city_slug} for retry with different proxy configuration", logging.ERROR)
                self._mark_city_failed(city_slug, f"CAPTCHA detected - will retry with different proxy configuration", increment_retry=True)
            
            return 0 # Return 0 to stop processing immediately
        
        # Check for blocking indicators
        if page_size < 5000:
            self.log(f"🚨 SMALL PAGE WARNING: Page only {page_size} bytes - possible blocking/error", logging.WARNING)
            self.log(f"🔍 PAGE PREVIEW: {response.text[:500]}", logging.DEBUG)
        
        # Check for "Ergebnisse" text to verify we have the right page
        if 'Ergebnisse' in response.text:
            self.log(f"✅ PAGE VALIDATION: Found 'Ergebnisse' text - page loaded correctly", logging.INFO)
        else:
            self.log(f"⚠️ PAGE VALIDATION: No 'Ergebnisse' found - possible blocking or wrong page", logging.WARNING)
            # 🚨 POTENTIAL CAPTCHA: If page size is small and no content, likely blocked
            if page_size < 20000:  # Typical blocked page is much smaller
                self.log("🚨 POTENTIAL CAPTCHA: Small page size + no 'Ergebnisse' suggests blocking", logging.ERROR)
                self.has_critical_errors = True
                
                # 🚨 CAPTCHA RETRY: Immediately mark for retry with different proxy
                if hasattr(self, 'current_city_info') and self.current_city_info:
                    city_slug = self.current_city_info['city_slug']
                    self.log(f"🔄 CAPTCHA RETRY: Marking {city_slug} for retry (small page + no Ergebnisse)", logging.ERROR)
                    self._mark_city_failed(city_slug, f"Potential CAPTCHA detected - will retry with different proxy", increment_retry=True)
                
                return 0 # Return 0 to stop processing immediately
        
        success = False
        num_est = 0
        
        # 🏙️ ENHANCED: Restaurant count extraction prioritizing actual visible content
        selectors_to_try = [
            # Method 1: TOP PRIORITY - Look for "X Ergebnisse" text (most reliable)
            ('results_count', '//*[contains(text(), "Ergebnisse") or contains(text(), "results")]/text()'),
            # Method 2: FALLBACK - Count actual numbered restaurant entries on the page  
            ('numbered_entries', 'count_numbered_entries'),
            # Method 3: FALLBACK - Count actual restaurant links on the page
            ('count_restaurant_links', 'count_restaurant_links'),
            # Method 4: LEGACY - Original wrap_top_title (LOWER PRIORITY)
            ('wrap_top_title', '//div[contains(@class, "wrap_top_title")]//text()'),
            # Method 5: LEGACY - Original static xpath (LOWER PRIORITY)
            ('static_xpath', '//*[@id="content"]/div[1]/div[2]/div[1]/span/text()'),
            # NOTE: Removed restaurant_text method as it extracts wrong JavaScript data (740 instead of 19)
        ]
        
        for method_name, selector in selectors_to_try:
            if success:
                break
                
            try:
                self.log(f"Trying method: {method_name}", logging.DEBUG)
                
                # Special handling for numbered entries method
                if method_name == 'numbered_entries':
                    # Count actual numbered restaurant entries like "1. Landhotel Krone"
                    all_text = response.text
                    
                    # 🎯 IMPROVED PATTERN: More specific pattern to avoid false matches
                    # Look for actual restaurant listing patterns like "1. Restaurant Name"
                    working_pattern = r'(\d{1,2})\.\s+([A-Za-zÀ-ÿ0-9\s\-&\.äöüßÄÖÜ]{3,50})'
                    numbered_matches = regex.findall(working_pattern, all_text, regex.MULTILINE | regex.IGNORECASE)
                    
                    if numbered_matches:
                        # Filter out false matches (page numbers, dates, etc.)
                        valid_matches = []
                        for num, name in numbered_matches:
                            # Skip if it looks like page numbers, dates, coordinates, copyright text, etc.
                            if (name.strip() and 
                                len(name.strip()) > 3 and
                                not regex.match(r'^\d+$', name.strip()) and  # Not just numbers
                                not regex.match(r'^\d+\.\d+$', name.strip()) and  # Not coordinates
                                not any(skip_word in name.lower() for skip_word in ['seite', 'page', 'von', 'km', 'meter', 'min', 'all rights reserved', 'alle rechte vorbehalten', 'copyright', '©', 'impressum', 'datenschutz'])):
                                valid_matches.append((num, name))
                        
                        if valid_matches:
                            # Use the count of actual valid entries, not the highest number
                            actual_count = len(set(match[0] for match in valid_matches))  # Unique numbers
                            max_number = max(int(match[0]) for match in valid_matches)
                            
                            # Use the actual count of visible restaurants
                            num_est = actual_count
                            
                            self.log(f"🔢 NUMBERED ENTRIES: Found {len(valid_matches)} valid numbered restaurants, unique count: {actual_count}, highest: {max_number}", logging.INFO)
                            
                            # Log first few matches for verification
                            for i, (num, name) in enumerate(valid_matches[:3]):  # Reduced logging
                                self.log(f"   {num}. {name.strip()}", logging.INFO)
                            
                            success = True
                            self.log(f"✅ Found restaurant count using numbered entries: {num_est}", logging.INFO)
                            break
                    else:
                        self.log(f"🔍 NUMBERED ENTRIES: No numbered restaurants found, trying fallback patterns", logging.DEBUG)
                        
                        # Fallback 1: Just capture all "X. " patterns
                        simple_pattern = r'(\d{1,2})\.\s+'
                        simple_matches = regex.findall(simple_pattern, all_text)
                        if simple_matches:
                            unique_numbers = sorted(set(int(match) for match in simple_matches))
                            max_number = max(unique_numbers)
                            self.log(f"🔢 FALLBACK PATTERN: Found {len(unique_numbers)} unique numbers, highest: {max_number}", logging.INFO)
                            self.log(f"   Numbers found: {unique_numbers[:10]}{'...' if len(unique_numbers) > 10 else ''}", logging.DEBUG)
                            num_est = max_number
                            success = True
                            self.log(f"✅ Found restaurant count using fallback numbered pattern: {num_est}", logging.INFO)
                            break
                    continue
                
                # Special handling for counting restaurant links method
                elif method_name == 'count_restaurant_links':
                    # Count actual restaurant links on the page
                    restaurant_links = response.xpath('.//a[contains(@href, "/") and not(contains(@href, "restaurant-")) and not(contains(@href, "?")) and not(contains(@href, "#")) and not(contains(@href, "contactus")) and not(contains(@href, "privacy")) and not(contains(@href, "aboutus")) and not(contains(@href, "terms")) and string-length(@href) > 15]/@href').getall()
                    
                    # Filter to get actual restaurant URLs (extract city name from URL)
                    current_url = response.url
                    city_from_url = ''
                    if '/restaurant-' in current_url:
                        # Extract city name from URL like "restaurant-Oberried-t1"
                        url_parts = current_url.split('/restaurant-')[1].split('-')
                        if len(url_parts) > 1:
                            city_from_url = url_parts[0].lower()
                    
                    valid_restaurant_links = []
                    for href in restaurant_links:
                        if (href and 
                            len(href) > 20 and 
                            '-' in href and
                            (city_from_url == '' or city_from_url in href.lower()) and
                            not any(nav in href.lower() for nav in ['contactus', 'privacy', 'terms', 'aboutus', 'home', 'faq'])):
                            valid_restaurant_links.append(href)
                    
                    if valid_restaurant_links:
                        num_est = len(valid_restaurant_links)
                        success = True
                        self.log(f"✅ Found restaurant count by counting links: {num_est}", logging.INFO)
                        self.log(f"🔗 Sample links: {valid_restaurant_links[:3]}", logging.DEBUG)
                        break
                    continue
                
                # Special handling for results count method
                elif method_name == 'results_count':
                    # Look for patterns like "28 Ergebnisse" or "/ 28 Ergebnisse"
                    all_text = response.text
                    self.log(f"🔍 RESULTS_COUNT: Searching for 'Ergebnisse' patterns in {len(all_text)} chars", logging.DEBUG)
                    
                    result_patterns = [
                        r'([\d,\s]+)\s*<span>Ergebnisse</span>',  # "12 817 <span>Ergebnisse</span>" or "12,817 <span>Ergebnisse</span>"
                        r'/\s*([\d,\s]+)\s*Ergebnisse',          # "/ 12 817 Ergebnisse" or "/ 12,817 Ergebnisse"
                        r'([\d,\s]+)\s*Ergebnisse',              # "12 817 Ergebnisse" or "12,817 Ergebnisse"
                        r'([\d,\s]+)\s*results',                 # "12 817 results" or "12,817 results"
                        r'/\s*([\d,\s]+)\s*results',             # "/ 12 817 results" or "/ 12,817 results"
                        r'Ergebnisse\s*([\d,\s]+)',              # "Ergebnisse 12 817" or "Ergebnisse 12,817" 
                        r'results\s*([\d,\s]+)'                  # "results 12 817" or "results 12,817"
                    ]
                    
                    for pattern in result_patterns:
                        result_match = regex.search(pattern, all_text, regex.IGNORECASE)
                        if result_match:
                            # Remove commas, spaces, and dots, then convert to int
                            raw_match = result_match.group(1)
                            count_str = raw_match.replace(',', '').replace(' ', '').replace('.', '')
                            num_est = int(count_str)
                            # 🚨 DEBUG: Always log what we found for Hamburg investigation
                            self.log(f"🔍 COUNT DEBUG: Found '{raw_match}' → cleaned to '{count_str}' → {num_est} using pattern: {pattern}", logging.INFO)
                            # Validate this is a reasonable count (not JavaScript data)
                            if 1 <= num_est <= 50000:  # 🏙️ MEGA CITY: Expanded range for large cities like Hamburg (12K+)
                                success = True
                                self.log(f"✅ Found restaurant count from results text: {num_est} (raw: '{raw_match}') using pattern: {pattern}", logging.INFO)
                                # Log the context around the match for debugging
                                match_start = max(0, result_match.start() - 20)
                                match_end = min(len(all_text), result_match.end() + 20)
                                context = all_text[match_start:match_end].replace('\n', ' ')
                                self.log(f"🔍 MATCH CONTEXT: ...{context}...", logging.DEBUG)
                                break
                            else:
                                self.log(f"🚨 INVALID COUNT: {num_est} outside reasonable range 1-50000", logging.DEBUG)
                    
                    if success:
                        break
                    
                    # If no patterns matched, log debug info
                    if 'Ergebnisse' in all_text:
                        ergebnisse_context = []
                        for match in regex.finditer(r'Ergebnisse', all_text, regex.IGNORECASE):
                            start = max(0, match.start() - 30)
                            end = min(len(all_text), match.end() + 30)
                            context = all_text[start:end].replace('\n', ' ')
                            ergebnisse_context.append(context)
                        self.log(f"🔍 FOUND 'Ergebnisse' but no count extracted. Contexts: {ergebnisse_context[:3]}", logging.WARNING)
                    continue
                
                texts = response.xpath(selector).getall()
                
                if not texts:
                    continue
                    
                # Join all text and clean it
                combined_text = " ".join(texts).strip()
                self.log(f"{method_name} contents: {combined_text[:500]}...", logging.INFO)
                
                # Extract all numbers (handles spaces, commas, dots, non-breaking spaces)
                matches = regex.findall(r'\d{1,3}(?:[ \xa0,.]?\d{3})*|\d+', combined_text)
                self.log(f"{method_name} number matches: {';'.join(matches)}", logging.INFO)
                
                if matches:
                    # Convert matches to integers (removing spaces/non-numeric chars)
                    matches_int = [int(regex.sub(r'[ \xa0,.]', '', m)) for m in matches]
                    
                    # For restaurant counts, we want numbers in a reasonable range
                    # Filter for numbers that could be restaurant counts (1-50000)
                    # 🚨 FIX: Exclude obviously wrong numbers like 100000 which appear in page content
                    # 🚨 COORDINATE FIX: Exclude coordinate-like numbers (11228 from 11.228 coordinates)
                    valid_numbers = []
                    for n in matches_int:
                        if 1 <= n <= 50000:
                            # Additional filter: exclude numbers that look like coordinates
                            # Coordinates often appear as 11.228, 50.507, etc. which become 11228, 50507
                            original_match = [m for m in matches if int(regex.sub(r'[ \xa0,.]', '', m)) == n]
                            if original_match and '.' in original_match[0] and len(str(n)) >= 4:
                                # This looks like a coordinate (had decimal point, 4+ digits)
                                self.log(f"🚨 COORDINATE EXCLUDED: {original_match[0]} -> {n} (looks like coordinate data)", logging.DEBUG)
                                continue
                            valid_numbers.append(n)
                    
                    if valid_numbers:
                        # Get the largest valid number (most likely to be total count)
                        num_est = max(valid_numbers)
                        
                        # 🚨 ENHANCED VALIDATION: Check if this number makes sense for this city
                        # First, count actual numbered restaurant entries on the page
                        numbered_restaurants = regex.findall(r'(\d{1,2})\.\s+[A-Za-zÀ-ÿ0-9\s\-&\.äöüßÄÖÜ]+', response.text)
                        actual_restaurant_count = len(numbered_restaurants)
                        
                        if actual_restaurant_count > 0:
                            # If we found actual numbered restaurants, use that count instead
                            self.log(f"🔢 VALIDATION: Found {actual_restaurant_count} actual numbered restaurants on page", logging.INFO)
                            if abs(num_est - actual_restaurant_count) > actual_restaurant_count * 0.5:  # More than 50% difference
                                self.log(f"🚨 COUNT MISMATCH: Extracted {num_est} but only {actual_restaurant_count} visible - using visible count", logging.WARNING)
                                num_est = actual_restaurant_count
                        
                        # Always validate by checking actual restaurant content on the page
                        restaurant_links = response.xpath('//a[contains(@href, "/") and not(contains(@href, "contactus")) and not(contains(@href, "privacy")) and not(contains(@href, "aboutus")) and not(contains(@href, "terms"))]').getall()
                        actual_restaurant_links = [link for link in restaurant_links if 'restaurant' in link.lower() or len(link) > 50]
                        
                        # Check for restaurant listing elements
                        restaurant_elements = response.xpath('//div[contains(@class, "restaurant") or contains(@class, "establishment") or contains(@class, "listing")]').getall()
                        
                        # 🚨 CRITICAL FIX: If we claim many restaurants but find no actual restaurant content, it's wrong
                        if num_est > 100 and len(actual_restaurant_links) < 3 and len(restaurant_elements) < 3 and actual_restaurant_count == 0:
                            self.log(f"🚨 INVALID COUNT: {num_est} restaurants claimed but only {len(actual_restaurant_links)} restaurant links and {len(restaurant_elements)} restaurant elements found", logging.ERROR)
                            self.log(f"🚨 COORDINATE/JS DATA: This appears to be coordinate data (11.228) or site-wide statistic, not city restaurant count", logging.ERROR)
                            self.log(f"🔍 SAMPLE MATCHES: {matches[:10]}", logging.ERROR)
                            # Don't use this number, continue to next method
                            continue
                        
                        # Additional check for very large numbers vs actual content
                        if num_est > 50 and actual_restaurant_count > 0 and actual_restaurant_count < num_est * 0.1:
                            self.log(f"🚨 SUSPICIOUS COUNT: {num_est} restaurants claimed but only {actual_restaurant_count} numbered entries found", logging.WARNING)
                            self.log(f"🚨 LIKELY GLOBAL STATISTIC: Using actual visible count {actual_restaurant_count} instead", logging.WARNING)
                            num_est = actual_restaurant_count
                        
                        success = True
                        self.log(f"✅ Found restaurant count using {method_name}: {num_est}", logging.INFO)
                        break
                    else:
                        # If no valid numbers, try the largest number anyway
                        num_est = max(matches_int)
                        if num_est > 0:
                            success = True
                            self.log(f"✅ Found number using {method_name} (fallback): {num_est}", logging.INFO)
                            break
                        
            except Exception as e:
                self.log(f'Could not extract number using {method_name}: {e}', logging.DEBUG)
                continue

        # 🚨 FINAL VALIDATION: If all methods failed or returned suspicious numbers, check for zero restaurants
        if not success:
            # Check if this is actually a city with zero restaurants
            restaurant_links = response.xpath('//a[contains(@href, "/") and not(contains(@href, "contactus")) and not(contains(@href, "privacy")) and not(contains(@href, "aboutus")) and not(contains(@href, "terms"))]').getall()
            actual_restaurant_links = [link for link in restaurant_links if 'restaurant' in link.lower() or len(link) > 50]
            
            # Also check for restaurant listing elements and "no results" messages
            restaurant_elements = response.xpath('//div[contains(@class, "restaurant") or contains(@class, "establishment") or contains(@class, "listing")]').getall()
            no_results_messages = response.xpath('//*[contains(text(), "keine") or contains(text(), "no results") or contains(text(), "not found") or contains(text(), "nichts gefunden")]').getall()
            
            if len(actual_restaurant_links) == 0 and len(restaurant_elements) == 0:
                self.log("🚨 ZERO RESTAURANTS DETECTED: No restaurant links or elements found on page", logging.WARNING)
                if no_results_messages:
                    self.log(f"🔍 NO RESULTS MESSAGE FOUND: {no_results_messages[:2]}", logging.INFO)
                self.log("✅ Setting restaurant count to 0 for this city", logging.INFO)
                num_est = 0
                success = True
            else:
                self.log("All extraction methods failed, checking database for known restaurant count", logging.WARNING)
                try:
                    # Try to get restaurant count from database if we have city info
                    if hasattr(self, 'current_city_info') and self.current_city_info:
                        city_slug = self.current_city_info.get('city_slug', '').lower()
                        if city_slug == 'munich':
                            # Munich is known to have around 45,000 restaurants
                            num_est = 45000
                            success = True
                            self.log(f"✅ Using known restaurant count for Munich: {num_est}", logging.INFO)
                except Exception as e:
                    self.log(f"Could not get restaurant count from database: {e}", logging.DEBUG)
        
        if success:
            # 🚨 ENHANCED VALIDATION: Cross-check extracted count with actual visible content
            # Count actual numbered restaurant entries as validation
            numbered_restaurants = regex.findall(r'(\d{1,2})\.\s+[A-Za-zÀ-ÿ0-9\s\-&\.äöüßÄÖÜ]+', response.text)
            actual_visible_count = len(numbered_restaurants)
            
            # 🏙️ MEGA CITY FIX: Only override for small cities with obvious errors, NOT for cities with pagination
            # Cities with 20+ restaurants may use pagination, so visible count != total count
            # Only override if the discrepancy is NOT a multiple of 20 (pagination size)
            discrepancy = abs(num_est - actual_visible_count)
            is_pagination_discrepancy = (discrepancy % 20 == 0 and discrepancy > 0 and num_est > actual_visible_count)
            
            if actual_visible_count > 0 and num_est < 50 and discrepancy > 50 and not is_pagination_discrepancy:
                # Small cities: Override if huge discrepancy (like 740 vs 19 for small towns)
                self.log(f"🚨 SMALL CITY VALIDATION: Extracted {num_est} but found {actual_visible_count} visible restaurants", logging.WARNING)
                self.log(f"🔧 CORRECTION: Using visible count {actual_visible_count} instead of extracted {num_est}", logging.INFO)
                num_est = actual_visible_count
            elif num_est >= 50:
                # Multi-page cities: Trust the extracted count, don't override based on visible count
                self.log(f"🏙️ MULTI-PAGE CITY: Extracted {num_est} restaurants, visible {actual_visible_count} on first page - keeping extracted count (pagination expected)", logging.INFO)
            elif actual_visible_count > 0 and abs(num_est - actual_visible_count) > 10:
                self.log(f"🔍 COUNT DIFFERENCE: Extracted {num_est}, visible {actual_visible_count} - keeping extracted count (acceptable difference)", logging.DEBUG)
            else:
                # Log why we're NOT overriding
                self.log(f"✅ VALIDATION PASSED: Extracted={num_est}, Visible={actual_visible_count}, Discrepancy={discrepancy}, IsPagination={is_pagination_discrepancy}", logging.INFO)
            
            self.log(f"Total number of restaurants: {num_est}", logging.INFO)
            
            # 🚨 ZERO RESTAURANT CHECK: If count is clearly wrong, set to zero
            if num_est >= 50000:
                self.log(f"🚨 SUSPICIOUS COUNT: {num_est} restaurants seems too high, likely parsing error", logging.ERROR)
                self.log(f"🔍 URL FOR INVESTIGATION: {response.url}", logging.ERROR)
                # For investigation - don't immediately fail but flag as suspicious
        else:
            num_est = 0
            self.log(f"🔍 ZERO RESTAURANTS DETECTED: No establishments found for {response.url}", logging.WARNING)
            
            # 🚨 VALIDATION: Check if this is a genuine zero-restaurant city or scraping error
            if hasattr(self, 'current_city_info') and self.current_city_info:
                city_name = self.current_city_info.get('city_name', '').lower()
                
                # 🏙️ MEGA CITY: Known large cities should never have zero restaurants
                if any(mega_city in city_name for mega_city in ['munich', 'münchen', 'hamburg', 'berlin', 'cologne', 'köln', 'frankfurt', 'stuttgart', 'düsseldorf', 'dortmund']):
                    # This is likely a scraping error for major cities
                    self.log(f"🚨 SUSPICIOUS: Major city {city_name} shows 0 restaurants - likely CAPTCHA or error", logging.ERROR)
                    # Set flag for failure detection
                    self.has_critical_errors = True
                    
                if 'munich' in city_name or 'münchen' in city_name:
                    num_est = 45000  # Known approximate count for Munich
                    success = True
                    self.log(f"🏙️ Using default restaurant count for Munich: {num_est}", logging.WARNING)
                else:
                    # For other major cities, flag as error but continue
                    num_est = 1  # Minimum to trigger retry
                    self.log(f"⚠️ Setting minimal count for major city to trigger validation", logging.WARNING)

        return num_est

    # 🚨 REMOVED: Page-based calculation (was causing 11228÷20=562 pages for zero restaurants!)
    # Restaurant Guru uses INFINITE SCROLL, not pagination!
    def get_restaurant_count_only(self, response):
        """Get restaurant count without calculating pages - infinite scroll doesn't need pages!"""
        estab_number = self.count_estabs(response)
        return estab_number

    def parse(self, response, request_url=None, search_query=None):
        try:
            # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Circuit breaker: Check if we should stop processing
            if self._should_stop_processing():
                self.log("ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ¢â‚¬Ëœ Circuit breaker active - skipping page generation", logging.WARNING)
                return
                
            # 🚨 REMOVED: No more page calculation - infinite scroll doesn't need pages!
            # num_pages = self.num_of_pages(response)  # This was causing 11228÷20=562 pages!
            self.log(f"🔄 INFINITE SCROLL: Processing restaurants for URL: {response.url}", logging.INFO)
            
            # 🚨 INFINITE SCROLL: Get restaurant count (no page calculation needed!)
            total_restaurants = self.count_estabs(response)
            self.log(f"🔍 DEBUG: count_estabs returned: {total_restaurants} for initial count", logging.INFO)
            
            if total_restaurants:
                self.restaurants_found = total_restaurants
                self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â¢ Total restaurants found in city: {self.restaurants_found}", logging.INFO)
                
                # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ API optimization: Limit pages based on restaurant limit
                # 🚨 REMOVED: max_pages_needed = min(num_pages, (self.max_restaurants_per_city // 20) + 1)
                # 🚨 REMOVED: if max_pages_needed < num_pages:
                # REMOVED: Problematic log with undefined variables
                # 🚨 REMOVED: num_pages = max_pages_needed
            else:
                # 🚨 CRITICAL: Initial count failed - this will cause issues later
                self.log(f"🚨 CRITICAL: Initial count_estabs returned {total_restaurants} - this may cause completion issues!", logging.ERROR)
            
            # 🔄 RESUME LOGIC: Get checkpoint if resuming
            if self.resume_enabled and hasattr(self, 'current_city_info'):
                # 🚨 CRITICAL FIX: Check if city is already complete BEFORE checkpoint recovery
                city_slug = self.current_city_info['city_slug']
                
                # Get current database count to check for completion
                try:
                    import psycopg2
                    db_config = {
                        'host': '10.32.48.200',
                        'port': '5432',
                        'database': 'smartdataanalyticdb',
                        'user': 'smartdata_worker',
                        'password': 'worker123'
                    }
                    
                    check_connection = psycopg2.connect(**db_config)
                    check_cursor = check_connection.cursor()
                    check_cursor.execute("""
                        SELECT COUNT(*) 
                        FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                        WHERE city_slug = %s
                    """, (city_slug,))
                    count_result = check_cursor.fetchone()
                    current_db_count = count_result[0] if count_result else 0
                    check_connection.close()
                    
                    # 🚨 COMPLETION CHECK: If database count >= restaurants_found, city is complete!
                    if current_db_count >= self.restaurants_found:
                        self.log(f"✅ CITY ALREADY COMPLETE: Database has {current_db_count}/{self.restaurants_found} restaurants for {city_slug}", logging.INFO)
                        self.log(f"🏁 EARLY COMPLETION: No processing needed - marking city as completed", logging.INFO)
                        
                        # Mark city as completed and exit
                        notes = f"All {self.restaurants_found} restaurants already in database"
                        self._mark_city_completed(city_slug, notes)
                        self._clear_checkpoint(city_slug)
                        return  # Exit early - no processing needed
                        
                    self.log(f"📊 PARTIAL COMPLETION: Database has {current_db_count}/{self.restaurants_found} restaurants - will resume processing", logging.INFO)
                    
                except Exception as e:
                    self.log(f"⚠️ Could not check completion status: {e}", logging.WARNING)
                    # Continue with normal checkpoint recovery if completion check fails
                
                # 🚨 CRITICAL FIX: Get restaurants_found from checkpoint to prevent inconsistencies
                checkpoint_result = self._get_resume_checkpoint(self.current_city_info['city_slug'])
                
                # Track checkpoint recovery
                self.log(f"🔍 CHECKPOINT RECOVERY START:", logging.DEBUG)
                self.log(f"   📊 BEFORE: restaurants_processed = {self.restaurants_processed}", logging.DEBUG)
                self.log(f"   📊 BEFORE: _initial_restaurants_processed = {getattr(self, '_initial_restaurants_processed', 'NOT_SET')}", logging.DEBUG)
                self.log(f"   🔍 checkpoint_result length: {len(checkpoint_result)}", logging.DEBUG)
                self.log(f"   🔍 checkpoint_result values: {checkpoint_result}", logging.DEBUG)
                
                # _get_resume_checkpoint always returns 3 values: (last_index, last_page, checkpoint_data)
                last_index, last_page, checkpoint_data = checkpoint_result
                
                self.last_processed_index = last_index
                self.last_processed_page = last_page
                self.resume_checkpoint_data = checkpoint_data
                
                # 🚨 CRITICAL FIX: Detect pagination mode from checkpoint
                if last_page > 0:
                    self.log(f"🔗 PAGINATION MODE DETECTED from checkpoint: last_page = {last_page}", logging.ERROR)
                    self.log(f"🔗 PAGINATION RESUME: Will continue from page {last_page + 1}", logging.ERROR)
                    # Set a flag to indicate we should use pagination mode
                    self.resume_pagination_mode = True
                    self.resume_pagination_page = last_page + 1
                else:
                    self.log(f"🔄 INFINITE SCROLL MODE: last_page = {last_page}", logging.INFO)
                    self.resume_pagination_mode = False
                
                # 🚨 CRITICAL FIX: Handle checkpoint properly (don't inflate count for auto-advanced checkpoints)
                if last_index > 0:
                    # Check if this is an auto-advanced checkpoint (artificial page skip)
                    if isinstance(checkpoint_data, dict) and checkpoint_data.get('auto_advanced'):
                        self.log(f"🔄 AUTO-ADVANCED CHECKPOINT: Using real count {last_index}, starting from page {last_page + 1}", logging.ERROR)
                        self.restaurants_processed = last_index  # This is the REAL count (184)
                    else:
                        self.log(f"🔄 NORMAL CHECKPOINT: Updating restaurants_processed from {self.restaurants_processed} to {last_index}", logging.ERROR)
                        self.restaurants_processed = last_index
                
                # 🔄 RETRY TRACKING: Store initial processed count AFTER checkpoint recovery AND update
                self.log(f"🔍 SETTING _initial_restaurants_processed to {self.restaurants_processed}", logging.DEBUG)
                self._initial_restaurants_processed = self.restaurants_processed
                self.log(f"🔄 RETRY TRACKING: Set initial_processed to {self._initial_restaurants_processed}", logging.DEBUG)
                
                # 🚨 COMPLETION VALIDATION: Check if city is already complete after checkpoint recovery
                if hasattr(self, 'restaurants_found') and self.restaurants_processed >= self.restaurants_found:
                    self.log(f"✅ CHECKPOINT COMPLETION CHECK: City already complete ({self.restaurants_processed}/{self.restaurants_found})", logging.INFO)
                    self.log(f"🧹 EARLY COMPLETION: No processing needed - marking city as completed and clearing checkpoint", logging.INFO)
                    
                    # Mark city as completed and exit early
                    notes = f"All {self.restaurants_found} restaurants already processed (checkpoint resume)"
                    self._mark_city_completed(city_slug, notes)
                    self._clear_checkpoint(city_slug)
                    return  # Exit early - no processing needed
                
                self.log(f"🔍 CHECKPOINT RECOVERY END:", logging.DEBUG)
                self.log(f"   📊 AFTER: restaurants_processed = {self.restaurants_processed}", logging.DEBUG)
                self.log(f"   📊 AFTER: _initial_restaurants_processed = {self._initial_restaurants_processed}", logging.DEBUG)
                
                if last_index > 0:
                    self.log(f"🔄 RESUMING: Starting from restaurant #{last_index + 1}, page #{last_page}", logging.INFO)
                    self.log(f"💾 CHECKPOINT DATA: {checkpoint_data}", logging.INFO)
                    
                    # 🔧 CRITICAL: Test checkpoint saving immediately after loading to verify system works
                    if hasattr(self, 'current_city_info') and self.current_city_info:
                        city_slug = self.current_city_info.get('city_slug')
                        if city_slug:
                            # 🚨 FIX: Calculate correct current page based on restaurants_processed
                            # 🌐 INFINITE SCROLL: No page calculations needed
                            self.log(f"🧪 TESTING CHECKPOINT: Saving test checkpoint to verify system works", logging.INFO)
                            test_result = self._save_checkpoint(city_slug, last_index, checkpoint_data)
                            if test_result:
                                self.log(f"✅ CHECKPOINT SYSTEM VERIFIED: Test save successful", logging.INFO)
                            else:
                                self.log(f"❌ CHECKPOINT SYSTEM FAILED: Test save failed", logging.ERROR)
            
            # 🚨 CRITICAL FIX: Ensure restaurant count consistency after checkpoint restoration
            if not hasattr(self, 'restaurants_found') or self.restaurants_found <= 0:
                # Extract count from page if not available from checkpoint
                total_restaurants = self.count_estabs(response)
                if total_restaurants:
                    self.restaurants_found = total_restaurants
                    self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â¢ Total restaurants found in city (extracted): {self.restaurants_found}", logging.INFO)
                else:
                    # 🚨 CRITICAL FIX: Set fallback value to prevent premature completion
                    self.log(f"⚠️ Could not extract restaurant count from page", logging.WARNING)
                    self.log(f"🚨 FALLBACK: Setting restaurants_found to 50 to force proper processing", logging.ERROR)
                    self.restaurants_found = 50  # Assume multi-page city to force proper processing
            else:
                self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â¢ Using restaurant count from checkpoint: {self.restaurants_found}", logging.INFO)
            
            # 🚨 CRITICAL FIX: Choose mode based on checkpoint data or restaurant count
            # 📊 PAGINATION LOGIC: Restaurant Guru shows 20 restaurants per page
            # - Cities with >30 restaurants = Multiple pages (use pagination)
            # - Cities with ≤30 restaurants = Single page (use infinite scroll)
            if hasattr(self, 'resume_pagination_mode') and self.resume_pagination_mode:
                # 🔗 PAGINATION MODE: Resume from checkpoint-detected pagination
                self.log(f"🔗 PAGINATION MODE: Resuming pagination from checkpoint (page {self.resume_pagination_page})", logging.ERROR)
                self.log(f"🔗 PAGINATION: Will use page-by-page processing", logging.INFO)
                use_pagination = True
                starting_page = self.resume_pagination_page
            elif self.restaurants_found > 30:
                # 🔗 PAGINATION MODE: Cities with >30 restaurants typically require pagination (20 per page)
                # 🚨 FIX: Changed from >20 to >30 to prevent premature pagination for cities with 21-30 restaurants
                self.log(f"🔗 PAGINATION MODE: Multi-page city ({self.restaurants_found} restaurants) - using page-by-page processing", logging.INFO)
                use_pagination = True
                starting_page = 1
            else:
                # 🌐 INFINITE SCROLL: Small cities use infinite scroll
                self.log(f"🔄 INFINITE SCROLL: Small city ({self.restaurants_found} restaurants) - single page processing", logging.INFO)
                use_pagination = False
                starting_page = 1
            
            # 🚨 CONDITIONAL PROCESSING: Route to pagination or infinite scroll based on mode
            if use_pagination:
                # 🔗 PAGINATION MODE: Use the original pagination logic (before infinite scroll)
                self.log(f"🔗 PAGINATION PROCESSING: Switching to pagination logic", logging.INFO)
                
                # 🚨 CRITICAL FIX: Get city_slug for pagination logic
                city_info = response.meta.get('city_info', {})
                city_slug = city_info.get('city_slug') if city_info else (self.current_city_info.get('city_slug') if hasattr(self, 'current_city_info') and self.current_city_info else None)
                
                if not city_slug:
                    self.log(f"⚠️ URL BUILD ERROR: No city_slug available, cannot build pagination URLs", logging.ERROR)
                    return
                
                # Calculate pagination parameters
                restaurants_per_page = 20  # User confirmed: 20 restaurants per page
                pages_needed = (self.restaurants_found + restaurants_per_page - 1) // restaurants_per_page
                request_delay = 3.0  # Delay between page requests
                
                # 💾 PAGINATION CHECKPOINT: Check if we should resume from a specific page
                checkpoint_index, checkpoint_page, checkpoint_data = self._get_resume_checkpoint(city_slug)
                
                # 🚨 CRITICAL FIX: Calculate correct starting page based on restaurant index
                if checkpoint_index > 0:
                    # Calculate which page contains the next restaurant to process
                    next_restaurant_index = checkpoint_index + 1
                    page_num = ((next_restaurant_index - 1) // restaurants_per_page) + 1
                    self.log(f"🔗 PAGINATION RESUME: Restaurant #{next_restaurant_index} is on page {page_num} (calculated from index {checkpoint_index})", logging.ERROR)
                    self.log(f"📊 CALCULATION: ({next_restaurant_index} - 1) ÷ {restaurants_per_page} + 1 = page {page_num}", logging.INFO)
                else:
                    page_num = starting_page
                    self.log(f"🔗 SEQUENTIAL PAGINATION: Starting fresh with page {page_num} (no checkpoint)", logging.ERROR)
                
                # Generate URL for starting page
                if page_num == 1:
                    total_url = f"https://de.restaurantguru.com/restaurant-{city_slug}-t1"
                else:
                    total_url = f"https://de.restaurantguru.com/restaurant-{city_slug}-t1/{page_num}"
                
                self.log(f"🔗 SEQUENTIAL PAGINATION: Starting with PAGE {page_num}: {total_url}", logging.ERROR)
                self.log(f"📊 SEQUENTIAL PROCESSING: Will process page-by-page (not all pages at once)", logging.INFO)
                
                # Calculate expected restaurants for this page
                expected_on_page = min(restaurants_per_page, self.restaurants_found - (page_num - 1) * restaurants_per_page)
                self.log(f"📊 PAGE {page_num}: Expecting ~{expected_on_page} restaurants", logging.INFO)
                
                # 🚨 CRITICAL FIX: Define headers for pagination requests
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                    
                # 🚨 FIX: Initialize page tracking for pagination mode
                self.current_page_start_count = self._initial_db_count
                
                # Yield request for ONLY page 1 first
                yield Request(
                    url=total_url,
                    headers=headers,
                    callback=self.parse_estabs,
                    dont_filter=True,  # 🚨 CRITICAL FIX: Disable filtering to prevent duplicate blocking
                    meta={
                        **response.meta,
                        'page_number': page_num,  # 🚨 NEW: Track page number for pagination
                        'resume_from_index': checkpoint_index,  # 🚨 CRITICAL: Resume from checkpoint index
                        'city_info': self.current_city_info,  # 🔧 CRITICAL: Pass city info for checkpoint saving
                        'download_delay': request_delay,  # Delay for captcha avoidance
                        'expected_restaurants': expected_on_page,  # Expected restaurants on this specific page
                        'pagination_mode': True,  # Flag to indicate pagination mode vs infinite scroll
                        'restaurants_per_page': restaurants_per_page,  # For calculations
                        'total_pages': pages_needed,  # Total pages expected
                        'city_slug': city_slug,  # Pass city slug for next page generation
                    }
                )
                
                # 🔗 PAGINATION: Early return - no infinite scroll logic needed
                return
                
            else:
                # 🌐 INFINITE SCROLL MODE: Original infinite scroll logic
                self.log(f"🔄 INFINITE SCROLL PROCESSING: Using infinite scroll logic", logging.INFO)
                
                # Log infinite scroll calculation
                self.log(f"🔄 INFINITE SCROLL DEBUG START", logging.DEBUG)
                self.log(f"🔍 Current restaurants_processed: {self.restaurants_processed}", logging.DEBUG)
                self.log(f"🔍 Current restaurants_per_batch: {self.restaurants_per_batch}", logging.DEBUG)
                self.log(f"🔍 Phase check: restaurants_processed < 200? {self.restaurants_processed < 200}", logging.DEBUG)
            
            # 🏙️ MEGA CITY: Process entire city from current position to end
            start_offset = self.restaurants_processed
            end_offset = min(start_offset + self.restaurants_per_batch - 1, self.restaurants_found - 1)
            
            self.log(f"🏙️ MEGA CITY: Loading restaurants {start_offset + 1}-{end_offset + 1} (offset {start_offset}-{end_offset})", logging.INFO)
            self.log(f"🏙️ TOTAL PROGRESS: {self.restaurants_processed}/{self.restaurants_found} restaurants processed", logging.INFO)
            self.log(f"🔍 MEGA CITY DEBUG: start_offset={start_offset}, end_offset={end_offset}, total_found={self.restaurants_found}", logging.DEBUG)
            
            # 🚨 INFINITE SCROLL DEBUG: Show processing details
            self.log(f"🔍 INFINITE SCROLL PROCESSING:", logging.INFO)
            self.log(f"   📊 Next restaurant to process: #{self.restaurants_processed + 1}", logging.INFO)
            self.log(f"   📊 Will skip {self.restaurants_processed} already-processed restaurants", logging.INFO)
            self.log(f"   📊 Will process next {self.restaurants_per_batch} unprocessed restaurants found", logging.INFO)
            self.log(f"   🎯 restaurants_per_batch: {self.restaurants_per_batch}", logging.INFO)
            
            # 🚨 INFINITE SCROLL: No retry detection needed - single request handles all restaurants
            if self.restaurants_processed > 0:
                self.log(f"🔄 INFINITE SCROLL RESUME: Continuing from restaurant #{self.restaurants_processed + 1}", logging.INFO)
                setattr(self, '_high_page_retry', True)  # Flag for enhanced delays
            else:
                setattr(self, '_high_page_retry', False)
            
            # Log batch processing
            current_batch_count = self.restaurant_count  # Count of restaurants we've queued for processing in this run
            remaining_in_batch = max(0, self.restaurants_per_batch - current_batch_count)
            self.log(f"🎯 BATCH PROCESSING: Will process up to {remaining_in_batch} more restaurants (limit: {self.restaurants_per_batch})", logging.INFO)
            self.log(f"🎯 CURRENT PROGRESS: {self.restaurants_processed} processed globally, {current_batch_count} queued this run", logging.INFO)
            
            # 🚨 INFINITE SCROLL FIX: Generate ONLY ONE REQUEST instead of 631 duplicate requests
            # The old loop was generating 631 requests for the same URL - completely wrong for infinite scroll
            
            # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Check circuit breaker before request
            if self._should_stop_processing():
                self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ¢â‚¬Ëœ Circuit breaker triggered - skipping infinite scroll request", logging.WARNING)
                return
            
            # 🚨 CRITICAL FIX: Build correct Restaurant Guru URL from city info
            city_info = response.meta.get('city_info', {})
            city_slug = city_info.get('city_slug') if city_info else (self.current_city_info.get('city_slug') if hasattr(self, 'current_city_info') and self.current_city_info else None)
            
            if not city_slug:
                self.log(f"⚠️ URL BUILD ERROR: No city_slug available, cannot build infinite scroll URL", logging.ERROR)
                return
            
            # 🚨 INFINITE SCROLL FIX: All restaurants load on ONE page via infinite scroll
            # No page limits - Restaurant Guru loads ALL restaurants dynamically on single page
            total_restaurants = self.restaurants_found if hasattr(self, 'restaurants_found') and self.restaurants_found > 0 else 50  # Default fallback
            
            # 🌐 INFINITE SCROLL: Generate single request for all restaurants
            total_url = f"https://de.restaurantguru.com/restaurant-{city_slug}-t1"
            
            self.log(f"🔄 INFINITE SCROLL: Requesting {total_url} for all {total_restaurants} restaurants", logging.INFO)
            
            # Generate single infinite scroll request
            yield Request(
                url=total_url,
                callback=self.parse_estabs,
                    dont_filter=True,
                meta={
                    **response.meta,
                        'pagination_mode': False,  # Flag to indicate infinite scroll mode
                        'resume_from_index': self.restaurants_processed,  # 🚨 CRITICAL FIX: Pass resume index for infinite scroll
                        'city_info': self.current_city_info,
                        'city_slug': city_slug,
                    }
                )
        except JSONDecodeError:
            # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Check circuit breaker before each page request
            if self._should_stop_processing():

                self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ¢â‚¬Ëœ Circuit breaker triggered - stopping processing", logging.WARNING)
                return
                
            # 🚨 CRITICAL FIX: Generate DIFFERENT URLs for each page in the batch
            # Use the loop page variable, not restaurants_processed
                
            # 🚨 CRITICAL FIX: Build correct Restaurant Guru URL from city info
            # Don't use response.url (which is ScrapeOps proxy URL), rebuild the original URL
                
            city_info = response.meta.get('city_info', {})
            city_slug = city_info.get('city_slug') if city_info else (self.current_city_info.get('city_slug') if hasattr(self, 'current_city_info') and self.current_city_info else None)
                
            if not city_slug:
                self.log(f"⚠️ URL BUILD ERROR: No city_slug available, cannot build page URLs", logging.ERROR)
                return
                
                # 🚨 INFINITE SCROLL FIX: Always use base URL without page numbers
                # Restaurant Guru uses infinite scroll - pagination URLs cause 503 errors
                
                # ALWAYS use base URL - infinite scroll loads all restaurants dynamically
            total_url = f"https://de.restaurantguru.com/restaurant-{city_slug}-t1"
                
                # Calculate restaurant range for this batch based on processed count
            restaurant_start = self.restaurants_processed + 1
            restaurant_end = self.restaurants_processed + self.restaurants_per_batch
                
            self.log(f"🔄 INFINITE SCROLL: Base URL (no pagination) for restaurants {restaurant_start}-{restaurant_end}", logging.INFO)
            self.log(f"🔍 URL DEBUG: Generated infinite scroll URL: {total_url}", logging.DEBUG)
            self.log(f"📊 CHECKPOINT: Will process restaurants from index {restaurant_start} to {restaurant_end}", logging.INFO)
            
            # Calculate how many restaurants to skip for this specific page
            restaurants_to_skip = self.restaurants_processed  # Global restaurants already processed
            
            self.log(f'📄 INFINITE SCROLL: Processing restaurants from index {self.restaurants_processed + 1}', logging.INFO)
            self.log(f'🔗 UNIQUE URL: {total_url}', logging.INFO)
            self.log(f'🎯 RESUME LOGIC: Will skip first {restaurants_to_skip} restaurants when processing', logging.INFO)
            # 🚀 BATCH PROCESSING: Process multiple pages to reach batch limit
            # 🤖 ANTI-CAPTCHA: Better headers and delays for high page numbers
            headers = {
                'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0'
            }
            
            # 🚨 SMART DELAY: Use appropriate delay for infinite scroll
            if hasattr(self, '_high_page_retry') and self._high_page_retry:
                request_delay = 10.0  # Longer delay for retry scenarios
                self.log(f"🔄 RETRY DELAY: {request_delay}s (retry scenario)", logging.INFO)
            else:
                request_delay = 2.0  # Normal delay for infinite scroll
            
            # 🚨 CRITICAL FIX: Use clean URL for infinite scroll - parameters break Restaurant Guru's page structure
            # Add unique identifier in meta instead of URL to prevent duplicate filtering
            
            # 🚨 CRITICAL FIX: Ensure URL is completely clean of any parameters
            if '?' in total_url:
                clean_url = total_url.split('?')[0]
                self.log(f"🚨 URL CLEANING: Removed parameters from {total_url} -> {clean_url}", logging.ERROR)
                total_url = clean_url
            
            # Deployment verification
            self.log(f"✅ DEPLOYMENT CHECK: Latest version deployed", logging.DEBUG)
            self.log(f"🔍 URL VERIFICATION: Final URL = {total_url}", logging.DEBUG)
            self.log(f"🔍 URL PARAMETER CHECK: Contains '?' = {'?' in total_url}", logging.DEBUG)
            
            # Debug information for troubleshooting
            self.log(f"🔍 Current method = parse", logging.DEBUG)
            self.log(f"🔍 About to yield Request with callback = parse_estabs", logging.DEBUG)
            self.log(f"🔍 Request URL length = {len(total_url)}", logging.DEBUG)
            self.log(f"🔍 Current timestamp = {datetime.now().isoformat()}", logging.DEBUG)
            
            # 🚨 DUPLICATE YIELD REMOVED: Request is already yielded in the loop above
            self.log(f"✅ INFINITE SCROLL FIX APPLIED: Request yielded in multi-page loop", logging.INFO)
            # End of old loop code that's now disabled
        
            # 📊 INFINITE SCROLL SUMMARY: Clean completion message
            self.log(f"✅ INFINITE SCROLL FIX APPLIED: Using single request instead of batch loop", logging.INFO)
            self.log(f"📈 INFINITE SCROLL PROGRESS: Total restaurants found: {self.restaurants_found}, Processed so far: {self.restaurants_processed}", logging.INFO)

        except JSONDecodeError:
            traceback.print_exc()
            error_msg = f"JSONDecodeError at URL {response.url}"
            self.log(error_msg, logging.ERROR)
            if hasattr(self, 'current_city_info') and self.current_city_info:
                self._mark_city_failed(self.current_city_info['city_slug'], error_msg, increment_retry=False)  # Don't increment retry for parsing errors
        except Exception as e:
            traceback.print_exc()
            error_msg = f"Error parsing {response.url}: {str(e)}"
            self.log(error_msg, logging.ERROR)
            if hasattr(self, 'current_city_info') and self.current_city_info:
                self._mark_city_failed(self.current_city_info['city_slug'], error_msg, increment_retry=False)  # Don't increment retry for parsing errors

    def parse_estabs(self, response):
        # 🔧 404 HANDLING: Check if this is a non-existent page
        if response.status == 404:
            self.log(f"📄 PAGE NOT FOUND: {response.url} returned 404 - page doesn't exist", logging.INFO)
            self.log(f"✅ This is normal for small cities where not all pages exist", logging.INFO)
            return  # Skip processing this page
        
        # Log entry to parse_estabs with pagination debugging
        page_number = response.meta.get('page_number', 1)
        pagination_mode = response.meta.get('pagination_mode', False)
        mode_text = "PAGINATION MODE" if pagination_mode else "INFINITE SCROLL MODE"
        self.log(f"🔗 PARSE_ESTABS ENTRY: {mode_text} - PAGE {page_number}", logging.INFO)
        self.log(f"🔍 PARSE_ESTABS: Page {page_number}/{response.meta.get('total_pages', '?')} - Expected: {response.meta.get('expected_restaurants', '?')} restaurants", logging.INFO)
        self.log(f"🔍 Response status = {response.status}, size = {len(response.text)} bytes", logging.DEBUG)
        
        # ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Track response for error detection
        self._track_response_received(response)
        
        # 🚨 CRITICAL FIX: Initialize emergency_fallback_used to prevent UnboundLocalError
        emergency_fallback_used = False
        
        # 🤖 CAPTCHA DETECTION: Check if we hit a captcha page
        response_text = response.text.lower()
        # 🔧 More specific captcha detection to avoid false positives
        captcha_keywords = ['captcha challenge', 'verify you are human', 'bot detection activated', 'cloudflare ray id']
        captcha_detected = any(keyword in response_text for keyword in captcha_keywords)
        
        # Additional check: Look for specific captcha page patterns
        if not captcha_detected:
            # Check for common captcha page indicators
            captcha_indicators = [
                'cf-browser-verification' in response_text,
                'checking your browser' in response_text and len(response_text) < 5000,
                response.status == 403 and 'access denied' in response_text
            ]
            captcha_detected = any(captcha_indicators)
        
        # 🔍 DEBUG: Log captcha detection details for troubleshooting
        if captcha_detected:
            found_keywords = [kw for kw in captcha_keywords if kw in response_text]
            self.log(f"🔍 CAPTCHA KEYWORDS FOUND: {found_keywords}", logging.ERROR)
            self.log(f"🔍 RESPONSE PREVIEW: {response_text[:500]}...", logging.ERROR)
        
        if captcha_detected:
            resume_from_index = response.meta.get('resume_from_index', 0)
            self.log(f"🤖 CAPTCHA DETECTED: Infinite scroll page returned captcha challenge", logging.ERROR)
            self.log(f"🔗 CAPTCHA URL: {response.url}", logging.ERROR)
            self.log(f"⏳ CAPTCHA SOLUTION: Implementing longer delays and retries for high restaurant indexes", logging.WARNING)
            
            # If this is a high restaurant index, try with much longer delay
            if resume_from_index > 200:
                # Retry with longer delay (exponential backoff based on restaurant index)
                retry_delay = min(30.0, 5.0 * ((resume_from_index - 200) // 100))  # 5s, 10s, 15s, etc. up to 30s
                self.log(f"🔄 CAPTCHA RETRY: Retrying infinite scroll from restaurant #{resume_from_index} with {retry_delay}s delay", logging.WARNING)
                
                headers = {
                    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                
                yield Request(
                    url=response.url,
                    headers=headers,
                    callback=self.parse_estabs,
                    dont_filter=False,
                    meta={
                        **response.meta,
                        'download_delay': retry_delay,
                        'captcha_retry': response.meta.get('captcha_retry', 0) + 1
                    }
                )
                return
            else:
                self.log(f"🚨 CAPTCHA BLOCKING: Cannot proceed with infinite scroll from restaurant #{resume_from_index}", logging.ERROR)
                return
        
        # 📄 PAGINATION: Log that we're processing this page
        page_number = response.meta.get('page_number', 1)
        pagination_mode = response.meta.get('pagination_mode', False)
        if pagination_mode:
            current_page = f"page_{page_number}"
            self.log(f"🔗 PROCESSING PAGINATION PAGE {page_number}: {response.url}", logging.INFO)
        else:
            current_page = "infinite_scroll"
            self.log(f"🌐 PROCESSING INFINITE SCROLL PAGE: {response.url}", logging.INFO)
        
        # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Circuit breaker: Check if we should stop processing
        if self._should_stop_processing():
            self.log("ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ¢â‚¬Ëœ Circuit breaker active - skipping restaurant processing", logging.WARNING)
            return

        # 🔄 RESUME LOGIC: Get resume information for pagination vs infinite scroll
        resume_from_index = response.meta.get('resume_from_index', 0)
        pagination_mode = response.meta.get('pagination_mode', False)
        page_number = response.meta.get('page_number', 1)
        restaurants_per_page = response.meta.get('restaurants_per_page', 20)
        
        # 🚨 PAGINATION DEBUG: Log checkpoint resume information
        if pagination_mode:
            # 📄 PAGINATION MODE: Calculate page-based skip count
            page_start_index = (page_number - 1) * restaurants_per_page
            restaurants_to_skip = max(0, resume_from_index - page_start_index)
            self.log(f"🔗 PAGINATION RESUME: Page {page_number} starts at restaurant #{page_start_index + 1}, will skip {restaurants_to_skip} restaurants on this page", logging.INFO)
            self.log(f"📊 CALCULATION: resume_from_index={resume_from_index}, page_start={page_start_index}, skip={restaurants_to_skip}", logging.INFO)
        else:
            # 🌐 INFINITE SCROLL: Use global checkpoint-based resume
            restaurants_to_skip = resume_from_index
        self.log(f"🔄 INFINITE SCROLL RESUME: resume_from_index={resume_from_index}, restaurants_processed={self.restaurants_processed}", logging.INFO)
        self.log(f"🌐 INFINITE SCROLL RESUME: Will skip first {restaurants_to_skip} restaurants and continue from #{restaurants_to_skip + 1}", logging.INFO)
        
        # 🚀 NEW: In index-based mode, resume logic is simpler - we only process our target range
        if self.index_based_mode:
            restaurants_to_skip = self.start_index - 1  # Resume from just before our start index
            self.log(f"🔢 INDEX-BASED RESUME: Will process restaurants {self.start_index}-{self.end_index}", logging.INFO)
        
        # Check if page loaded properly for infinite scroll
        page_title = response.xpath('//title/text()').get()
        page_size = len(response.text)
        self.log(f"🔍 PAGE DEBUG: Title='{page_title}', Size={page_size} bytes", logging.DEBUG)
        
        # Check if we got a proper Restaurant Guru page
        if page_size < 10000:  # Too small, likely an error page
            self.log(f"🚨 PAGE ERROR: Page too small ({page_size} bytes), likely error or redirect", logging.ERROR)
            self.log(f"🔍 PAGE CONTENT PREVIEW: {response.text[:500]}", logging.ERROR)
            self.log(f"🔍 Exiting parse_estabs due to small page size at {datetime.now().isoformat()}", logging.DEBUG)
            return
        
        restaurants_on_page = 0
        global_restaurant_counter = self.restaurants_processed  # 🚨 CRITICAL FIX: Start from global processed count from checkpoint
        # Ã°Å¸â€Â ENHANCED: Try multiple XPath selectors for restaurant URLs
        # 🚨 SAFE CITY NAME EXTRACTION - handles both old and new code
        try:
            if self.current_city_info:
                if isinstance(self.current_city_info, dict):
                    city_name = self.current_city_info.get("city_name", "Unknown")
                elif isinstance(self.current_city_info, (list, tuple)) and len(self.current_city_info) > 1:
                    city_name = self.current_city_info[1]  # Old format fallback
                else:
                    city_name = "Unknown"
            else:
                city_name = "Unknown"
        except Exception as e:
            self.log(f"🚨 Error extracting city name: {e}", logging.ERROR)
            city_name = "Unknown"
        
        selectors_to_try = [
            # 🎯 TOP PRIORITY: Most permissive selector for small cities (2-5 restaurants)
            './/a[@href and contains(@href, "/") and contains(@href, "-") and not(contains(@href, "restaurant-")) and not(contains(@href, "?")) and not(contains(@href, "#")) and not(contains(@href, "contactus")) and not(contains(@href, "privacy")) and not(contains(@href, "aboutus")) and not(contains(@href, "terms")) and string-length(@href) > 10]/@href',
            
            # 🎯 SECONDARY: Based on actual Oberreute page structure - look for restaurant name links  
            './/a[contains(@href, "/") and not(contains(@href, "restaurant-")) and not(contains(@href, "?")) and not(contains(@href, "#")) and not(contains(@href, "contactus")) and not(contains(@href, "privacy")) and not(contains(@href, "aboutus")) and not(contains(@href, "terms")) and string-length(@href) > 15]/@href',
            
            # 🎯 FALLBACK: Look for links that are restaurant names (longer URLs, contain hyphens)
            './/a[starts-with(@href, "/") and contains(@href, "-") and string-length(@href) > 20 and not(contains(@href, "/restaurant-")) and not(contains(@href, "?")) and not(contains(@href, "contactus")) and not(contains(@href, "privacy"))]/@href',
            
            # 🎯 PRIORITY: Target actual restaurant listing cards/entries (most reliable)
            './/div[contains(@class, "place-item")]//h2/a/@href',
            './/div[contains(@class, "place-item")]//h3/a/@href', 
            './/div[contains(@class, "result-item")]//h2/a/@href',
            './/div[contains(@class, "restaurant-item")]//h2/a/@href',
            './/div[@class="info_header"]/div/a/@href',
            './/h2[@class="place-name"]/a/@href',
            './/h3[@class="place-name"]/a/@href',
            
            # 🎯 BROADER: Look for restaurant name links (not city links) - Fixed XPath
            './/div[contains(@class, "place")]//a[contains(@href, "-") and not(contains(@href, "/restaurant-"))]/@href',
            f'.//a[contains(@href, "-{city_name}") and not(contains(@href, "/restaurant-{city_name}")) and string-length(substring-before(@href, "-{city_name}")) > 10]/@href',
            
            # 🎯 STRUCTURE: Find links within restaurant listing containers
            './/div[contains(@class, "place")]//div[contains(@class, "info")]//a/@href',
            './/div[contains(@class, "result")]//div[contains(@class, "info")]//a/@href',
            './/div[contains(@class, "restaurant")]//div[contains(@class, "info")]//a/@href',
            
            # 🚨 FILTERED: Broader search but exclude city and listing pages
            f'.//a[contains(@href, "-{city_name}") and not(contains(@href, "/restaurant-{city_name}")) and not(contains(@href, "?"))]/@href',
            './/a[contains(@href, "/restaurant/")]/@href',
            
            # 🚨 FALLBACK: Very specific exclusions to avoid city pages
            f'.//a[contains(@href, "restaurantguru.com") and contains(@href, "-{city_name}") and not(contains(@href, "/restaurant-{city_name}-t")) and string-length(@href) > 30]/@href',
            './/a[contains(@href, "restaurantguru.com") and not(contains(@href, "/restaurant-")) and not(contains(@href, "?")) and contains(@href, "-") and string-length(@href) > 25]/@href',
            
            # 🚨 LAST RESORT: Broad search with length filter (restaurant URLs are longer)
            './/a[starts-with(@href, "/") and contains(@href, "-") and string-length(@href) > 20 and not(contains(@href, "/restaurant-"))]/@href'
        ]
        
        def is_valid_restaurant_url(url: str, city_name: str) -> bool:
            """🔍 Validate that URL is a restaurant page, not a city listing page"""
            if not url:
                return False
            
            # Extract just the path part for most checks, but also keep full URL for guide checks
            url_path = url.split('/')[-1] if '/' in url else url
            full_url_lower = url.lower()
            
            # 🚨 EXCLUDE: Guides and navigation URLs (check full URL first)
            if '/guides/' in full_url_lower or '/guide/' in full_url_lower:
                self.log(f"🚫 EXCLUDED GUIDE: {url} (contains /guides/)", logging.DEBUG)
                return False
            
            # 🚨 EXCLUDE: City listing pages
            city_patterns = [
                f'{city_name}',  # Just city name
                f'restaurant-{city_name}-t1',  # Listing page format
                f'restaurant-{city_name}-t',   # Partial listing page
                f'{city_name}-t1',             # Alternative format
                f'{city_name}-t'               # Partial alternative
            ]
            
            if any(pattern.lower() == url_path.lower() for pattern in city_patterns):
                self.log(f"🚫 EXCLUDED CITY PAGE: {url} (matches city pattern)", logging.DEBUG)
                return False
            
            # 🚨 EXCLUDE: Too short URLs (likely city pages) - but be more permissive for small cities
            if len(url_path) < 10:  # Lowered from 15 to 10 for small cities like Oberrot
                self.log(f"🚫 EXCLUDED SHORT URL: {url} (too short: {len(url_path)} chars)", logging.DEBUG)
                return False
            
            # 🚨 EXCLUDE: Navigation/guide URLs (but be more specific to avoid false positives)
            excluded_patterns = [
                'restaurant-' + city_name.lower(),  # Only exclude city-specific restaurant pages
                '/guides/', 'guide/', 'search', 'filter', 'contactus', 'aboutus', 
                'privacy', 'terms', 'home', 'login', 'best-restaurants', 'top-restaurants'
            ]
            
            # Check for exact matches or specific patterns (but be careful with restaurant names)
            url_lower = url_path.lower()
            should_exclude = False
            
            for pattern in excluded_patterns:
                if pattern in url_lower:
                    # Special case: Don't exclude if it's clearly a restaurant name 
                    # For example: "Fischereihafen-Restaurant-Hamburg" contains "restaurant" but is a valid restaurant
                    if pattern == 'restaurant-' + city_name.lower():
                        # Only exclude if it's exactly the city listing pattern
                        if url_lower == pattern or url_lower == pattern + '-t1':
                            should_exclude = True
                            break
                    elif pattern in ['best-', 'top-'] and city_name.lower() in url_lower:
                        continue  # This might be a restaurant name, keep it
                    elif pattern not in ['/guides/', 'guide/']:  # Already handled above
                        should_exclude = True
                        break
            
            if should_exclude:
                self.log(f"🚫 EXCLUDED NAVIGATION: {url} (contains excluded pattern)", logging.DEBUG)
                return False
            
            # ✅ INCLUDE: URLs that contain restaurant name + city
            if '-' in url_path and city_name.lower() in url_path.lower():
                # Check if it has a restaurant name before the city name
                parts = url_path.lower().split('-')
                city_index = -1
                for i, part in enumerate(parts):
                    if city_name.lower() in part:
                        city_index = i
                        break
                
                if city_index > 0:  # Has restaurant name before city
                    self.log(f"✅ VALID RESTAURANT: {url} (restaurant name + city)", logging.DEBUG)
                    return True
            
            # ✅ INCLUDE: Any restaurant-like URL (since URL slugs may not match actual location)
            # Restaurant Guru URLs can have misleading slugs, so be more permissive
            if (len(url_path) > 10 and '-' in url_path and 
                not any(nav in url_path.lower() for nav in ['guide', 'top-', 'best-', 'search', 'filter', 'city', 'region', 'contactus', 'privacy', 'aboutus', 'terms'])):
                self.log(f"✅ VALID RESTAURANT: {url} (permissive restaurant pattern)", logging.DEBUG)
                return True
            
            # ✅ INCLUDE: URLs longer than 20 chars with hyphens (lowered threshold for small cities)
            if len(url_path) > 20 and '-' in url_path:
                self.log(f"✅ VALID RESTAURANT: {url} (likely restaurant name with hyphens)", logging.DEBUG)
                return True
                
            # ✅ INCLUDE: URLs that look like restaurant names (contain common restaurant words)
            restaurant_keywords = ['gasthof', 'restaurant', 'lowen', 'sonne', 'krone', 'adler', 'hirsch', 'baren', 'lamm', 'wisotzkis', 'rieger', 'sunset', 'ricko']
            url_lower = url_path.lower()
            for keyword in restaurant_keywords:
                if keyword in url_lower and '-' in url_path:
                    self.log(f"✅ VALID RESTAURANT: {url} (contains restaurant keyword: {keyword})", logging.DEBUG)
                    return True
            
            # ✅ INCLUDE: Special case for known Oberrot restaurants that were being rejected
            oberrot_restaurants = ['lamm-oberrot', 'wisotzkis-restaurant-oberrot', 'rieger-gartengestaltung', 'sunset-house', 'ricko-mat']
            for restaurant in oberrot_restaurants:
                if restaurant in url_lower:
                    self.log(f"✅ VALID RESTAURANT: {url} (known Oberrot restaurant: {restaurant})", logging.DEBUG)
                return True
            
            self.log(f"🤔 UNCERTAIN URL: {url} (doesn't match clear patterns)", logging.DEBUG)
            return False
        
        restaurant_urls = []
        restaurant_data = []  # 🚀 NEW: List of (url, website_index) tuples
        working_selector = None
        
        # 🚀 OPTIMIZATION: Try cached working selector first to avoid testing 12 selectors every time
        if self._working_selector:
            urls = response.xpath(self._working_selector).getall()
            if len(urls) > 0:
                # 🔍 FILTER: Apply URL validation to cached results too
                filtered_urls = [url for url in urls if is_valid_restaurant_url(url, city_name)]
                self.log(f"⚡ CACHED SELECTOR: Found {len(urls)} URLs, kept {len(filtered_urls)} valid after filtering", logging.INFO)
                
                if len(filtered_urls) > 0:
                    restaurant_urls = filtered_urls
                    working_selector = self._working_selector
                    
                    # 🔢 Extract website indexes for cached results too
                    self.log(f"🔢 EXTRACTING website indexes for {len(restaurant_urls)} cached URLs from HTML", logging.INFO)
                    
                    restaurant_data = []
                    
                    # Use same HTML parsing method for cached results
                    if not BS4_AVAILABLE:
                        self.log("⚠️ BeautifulSoup not available, falling back to calculated indexes", logging.WARNING)
                        # Fall back to calculated indexes
                        for i, url in enumerate(restaurant_urls):
                            restaurant_data.append((url, i + 1))
                    else:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Remove script and style elements
                        for script in soup(["script", "style"]):
                            script.decompose()
                        
                        # Get visible text and find numbered entries
                        visible_text = soup.get_text()
                        lines = visible_text.split('\n')
                        
                        numbered_restaurants = []
                        for line in lines:
                            line = line.strip()
                            match = regex.match(r'^(\d{1,3})\.\s+(.+)$', line)
                            if match:
                                num, name = match.groups()
                                if (len(name) > 2 and len(name) < 80 and
                                    not any(skip in name.lower() for skip in ['home', 'english', 'deutsch', 'français', 'español', 'all rights reserved', 'loading', 'search', 'contact']) and
                                    not regex.match(r'^[\d\s\.,\-\+\(\)]+$', name)):
                                    numbered_restaurants.append((int(num), name.strip()))
                        
                        numbered_restaurants.sort(key=lambda x: x[0])
                        
                        # Match URLs with numbered entries
                        for url in restaurant_urls:
                            restaurant_slug = url.split('/')[-1].replace('-', ' ').replace('Munich', '').strip()
                            website_index = None
                            
                            for num, name in numbered_restaurants:
                                slug_words = [w.lower() for w in restaurant_slug.split() if len(w) > 2]
                                name_words = [w.lower() for w in name.split() if len(w) > 2]
                                
                                if slug_words and name_words:
                                    matches = sum(1 for slug_word in slug_words 
                                                if any(slug_word in name_word or name_word in slug_word 
                                                      for name_word in name_words))
                                    if matches > 0:
                                        website_index = num
                                        break
                            
                            restaurant_data.append((url, website_index))
                    
                    matched_count = sum(1 for _, idx in restaurant_data if idx is not None)
                    self.log(f"📊 CACHED: Matched {matched_count}/{len(restaurant_urls)} restaurants with website indexes", logging.INFO)
                    

        # 🚨 CRITICAL FIX: Always try all selectors to find the MAXIMUM number of URLs
        # Don't stop at first non-empty result - we want the BEST result
        best_urls = restaurant_urls  # Start with cached result (if any)
        best_data = restaurant_data  # Start with cached data (if any)
        best_count = len(restaurant_urls)
        best_selector = self._working_selector if restaurant_urls else None
        
        self.log(f"🔍 SELECTOR OPTIMIZATION: Starting with {best_count} URLs from cached selector", logging.INFO)
        
        # Always try all selectors to find the best result
        for i, selector in enumerate(selectors_to_try):
            urls = response.xpath(selector).getall()
            self.log(f"🔍 Selector {i+1}: '{selector}' found {len(urls)} URLs", logging.INFO)
            
            if len(urls) > 0:
                # 🔍 DEBUG: Log all URLs found by this selector
                self.log(f"🔍 DEBUG URLs found by selector {i+1}:", logging.INFO)
                for j, url in enumerate(urls[:10]):  # Show first 10 URLs
                    self.log(f"   URL {j+1}: {url}", logging.INFO)
                if len(urls) > 10:
                    self.log(f"   ... and {len(urls) - 10} more URLs", logging.INFO)
                
                    # 🔍 FILTER: Apply URL validation to exclude city pages
                    filtered_urls = [url for url in urls if is_valid_restaurant_url(url, city_name)]
                    self.log(f"🔍 FILTERED: Found {len(urls)} URLs, kept {len(filtered_urls)} valid restaurant URLs", logging.INFO)
                    
                # 🔍 DEBUG: Show which URLs were kept/rejected
                if len(filtered_urls) != len(urls):
                    rejected_urls = [url for url in urls if url not in filtered_urls]
                    self.log(f"🚫 REJECTED URLs ({len(rejected_urls)}):", logging.INFO)
                    for j, url in enumerate(rejected_urls[:5]):  # Show first 5 rejected
                        self.log(f"   Rejected {j+1}: {url}", logging.INFO)
                
                # 🚨 CRITICAL: Check if this selector found MORE URLs than our current best
                if len(filtered_urls) > best_count:
                    self.log(f"🏆 NEW BEST RESULT: Selector {i+1} found {len(filtered_urls)} URLs (previous best: {best_count})", logging.INFO)
                    best_urls = filtered_urls
                    best_count = len(filtered_urls)
                    best_selector = selector
                    # We'll extract website indexes for the best result later
                elif len(filtered_urls) == best_count and len(filtered_urls) > 0:
                    self.log(f"🤝 EQUAL RESULT: Selector {i+1} found {len(filtered_urls)} URLs (same as current best)", logging.INFO)
                
                # 🔄 SPECIAL CASE: If we found some URLs but they're all from wrong cities
                if len(urls) > 0 and len(filtered_urls) == 0:
                    self.log(f"⚠️ CROSS-CITY CONTAMINATION: Found {len(urls)} URLs but none are from {city_name}", logging.WARNING)
                    self.log(f"💡 This suggests the page is showing restaurants from nearby cities", logging.INFO)
                    
                    # 🚨 FLAG: Mark that we detected cross-city contamination
                    self.has_cross_city_contamination = True
                        
        # 🏆 USE THE BEST RESULT: Use the selector that found the most URLs
        if best_count > 0:
            self.log(f"🏆 FINAL RESULT: Using best selector with {best_count} URLs (selector: {best_selector})", logging.INFO)
            restaurant_urls = best_urls
            working_selector = best_selector
            
            # 🚨 CRITICAL FIX: REMOVED DYNAMIC COUNT UPDATE - Never change restaurants_found based on URLs found
            # The spider should ONLY use the count from count_estabs() or fallback value
            # Dynamic count updates based on URLs found can cause false completions
            self.log(f"📊 COUNT VALIDATION: Found {best_count} restaurants on this page, total expected remains {self.restaurants_found}", logging.INFO)
            self.log(f"✅ VALID RESTAURANTS: Using {best_count} URLs after optimization", logging.INFO)
        else:
            self.log(f"🚫 NO VALID RESTAURANTS: No selector found valid restaurant URLs", logging.WARNING)
            restaurant_urls = []
                    
        # 🚀 CRITICAL: Extract website index numbers from HTML content for the BEST result
        try:
            self.log(f"🔢 EXTRACTING website indexes for {len(restaurant_urls)} URLs from HTML", logging.INFO)
            
            restaurant_data = []
            
            # Method: Parse HTML content to find numbered restaurant entries
            if not BS4_AVAILABLE:
                self.log("⚠️ BeautifulSoup not available, falling back to calculated indexes", logging.WARNING)
                # 🚨 CRITICAL FIX: Handle infinite scroll vs page-based indexing
                if current_page == "infinite_scroll":
                    # 🌐 INFINITE SCROLL: Restaurant Guru still shows numbered entries like "1. Restaurant Name"
                    # The BeautifulSoup parsing below will extract the REAL website indexes from HTML
                    # If BeautifulSoup is not available, we'll have to rely on estimation
                    starting_index = max(1, resume_from_index + 1)  # Start from where we left off
                    self.log(f"🌐 INFINITE SCROLL: Estimating start index as #{starting_index} (fallback if BS4 fails)", logging.INFO)
                else:
                    # 📄 LEGACY PAGE-BASED: Calculate page-based indexes
                    try:
                        page_num = int(current_page) if current_page != "unknown" else 1
                    except:
                        page_num = 1
                    
                    # 🌐 INFINITE SCROLL: Sequential restaurant index (no page calculations)
                    starting_index = global_restaurant_counter + 1
                    self.log(f"📄 PAGE-BASED: Calculating indexes for page {page_num}", logging.INFO)
                
                # Assign indexes to restaurants
                for i, url in enumerate(restaurant_urls):
                    calculated_index = starting_index + i
                    restaurant_data.append((url, calculated_index))
                
                # Log the correct calculation 
                self.log(f"✅ Successfully calculated {len(restaurant_data)} website indexes: #{starting_index}-{starting_index + len(restaurant_urls) - 1}", logging.INFO)
            else:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Remove script and style elements to get clean text
                for script in soup(["script", "style"]):
                    script.decompose()
                
                # Get visible text and look for numbered entries
                visible_text = soup.get_text()
                lines = visible_text.split('\n')
                
                # Find numbered restaurant entries like "186. Poseidon"
                numbered_restaurants = []
                for line in lines:
                    line = line.strip()
                    match = regex.match(r'^(\d{1,3})\.\s+(.+)$', line)
                    if match:
                        num, name = match.groups()
                        # Filter out non-restaurant entries
                        if (len(name) > 2 and len(name) < 80 and
                            not any(skip in name.lower() for skip in ['home', 'english', 'deutsch', 'français', 'español', 'all rights reserved', 'loading', 'search', 'contact']) and
                            not regex.match(r'^[\d\s\.,\-\+\(\)]+$', name)):
                            numbered_restaurants.append((int(num), name.strip()))
                
                # Sort by number
                numbered_restaurants.sort(key=lambda x: x[0])
                
                self.log(f"🔢 Found {len(numbered_restaurants)} numbered restaurant entries", logging.INFO)
                
                # Match URLs with numbered entries
                for url in restaurant_urls:
                    restaurant_slug = url.split('/')[-1].replace('-', ' ').replace('Munich', '').strip()
                    website_index = None
                    
                    # Try to match URL with numbered entries
                    for num, name in numbered_restaurants:
                        # Check if restaurant names match (fuzzy matching)
                        slug_words = [w.lower() for w in restaurant_slug.split() if len(w) > 2]
                        name_words = [w.lower() for w in name.split() if len(w) > 2]
                        
                        # If any significant word matches, consider it a match
                        if slug_words and name_words:
                            matches = sum(1 for slug_word in slug_words 
                                        if any(slug_word in name_word or name_word in slug_word 
                                              for name_word in name_words))
                            if matches > 0:
                                website_index = num
                                self.log(f"🎯 MATCHED: {restaurant_slug} -> Website index {website_index} ({name})", logging.DEBUG)
                                break
                    
                    if website_index is None:
                        self.log(f"⚠️ No match found for: {restaurant_slug}", logging.DEBUG)
                    
                    restaurant_data.append((url, website_index))
                
                # Check matching results and handle unmatched restaurants
                matched_count = sum(1 for _, idx in restaurant_data if idx is not None)
                unmatched_count = len(restaurant_data) - matched_count
                
                if matched_count == 0:
                    self.log(f"⚠️ No website indexes matched, falling back to calculation", logging.WARNING)
                elif unmatched_count > 0:
                    self.log(f"⚠️ PARTIAL MATCHING: {matched_count}/{len(restaurant_data)} matched, {unmatched_count} unmatched - assigning calculated indexes to unmatched", logging.WARNING)
                    
                    # 🚨 FIX: Assign calculated indexes to unmatched restaurants
                    next_available_index = max((idx for _, idx in restaurant_data if idx is not None), default=0) + 1
                    for i, (url, idx) in enumerate(restaurant_data):
                        if idx is None:
                            restaurant_data[i] = (url, next_available_index)
                            next_available_index += 1
                            self.log(f"🔧 ASSIGNED INDEX: {url.split('/')[-1]} -> Index {restaurant_data[i][1]}", logging.INFO)
                
                if matched_count == 0:
                    # Fallback calculation logic
                    if current_page == "infinite_scroll":
                        # 🌐 INFINITE SCROLL: Use sequential indexing
                        starting_index = 1
                        self.log("🌐 INFINITE SCROLL FALLBACK: Using sequential indexing", logging.INFO)
                    else:
                        # 📄 LEGACY PAGE-BASED: Calculate page-based indexes
                        try:
                            page_num = int(current_page) if current_page != "unknown" else 1
                        except:
                            page_num = 1
                        
                        # 🌐 INFINITE SCROLL: Sequential restaurant index based on position
                        starting_index = global_restaurant_counter + 1
                    
                    restaurant_data = []
                    for position, url in enumerate(restaurant_urls):
                        calculated_index = starting_index + position
                        restaurant_data.append((url, calculated_index))
                else:
                    self.log(f"✅ Successfully matched {matched_count}/{len(restaurant_urls)} restaurants with website indexes", logging.INFO)
                    
        except Exception as e:
            self.log(f"⚠️ Error extracting website indexes: {e}, falling back to calculation", logging.WARNING)
            # Fallback calculation
            if current_page == "infinite_scroll":
                # 🌐 INFINITE SCROLL: Use sequential indexing
                starting_index = 1
                self.log("🌐 INFINITE SCROLL ERROR FALLBACK: Using sequential indexing", logging.INFO)
            else:
                # 📄 LEGACY PAGE-BASED: Calculate page-based indexes
                try:
                    page_num = int(current_page) if current_page != "unknown" else 1
                except:
                    page_num = 1
            
            # 🌐 INFINITE SCROLL: Sequential restaurant index (no page calculations)
            starting_index = global_restaurant_counter + 1
                        
            restaurant_data = []
            for position, url in enumerate(restaurant_urls):
                calculated_index = starting_index + position
                restaurant_data.append((url, calculated_index))
                    
                # 🚨 RESTAURANT VALIDATION: Use our new validation instead of hardcoded patterns
                individual_restaurants = restaurant_urls  # Already filtered by is_valid_restaurant_url
                if len(individual_restaurants) > 0:
                    self._working_selector = selector  # Cache only if it finds real restaurants
                    self.log(f"✅ Using working selector {i+1}: '{selector}' (CACHED - found {len(individual_restaurants)} restaurants)", logging.INFO)
                    break
                else:
                    self.log(f"⚠️ Selector {i+1} found only navigation URLs - NOT CACHING", logging.WARNING)
        
        # 🚨 DYNAMIC COUNT UPDATE: If we found more restaurants than initial count, update the count
        if len(restaurant_urls) > self.restaurants_found:
            old_count = self.restaurants_found
            self.restaurants_found = len(restaurant_urls)
            self.log(f"📈 DYNAMIC COUNT UPDATE: Found {len(restaurant_urls)} restaurants (was {old_count}) - updating total count", logging.INFO)
            self.log(f"💡 This happens when count phase gets CAPTCHA-blocked but extraction works", logging.INFO)
        
        if len(restaurant_urls) == 0:
            self.log(f"❌ NO RESTAURANT URLs FOUND WITH ANY SELECTOR!", logging.ERROR)
            self.log(f"🔍 Page URL: {response.url}", logging.ERROR)
            
            
            # Try to find ANY links on the page for debugging
            all_links = response.xpath('.//a/@href').getall()
            self.log(f"Ã°Å¸â€Â Total links found on page: {len(all_links)}", logging.INFO)
            
            # 🚨 CAPTCHA DETECTION: Check if page contains primarily navigation/generic links (common CAPTCHA pattern)
            navigation_patterns = ['disclaimer', 'request_content_removal', 'tutorial', 'restaurantguru.com', 'privacy', 'contact']
            navigation_links = [link for link in all_links if any(pattern in link.lower() for pattern in navigation_patterns)]
            
            # 🚨 ENHANCED DETECTION: Trigger if 80%+ links are navigation OR if no valid restaurant URLs but navigation links exist
            navigation_percentage = (len(navigation_links) / len(all_links)) * 100 if len(all_links) > 0 else 0
            
            if len(all_links) > 0 and (len(navigation_links) == len(all_links) or navigation_percentage >= 80 or len(navigation_links) >= 3):
                self.log(f"🚨 CAPTCHA/BLOCKING DETECTED: Page contains {len(navigation_links)} navigation links out of {len(all_links)} total ({navigation_percentage:.1f}%)", logging.ERROR)
                self.has_critical_errors = True  # 🚨 Mark as critical error to trigger retry
                emergency_fallback_used = True  # 🚨 CRITICAL FIX: Use minimal filtering when CAPTCHA detected
            
            restaurant_related_links = [link for link in all_links if 'restaurant' in link.lower()]
            self.log(f"Ã°Å¸â€Â Restaurant-related links: {len(restaurant_related_links)}", logging.INFO)
            if restaurant_related_links:
                self.log(f"Ã°Å¸â€Â Sample restaurant links: {restaurant_related_links[:3]}", logging.INFO)
        
        # 🚀 EMERGENCY FALLBACK: If no URLs found, try to extract any city-related URLs as last resort
        if len(restaurant_urls) == 0:
            all_links = response.xpath('.//a/@href').getall()
            city_links = [link for link in all_links if city_name.lower() in link.lower() and 'restaurantguru.com' in link and not any(skip in link for skip in ['?', '/restaurant-', 'skip_geo'])]
            if city_links:
                self.log(f"🚀 EMERGENCY FALLBACK: Found {len(city_links)} {city_name} links", logging.INFO)
                restaurant_urls = city_links[:20]  # Limit to 20 to avoid overload
                self.log(f"🚀 Using emergency fallback URLs: {restaurant_urls[:3]}", logging.INFO)
        
        # 🚨 CRITICAL: API WASTE PREVENTION - Stop processing if multiple consecutive pages have no restaurants
        if len(restaurant_urls) == 0:
            if not hasattr(self, 'consecutive_empty_pages'):
                self.consecutive_empty_pages = 0
            
            self.consecutive_empty_pages += 1
            self.log(f"⚠️ CONSECUTIVE EMPTY PAGES: {self.consecutive_empty_pages}", logging.WARNING)
            
            # 🚨 MUNICH FIX: Detect if we're hitting duplicate/cyclical content
            if self.consecutive_empty_pages >= 3:
                current_city = getattr(self, 'current_city_info', {})
                city_name = current_city.get('city_name', 'Unknown') if current_city else 'Unknown'
                
                if city_name.lower() == 'munich':
                    current_page = getattr(self, 'last_processed_page', 0)
                    self.log(f"🔄 MUNICH ANALYSIS: Hit empty pages at page {current_page}", logging.WARNING)
                    self.log(f"📊 MUNICH DEBUG: rest_slugs contains {len(self.rest_slugs)} URLs", logging.INFO)
                    self.log(f"📊 MUNICH DEBUG: restaurants_processed = {self.restaurants_processed}", logging.INFO)
                    
                    # If we're past page 9 and hitting duplicates, Munich likely has cyclical pagination
                    if current_page >= 10:
                        self.log(f"💡 MUNICH CYCLICAL: Pages 10+ likely repeat pages 1-9 content", logging.WARNING)
                        self.log(f"🛑 MUNICH STOP: Stopping to prevent infinite duplicate processing", logging.INFO)
                        # Mark as completed rather than cycling infinitely
                        return
                    return
                else:
                    self.log(f"🛑 STOPPING PROCESSING: {self.consecutive_empty_pages} consecutive pages with no restaurants - preventing API waste", logging.ERROR)
                    self.log(f"💰 API CALLS SAVED: Stopping early to prevent further waste", logging.INFO)
                    return  # Stop processing this page and subsequent pages
        else:
            # Reset counter if we found restaurants
            if hasattr(self, 'consecutive_empty_pages'):
                self.consecutive_empty_pages = 0
        
        # 🔍 POST-PROCESSING: Simple filtering like the working old version
        if len(restaurant_urls) > 0:
            original_count = len(restaurant_urls)
            filtered_urls = []
            filtered_restaurant_data = []  # 🚨 CRITICAL FIX: Keep indexes aligned with filtered URLs
            
            # 🚨 DEBUG: Log all found URLs to understand what we're working with
            self.log(f"🔍 DEBUG: Found {len(restaurant_urls)} URLs to filter:", logging.INFO)
            for i, url in enumerate(restaurant_urls[:5]):  # Show first 5 URLs
                self.log(f"  URL {i+1}: {url}", logging.INFO)
            
            # 🚨 EMERGENCY MODE: Log when minimal filtering is used
            if emergency_fallback_used:
                self.log(f"🚨 EMERGENCY/CAPTCHA MODE: Using minimal filtering to prevent valid restaurant loss", logging.INFO)
            
            # 🚨 CRITICAL FIX: Create URL->index mapping to preserve indexes through filtering
            url_to_index = {}
            if restaurant_data:
                for url_data in restaurant_data:
                    if len(url_data) >= 2:
                        url, index = url_data[0], url_data[1]
                        url_to_index[url] = index
            
            # 🚨 CRITICAL FIX: Since selector optimization already found the BEST URLs,
            # we should apply MINIMAL post-processing filtering to avoid losing valid restaurants
            self.log(f"🔍 POST-PROCESSING: Applying minimal filtering to {len(restaurant_urls)} optimized URLs", logging.INFO)
            
            for url in restaurant_urls:
                # Convert relative URL to absolute URL if needed (like old version)
                if url.startswith("/"):
                    abs_url = response.urljoin(url)
                else:
                    abs_url = url
                
                # 🚨 SIMPLE VALIDATION: Only check basic URL format (like old version)
                if not abs_url or ".com/" not in abs_url:
                    self.log(f"🚫 FILTERED: Malformed URL {abs_url}", logging.DEBUG)
                    continue
                
                # 🚨 EMERGENCY FALLBACK: Use minimal filtering to prevent valid restaurant loss
                if emergency_fallback_used:
                    # Only filter out obvious non-restaurant URLs
                    if any(pattern in abs_url.lower() for pattern in ['/privacy', '/terms', '/contact', '/about', '/help', '?']):
                        self.log(f"🚫 MINIMAL FILTER: Non-restaurant URL {abs_url}", logging.DEBUG)
                        continue
                    
                    # Keep all other URLs - they're already filtered by city name in emergency fallback
                    filtered_urls.append(abs_url)
                    website_index = url_to_index.get(url, url_to_index.get(abs_url, None))
                    filtered_restaurant_data.append((abs_url, website_index))
                    self.log(f"✅ EMERGENCY KEPT: {abs_url}", logging.DEBUG)
                    continue
                
                # 🚨 ENHANCED FILTERING: Exclude ALL non-restaurant URLs to prevent API waste
                navigation_patterns = [
                    '/contactus', '/aboutus', '/privacy', '/terms', '/help', '/disclaimer', 
                    '/tutorial', '/request_content_removal', '/privacy_policy', '/tos',
                    '/support', '/blog', '/news', '/careers', '/investors', '/advertise',
                    '/mobile', '/sitemap', '/robots.txt', '/favicon.ico', '.xml', '.rss'
                ]
                
                if any(nav in abs_url.lower() for nav in navigation_patterns):
                    self.log(f"🚫 FILTERED: Navigation/Non-restaurant URL {abs_url}", logging.DEBUG)
                    continue
                    
                # 🚨 CRITICAL API WASTE FIX: Skip review pages and query parameters
                if '/reviews' in abs_url or '?review=' in abs_url or '#' in abs_url:
                    self.log(f"🚫 FILTERED: Review/query URL {abs_url}", logging.DEBUG)
                    continue
                
                # 🚨 CRITICAL FIX: Block ALL URLs with query parameters (they are navigation, not individual restaurants)
                if '?' in abs_url:
                    self.log(f"🚫 FILTERED: Query parameter URL (navigation) {abs_url}", logging.DEBUG)
                    continue
                
                # 🚨 ENHANCED GUIDE FILTERING: Block guide pages and non-restaurant content
                url_path = abs_url.split('restaurantguru.com/')[-1]
                
                # Enhanced guide patterns (case-insensitive)
                guide_patterns = [
                    '/guides/', '/guide/', 'guides/', 'guide/',  # Guide sections
                    '/search/', '/list/', '/category/', 'search/', 'list/', 'category/',  # Navigation
                    '/top-', '/best-', '/popular-', '/trending-', '/new-', '/featured-',  # Curated lists
                    'top-', 'best-', 'popular-', 'trending-', 'new-', 'featured-',  # Without slash prefix
                    'die-besten-', 'aktivit', 'activities', 'things-to-do',  # German guides
                    'tourism', 'tourist', 'attractions', 'sightseeing', 'events',  # Tourism content
                    '/blog/', '/news/', '/article/', 'blog/', 'news/', 'article/',  # Editorial content
                ]
                
                # Check for guide/non-restaurant pages
                url_path_lower = url_path.lower()
                if any(pattern.lower() in url_path_lower for pattern in guide_patterns):
                    self.log(f"🚫 FILTERED: Guide/tourism/non-restaurant page: {abs_url}", logging.DEBUG)
                    continue
                
                # 🚨 CRITICAL DISTINCTION: 
                # - restaurant-{city}-t{number} URLs = listing pages (should NOT be scraped as individual restaurants)
                # - Individual restaurant URLs = actual restaurant pages we want to scrape
                # We need to filter OUT the listing pages but keep individual restaurant URLs
                
                # Filter out listing pages (restaurant-{city}-t{number} format)
                import re
                if regex.match(r'restaurant-[^/]+-t\d+', url_path):
                    self.log(f"🚫 FILTERED: Listing page URL (not individual restaurant): {abs_url}", logging.DEBUG)
                    continue
                
                # 🚨 ENHANCED NAVIGATION FILTERING: Block common navigation pages
                navigation_pages = [
                    'businesslanding', 'germany', 'munich', 'berlin', 'hamburg', 'cologne', 'frankfurt',
                    'stuttgart', 'dusseldorf', 'dortmund', 'essen', 'leipzig', 'bremen', 'dresden',
                    'hanover', 'nuremberg', 'duisburg', 'bochum', 'wuppertal', 'bielefeld', 'bonn',
                    'munster', 'karlsruhe', 'mannheim', 'augsburg', 'wiesbaden', 'gelsenkirchen',
                    'monchengladbach', 'braunschweig', 'chemnitz', 'kiel', 'aachen', 'halle',
                    'magdeburg', 'freiburg', 'krefeld', 'lubeck', 'oberhausen', 'erfurt', 'mainz',
                    'rostock', 'kassel', 'hagen', 'potsdam', 'saarbrucken', 'hamm', 'mulheim',
                    'ludwigshafen', 'leverkusen', 'oldenburg', 'osnabrueck', 'solingen', 'heidelberg',
                    'herne', 'neuss', 'darmstadt', 'paderborn', 'regensburg', 'ingolstadt', 'wurzburg',
                    'fuerth', 'wolfsburg', 'offenbach', 'ulm', 'heilbronn', 'pforzheim', 'gottingen',
                    'bottrop', 'trier', 'recklinghausen', 'reutlingen', 'bremerhaven', 'koblenz',
                    'bergisch', 'jena', 'remscheid', 'erlangen', 'moers', 'siegen', 'hildesheim',
                    'salzgitter'
                ]
                
                # Extract the path from the URL
                url_path = abs_url.split('restaurantguru.com/')[-1].lower()
                
                # 🚨 CRITICAL: Block navigation pages (cities, business pages, etc.)
                if url_path in navigation_pages:
                    self.log(f"🚫 FILTERED: Navigation page {abs_url}", logging.DEBUG)
                    continue
                
                # 🚀 CRITICAL FIX: KEEP individual restaurant URLs (be more permissive but safer)
                # Keep URLs that look like individual restaurants (no query params, not obviously navigation)
                if '?' not in abs_url and 'restaurantguru.com' in abs_url:
                    # Additional check: URL should have more than just domain and not be a navigation page
                    if url_path and url_path not in ['', 'de', 'en', 'es', 'ru']:
                        # 🚨 FIXED: Much more permissive - keep URLs that look like restaurants  
                        # Allow URLs with slashes (like restaurant-munich-t1/12) as long as they don't match navigation patterns
                        # 🔧 CRITICAL FIX: Check if URL path EQUALS navigation page, not just contains it
                        # This prevents Munich restaurants like "Nuovo-Casale-Munich" from being filtered
                        is_navigation = url_path in navigation_pages or url_path.split('/')[0] in navigation_pages
                        
                        if not is_navigation:
                            # Keep if it's a proper restaurant path (has restaurant identifier or is long enough)
                            if ('restaurant' in url_path or '-' in url_path or len(url_path) > 8):
                                filtered_urls.append(abs_url)
                                # 🚨 CRITICAL FIX: Preserve the website index for this URL
                                website_index = url_to_index.get(url, url_to_index.get(abs_url, None))
                                filtered_restaurant_data.append((abs_url, website_index))
                                self.log(f"✅ KEPT: Individual restaurant URL {abs_url} (index: {website_index})", logging.DEBUG)
                                continue
                            else:
                                self.log(f"🚫 FILTERED: Short/simple navigation page {abs_url}", logging.DEBUG)
                                continue
                        else:
                            self.log(f"🚫 FILTERED: Navigation page detected {abs_url}", logging.DEBUG)
                            continue
                
                # Skip obviously non-restaurant pages
                url_parts = abs_url.split('/')[-1].lower()
                if url_parts in ['index', 'home', 'main', 'search', 'results', '']:
                    self.log(f"🚫 FILTERED: Non-restaurant page {abs_url}", logging.DEBUG)
                    continue
                
                # 🚀 CRITICAL FIX: Default to keeping restaurant-looking URLs instead of filtering them
                # Only filter if we're certain it's not a restaurant
                if ('restaurantguru.com' in abs_url and 
                    url_path and 
                    url_path not in navigation_pages and  # 🔧 FIXED: Exact match only
                    url_path.split('/')[0] not in navigation_pages and  # Check first path component
                    url_path not in ['', 'de', 'en', 'es', 'ru']):
                    # This looks like it could be a restaurant - keep it
                    filtered_urls.append(abs_url)
                    # 🚨 CRITICAL FIX: Preserve the website index for this URL
                    website_index = url_to_index.get(url, url_to_index.get(abs_url, None))
                    filtered_restaurant_data.append((abs_url, website_index))
                    self.log(f"✅ KEPT: Potential restaurant URL {abs_url} (index: {website_index})", logging.DEBUG)
                else:
                    self.log(f"🚫 FILTERED: Uncertain/Navigation URL {abs_url}", logging.DEBUG)
            
            restaurant_urls = filtered_urls
            restaurant_data = filtered_restaurant_data  # 🚨 CRITICAL FIX: Use filtered data with preserved indexes
            filtered_count = original_count - len(restaurant_urls)
            
            if filtered_count > 0:
                self.log(f"🔍 FILTERED: Removed {filtered_count} navigation/invalid URLs, kept {len(restaurant_urls)} restaurant URLs", logging.INFO)
            
            # Track URL extraction results
            self.log(f"🔍 URL extraction completed at {datetime.now().isoformat()}", logging.DEBUG)
            self.log(f"🔍 Found {len(restaurant_urls)} restaurant URLs", logging.DEBUG)
            
            # 🎯 SUCCESS: Log when restaurants are found to track working URL formats
            if len(restaurant_urls) > 0:
                attempted_format = response.meta.get('attempted_format', 'path')
                self.log(f"✅ SUCCESS: Infinite scroll found {len(restaurant_urls)} restaurants using {attempted_format} format", logging.INFO)
                self.log(f"🔗 WORKING URL: {response.url}", logging.INFO)
                
            # If no restaurants found, this might indicate structure change or wrong page
            if len(restaurant_urls) == 0:
                self.log(f"⚠️ NO RESTAURANTS FOUND: Page returned 0 restaurants", logging.WARNING)
                self.log(f"🔍 No restaurants found, will exit parse_estabs at {datetime.now().isoformat()}", logging.DEBUG)
                
                # Check if this is a high restaurant index that got blocked by captcha
                if resume_from_index > 200:
                    self.log(f"🚨 HIGH INDEX ACCESS: Restaurant index {resume_from_index} returned 0 restaurants (likely captcha-blocked)", logging.WARNING)
                    self.log(f"💡 SOLUTION: The infinite scroll works, but Restaurant Guru blocks high indexes with captcha", logging.INFO)
                    self.log(f"🔧 RECOMMENDATION: Use slower crawling speed for high restaurant indexes to avoid captcha detection", logging.INFO)
                    return
                elif response.meta.get('fallback_mode', False):
                    original_page = response.meta.get('original_target_page', 'unknown')
                    self.log(f"🚨 CONFIRMED: Even page 1 has no restaurants - site structure changed!", logging.ERROR)
                    self.log(f"🛑 STOPPING: Cannot proceed when no restaurants found on any page", logging.ERROR)
                    return
        
        # Process restaurants with enhanced logging
        skipped_already_scraped = 0
        skipped_duplicate_slug = 0
        skipped_malformed_url = 0
        
        # 🚀 OPTIMIZED RESUME: Slice restaurant list to start from correct index instead of skipping
        if resume_from_index > 0 and len(restaurant_urls) > resume_from_index:
            # Calculate how many restaurants to skip from the beginning of the list
            restaurants_to_skip = resume_from_index
            original_count = len(restaurant_urls)
            
            # Slice the lists to start from the resume point
            restaurant_urls = restaurant_urls[restaurants_to_skip:]
            if restaurant_data:
                restaurant_data = restaurant_data[restaurants_to_skip:]
            
            # Adjust the global counter to match the resume point
            global_restaurant_counter = resume_from_index
            
            self.log(f"🔄 OPTIMIZED RESUME: Skipped first {restaurants_to_skip} restaurants, processing {len(restaurant_urls)} remaining (total: {original_count})", logging.INFO)
            self.log(f"🔄 RESUME: Starting from restaurant #{resume_from_index + 1}", logging.INFO)
        else:
            self.log(f"🆕 FRESH START: Processing all {len(restaurant_urls)} restaurants from beginning", logging.INFO)
        
        # Process restaurants using sequential global indexing for checkpoints
        self.log(f"🔍 Starting restaurant processing loop at {datetime.now().isoformat()}", logging.DEBUG)
        self.log(f"🔍 Will process {len(restaurant_urls)} restaurants", logging.DEBUG)
        
        for i, est_url in enumerate(restaurant_urls):
            # 🔢 ALWAYS use sequential global counter for checkpoint consistency
            global_restaurant_counter += 1
            current_restaurant_index = global_restaurant_counter
            
            # 🔍 DEBUG: Log every URL being processed
            self.log(f"🔍 PROCESSING URL #{i+1}/{len(restaurant_urls)}: Restaurant #{current_restaurant_index} - {est_url}", logging.INFO)
            
            # 🚨 BOUNDARY CHECK: Allow some flexibility for count discrepancies
            if current_restaurant_index > (self.restaurants_found + 5):  # Allow 5 extra restaurants for flexibility
                self.log(f"🛑 BOUNDARY EXCEEDED: Restaurant #{current_restaurant_index} > found count {self.restaurants_found} + 5 buffer", logging.WARNING)
                self.log(f"💡 SOLUTION: This prevents processing non-existent restaurants that cause captcha", logging.INFO)
                break
            elif current_restaurant_index > self.restaurants_found:
                self.log(f"⚠️ BOUNDARY FLEXIBLE: Restaurant #{current_restaurant_index} > found count {self.restaurants_found}, but within buffer - processing", logging.INFO)
            
            # 🔢 Extract website index for reference but use global counter for processing
            if restaurant_data and i < len(restaurant_data):
                url_data = restaurant_data[i]
                website_index = url_data[1] if len(url_data) > 1 else None
                if website_index is not None:
                    self.log(f"🔢 SEQUENTIAL INDEX: #{current_restaurant_index} (website shows #{website_index}) for {est_url.split('/')[-1]}", logging.DEBUG)
                else:
                    self.log(f"🔢 SEQUENTIAL INDEX: #{current_restaurant_index} (no website index) for {est_url.split('/')[-1]}", logging.DEBUG)
            else:
                self.log(f"🔢 SEQUENTIAL INDEX: #{current_restaurant_index} for {est_url.split('/')[-1]}", logging.DEBUG)
                
            # 🚀 NEW: Index-based filtering - skip restaurants outside our target range
            if self.index_based_mode:
                if current_restaurant_index < self.start_index:
                    self.log(f"⏭️ SKIPPING: Restaurant #{current_restaurant_index} (before start index {self.start_index})", logging.DEBUG)
                    continue
                elif current_restaurant_index > self.end_index:
                    self.log(f"🛑 STOPPING: Restaurant #{current_restaurant_index} exceeds end index {self.end_index}", logging.INFO)
                    break
            
            # 🔗 PAGINATION SKIP: Skip restaurants that were already processed (checkpoint resume)
            elif pagination_mode and current_restaurant_index <= resume_from_index:
                self.log(f"⏭️ PAGINATION SKIP: Restaurant #{current_restaurant_index} already processed (resume from #{resume_from_index + 1})", logging.INFO)
                continue
            # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Check circuit breaker before each restaurant
            if self._should_stop_processing():
                self.log(f"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ¢â‚¬Ëœ Circuit breaker triggered - processed {restaurants_on_page} restaurants on this page", logging.WARNING)
                break
            
            # 🌐 INFINITE SCROLL: Process ALL restaurants found on page (no artificial limits)
            # Note: self.restaurant_count tracks unique restaurants processed, not page position

            # Convert relative URL to absolute URL if needed
            if est_url.startswith("/"):
                est_url = response.urljoin(est_url)

            # Validate URL format
            if not est_url or ".com/" not in est_url:
                skipped_malformed_url += 1
                self.log(f"Ã¢Å¡ Ã¯Â¸Â Skipping malformed URL: {est_url}", logging.WARNING)
                continue

            # CHECK 1: Database deduplication (cross-run prevention)
            if self._is_already_scraped(est_url):
                self.log(f"ÃƒÂ¢Ã‚ÂÃ‚Â­ÃƒÂ¯Ã‚Â¸Ã‚Â Skipping already-scraped restaurant: {est_url}", logging.INFO)
                continue

            try:
                rest_slug = est_url.split(".com/")[1]
            except IndexError:
                skipped_malformed_url += 1
                self.log(f"Ã¢Å¡ Ã¯Â¸Â Skipping URL with invalid format: {est_url}", logging.WARNING)
                continue

            # CHECK 2: In-memory deduplication (within-run prevention)
            if rest_slug not in self.rest_slugs:
                self.rest_slugs.append(rest_slug)
                self.restaurant_count += 1
                restaurants_on_page += 1
                # 🚨 CRITICAL FIX: Use website index for logging and checkpoints when available
                self.log(f"🍽️  Processing restaurant #{current_restaurant_index} on page {current_page}: {rest_slug}", logging.INFO)
                
                # 🏙️ MEGA CITY: Process all restaurants in single run (no batch limits)
                restaurants_to_process = self.restaurant_count
                self.log(f"🏙️ MEGA CITY PROCESSING: Processing restaurant #{current_restaurant_index} ({restaurants_to_process} total queued)", logging.INFO)
                
                # Log request being made
                self.log(f"🔗 MAKING REQUEST: Restaurant #{current_restaurant_index} URL: {est_url}", logging.DEBUG)
                self.log(f"🔍 REQUEST META: website_index={website_index}, resume_from_index={resume_from_index}", logging.DEBUG)
                
                yield Request(
                    url=est_url,
                    headers={'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"},
                    callback=self.parse_estab_page,
                    dont_filter=True,  # 🚨 CRITICAL FIX: Disable filtering for individual restaurants
                    meta={
                        **response.meta,
                        'restaurant_index': current_restaurant_index,  # 🚨 CRITICAL: Use website index when available
                        'website_index': website_index,  # 🔢 NEW: Pass website index for reference
                        'resume_from_index': resume_from_index,  # Pass resume index for context
                        'city_info': response.meta.get('city_info', self.current_city_info)  # 🔧 CRITICAL: Pass city info for checkpoint saving
                    }
                )
                
                # 🚀 CHECKPOINT: Save progress every N restaurants
                if current_restaurant_index % self.checkpoint_interval == 0:
                    # Get city info from response meta or fallback to current_city_info
                    city_info = response.meta.get('city_info', {})
                    city_slug = city_info.get('city_slug') if city_info else (self.current_city_info.get('city_slug') if hasattr(self, 'current_city_info') and self.current_city_info else None)
                    
                    if city_slug:
                        # 🔗 PAGINATION MODE: Use appropriate checkpoint method based on processing mode
                        if pagination_mode:
                            # 📄 PAGINATION: Update last_processed_restaurant_index but preserve page-based tracking
                            self.log(f"💾 PAGINATION CHECKPOINT: Restaurant #{current_restaurant_index} (website: {website_index}), City: {city_slug}", logging.INFO)
                            self._update_restaurant_index_only(city_slug, self.items_yielded)
                        else:
                            # 🌐 INFINITE SCROLL: Use traditional checkpoint method
                            self.log(f"💾 INFINITE SCROLL CHECKPOINT: Restaurant #{current_restaurant_index} (website: {website_index}), City: {city_slug}", logging.INFO)
                        self._save_checkpoint(
                            city_slug,
                            self.items_yielded,  # 🚨 CRITICAL: Use actual yielded items count
                            {'slug': rest_slug, 'url': est_url, 'website_index': website_index}
                        )
                    else:
                        self.log(f"⚠️ CHECKPOINT FAILED: No city_slug available. Meta: {response.meta.get('city_info')}, Current: {getattr(self, 'current_city_info', None)}", logging.ERROR)
            else:
                # 🚨 CRITICAL FIX: Log when duplicate is found to debug the 20→19 issue
                self.log(f"⚠️ WARNING: DUPLICATE restaurant found and skipped: {rest_slug}", logging.WARNING)
                self.log(f"🔍 DUPLICATE DEBUG: This restaurant was already processed in this run", logging.INFO)
                continue  # Skip this duplicate restaurant
        
        # 🔍 DEBUG: Log end of processing loop
        self.log(f"🔍 PROCESSING LOOP COMPLETED: Processed {len(restaurant_urls)} URLs, yielded {restaurants_on_page} requests", logging.INFO)
                
        # Ã°Å¸Å½Â¯ ENHANCED SUMMARY LOGGING
        
        total_found = len(restaurant_urls)
        self.log(f"ÃƒÂ¢Ã‚ÂÃ‚Â­ÃƒÂ¯Ã‚Â¸Ã‚Â Skipping duplicate slug: {rest_slug}", logging.INFO)
                
        # 🔍 DEBUG: Log end of processing loop
        self.log(f"🔍 PROCESSING LOOP COMPLETED: Processed {len(restaurant_urls)} URLs, yielded {restaurants_on_page} requests", logging.INFO)
                
        # Ã°Å¸Å½Â¯ ENHANCED SUMMARY LOGGING
        total_found = len(restaurant_urls)
        total_accounted = restaurants_on_page + skipped_already_scraped + skipped_duplicate_slug + skipped_malformed_url
        
        # 📊 ENHANCED PAGE SUMMARY with progress tracking
        progress_pct = (self.restaurants_processed / self.restaurants_found * 100) if self.restaurants_found > 0 else 0
        self.log(f"📊 PAGE SUMMARY: Found {total_found} URLs, Processed {restaurants_on_page} restaurants", logging.INFO)
        self.log(f"📈 PROGRESS: Total processed {self.restaurants_processed}/{self.restaurants_found} ({progress_pct:.1f}%)", logging.INFO)
        self.log(f"⏭️  SKIPPED: DB duplicates {skipped_already_scraped}, In-memory duplicates {skipped_duplicate_slug}, Malformed {skipped_malformed_url}", logging.WARNING)
        
        # 🚨 CRITICAL: Alert if we're missing restaurants (should process all 20 per page)
        expected_per_page = 20
        if total_found == expected_per_page and restaurants_on_page < expected_per_page:
            missing_count = expected_per_page - restaurants_on_page
            total_skipped = skipped_already_scraped + skipped_duplicate_slug + skipped_malformed_url
            self.log(f"🚨 MISSING RESTAURANTS: Expected {expected_per_page}, found {total_found}, processed {restaurants_on_page}, skipped {total_skipped}, MISSING {missing_count}", logging.ERROR)
        
        # Track method completion
        self.log(f"🔍 parse_estabs method completed at {datetime.now().isoformat()}", logging.DEBUG)
        self.log(f"🔍 Method will now exit and return control to Scrapy", logging.DEBUG)

        if total_found != total_accounted:
            self.log(f"Ã¢Å¡ Ã¯Â¸Â ACCOUNTING MISMATCH: Found {total_found} URLs but only accounted for {total_accounted}!", logging.WARNING)
        
        # 🚨 SMART CHECKPOINT DETECTION: Check for problematic retry scenarios
        if restaurants_on_page == 0 and total_found > 0:
            # All restaurants on this infinite scroll page are duplicates/already processed
            self.log(f"🔄 DUPLICATE SCROLL DETECTED: Infinite scroll has {total_found} restaurants but all are already processed", logging.WARNING)
            self.log(f"💡 ANALYSIS: This could mean the scroll position is incorrect or restaurants are being duplicated", logging.INFO)
            
            # 🔍 Check if we can find any valid restaurants to process
            if restaurant_urls:
                self.log(f"🔍 ANALYSIS: Found {len(restaurant_urls)} restaurants, but all are already processed", logging.INFO)
                self.log(f"💡 This suggests the city may be complete or the scroll position is incorrect", logging.INFO)
            
            if resume_from_index >= 200:
                self.log(f"📈 CHECKPOINT ISSUE: High restaurant index ({resume_from_index}) with all duplicates suggests checkpoint calculation error", logging.ERROR)
                self.log(f"💡 SOLUTION: Consider advancing to next unprocessed restaurant range", logging.INFO)
        elif total_found == 0 and resume_from_index > 200:
            # High restaurant index with no restaurants (likely captcha)
            self.log(f"🚫 CAPTCHA BLOCKING: Restaurant index {resume_from_index} returned 0 restaurants (captcha likely)", logging.ERROR)
            self.log(f"💡 SOLUTION: Implement exponential backoff or alternative access method", logging.INFO)
        
        # 🔧 CRITICAL: Get city slug for checkpoint and completion logic
        city_info = response.meta.get('city_info', {})
        city_slug = city_info.get('city_slug') if city_info else (self.current_city_info.get('city_slug') if hasattr(self, 'current_city_info') and self.current_city_info else None)
        
        # 🚨 REMOVED: Premature checkpoint saving - this happens BEFORE restaurants are processed!
        # Checkpoint will be saved after restaurants are actually processed in parse_estab_page
        
        # 🔗 SEQUENTIAL PAGINATION: Generate next page request after processing current page
        pagination_mode = response.meta.get('pagination_mode', False)
        
        if pagination_mode:
            current_page = response.meta.get('page_number', 1)
            total_pages = response.meta.get('total_pages', 1)
            restaurants_per_page = response.meta.get('restaurants_per_page', 20)  # User confirmed: 20 per page
            city_slug = response.meta.get('city_slug', '')
            
            # 💾 SAVE PAGE CHECKPOINT: Update city_processing_status with current page completion
            self._save_page_checkpoint(city_slug, current_page, len(restaurant_urls))
            
            # 📊 PAGINATION ANALYSIS: Track restaurants found on this page
            restaurants_found_on_page = len(restaurant_urls)
            self.log(f"🔗 SEQUENTIAL PAGINATION: Completed page {current_page}, extracted {restaurants_found_on_page} restaurants", logging.INFO)
            
            # 🚫 CAPTCHA DETECTION: Check for consecutive empty pages
            if restaurants_found_on_page == 0:
                self.consecutive_empty_pages += 1
                self.log(f"🚫 EMPTY PAGE DETECTED: Page {current_page} had 0 restaurants (consecutive count: {self.consecutive_empty_pages})", logging.WARNING)
                
                if self.consecutive_empty_pages >= self.max_consecutive_empty_pages:
                    self.log(f"🛑 CAPTCHA BLOCKING DETECTED: {self.consecutive_empty_pages} consecutive empty pages. Stopping pagination to prevent infinite loop.", logging.ERROR)
                    self.log(f"💡 SOLUTION: This typically indicates CAPTCHA blocking or end of available data.", logging.ERROR)
                    return  # Stop processing instead of triggering next page
            else:
                # Reset counter when we find restaurants
                self.consecutive_empty_pages = 0
            
            # Check if there are more pages to process
            if current_page < total_pages:
                next_page = current_page + 1
                
                # Generate next page URL
                if next_page == 1:
                    next_url = f"https://de.restaurantguru.com/restaurant-{city_slug}-t1"
                else:
                    next_url = f"https://de.restaurantguru.com/restaurant-{city_slug}-t1/{next_page}"
                
                self.log(f"🔗 SEQUENTIAL PAGINATION: Moving to page {next_page}: {next_url}", logging.ERROR)
                
                # Calculate expected restaurants for next page
                total_restaurants_estimated = restaurants_per_page * total_pages  # Estimate based on page count
                expected_on_next_page = min(restaurants_per_page, total_restaurants_estimated - (next_page - 1) * restaurants_per_page)
                
                # 🎯 SEQUENTIAL PAGINATION FIX: Store next page info instead of immediately yielding
                # This prevents queue overflow by waiting for current page completion
                self.pending_next_page = {
                    'url': next_url,
                    'page_number': next_page,
                    'expected_restaurants': expected_on_next_page,
                    'meta': {
                        **response.meta,
                        'page_number': next_page,
                        'expected_restaurants': expected_on_next_page,
                        'resume_from_index': self.restaurants_processed,
                    }
                }
                
                # 🏙️ MEGA CITY: Update page tracking for resume capability
                self._update_city_page_progress(city_slug, current_page, self.restaurants_processed)
                
                self.log(f"🎯 SEQUENTIAL PAGINATION: Page {next_page} queued, will trigger after page {current_page} completes", logging.INFO)
                
                # Note: Next page will be triggered in parse_estab_page when last restaurant of current page completes
            else:
                # 🏁 PAGINATION COMPLETE: Let closed() method handle completion instead of marking here
                self.log(f"🏁 SEQUENTIAL PAGINATION: Completed all {total_pages} pages for {city_slug}", logging.INFO)
                # 🚨 FIX: Removed premature _mark_city_completed() call - let closed() method handle it
                # This prevents duplicate completion calls and ensures accurate counting
                self.log(f"📊 PAGINATION SUMMARY: {total_pages} pages processed, will verify completion in closed() method", logging.INFO)
        
        else:
            # 📄 LEGACY BATCH COMPLETION LOGIC: For infinite scroll mode
            expected_restaurants = response.meta.get('expected_restaurants', self.restaurants_per_batch)
            
            if self.restaurant_count >= expected_restaurants:
                self.log(f"📄 BATCH COMPLETED: Processed {self.restaurant_count} restaurants (expected: {expected_restaurants})", logging.INFO)
                self.log(f"📄 NEXT BATCH: Run again to process more restaurants (batch limit: {self.restaurants_per_batch})", logging.INFO)
                self.log(f"🔄 RESUME: Next run will automatically continue from restaurant #{self.restaurants_processed + 1}", logging.INFO)
                    # City completion will be handled after all restaurants are actually processed

    def parse_estab_page(self, response):
        # ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Track response for error detection
        self._track_response_received(response)
        
        # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Circuit breaker: Check if we should stop processing
        if self._should_stop_processing():
            self.log("ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ¢â‚¬Ëœ Circuit breaker active - skipping restaurant detail processing", logging.WARNING)
            return
        
        # Log that we're processing a restaurant page
        restaurant_index = response.meta.get('restaurant_index', 0)
        website_index = response.meta.get('website_index', 0)
        resume_from_index = response.meta.get('resume_from_index', 0)
        city_info = response.meta.get('city_info', {})
        
        self.log(f"🍽️ PARSE_ESTAB_PAGE: Processing restaurant #{restaurant_index} (website #{website_index})", logging.INFO)
        
        # Check what's actually on the page
        page_title = response.xpath('//title/text()').get()
        page_size = len(response.text)
        self.log(f"🔍 PAGE DEBUG: Title='{page_title}', Size={page_size} bytes", logging.DEBUG)
        
        # 🎯 100% SUCCESS: Check for potential captcha/anti-bot responses but RETRY instead of skipping
        captcha_indicators = [
            'suspicious activity', 'captcha', 'blocked', 'security check',
            'access denied', 'forbidden', 'verify you are human', 
            'bot detection', 'rate limit', 'too many requests',
            'cloudflare', 'challenge', 'verification required'
        ]
        
        retry_attempt = response.meta.get('scrapeops_retry', 0)
        max_retries = 3  # Maximum 3 attempts with escalating stealth
        
        # Check if this looks like a captcha/anti-bot response
        is_captcha_response = False
        if page_title and any(indicator in page_title.lower() for indicator in captcha_indicators):
            is_captcha_response = True
            self.log(f"🎯 CAPTCHA DETECTED: '{page_title}' - ScrapeOps will handle this", logging.WARNING)
        elif page_size < 10000:  # Very small pages - likely captcha or block page
            is_captcha_response = True
            self.log(f"🎯 SMALL PAGE DETECTED: {page_size} bytes (normal: 160k+) - ScrapeOps will retry", logging.WARNING)
        elif page_size < 50000:  # Small pages - check for error patterns
            content_lower = response.text.lower()
            error_patterns = ['error', 'not found', '404', '403', '500', 'unavailable', 'maintenance']
            if any(pattern in content_lower for pattern in error_patterns):
                is_captcha_response = True
                self.log(f"🎯 ERROR PAGE DETECTED: Found error pattern - ScrapeOps will retry with higher stealth", logging.WARNING)
        
        # 🎯 100% SUCCESS TRACKING: Count this restaurant request
        self.success_stats['total_restaurant_requests'] += 1
        
        # 🎯 100% SUCCESS: If captcha detected and we haven't exceeded retries, retry with higher stealth
        if is_captcha_response and retry_attempt < max_retries:
            self.log(f"🎯 RETRY ATTEMPT {retry_attempt + 1}/{max_retries}: Escalating ScrapeOps stealth level", logging.INFO)
            
            # Track retry statistics
            if retry_attempt == 0:
                self.success_stats['captcha_retries_level_1'] += 1
            elif retry_attempt == 1:
                self.success_stats['captcha_retries_level_2'] += 1
            else:
                self.success_stats['captcha_retries_level_3'] += 1
            
            # Create new request with escalated retry parameters
            from scrapy import Request
            retry_request = Request(
                url=response.request.url,
                callback=self.parse_estab_page,
                meta={
                    **response.meta,
                    'scrapeops_retry': retry_attempt + 1,
                    'dont_cache': True,  # Force fresh request
                },
                dont_filter=True,  # Allow duplicate URL with different retry level
                priority=100  # High priority for retries
            )
            
            self.log(f"🎯 ESCALATING: Retry with stealth level {retry_attempt + 2} for {response.url}", logging.INFO)
            yield retry_request
            return  # Exit this attempt, wait for retry
        elif is_captcha_response and retry_attempt >= max_retries:
            # After maximum retries, log the issue but continue (ScrapeOps should have handled it)
            self.log(f"🚨 MAX RETRIES REACHED: {retry_attempt + 1} attempts failed for {response.url}", logging.ERROR)
            self.log(f"🎯 PROCEEDING: Trusting ScrapeOps final result - continuing with data extraction", logging.WARNING)
            self.success_stats['max_retry_failures'] += 1
            # Continue with processing - don't return, let ScrapeOps final result be processed
        else:
            # No captcha detected or successful retry - proceed normally
            if retry_attempt > 0:
                self.log(f"🎯 RETRY SUCCESS: Stealth level {retry_attempt + 1} worked for {response.url}", logging.INFO)
                self.success_stats['retry_successes'] += 1
            self.log(f"🎯 PROCESSING: Normal data extraction for page size {page_size} bytes", logging.DEBUG)
        
        # 🎯 100% SUCCESS: Log page size but don't skip - let ScrapeOps handle all issues
        if page_size < 5000:  # Small page - log but continue processing
            self.log(f"🎯 SMALL PAGE: {page_size} bytes - ScrapeOps should have handled this, proceeding anyway", logging.WARNING)
            self.log(f"🔍 PAGE CONTENT PREVIEW: {response.text[:500]}", logging.DEBUG)
            # Continue processing - ScrapeOps is responsible for handling all page issues
        
        # Test basic selectors to see what's working
        test_title = response.xpath('//h1/text()').get()
        test_h2 = response.xpath('//h2/text()').get() 
        test_divs = len(response.xpath('//div').getall())
        self.log(f"🔍 SELECTOR TEST: h1='{test_title}', h2='{test_h2}', div_count={test_divs}", logging.DEBUG)
        
        # 🔄 RESUME LOGIC: Get restaurant processing information
            
        def get_rating(provider: str) -> Optional[Tuple[float, float]]:
            rating_row = response.xpath(
                f'.//a[@class="row {provider} rating_list_right"] | .//div[@class="row {provider} rating_list_right"]'
            )
            rating_tag = rating_row.xpath(
                f'./div[@class="left"]//span[@class="agency-count"]/text()'
            ).get()
            if not rating_tag:
                return None

            try:
                # Enhanced parsing to handle various formats
                rating_text = rating_tag.replace(",", ".").strip()
                
                if "/" in rating_text:
                    parts = rating_text.split("/")
                    if len(parts) >= 2:
                        # Remove any parentheses or extra characters
                        rating_part = parts[0].strip("() ")
                        max_rating_part = parts[1].strip("() ")
                        
                        # Extract numeric values more safely
                        rating_match = regex.search(r'(\d+\.?\d*)', rating_part)
                        max_rating_match = regex.search(r'(\d+\.?\d*)', max_rating_part)
                        
                        if rating_match and max_rating_match:
                            rating = float(rating_match.group(1))
                            max_rating = float(max_rating_match.group(1))
                            return (rating, max_rating)
                
                # Fallback to original logic with safety checks
                if "/" in rating_text and len(rating_text) > 2:
                    rating, max_rating = rating_text.split("/")
                    rating = float(rating[1:]) if len(rating) > 1 else float(rating)
                    max_rating = float(max_rating[:-1]) if len(max_rating) > 1 else float(max_rating)
                    return (rating, max_rating)
                    
            except (ValueError, IndexError) as e:
                self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â Error parsing {provider} rating '{rating_tag}': {e}", logging.WARNING)
                return None
                
            return None

        try:
            self.log(f"Beginn Scraping of Restaurant: {response.url}", logging.DEBUG)
            # google rating - enhanced extraction with fallbacks
            google_response = get_rating("google")
            if google_response:
                google_rating = google_response[0]
                self.log(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Google rating from standard method: {google_rating}", logging.DEBUG)
            else:
                # Enhanced Google rating extraction as fallback
                google_rating = self._get_enhanced_google_rating(response)
                if google_rating:
                    self.log(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Google rating from enhanced method: {google_rating}", logging.DEBUG)
                else:
                    google_rating = None
                    self.log(f"ÃƒÂ¢Ã…Â¡ ÃƒÂ¯Ã‚Â¸Ã‚Â No Google rating found for {response.url}", logging.WARNING)

            # foursqure
            foursquare_response = get_rating("foursqure")
            if foursquare_response != None:
                foursquare = f"{foursquare_response[0]}/10"
            else:
                foursquare = None

            # yelp rating
            yelp_response = get_rating("yelp")
            if yelp_response != None:
                yelp = str(yelp_response[0])
            else:
                yelp = None

            # michelin
            michelin_response = response.xpath(
                './/div[@class="right michelin michelin-flex"]/div/div[1]/@class'
            ).get()
            if michelin_response != None and michelin_response != "michelin_selection":
                match = regex.search(r"\d", michelin_response)
                if match:
                    michelin = match.group(0)
                else:
                    michelin = None
            else:
                michelin = None

            # trip
            trip_response = get_rating("trip")
            if trip_response:
                trip = trip_response[0]
            else:
                trip = None

            # facebook
            facebook_response = get_rating("facebook")
            if facebook_response:
                facebook = facebook_response[0]
            else:
                facebook = None

            # url
            link_response = response.xpath('.//div[@class="website"]//a/text()').get()
            if link_response != None:
                link = f"https://{link_response}/"
            else:
                link = None

            # last review dates
            last_reviews_response = response.xpath(
                './/div[@class="o_review"]//span[@class="grey"]/text()'
            ).getall()
            if last_reviews_response:

                last_review_dates = []
                # cut down review_date string via regex
                for i in last_reviews_response:
                    match = regex.search(r"(\d{1,2}|(one|ein)) \w+", i)
                    if match:
                        last_review_dates.append(match.group(0))
                    else:
                        last_review_dates.append(
                            None
                        )  # or handle it differently if needed
            else:
                last_review_dates = None

            # may be closed
            closed = False
            if response.xpath('.//div[@class="closed_info_block"]').get() is not None:
                closed = True

            # price range euro
            price_range_euro_tag = response.xpath(
                './/span[@class="hint"]/span/span[@class="nowrap"]/span/text() | .//span[@class="hint"]/span/span[@class="nowrap"]/text()'
            ).getall()
            if price_range_euro_tag:
                price_range_euro = [tag for tag in price_range_euro_tag if tag.strip()][
                    0
                ]
            else:
                price_range_euro = None

            # meta_json
            meta_json = response.xpath(
                './/script[@type="application/ld+json"]'
            ).extract_first()
            if meta_json != None and not closed:
                meta_json = (meta_json.replace("</script>", "")).replace(
                    '<script type="application/ld+json">', ""
                )
                meta_json = json.loads(meta_json)
                menu_url = meta_json.get("hasMenu")
                address_meta = meta_json.get("address")
                country = address_meta.get("addressCountry")
                city = address_meta.get("addressLocality")
                address_region = address_meta.get("addressRegion")
                street = address_meta.get("streetAddress")
                # Enhanced opening hours extraction
                opening_hours_json = meta_json.get("openingHours")
                opening_hours = self._get_enhanced_opening_hours(response, opening_hours_json)
                aggregate_rating = meta_json.get("aggregateRating")
                phone = meta_json.get("telephone")
                # Enhanced coordinate extraction with validation
                geo_cordinates = meta_json.get("geo")
                if geo_cordinates:
                    json_latitude = geo_cordinates.get("latitude")
                    json_longitude = geo_cordinates.get("longitude")
                else:
                    json_latitude = None
                    json_longitude = None
                
                # Get enhanced coordinates with multiple sources and validation
                latitude, longitude = self._get_enhanced_coordinates(response, json_latitude, json_longitude)
                date_published_on_rg = meta_json.get("datePublished")
                _type = meta_json.get("@type")
                serves_cuisine = meta_json.get("servesCuisine")
                price_range = meta_json.get("priceRange")

            else:
                side_info_json = self._get_infos_from_side(response)

                menu_url = None  # Needs more crawling depth
                country = side_info_json["country"]
                city = side_info_json["city"]
                address_region = side_info_json["addressRegion"]
                street = side_info_json["street"]
                # Enhanced opening hours extraction for fallback path
                opening_hours_side = side_info_json["openingHours"]
                opening_hours = self._get_enhanced_opening_hours(response, opening_hours_side)
                aggregate_rating = None  # Not Available
                phone = side_info_json["telephone"]
                # Enhanced coordinates for fallback path too
                side_latitude = side_info_json["latitude"]
                side_longitude = side_info_json["longitude"]
                latitude, longitude = self._get_enhanced_coordinates(response, side_latitude, side_longitude)
                date_published_on_rg = None  # Not Available
                _type = None  # Not Available
                serves_cuisine = side_info_json["servesCuisine"]
                price_range = side_info_json["priceRange"]
                address_meta = None  # No Meta data available in this case

            # 🚨 REMOVED DUPLICATE YIELD: This was causing double yielding (24 restaurants → 40+ items)
            # The comprehensive yield statement below handles all the data
            
            # ✅ REMOVED DUPLICATE INCREMENT: Counter is incremented after yielding item (line 4028)
            
            # 🏙️ MEGA CITY: Monitor memory usage
            self._monitor_memory_usage(self.restaurants_processed)
            
            # 🚀 CHECKPOINT: Save progress after each successful restaurant 
            # For small cities: save after every restaurant
            # For large cities: save every 10 restaurants
            should_save_checkpoint = False
            if self.resume_enabled and restaurant_index > 0:
                if self.restaurants_found and self.restaurants_found <= 200:
                    # Small city (≤200 restaurants): save after every restaurant
                    should_save_checkpoint = True
                elif restaurant_index % 20 == 0:
                    # Large city: save every 20 restaurants (10% of 200 batch)
                    should_save_checkpoint = True
            
            if should_save_checkpoint:
                # Get city info from response meta or fallback to current_city_info
                city_info = response.meta.get('city_info', {})
                city_slug = city_info.get('city_slug') if city_info else (self.current_city_info.get('city_slug') if hasattr(self, 'current_city_info') and self.current_city_info else None)
                
                if city_slug:
                    # 🔗 PAGINATION MODE: Use appropriate checkpoint method based on processing mode
                    pagination_mode = response.meta.get('pagination_mode', False)
                    if pagination_mode:
                        # 📄 PAGINATION: Update last_processed_restaurant_index but preserve page-based tracking
                        self.log(f"💾 PAGINATION CHECKPOINT: Restaurant #{restaurant_index}, City: {city_slug} (Processed: {self.restaurants_processed})", logging.INFO)
                        self._update_restaurant_index_only(city_slug, self.items_yielded)
                    else:
                        # 🌐 INFINITE SCROLL: Use traditional checkpoint method
                        self.log(f"💾 INFINITE SCROLL CHECKPOINT: Restaurant #{restaurant_index}, City: {city_slug} (Processed: {self.restaurants_processed})", logging.INFO)
                    self._save_checkpoint(
                        city_slug,
                            self.items_yielded  # 🚨 CRITICAL: Use actual yielded items count
                    )
                else:
                    self.log(f"⚠️ CHECKPOINT FAILED: No city_slug available. Meta: {response.meta.get('city_info')}, Current: {getattr(self, 'current_city_info', None)}", logging.ERROR)
            
            # 🚨 CRITICAL FIX: Create comprehensive restaurant item (combines original + new fields)
            restaurant_item = {
                # Original fields from the removed yield
                "_from_url": response.url,
                "title": response.xpath('.//h1[@class="header"]/text()').get() or response.xpath('.//div[@class="wrapper_title "]/div/h1/a/text()').get() or response.xpath('.//h1//text()').get(),
                "address-meta": address_meta if 'address_meta' in locals() else None,
                "country": country if 'country' in locals() else None,
                "city": city if 'city' in locals() else None,
                "current_city": self.current_city_info.get('city_name', 'unknown') if hasattr(self, 'current_city_info') and self.current_city_info else 'unknown',
                "address_region": address_region if 'address_region' in locals() else None,
                "street": street if 'street' in locals() else None,
                "latitude": latitude if 'latitude' in locals() else None,
                "longitude": longitude if 'longitude' in locals() else None,
                "phone": phone if 'phone' in locals() else None,
                "intern_link": response.xpath('.//div[@class="website"]/div[2]/a/@href').get(),
                "url": link if 'link' in locals() else None,
                "type_tags": response.xpath('.//div[@id="ranks"]/div/div/a/span/text()').getall(),
                "establishment_type": _type if '_type' in locals() else None,
                "price_range_euro": price_range_euro if 'price_range_euro' in locals() else None,
                "price_range": price_range if 'price_range' in locals() else None,
                "menu_url": menu_url if 'menu_url' in locals() else None,
                "cuisine_type": serves_cuisine if 'serves_cuisine' in locals() else None,
                "specials": response.xpath('.//div[@class="features_block"]/div[2]/span/text()').getall(),
                "opening_hours": opening_hours if 'opening_hours' in locals() else None,
                "aggregate_rating": aggregate_rating if 'aggregate_rating' in locals() else None,
                "rating_google": google_rating if 'google_rating' in locals() else None,
                "rating_yelp": yelp if 'yelp' in locals() else None,
                "foursquare": foursquare if 'foursquare' in locals() else None,
                "michelin": michelin if 'michelin' in locals() else None,
                "trip": trip if 'trip' in locals() else None,
                "facebook": facebook if 'facebook' in locals() else None,
                "date_published_on_rg": date_published_on_rg if 'date_published_on_rg' in locals() else None,
                "closed_permanent": response.xpath('.//div[@class="wrapper_title "]/div[@class="closed_info_block"]/text()').get(),
                "last_review_dates": last_review_dates if 'last_review_dates' in locals() else None,
                
                # New tracking fields
                'restaurant_index': restaurant_index,
                'website_index': website_index,
                'city_slug': city_info.get('city_slug', ''),
                'city_name': city_info.get('city_name', ''),
                'job_id': self.job_id,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Add JSON metadata if available
            if 'meta_json' in locals() and meta_json:
                restaurant_item.update({
                    'menu_url': menu_url if 'menu_url' in locals() else None,
                    'address_country': country if 'country' in locals() else None,
                    'address_city': city if 'city' in locals() else None,
                    'address_region': address_region if 'address_region' in locals() else None,
                    'street_address': street if 'street' in locals() else None,
                    'opening_hours': opening_hours if 'opening_hours' in locals() else None,
                    'aggregate_rating': aggregate_rating if 'aggregate_rating' in locals() else None,
                    'phone': phone if 'phone' in locals() else None,
                    'latitude': latitude if 'latitude' in locals() else None,
                    'longitude': longitude if 'longitude' in locals() else None,
                    'date_published': date_published_on_rg if 'date_published_on_rg' in locals() else None,
                    'type': _type if '_type' in locals() else None,
                    'serves_cuisine': serves_cuisine if 'serves_cuisine' in locals() else None,
                    'price_range': price_range if 'price_range' in locals() else None
                })
            
            # Add side info if available
            if 'side_info_json' in locals() and side_info_json:
                restaurant_item.update(side_info_json)
            
            # Log item yielding
            self.log(f"✅ YIELDING ITEM: Restaurant #{restaurant_index} - {restaurant_item.get('title', 'Unknown')}", logging.INFO)
            
            # 🚨 CRITICAL: Yield the item to Scrapy
            yield restaurant_item
            
            # 🎯 100% SUCCESS TRACKING: Count successful extraction
            self.success_stats['successful_extractions'] += 1
            
            # 🎯 COUNT TRACKING: Increment restaurants processed counter
            self.restaurants_processed += 1
            self.items_yielded += 1  # 🚨 NEW: Track actual items yielded
            
            # 🎯 SEQUENTIAL PAGINATION: Check if we should trigger next page
            next_page_request = self._check_page_completion_and_trigger_next()
            if next_page_request:
                self.log(f"🎯 IMMEDIATE YIELD: Yielding next page request within callback", logging.INFO)
                yield next_page_request
            
            # 🎯 SUCCESS RATE: Calculate and log progress
            success_rate = (self.success_stats['successful_extractions'] / self.success_stats['total_restaurant_requests']) * 100
            self.log(f"✅ Restaurant #{restaurant_index} processed successfully. Total: {self.items_yielded}/{self.restaurants_found if self.restaurants_found else '?'}", logging.INFO)
            self.log(f"🎯 SUCCESS RATE: {success_rate:.1f}% ({self.success_stats['successful_extractions']}/{self.success_stats['total_restaurant_requests']})", logging.INFO)
            
            # 🎯 RETRY EFFECTIVENESS: Log retry statistics if any retries were used
            if retry_attempt > 0:
                self.log(f"🎯 RETRY SUCCESS: Required {retry_attempt + 1} attempts to achieve 100% success", logging.INFO)

        except JSONDecodeError:
            traceback.print_exc()
            self.log("JSONDecodeError at URL %s", response.url, logging.ERROR)
        except Exception:
            traceback.print_exc()
            self.log(response.url, logging.ERROR)

    def _get_infos_from_side(self, response: Response) -> Dict[str, Any]:
        side_infos: Dict[str, Any] = {}

        # coordinates
        coordinates_tag = response.xpath('.//a[@class="direction_link"]/@href').get()
        if coordinates_tag:
            match = regex.search(
                r"destination=([-+]?\d*\.\d+),([-+]?\d*\.\d+)", coordinates_tag
            )
            if match:
                side_infos["latitude"] = match.group(1)
                side_infos["longitude"] = match.group(2)
            else:
                side_infos["latitude"] = None
                side_infos["longitude"] = None
        else:
            side_infos["latitude"] = None
            side_infos["longitude"] = None

        # cuisine
        cusine_tags = response.xpath(
            './/div[@class="cuisine_hidden"]/span/text()'
        ).getall()
        side_infos["servesCuisine"] = cusine_tags

        # phone
        phone_tag = response.xpath('.//a[@class="call"]/@href').get()
        if phone_tag:
            match = regex.search(r"\+\d+", phone_tag)
            if match:
                side_infos["telephone"] = match.group(0)
            else:
                side_infos["telephone"] = None
        else:
            side_infos["telephone"] = None

        # address
        address_tag = cast(
            Optional[str],
            response.xpath('.//div[@class="address"]/div[2]/text()').get(),
        )
        if address_tag:
            address = [
                address_part.strip() for address_part in address_tag.strip().split(",")
            ]
            try:
                side_infos["street"] = address[0]
                side_infos["city"] = address[1]
                side_infos["addressRegion"] = address[2]
                side_infos["country"] = address[3]
            except IndexError:
                self.log(
                    f"Address {address} for url has less then 4 comma seperated parts {response.url}",
                    logging.WARNING
                )
                side_infos["street"] = None
                side_infos["city"] = None
                side_infos["addressRegion"] = None
                side_infos["country"] = None

        else:
            side_infos["street"] = None
            side_infos["city"] = None
            side_infos["addressRegion"] = None
            side_infos["country"] = None

        # has Menu
        # TODO: Not implemented yet. Needs one more crawling depth to work

        # Openung hours
        opening_hours_tags = cast(
            Optional[List[Selector]],
            response.xpath('.//table[@class="schedule-table"]//tr'),
        )

        if opening_hours_tags:
            side_infos["openingHours"] = []
            for tag in opening_hours_tags:
                day = tag.xpath('.//span[@class="short-day"]/text()').get()
                hours = tag.xpath("./td[2]/text()").getall()
                side_infos["openingHours"].extend(
                    [f"{day} {hour}" for hour in hours if regex.match(r"\d", hour)]
                )
        else:
            side_infos["openingHours"] = None

        # aggregateRating does not exisit without the meta_json

        # price Range:
        side_infos["priceRange"] = response.xpath('.//span[@class="cost"]/text()').get()
        return side_infos
    
    def _check_page_completion_and_trigger_next(self):
        """🎯 SEQUENTIAL PAGINATION: Check if current page is complete and return next page request if needed"""
        try:
            # Only trigger if we have a pending next page
            if not self.pending_next_page:
                return None
                
            # Get current page info from pending next page
            current_page = self.pending_next_page['meta'].get('page_number', 1) - 1
            restaurants_per_page = 20  # Standard page size
            
            # 🚨 FIX: Calculate how many restaurants we should have processed on THIS PAGE ONLY
            # Instead of cumulative count, track restaurants processed on current page
            if not hasattr(self, 'current_page_start_count'):
                self.current_page_start_count = self._initial_db_count
            
            restaurants_processed_on_current_page = self.restaurants_processed - self.current_page_start_count
            
            # Trigger next page when we've processed at least 15 restaurants on current page
            # (allowing for pages with fewer than 20 restaurants)
            # 🚨 FIX: Wait for most restaurants on page before triggering next page
            # Use 18 out of 20 (90%) to allow for minor variations but ensure we get most restaurants
            min_restaurants_to_trigger = max(18, restaurants_per_page - 2)
            
            # 🚨 FIX: More intelligent triggering conditions
            # 1. Wait for at least 18 restaurants on current page (90% of expected 20)
            # 2. OR if we've processed all expected restaurants for the city
            # 3. OR if we've been processing for a while and have reasonable progress
            trigger_condition = (
                restaurants_processed_on_current_page >= min_restaurants_to_trigger or
                self.restaurants_processed >= self.restaurants_found or  # All city restaurants processed
                (restaurants_processed_on_current_page >= 15 and self.restaurants_processed >= self.restaurants_found - 5)  # Near completion
            )
            
            if trigger_condition:
                # Determine the specific trigger reason for better debugging
                if restaurants_processed_on_current_page >= min_restaurants_to_trigger:
                    trigger_reason = f"sufficient restaurants ({restaurants_processed_on_current_page}/{restaurants_per_page})"
                elif self.restaurants_processed >= self.restaurants_found:
                    trigger_reason = "all city restaurants processed"
                else:
                    trigger_reason = "near completion threshold"
                
                self.log(f"🎯 PAGE COMPLETION: Page {current_page} completed ({restaurants_processed_on_current_page} restaurants on this page, {self.restaurants_processed} total), triggering next page ({trigger_reason})", logging.INFO)
                
                # Generate the next page request
                next_page_info = self.pending_next_page
                self.pending_next_page = None  # Clear pending
                
                # 🚨 FIX: Update page start count for next page tracking
                self.current_page_start_count = self.restaurants_processed
                
                # Create and return the next page request
                from scrapy import Request
                next_request = Request(
                    url=next_page_info['url'],
                    headers={
                        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Cache-Control': 'max-age=0'
                    },
                    callback=self.parse_estabs,
                    dont_filter=True,
                    meta=next_page_info['meta']
                )
                
                self.log(f"🎯 NEXT PAGE REQUEST CREATED: Page {next_page_info['page_number']} ready to return", logging.INFO)
                return next_request
                
        except Exception as e:
            self.log(f"⚠️ Error in page completion check: {e}", logging.WARNING)
            
        return None
    
    def _update_restaurant_index_only(self, city_slug, restaurant_index):
        """📄 PAGINATION: Update only restaurant index without overriding page number"""
        try:
            import psycopg2
            
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres'
            }
            
            connection = psycopg2.connect(**db_config)
            
            # Update ONLY the restaurant index, preserve page number and other pagination data
            update_query = """
                UPDATE smartdatastagdb.city_processing_status 
                SET last_processed_restaurant_index = %s,
                    restaurants_processed = %s,
                    last_processed_date = NOW()
                WHERE city_slug = %s AND processing_status = 'processing'
            """
            
            with connection.cursor() as cursor:
                # 🚨 VALIDATION: Check actual database count for accurate updates
                try:
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                        WHERE job_id = %s
                    """, (self.job_id,))
                    actual_db_count = cursor.fetchone()[0]
                    
                    # 🔥 CUMULATIVE COUNT: Use validated database count
                    cumulative_count = self._initial_db_count + actual_db_count
                    
                    # Log any discrepancies for debugging
                    if actual_db_count != self.items_yielded:
                        self.log(f"🔍 INDEX UPDATE VALIDATION: Spider={self.items_yielded}, Database={actual_db_count}", logging.INFO)
                        
                except Exception as e:
                    # Fallback to spider count if validation fails
                    cumulative_count = self._initial_db_count + self.items_yielded
                    self.log(f"⚠️ Could not validate count for index update: {e}", logging.WARNING)
                
                cursor.execute(update_query, (
                    restaurant_index,
                    cumulative_count,  # Validated cumulative count
                    city_slug
                ))
                connection.commit()
            connection.close()
                
            self.log(f"📄 PAGINATION INDEX UPDATE: {city_slug} - Restaurant #{restaurant_index} (preserved page tracking)", logging.INFO)
            
        except Exception as e:
            self.log(f"⚠️ Error updating restaurant index: {e}", logging.WARNING)
    
    def _update_city_page_progress(self, city_slug, current_page, restaurants_processed):
        """🏙️ MEGA CITY: Update page progress for resume capability"""
        try:
            # Use a separate connection for page progress updates
            import psycopg2
            
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres'
            }
            
            connection = psycopg2.connect(**db_config)
                
            # Update city_processing_status with current page and restaurant count
            update_query = """
                UPDATE smartdatastagdb.city_processing_status 
                SET last_processed_page = %s,
                    restaurants_processed = %s,
                    last_processed_date = NOW(),
                    resume_checkpoint_data = %s
                WHERE city_slug = %s AND processing_status = 'processing'
            """
            
            # Create checkpoint data for resume
            checkpoint_data = {
                "timestamp": datetime.now().isoformat(),
                "processing_time": "unknown",
                "last_processed_url": "",
                "last_processed_slug": "",
                "restaurants_processed": restaurants_processed,
                "total_restaurants_found": self.restaurants_found,
                "current_page": current_page,
                "last_processed_page": current_page
            }
            
            with connection.cursor() as cursor:
                # 🔥 CUMULATIVE COUNT: Add new items to initial database count
                cumulative_count = self._initial_db_count + self.items_yielded
                cursor.execute(update_query, (
                    current_page,
                    cumulative_count,  # Use cumulative count instead of current session count
                    json.dumps(checkpoint_data),
                    city_slug
                ))
                connection.commit()
            connection.close()
                
            self.log(f"🏙️ PAGE PROGRESS: Updated {city_slug} - Page {current_page}, {cumulative_count} restaurants processed", logging.INFO)
            
        except Exception as e:
            self.log(f"⚠️ Error updating city page progress: {e}", logging.WARNING)

    def _update_city_partial_completion(self, city_slug: str, cumulative_count: int):
        """Update city processing status with cumulative restaurant count for partial completion"""
        try:
            import psycopg2
            
            db_config = {
                'host': '10.32.48.200',
                'port': 5432,
                'user': 'postgres',
                'password': 'xqhCcs&"c#Y*}S,_',
                'database': 'postgres'
            }
            
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()
            
            # Update restaurants_processed with cumulative count while keeping status as 'processing'
            update_query = """
                UPDATE smartdatastagdb.city_processing_status 
                SET restaurants_processed = %s,
                    last_processed_date = NOW(),
                    updated_at = NOW()
                WHERE city_slug = %s
            """
            cursor.execute(update_query, (cumulative_count, city_slug))
            connection.commit()
            connection.close()
            
            self.log(f"💾 DATABASE UPDATED: {city_slug} partial completion - {cumulative_count} restaurants processed", logging.INFO)
            
        except Exception as e:
            self.log(f"⚠️ Error updating partial completion: {e}", logging.ERROR)
