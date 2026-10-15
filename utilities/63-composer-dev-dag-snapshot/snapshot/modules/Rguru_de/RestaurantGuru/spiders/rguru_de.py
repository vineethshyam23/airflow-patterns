import logging
import traceback
from json.decoder import JSONDecodeError
from scrapy import Spider, Selector
from scrapy.http import Response
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
import re
from math import ceil
import json
from typing import Optional, Any, cast, Tuple, Dict, List
import psycopg2
from datetime import datetime
import time
import random

class RestGuruSpider(Spider):
    name = "rguru_de"

    rest_slugs = []
    current_city = None
    current_city_id = None
    job_id = None
    restaurants_found = 0
    restaurants_processed = 0
    
    # Spider arguments from DAG
    city_id = None
    city_name = None
    city_slug = None

    custom_settings = {
        "CONCURRENT_REQUESTS": 1,  # Process ONE page at a time for accurate last_processed_page tracking
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,  # ONE page at a time
        "DOWNLOAD_TIMEOUT": 60.0,  # 60 seconds timeout
        "DOWNLOAD_DELAY": 2.0,  # Increased to 2 seconds to avoid bot detection (more human-like)
        "RANDOMIZE_DOWNLOAD_DELAY": True,  # Randomize delay to appear more human (1-3 seconds)
        "COOKIES_ENABLED": False,
        "SCRAPEOPS_API_ENABLED": True,  # Use API-based proxy
        "SCRAPEOPS_RESIDENTIAL_PROXY_ENABLED": False,  # Residential proxy disabled (causes 401 errors)
        # AutoThrottle will dynamically adjust based on response times
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2.0,  # Start with 2 second delay
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.5,  # Reduced from 2.0 to be more conservative
        "AUTOTHROTTLE_MAX_DELAY": 10.0,  # Max 10 seconds if site is slow
    }

    def get_db_connection(self):
        """Get database connection using settings from scrapy settings"""
        return psycopg2.connect(
            host=self.settings.get('POSTGRES_HOST'),
            user=self.settings.get('POSTGRES_USER'),
            password=self.settings.get('POSTGRES_PASSWORD'),
            database=self.settings.get('POSTGRES_DATABASE'),
            port=self.settings.get('POSTGRES_PORT'),
            connect_timeout=10
        )

    def get_next_job_id(self):
        """Get the next job_id from the database"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Get the maximum job_id and increment by 1
            cursor.execute("""
                SELECT COALESCE(MAX(job_id), 0) + 1 
                FROM smartdata_analyticdb.restaurantguru_raw_de
            """)
            
            result = cursor.fetchone()
            next_job_id = result[0] if result else 1
            
            cursor.close()
            conn.close()
            
            return next_job_id
                
        except Exception as e:
            self.log(f"Error getting next job_id: {e}", logging.ERROR)
            return 1  # Default to 1 if error

    def get_pending_city(self):
        """Get a pending city from the city_processing_status table"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Get a pending city
            cursor.execute("""
                SELECT city_id, city_name, city_slug 
                FROM smartdatastagdb.city_processing_status 
                WHERE processing_status = 'pending' 
                ORDER BY city_id 
                LIMIT 1
            """)
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                city_id, city_name, city_slug = result
                self.current_city_id = city_id
                self.current_city = city_slug
                self.job_id = self.get_next_job_id()
                
                self.log(f"Selected city: {city_name} (ID: {city_id}, Slug: {city_slug}) with job_id: {self.job_id}", logging.INFO)
                return city_slug
            else:
                self.log("No pending cities found", logging.WARNING)
                return None
                
        except Exception as e:
            self.log(f"Error getting pending city: {e}", logging.ERROR)
            return None

    def update_city_status(self, status, restaurants_found=0, restaurants_processed_this_chunk=0, error_message=None):
        """Update city processing status in the database with accumulative processing"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            if status == 'completed':
                # For completed status, get current total and add this chunk's processing
                cursor.execute("""
                    SELECT restaurants_processed 
                    FROM smartdatastagdb.city_processing_status 
                    WHERE city_id = %s
                """, (self.current_city_id,))
                
                result = cursor.fetchone()
                current_total = result[0] if result and result[0] else 0
                new_total = current_total + restaurants_processed_this_chunk
                
                # RESET retry_count on successful completion!
                cursor.execute("""
                    UPDATE smartdatastagdb.city_processing_status 
                    SET processing_status = %s,
                        restaurants_found = %s,
                        restaurants_processed = %s,
                        last_processed_date = CURRENT_TIMESTAMP,
                        last_execution_id = %s,
                        job_id = %s,
                        retry_count = 0,
                        notes = %s
                    WHERE city_id = %s
                """, (status, restaurants_found, new_total, self.job_id, self.job_id, 
                     f"Job ID: {self.job_id} | Total restaurants processed: {new_total} | Restaurants found: {restaurants_found}", 
                     self.current_city_id))
                
                self.log(f"📊 Updated total restaurants_processed: {current_total} + {restaurants_processed_this_chunk} = {new_total}", logging.INFO)
                self.log(f"🔄 Reset retry_count to 0 after successful completion", logging.INFO)
                
            elif status == 'processing':
                # For processing status - use the TOTAL count (already cumulative from spider's self.restaurants_processed)
                # The spider loads existing count at start and increments during processing, so this is already cumulative!
                cursor.execute("""
                    UPDATE smartdatastagdb.city_processing_status 
                    SET processing_status = %s,
                        restaurants_found = %s,
                        restaurants_processed = %s,
                        error_message = %s,
                        last_processed_date = CURRENT_TIMESTAMP,
                        last_execution_id = %s,
                        job_id = %s,
                        notes = %s
                    WHERE city_id = %s
                """, (status, restaurants_found, restaurants_processed_this_chunk, error_message, self.job_id, self.job_id,
                     f"Job ID: {self.job_id} | In progress: {restaurants_processed_this_chunk}/{restaurants_found} restaurants | Will resume on next run",
                     self.current_city_id))
                
                self.log(f"📊 Updated restaurants_processed to: {restaurants_processed_this_chunk} (cumulative total)", logging.INFO)
                
            else:
                # For failed status - ALSO save restaurants_found and restaurants_processed!
                cursor.execute("""
                    UPDATE smartdatastagdb.city_processing_status 
                    SET processing_status = %s,
                        restaurants_found = %s,
                        restaurants_processed = %s,
                        error_message = %s,
                        last_processed_date = CURRENT_TIMESTAMP,
                        last_execution_id = %s,
                        job_id = %s,
                        notes = %s
                    WHERE city_id = %s
                """, (status, restaurants_found, restaurants_processed_this_chunk, error_message, self.job_id, self.job_id,
                     f"Job ID: {self.job_id} | Partial data: {restaurants_processed_this_chunk}/{restaurants_found} restaurants | {error_message}",
                     self.current_city_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.log(f"Updated city status to {status} for city_id {self.current_city_id}", logging.INFO)
            
        except Exception as e:
            self.log(f"Error updating city status: {e}", logging.ERROR)

    def get_last_processed_page(self):
        """Get the last processed page from database for resuming large cities"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT last_processed_page 
                FROM smartdatastagdb.city_processing_status 
                WHERE city_id = %s
            """, (self.current_city_id,))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            last_page = result[0] if result and result[0] else 0
            self.log(f"📖 Retrieved last processed page: {last_page} for city_id {self.current_city_id}", logging.INFO)
            return last_page
            
        except Exception as e:
            self.log(f"Error getting last processed page: {e}", logging.ERROR)
            return 0  # Start from beginning if error

    def get_existing_processed_count(self):
        """Get existing restaurants_processed count from database for cumulative counting"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT restaurants_processed 
                FROM smartdatastagdb.city_processing_status 
                WHERE city_id = %s
            """, (self.current_city_id,))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            existing_count = result[0] if result and result[0] else 0
            self.log(f"📖 Retrieved existing processed count: {existing_count} for city_id {self.current_city_id}", logging.INFO)
            return existing_count
            
        except Exception as e:
            self.log(f"Error getting existing processed count: {e}", logging.ERROR)
            return 0  # Start from 0 if error

    def update_last_processed_page(self, page_number):
        """Update the last processed page in database - ONLY if it's higher than current"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Only update if this page number is HIGHER than the existing one (since pages process out of order)
            cursor.execute("""
                UPDATE smartdatastagdb.city_processing_status 
                SET last_processed_page = GREATEST(COALESCE(last_processed_page, 0), %s)
                WHERE city_id = %s
            """, (page_number, self.current_city_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.log(f"📝 Updated last processed page to MAX({page_number}, existing) for city_id {self.current_city_id}", logging.DEBUG)
            
        except Exception as e:
            self.log(f"Error updating last processed page: {e}", logging.ERROR)

    def check_and_increment_retry(self):
        """Check retry count and increment it. Return False if max retries exceeded."""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Get current retry count and max retries
            cursor.execute("""
                SELECT retry_count, max_retries 
                FROM smartdatastagdb.city_processing_status 
                WHERE city_id = %s
            """, (self.current_city_id,))
            
            result = cursor.fetchone()
            if not result:
                cursor.close()
                conn.close()
                return False
            
            current_retry_count, max_retries = result
            new_retry_count = current_retry_count + 1
            
            self.log(f"🔄 Retry attempt {new_retry_count}/{max_retries} for city_id {self.current_city_id}", logging.INFO)
            
            # Check if we've exceeded max retries
            if new_retry_count > max_retries:
                # Mark as failed due to max retries exceeded
                cursor.execute("""
                    UPDATE smartdatastagdb.city_processing_status 
                    SET processing_status = 'failed',
                        error_message = 'Max retries exceeded',
                        retry_count = %s
                    WHERE city_id = %s
                """, (new_retry_count, self.current_city_id))
                
                conn.commit()
                cursor.close()
                conn.close()
                
                self.log(f"❌ Max retries ({max_retries}) exceeded for city_id {self.current_city_id}", logging.ERROR)
                return False
            
            # Increment retry count
            cursor.execute("""
                UPDATE smartdatastagdb.city_processing_status 
                SET retry_count = %s
                WHERE city_id = %s
            """, (new_retry_count, self.current_city_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            self.log(f"Error checking retry count: {e}", logging.ERROR)
            return False  # Fail safe - don't process if we can't check retries

    def update_progress_during_chunk(self, restaurants_processed_this_chunk):
        """Update restaurants_processed accumulative during chunk processing (for large cities)"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Get current total and add this chunk's processing
            cursor.execute("""
                SELECT restaurants_processed 
                FROM smartdatastagdb.city_processing_status 
                WHERE city_id = %s
            """, (self.current_city_id,))
            
            result = cursor.fetchone()
            current_total = result[0] if result and result[0] else 0
            new_total = current_total + restaurants_processed_this_chunk
            
            # Update the accumulative count AND reset retry_count (chunk succeeded!)
            cursor.execute("""
                UPDATE smartdatastagdb.city_processing_status 
                SET restaurants_processed = %s,
                    retry_count = 0
                WHERE city_id = %s
            """, (new_total, self.current_city_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.log(f"📊 Updated accumulative restaurants_processed: {current_total} + {restaurants_processed_this_chunk} = {new_total}", logging.INFO)
            self.log(f"🔄 Reset retry_count to 0 after successful chunk", logging.INFO)
            
        except Exception as e:
            self.log(f"Error updating progress during chunk: {e}", logging.ERROR)

    def insert_raw_data(self, raw_json_data):
        """Insert raw JSON data into restaurantguru_raw_de table
        
        Note: job_id is now auto-generated by database using IDENTITY column
        """
        # Skip database insertion if we don't have a proper city_id (fallback mode)
        if not self.current_city_id:
            self.log("Skipping database insertion (fallback mode)", logging.DEBUG)
            return
            
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Insert raw data - job_id is now auto-generated by database (GENERATED BY DEFAULT AS IDENTITY)
            cursor.execute("""
                INSERT INTO smartdata_analyticdb.restaurantguru_raw_de (raw_json)
                VALUES (%s)
                RETURNING job_id
            """, (json.dumps(raw_json_data),))
            
            # Get the auto-generated job_id for logging
            inserted_job_id = cursor.fetchone()[0]
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.log(f"✅ Inserted raw data with auto-generated job_id: {inserted_job_id}", logging.DEBUG)
            
        except Exception as e:
            self.log(f"❌ Error inserting raw data: {e}", logging.ERROR)

    def start_requests(self):
        # Use city information passed from DAG as arguments
        if hasattr(self, 'city_id') and self.city_id:
            # Convert string arguments to appropriate types
            self.current_city_id = int(self.city_id)
            self.current_city = self.city_slug
            self.job_id = self.get_next_job_id()
            
            self.log(f"Using city from DAG arguments: {self.city_name} (ID: {self.current_city_id}, Slug: {self.city_slug})", logging.INFO)
            city = self.city_slug
            
            # Check retry count (DON'T update status here - it's already 'processing' from DAG)
            if not self.check_and_increment_retry():
                self.log(f"❌ City {self.city_name} exceeded max retries. Marking as failed.", logging.ERROR)
                return  # Don't process if max retries exceeded
            
            # Load existing restaurants_processed count from database (for resume/cumulative counting)
            # IMPORTANT: Load this BEFORE any update_city_status calls to preserve cumulative count!
            self.restaurants_processed = self.get_existing_processed_count()
            self.log(f"📊 Starting with existing processed count: {self.restaurants_processed}", logging.INFO)
            
            # Note: Status is already 'processing' (set by DAG). We'll update it at the END in closed() method.
            
        else:
            # Fallback: Try to get a pending city from the database
            self.log("No city arguments provided, trying database fallback", logging.WARNING)
            city = self.get_pending_city()
            
            # Final fallback to hardcoded city if database fails
            if not city:
                self.log("Database connection failed, using fallback city: Zaberfeld", logging.WARNING)
                city = "Zaberfeld"
                self.current_city = city
                self.job_id = 1  # Use simple job_id for fallback
        
        if city:
            self.log(f"🏙️ Starting crawl for city: {city} (ID: {self.current_city_id})", logging.INFO)
            
            # Store the ORIGINAL URL in meta so middleware can add proxy+session params
            original_url = f"https://de.restaurantguru.com/restaurant-{city}-t1"
            
            yield Request(
                url=original_url,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"},
                callback=self.parse,
                dont_filter=True,
                meta={'original_url': original_url}  # ✅ Pass original URL through chain
            )
        else:
            self.log("No cities available to crawl!", logging.ERROR)

    # Funktion zum Zählen der Estabs pro Stadt
    def count_estabs(self, response):
        self.log(f'URL: {response.url}',logging.INFO)
        success = False
        try:
            # try to find a class wrap_top_title, try to extract the number from there: 
            num_text = response.xpath('//div[contains(@class, "wrap_top_title")]//text()').getall()
            # Join all extracted text into one string
            num_text_clean = " ".join(num_text).strip()
            self.log(f"wrap_top_title contents: {num_text_clean}", logging.DEBUG)
            # Extract all numbers (handles spaces, commas, dots)
            matches = re.findall(r'\d{1,3}(?:[ \xa0,.]?\d{3})*|\d+', num_text_clean)
            self.log(f"number matches: {';'.join(matches)}", logging.DEBUG)
            # Convert matches to integers (removing spaces/non-numeric chars)
            matches_int = [int(re.sub(r'[ \xa0,.]', '', m)) for m in matches]
            # Get the largest number
            num_est = max(matches_int)
            success = True
        except Exception as e:
            self.log(f'Could not extract the est number based on wrap_top_title : {e}', logging.WARNING)

        if not success:
            self.log(f'Trying based on static xpath expression', logging.INFO)
            try:
                num_tag = (
                        response.xpath('//*[@id="content"]/div[1]/div[2]/div[1]/span/text()')
                        .get()
                        .strip()
                        )
                self.log(f"NumberTag: {num_tag}", logging.DEBUG)
                num_est = int("".join(num_tag.split("\xa0")[1:]))
                success = True
            except Exception as e:
                self.log(f'Could not est number based on static xpath expression: {e}', logging.WARNING)
        
        if success:
            self.log(f"Total number of restaurants: {num_est}", logging.INFO)
            self.restaurants_found = num_est
        else:
            num_est = 0
            self.log(f"No Establishments found for {response.url}", logging.WARNING)
            # with open("responselog.html", "w") as out:
                    #    out.write(response.text)

        return num_est

    # Berechnung der Anzahl Seiten mit Restaurants auf basis der Estab-Anzahl (immer 20 pro Seite)
    def num_of_pages(self, response):
        estab_number = self.count_estabs(response)
        num_pages = ceil(estab_number/20)

        return num_pages

    def parse(self, response, request_url=None, search_query=None):
        try:
            num_pages = self.num_of_pages(response)
            self.log(f"🔍 Found {self.restaurants_found} restaurants across {num_pages} pages for URL: {response.url}", logging.INFO)
            
            # For large cities (>3000 restaurants), implement chunking strategy
            if self.restaurants_found > 3000:
                self.log(f"🏙️ Large city detected ({self.restaurants_found} restaurants). Implementing chunking strategy.", logging.WARNING)
                
                # Dynamic chunk size based on city size for better timeout safety
                if self.restaurants_found > 20000:
                    # Very large cities (Berlin, Munich): 50 pages = 1000 restaurants = ~2 hours
                    max_pages_per_chunk = 50
                    self.log(f"📊 MEGA CITY detected! Using smaller chunks (50 pages = 1000 restaurants)", logging.WARNING)
                elif self.restaurants_found > 10000:
                    # Large cities: 75 pages = 1500 restaurants = ~3 hours
                    max_pages_per_chunk = 75
                    self.log(f"📊 Large city: Using medium chunks (75 pages = 1500 restaurants)", logging.INFO)
                else:
                    # Medium cities (3000-10000): 100 pages = 2000 restaurants = ~3.5 hours
                    max_pages_per_chunk = 100
                    self.log(f"📊 Medium-large city: Using standard chunks (100 pages = 2000 restaurants)", logging.INFO)
                
                total_chunks = (num_pages + max_pages_per_chunk - 1) // max_pages_per_chunk
                
                # Get the last processed page from database to resume from correct position
                last_processed_page = self.get_last_processed_page()
                current_chunk = (last_processed_page // max_pages_per_chunk) + 1
                
                chunk_start = last_processed_page + 1
                chunk_end = min(chunk_start + max_pages_per_chunk - 1, num_pages)
                
                pages_in_this_chunk = chunk_end - chunk_start + 1
                restaurants_in_chunk = pages_in_this_chunk * 20
                estimated_time_minutes = int((restaurants_in_chunk * 7) / 60)  # 7 sec per restaurant average
                
                self.log(f"🔄 Resuming from page {chunk_start}. Processing chunk {current_chunk}/{total_chunks}", logging.INFO)
                self.log(f"📄 Pages in this chunk: {chunk_start}-{chunk_end} ({pages_in_this_chunk} pages)", logging.INFO)
                self.log(f"🍽️ Estimated restaurants in chunk: ~{restaurants_in_chunk}", logging.INFO)
                self.log(f"⏱️ Estimated processing time: ~{estimated_time_minutes} minutes", logging.INFO)
                self.log(f"⚠️ City will need {total_chunks} separate DAG runs to complete fully", logging.WARNING)
                self.log(f"📈 Progress: {last_processed_page}/{num_pages} pages done ({last_processed_page/num_pages*100:.1f}%)", logging.INFO)
                
                # Update the last processed page in database for next run
                self.update_last_processed_page(chunk_end)
                
                for page in range(chunk_start, chunk_end + 1):
                    self.log(f"📄 Processing page {page}/{num_pages} (chunk 1/{total_chunks})", logging.INFO)
                    
                    # ✅ Build URL from ORIGINAL URL (not proxy response.url)
                    original_base_url = response.request.meta.get('original_url', response.url)
                    total_url = f"{original_base_url}/{page}?skip_geo=1"
                    self.log(f'ESTAB_URL: {total_url}', logging.INFO)

                    yield Request(
                        url=total_url,
                        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"},
                        callback=self.parse_estabs,
                        dont_filter=True,
                        meta={'original_url': original_base_url}  # ✅ Pass original URL to next request
                    )
            else:
                # Process ALL pages for smaller cities, but support resume from last processed page
                last_processed_page = self.get_last_processed_page()
                
                if last_processed_page > 0:
                    self.log(f"🔄 Resuming from page {last_processed_page + 1}/{num_pages} (last successful page: {last_processed_page})", logging.INFO)
                    start_page = last_processed_page + 1
                else:
                    start_page = 1
                
                # 🎭 STRATEGY: Process pages 11+ in REVERSE order to avoid sequential detection
                pages_to_process = list(range(start_page, num_pages + 1))
                
                # If starting from page 11+, shuffle to avoid sequential pattern
                if start_page > 10:
                    # Reverse order: 13, 12, 11 instead of 11, 12, 13
                    pages_to_process.reverse()
                    self.log(f"🔀 Processing pages {start_page}-{num_pages} in REVERSE order to avoid detection", logging.WARNING)
                
                for page in pages_to_process:
                    self.log(f"📄 Processing page {page}/{num_pages}", logging.INFO)

                    # ✅ Build URL from ORIGINAL URL (not proxy response.url)
                    original_base_url = response.request.meta.get('original_url', response.url)
                    total_url = f"{original_base_url}/{page}?skip_geo=1"
                    self.log(f'ESTAB_URL: {total_url}', logging.INFO)

                    # 🎭 STRATEGY: Use different session for pages 11+ to avoid detection
                    request_meta = {'original_url': original_base_url}  # ✅ Always pass original URL
                    if page > 10:
                        # NEW SESSION for pages 11+ - appear as different user!
                        session_id = f"{self.current_city}_session_{page}"
                        request_meta['scrapeops_session'] = session_id
                        request_meta['scrapeops_country'] = 'de'  # German IP
                        request_meta['scrapeops_residential'] = True  # Use residential proxy if available
                        self.log(f"🎭 Page {page}: Using NEW session '{session_id}' + German IP + Residential to bypass detection", logging.INFO)

                    yield Request(
                        url=total_url,
                        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"},
                        callback=self.parse_estabs,
                        dont_filter=True,
                        meta=request_meta,  # Pass session metadata + original URL
                    )

        except JSONDecodeError:
            traceback.print_exc()
            self.log("JSONDecodeError at URL %s", response.url, logging.ERROR)
        except Exception:
            traceback.print_exc()
            self.log(response.url, logging.ERROR)

    def parse_estabs(self, response):
        # Extract page number from URL for tracking
        page_match = re.search(r'/(\d+)\?skip_geo=1', response.url)
        current_page = int(page_match.group(1)) if page_match else 0
        
        # Debug: Log the current URL being processed
        self.log(f"📄 Processing restaurant listing page {current_page}: {response.url}", logging.INFO)
        
        # Try multiple selectors to find individual restaurant links
        restaurant_links = []
        
        # Try the original selector for restaurant cards
        links1 = response.xpath('.//div[@class="info_header"]/div/a/@href').getall()
        self.log(f"Found {len(links1)} links with info_header selector", logging.DEBUG)
        
        # Try alternative selectors for restaurant cards
        links2 = response.xpath('.//div[contains(@class, "info_header")]//a/@href').getall()
        self.log(f"Found {len(links2)} links with info_header class selector", logging.DEBUG)
        
        # Try finding restaurant cards by different structure
        links3 = response.xpath('.//div[contains(@class, "restaurant")]//a/@href').getall()
        self.log(f"Found {len(links3)} links with restaurant class selector", logging.DEBUG)
        
        # Try finding any links that look like individual restaurants (not city pages)
        links4 = response.xpath('.//a[contains(@href, "/") and not(contains(@href, "-t1")) and not(contains(@href, "skip_geo")) and not(contains(@href, "cities")) and not(contains(@href, "search"))]/@href').getall()
        self.log(f"Found {len(links4)} links with individual restaurant selector", logging.DEBUG)
        
        # Combine all found links
        all_links = links1 + links2 + links3 + links4
        
        # Filter for actual restaurant pages (not city pages, language variants, etc.)
        for link in all_links:
            # Skip city pages, language variants, and other non-restaurant pages
            if any(skip in link for skip in ['-t1', 'skip_geo=1', '/cities', '/search', 'restaurant-Zaberfeld-t1', 'businessLanding', 'restaurantadvisor.app']):
                continue
            
            # Skip language variants and external links
            if any(lang in link for lang in ['es.restaurantguru', 'fr.restaurantguru', 'ru.restaurantguru', 'en.restaurantguru']):
                continue
            
            # Skip non-restaurant URLs and utility pages
            if any(skip in link for skip in ['api/', 'app/', 'landing', 'business', 'login', 'register', 'disclaimer', 'privacy_policy', 'contactus', 'aboutus', 'best-restaurants-nearby', '#pierre_assist__activate', 'request_content_removal']):
                continue
            
            # Skip reviews pages and other non-main restaurant pages
            if '/reviews?' in link or link.endswith('/reviews'):
                continue
                
            # Skip if it's just the domain or city pages
            if link in ['https://de.restaurantguru.com', 'https://de.restaurantguru.com/', '/Germany'] or link.endswith('/Germany') or (self.current_city and link.endswith(f'/{self.current_city}')):
                continue
                
            # Only process links that look like individual restaurants (contain restaurant name)
            if ('/' in link and not link.endswith('-t1') and 
                ('de.restaurantguru.com' in link or link.startswith('/')) and 
                len(link.split('/')[-1]) > 3):  # Restaurant name should be reasonably long
                restaurant_links.append(link)
        
        # Remove duplicates
        restaurant_links = list(set(restaurant_links))
        self.log(f"📄 Page {current_page}: Found {len(restaurant_links)} unique restaurant links", logging.INFO)
        
        # If we still don't have links, implement retry logic
        if not restaurant_links:
            # DON'T update last_processed_page for empty pages!
            
            # 🚨 SMART CAPTCHA DETECTION: If page 11+ has no links, likely CAPTCHA
            if current_page > 10:
                self.log(f"🚨 Page {current_page} (>10) has no restaurant links - likely CAPTCHA blocking", logging.ERROR)
                # Save page for analysis
                with open(f"debug_captcha_page_{current_page}.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                self.log(f"💾 Saved page to debug_captcha_page_{current_page}.html for analysis", logging.INFO)
                # Don't retry CAPTCHA pages - won't help
                return
            
            # Get retry attempt number from request meta (default 0)
            retry_attempt = response.meta.get('empty_page_retry', 0)
            max_retries = 5  # Try 5 times total (original + 4 retries)
            
            if retry_attempt < max_retries:
                retry_attempt += 1
                retry_delay = 12  # 12 seconds between retries (to appear human-like)
                
                self.log(f"⚠️ Page {current_page}: No restaurant links found. Retry {retry_attempt}/{max_retries} after {retry_delay}s delay", logging.WARNING)
                
                # Sleep for delay (make it look human)
                time.sleep(retry_delay)
                
                # Retry the same page with incremented retry counter
                # ✅ Preserve original_url and session metadata from the original request
                retry_meta = {
                    'empty_page_retry': retry_attempt,
                    'original_url': response.meta.get('original_url', response.url)
                }
                # Preserve session/country/residential params if they exist
                for key in ['scrapeops_session', 'scrapeops_country', 'scrapeops_residential']:
                    if key in response.meta:
                        retry_meta[key] = response.meta[key]
                
                yield Request(
                    url=response.url,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"},
                    callback=self.parse_estabs,
                    dont_filter=True,
                    meta=retry_meta  # ✅ Pass all metadata including original_url
                )
                return
            else:
                # Exhausted all retries - page is truly empty
                self.log(f"❌ Page {current_page}: No links after {max_retries} attempts. Page is empty.", logging.ERROR)
                # Save page for debugging
                with open(f"debug_empty_page_{current_page}.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                self.log(f"Saved empty page content to debug_empty_page_{current_page}.html", logging.INFO)
                return
        
        # ✅ ONLY update last_processed_page when page has restaurants!
        if current_page > 0 and self.current_city_id and len(restaurant_links) > 0:
            self.update_last_processed_page(current_page)
            self.log(f"✅ Page {current_page} has {len(restaurant_links)} restaurants - updating last_processed_page", logging.DEBUG)
            
            # 🔥 CAPTCHA AVOIDANCE: Add 1-minute cooldown after page 10 to appear human
            if current_page == 10:
                # Random cooldown between 50-70 seconds to appear more human (not exact 60s)
                cooldown_time = random.randint(50, 70)
                self.log(f"⏸️ Reached page 10 - Taking a break to avoid CAPTCHA detection", logging.WARNING)
                self.log(f"😴 Sleeping for {cooldown_time} seconds to appear human-like...", logging.INFO)
                time.sleep(cooldown_time)
                self.log(f"✅ Cooldown complete! Continuing to page 11...", logging.INFO)
        
        for i, est_url in enumerate(restaurant_links, 1):  # Process all found restaurant links
            # Make sure we have a full URL
            if not est_url.startswith('http'):
                if est_url.startswith('/'):
                    est_url = f"https://de.restaurantguru.com{est_url}"
                else:
                    est_url = f"https://de.restaurantguru.com/{est_url}"
            
            self.log(f"📄 Page {current_page}: Processing restaurant {i}/{len(restaurant_links)}: {est_url}", logging.INFO)
            rest_slug = est_url.split(".com/")[1] if ".com/" in est_url else est_url

            if rest_slug not in self.rest_slugs:
                self.rest_slugs.append(rest_slug)
                yield Request(
                    url=est_url,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"},
                    callback=self.parse_estab_page,
                    dont_filter=True,
                )
            else:
                self.log(f"Skipping Estab due to duplicate ({rest_slug})", logging.INFO)

    def parse_estab_page(self, response):
        def get_rating(provider: str) -> Optional[Tuple[float, float]]:
            rating_row = response.xpath(
                f'.//a[@class="row {provider} rating_list_right"] | .//div[@class="row {provider} rating_list_right"]'
            )
            rating_tag = rating_row.xpath(
                f'./div[@class="left"]//span[@class="agency-count"]/text()'
            ).get()
            if not rating_tag:
                return None

            rating, max_rating = rating_tag.replace(",", ".").split("/")
            rating = float(rating[1:])
            max_rating = float(max_rating[:-1])
            return (rating, max_rating)

        try:
            self.log(f"Beginn Scraping of Restaurant: {response.url}", logging.DEBUG)
            # google rating
            google_response = get_rating("google")
            if google_response:
                google_rating = google_response[0]
            else:
                google_rating = None

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
                match = re.search(r"\d", michelin_response)
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
                    match = re.search(r"(\d{1,2}|(one|ein)) \w+", i)
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
                country = address_meta.get("addressCountry") if address_meta else None
                city = address_meta.get("addressLocality") if address_meta else None
                address_region = address_meta.get("addressRegion") if address_meta else None
                street = address_meta.get("streetAddress") if address_meta else None
                opening_hours = meta_json.get("openingHours") if meta_json else None
                aggregate_rating = meta_json.get("aggregateRating")
                phone = meta_json.get("telephone")
                geo_cordinates = meta_json.get("geo")
                latitude = geo_cordinates.get("latitude")
                longitude = geo_cordinates.get("longitude")
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
                opening_hours = side_info_json["openingHours"]
                aggregate_rating = None  # Not Available
                phone = side_info_json["telephone"]
                latitude = side_info_json["latitude"]
                longitude = side_info_json["longitude"]
                date_published_on_rg = None  # Not Available
                _type = None  # Not Available
                serves_cuisine = side_info_json["servesCuisine"]
                price_range = side_info_json["priceRange"]
                address_meta = None  # No Meta data available in this case

            restaurant_data = {
                "_from_url": response.url,
                "title": response.xpath(
                    './/div[@class="wrapper_title "]/div/h1/a/text()'
                ).get(),
                "address-meta": address_meta,
                "country": country,
                "city": city,
                "address_region": address_region,
                "street": street,
                "latitude": latitude,
                "longitude": longitude,
                "phone": phone,
                "intern_link": response.xpath(
                    './/div[@class="website"]/div[2]/a/@href'
                ).get(),
                "url": link,
                "type_tags": response.xpath(
                    './/div[@id="ranks"]/div/div/a/span/text()'
                ).getall(),
                "establishment_type": _type,
                "price_range_euro": price_range_euro,
                "price_range": price_range,
                "menu_url": menu_url,
                "cuisine_type": serves_cuisine,
                "specials": response.xpath(
                    './/div[@class="features_block"]/div[2]/span/text()'
                ).getall(),
                "opening_hours": opening_hours,
                "aggregate_rating": aggregate_rating,
                "rating_google": google_rating,
                "rating_yelp": yelp,
                "foursquare": foursquare,
                "michelin": michelin,
                "trip": trip,
                "facebook": facebook,
                "date_published_on_rg": date_published_on_rg,
                "closed_permanent": response.xpath(
                    './/div[@class="wrapper_title "]/div[@class="closed_info_block"]/text()'
                ).get(),
                "last_review_dates": last_review_dates,
                "job_id": self.job_id,
                "city_id": self.current_city_id,
                "city_name": self.current_city,
                "processed_at": datetime.now().isoformat()
            }
            
            # Insert raw data into database
            self.insert_raw_data(restaurant_data)
            
            # Increment processed count
            self.restaurants_processed += 1
            
            yield restaurant_data

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
            match = re.search(
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
            match = re.search(r"\+\d+", phone_tag)
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
                    [f"{day} {hour}" for hour in hours if re.match(r"\d", hour)]
                )
        else:
            side_infos["openingHours"] = None

        # aggregateRating does not exisit without the meta_json

        # price Range:
        side_infos["priceRange"] = response.xpath('.//span[@class="cost"]/text()').get()

        return side_infos

    def closed(self, reason):
        """Called when spider closes - update city status based on completion"""
        if self.current_city_id:
            # Check if we processed all restaurants
            completion_percentage = (self.restaurants_processed / self.restaurants_found * 100) if self.restaurants_found > 0 else 0
            
            # Get current retry count to decide if we should mark as failed or keep retrying
            try:
                conn = self.get_db_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT retry_count, max_retries 
                    FROM smartdatastagdb.city_processing_status 
                    WHERE city_id = %s
                """, (self.current_city_id,))
                result = cursor.fetchone()
                current_retry_count = result[0] if result else 0
                max_retries = result[1] if result else 3
                cursor.close()
                conn.close()
            except Exception as e:
                self.log(f"Error getting retry count: {e}", logging.ERROR)
                current_retry_count = 0
                max_retries = 3
            
            self.log(f"🏁 Spider closing for city {self.current_city}. Found: {self.restaurants_found}, Processed: {self.restaurants_processed} ({completion_percentage:.1f}%). Retry: {current_retry_count}/{max_retries}. Reason: {reason}", logging.INFO)
            
            # Handle large cities with chunking strategy
            if self.restaurants_found > 3000:
                # Check if all pages have been processed
                last_processed_page = self.get_last_processed_page()
                total_pages = ceil(self.restaurants_found / 20)
                
                if last_processed_page >= total_pages:
                    # All chunks completed - mark as completed
                    self.update_city_status('completed', self.restaurants_found, self.restaurants_processed)
                    self.log(f"✅ Large city {self.current_city} FULLY COMPLETED after processing all {total_pages} pages!", logging.INFO)
                elif self.restaurants_processed > 0:
                    # Chunk completed successfully - update accumulative count and keep as 'processing' for next chunk
                    self.update_progress_during_chunk(self.restaurants_processed)
                    self.log(f"🔄 Large city chunk completed. Processed {self.restaurants_processed} restaurants in this chunk.", logging.INFO)
                    self.log(f"📊 Progress: {last_processed_page}/{total_pages} pages completed ({last_processed_page/total_pages*100:.1f}%)", logging.INFO)
                    self.log(f"🔄 City remains in 'processing' status for next chunk.", logging.INFO)
                    # Don't change status - leave as 'processing' for next chunk
                else:
                    # No progress in this chunk - check retry count
                    if current_retry_count >= max_retries:
                        error_msg = f"Max retries ({max_retries}) exceeded. Large city chunk failed: no restaurants processed"
                        self.update_city_status('failed', self.restaurants_found, self.restaurants_processed, error_msg)
                        self.log(f"❌ City {self.current_city} marked as FAILED - {error_msg}", logging.ERROR)
                    else:
                        error_msg = f"Large city chunk failed: no restaurants processed. Will retry ({current_retry_count + 1}/{max_retries})"
                        self.update_city_status('processing', self.restaurants_found, self.restaurants_processed, error_msg)
                        self.log(f"⚠️ City {self.current_city} marked as PROCESSING (will retry) - {error_msg}", logging.WARNING)
            else:
                # Normal completion logic for smaller cities
                if self.restaurants_found == 0:
                    # City has no restaurants - mark as completed (nothing to scrape)
                    self.update_city_status('completed', 0, 0)
                    self.log(f"✅ City {self.current_city} marked as COMPLETED - No restaurants found (city has 0 restaurants)", logging.INFO)
                elif completion_percentage >= 100:
                    self.update_city_status('completed', self.restaurants_found, self.restaurants_processed)
                    self.log(f"✅ City {self.current_city} marked as COMPLETED - {completion_percentage:.1f}% processed", logging.INFO)
                elif self.restaurants_processed > 0 and completion_percentage >= 30:
                    # Got partial data (30%+) - check retry count to decide if we should retry or fail
                    if current_retry_count >= max_retries:
                        # Max retries exceeded - mark as failed even with partial data
                        error_msg = f"Max retries ({max_retries}) exceeded. Partial data: {self.restaurants_processed}/{self.restaurants_found} restaurants ({completion_percentage:.1f}%)"
                        self.update_city_status('failed', self.restaurants_found, self.restaurants_processed, error_msg)
                        self.log(f"❌ City {self.current_city} marked as FAILED - {error_msg}", logging.ERROR)
                    else:
                        # Still have retries left - mark as 'processing' to allow resume
                        error_msg = f"Partial scrape (possibly rate-limited): {self.restaurants_processed}/{self.restaurants_found} restaurants ({completion_percentage:.1f}%). Will retry ({current_retry_count + 1}/{max_retries})"
                        self.update_city_status('processing', self.restaurants_found, self.restaurants_processed, error_msg)
                        self.log(f"⚠️ City {self.current_city} marked as PROCESSING (resume on next run) - {error_msg}", logging.WARNING)
                else:
                    # Very low completion (<30%) or no data - check retry count
                    if current_retry_count >= max_retries:
                        # Max retries exceeded - mark as failed
                        error_msg = f"Max retries ({max_retries}) exceeded. Only {self.restaurants_processed}/{self.restaurants_found} restaurants processed ({completion_percentage:.1f}%)"
                        self.update_city_status('failed', self.restaurants_found, self.restaurants_processed, error_msg)
                        self.log(f"❌ City {self.current_city} marked as FAILED - {error_msg}", logging.ERROR)
                    else:
                        # Still have retries left - mark as 'processing' to give it another chance
                        error_msg = f"Incomplete processing: {self.restaurants_processed}/{self.restaurants_found} restaurants ({completion_percentage:.1f}%). Will retry ({current_retry_count + 1}/{max_retries})"
                        self.update_city_status('processing', self.restaurants_found, self.restaurants_processed, error_msg)
                        self.log(f"⚠️ City {self.current_city} marked as PROCESSING (will retry) - {error_msg}", logging.WARNING)
        else:
            self.log(f"Spider completed for city {self.current_city}. Found: {self.restaurants_found}, Processed: {self.restaurants_processed}. Reason: {reason}", logging.INFO)
