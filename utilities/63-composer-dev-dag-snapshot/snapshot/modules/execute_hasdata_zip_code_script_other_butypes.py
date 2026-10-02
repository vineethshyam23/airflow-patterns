import os
import sys
import pandas as pd
import json
from datetime import datetime
# from loguru import logger
import logging
from typing import Dict, List, Optional, Union, Tuple
from enum import Enum
import asyncio
import aiohttp
import traceback
from dataclasses import dataclass
# import root_path as root_path
import asyncpg

# Fix sys.path to find the shared module
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# Go up 3 levels: hasdata_api -> hasdata -> scripts -> source_app_analytic
project_root = os.path.join(current_script_dir, '..', '..', '..')
project_root = os.path.abspath(project_root)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# from shared.ReadConfiguration import ReadConfig
# from root_path import root_path

@dataclass
class OutputConfig: 
    """Configuration for output handling."""
    use_database: bool = True
    #results_table: str = "smartdata_analyticdb.hasdata_query_raw_germany"
    #logs_table: str = "smartdata_analyticdb.hasdata_query_log_raw_germany" 
    #plz_table: str = "public.countries_zip_codes"
    results_table: str = "smartdata_analyticdb.hasdata_query_raw_germany_other_butypes"
    logs_table: str = "smartdata_analyticdb.hasdata_query_log_raw_germany_other_butypes"
    plz_table: str = "public.countries_zip_codes_other_butypes"
    # "smartdata_analyticdb.zip_codes_de"
    excel_output_path: Optional[str] = None

    def __post_init__(self):
        if not self.use_database and not self.excel_output_path:
            raise ValueError("When database is disabled, excel_output_path must be provided")


# Keywords list is now passed from DAG level
# DEFAULT_KEYWORDS removed - keywords must be provided from the calling DAG


# Commented out - QueryType enum is no longer used. Keywords are now passed directly.
# class QueryType(str, Enum):
#     """Enum for standard query types."""
#     # CAFE = "Cafe"
#     # BAR = "Bar"
#     # RESTAURANT = "Restaurant"
#     # BISTRO = "Bistro"
#
#     COURT = "Padel court"
#     TENNIS_CENTER = "Tennis center"
#     SPORTS_COMPLEX = "Sports complex"
#     SOCCER_FIELD = "Soccer field"
#     INDOOR_PLAYGROUND = "Indoor playground"
#     CHILDRENS_AMUSEMENT_CENTER = "children's amusement center"
#     
#     # Add new query types here as needed:
#     # EXAMPLE_TYPE = "Example query string"
#     
#     @classmethod
#     def get_combined_query(cls) -> List[str]:
#         """Get all query types as a list for new API format."""
#         return [q.value for q in cls]


class HasDataAPI:
    """Class to handle direct API interactions with HasData."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        # Updated to new API endpoints
        self.base_url = 'https://api.hasdata.com'
        self.session = None

    async def create_session(self):
        """Create aiohttp session for reuse."""
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))

    async def close_session(self):
        """Close aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()

    async def create_job_async(self, location: str, country: str, query: Union[str, List[str]], num_results: int,
                               extract_emails: int) -> Optional[Dict]:
        """Create a new scraper job using NEW API format."""
        await self.create_session()
        try:
            # Original API request data
            # data = {
            #     "keywords": query if isinstance(query, list) else [query],  # Ensure array format
            #     "locations": [f"CUSTOM>{location}, {country}"],
            #     "extractEmails": bool(extract_emails)
            # }

            # Updated API request data with maxResults parameter
            data = {
                "keywords": query if isinstance(query, list) else [query],  # Ensure array format -> query = ["Cafe","Bar","Restaurant","Bistro"]
                "locations": [f"CUSTOM>{location}, {country}"], # location is f"{city} {plz}" => Düsseldorf 40211, Germany
                "extractEmails": bool(extract_emails),
                "maxResults": num_results  # Added to control number of results per location
            }

            async with self.session.post(
                f'{self.base_url}/scrapers/google-maps/jobs',
                headers={'Content-Type': 'application/json', 'x-api-key': self.api_key},
                json=data
            ) as response:
                if response.status == 200 or response.status == 201:
                    return await response.json()
                else:
                    logging.error(f"Failed to create job for location {location}. Status: {response.status}")
                    logging.error(f"Response: {await response.text()}")
                    return None
        except Exception as e:
            logging.error(f"Error creating job for location {location}: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            return None

    async def get_job_status_async(self, job_id: int) -> Optional[Dict]:
        """Get the status of a specific job using NEW API format."""
        await self.create_session()
        try:
            async with self.session.get(
                f'{self.base_url}/scrapers/jobs/{job_id}',
                headers={'x-api-key': self.api_key}
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logging.error(f"Failed to get job status for job_id {job_id}. Status: {response.status}")
                    logging.error(f"Response: {await response.text()}")
                    return None
        except Exception as e:
            logging.error(f"Error checking status for job_id {job_id}: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            return None

    async def get_results_async(self, job_id: int, page: int = 1, limit: int = 1) -> Optional[Tuple[pd.DataFrame, str]]:
        """Get job results using NEW API format with pagination."""
        await self.create_session()
        
        all_scraped_data = []
        all_results = []
        current_page = page
        
        try:
            while True:
                url = f'{self.base_url}/scrapers/jobs/{job_id}/results'
                params = {'page': current_page, 'limit': limit}
                
                # logging.info(f"Fetching results from: {url} (page {current_page}, limit {limit})")

                async with self.session.get(url, headers={'x-api-key': self.api_key}, params=params) as response:
                    # logging.info(f"Response status: {response.status}")

                    if response.status == 200:
                        result = await response.json()
                        
                        # Store this page's complete result
                        all_results.append(result)
                        
                        # NEW API returns paginated format
                        data_list = result.get('data', [])
                        
                        if data_list:
                            # Extract the actual scraped data from each result item
                            scraped_data = [item.get('data', {}) for item in data_list if item.get('data')]
                            all_scraped_data.extend(scraped_data)
                            # logging.info(f"Retrieved {len(scraped_data)} results from page {current_page}")
                        
                        # Check pagination - if next_page_url is null, we're done
                        meta = result.get('meta', {})
                        next_page_url = meta.get('nextPageUrl')
                        
                        if next_page_url is None:
                            logging.info(f"Pagination complete. Total results: {len(all_scraped_data)}")
                            break
                        else:
                            current_page += 1
                            # logging.info(f"Found next page, continuing to page {current_page}")
                            # Small delay between pagination requests
                            await asyncio.sleep(0.5)
                    else:
                        logging.error(f"Request failed with status {response.status}")
                        error_body = await response.text()
                        logging.error(f"Error response body: {error_body[:500]}")
                        return None

            # Combine all results
            if all_scraped_data:
                df = pd.DataFrame(all_scraped_data)
                # Strip meta data from all_pages - keep only data portion
                pages_without_meta = []
                for page in all_results:
                    page_without_meta = {
                        'data': page.get('data', [])
                    }
                    pages_without_meta.append(page_without_meta)
                
                # Combine all page results into a single JSON structure
                combined_result = {
                    'meta': {
                        'total_pages_fetched': len(all_results),
                        'total_results': len(all_scraped_data),
                        'final_meta': all_results[-1].get('meta', {}) if all_results else {}
                    },
                    'all_pages': pages_without_meta
                }
                raw_json = json.dumps(combined_result)
                logging.info(f"Successfully retrieved {len(all_scraped_data)} total results across {len(all_results)} pages")
                return df, raw_json
            else:
                logging.warning("No valid scraped data found across all pages")
                return pd.DataFrame(), json.dumps({'all_pages': all_results})

        except aiohttp.ClientError as e:
            logging.error(f"Network error getting results for job {job_id}: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting results for job {job_id}: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            return None


class DatabaseManager:
    """Asynchronous database manager using asyncpg."""

    def __init__(self, output_config: OutputConfig,creds):
        # self.config = ReadConfig()
        self.cred = creds
        self.output_config = output_config
        self.pool = None

    async def _init_connection(self, connection):
        """Initialize each connection in the pool with custom settings."""
        try:
            # Set connection-specific parameters
            await connection.execute("SET statement_timeout = '120s'")
            await connection.execute("SET idle_in_transaction_session_timeout = '60s'")
            logging.debug("Connection initialized with custom settings")
        except Exception as e:
            logging.warning(f"Could not set connection parameters: {str(e)}")
            # Don't raise exception here as connection might still work

    async def connect(self):
        if self.output_config.use_database:
            max_retries = 3
            retry_delay = 5  # seconds
            
            for attempt in range(max_retries):
                try:
                    logging.info(f"Attempting to connect to database (attempt {attempt + 1}/{max_retries})...")
                    
                    logging.info(f"Connecting to database at {self.cred.get('host')}")
                    logging.info(f"Database: {self.cred.get('database')}")
                    logging.info(f"User: {self.cred.get('user')}")
                    logging.info(f"password: {self.cred.get('password')}")
                    # logging.info(f"SSL mode: prefer")
                    
                    # Create pool with timeout
                    self.pool = await asyncio.wait_for(
                        asyncpg.create_pool(
                            host=self.cred.get('host'),
                            database=self.cred.get('database'),
                            user=self.cred.get('user'),
                            password=self.cred.get('password'),
                            min_size=1,
                            max_size=10,
                            command_timeout=120,  # 2 minutes timeout for individual commands
                            server_settings={
                                'application_name': 'hasdata_scraper',
                                'statement_timeout': '120s'
                            },
                            # ssl='prefer',  # Use SSL if available, but don't require it -> ['prefer', 'require', 'disable']
                            init=self._init_connection
                        ),
                        timeout=60.0  # 1 minute timeout for pool creation
                    )
                    
                    # Test the connection
                    async with self.pool.acquire() as conn:
                        await conn.execute("SELECT 1")
                    
                    logging.info("Database connection pool created successfully")
                    return
                    
                except asyncio.TimeoutError:
                    logging.error(f"Connection attempt {attempt + 1} timed out")
                    if attempt < max_retries - 1:
                        logging.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        logging.error("All connection attempts timed out")
                        raise
                        
                except asyncpg.InvalidAuthorizationSpecificationError as e:
                    logging.error(f"Database authentication failed: {str(e)}")
                    logging.error("Please check your database credentials (username/password)")
                    raise
                    
                except asyncpg.InvalidCatalogNameError as e:
                    logging.error(f"Database does not exist: {str(e)}")
                    logging.error(f"Please verify the database name: {self.cred.get('database')}")
                    raise
                    
                except asyncpg.ConnectionFailureError as e:
                    logging.error(f"Failed to connect to database: {str(e)}")
                    logging.error(f"Please check database host and port: {self.cred.get('hostname')}")
                    if attempt < max_retries - 1:
                        logging.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        raise
                        
                except asyncpg.PostgresError as e:
                    logging.error(f"PostgreSQL error during connection: {str(e)}")
                    logging.error(f"Error code: {e.pgcode if hasattr(e, 'pgcode') else 'N/A'}")
                    raise
                    
                except Exception as e:
                    logging.error(f"Unexpected error connecting to database (attempt {attempt + 1}): {str(e)}")
                    logging.error(f"Traceback: {traceback.format_exc()}")
                    if attempt < max_retries - 1:
                        logging.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        raise
        else:
            logging.info("Database usage disabled in configuration")

    async def create_tables(self):
        if not self.output_config.use_database:
            return

        async with self.pool.acquire() as conn:
            try:
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.output_config.results_table} (
                        job_id INTEGER PRIMARY KEY,
                        raw_json TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.output_config.logs_table} (
                        job_id INTEGER PRIMARY KEY,
                        raw_json TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            except Exception as e:
                logging.error(f"Error creating tables: {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                raise

    async def save_results(self, job_id: int, raw_json: str):
        """Save raw JSON results to database."""
        if not self.output_config.use_database:
            return
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(f"""
                    INSERT INTO {self.output_config.results_table} (job_id, raw_json)
                    VALUES ($1, $2)
                    ON CONFLICT (job_id) DO UPDATE SET 
                        raw_json = EXCLUDED.raw_json,
                        created_at = CURRENT_TIMESTAMP
                """, job_id, raw_json)
            except Exception as e:
                logging.error(f"Error saving results for job_id {job_id}: {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                raise e

    async def save_log(self, job_id: int, raw_json: str):
        """Save raw JSON log to database."""
        if not self.output_config.use_database:
            return
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(f"""
                    INSERT INTO {self.output_config.logs_table} (job_id, raw_json)
                    VALUES ($1, $2)
                    ON CONFLICT (job_id) DO UPDATE SET 
                        raw_json = EXCLUDED.raw_json,
                        created_at = CURRENT_TIMESTAMP
                """, job_id, raw_json)
            except Exception as e:
                logging.error(f"Error saving log for job_id {job_id}: {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}") 
                raise e

    async def get_plz_batch(self, status: str, batch_size: int) -> List[Tuple[str, str]]:
        """Get a random batch of PLZ codes based on status."""
        if not self.output_config.use_database:
            return []
        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(f"""
                    SELECT plz, city
                    FROM {self.output_config.plz_table}
                    WHERE queried = $1
                    and country_code ='DE'
                    ORDER BY city
                    LIMIT $2
                """, status, batch_size)
                return [(r['plz'], r['city']) for r in rows] if rows else []
            except Exception as e:
                logging.error(f"Error getting PLZ batch with status '{status}': {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                return []

    async def mark_plz_as_queried(self, plz: str, status: str = 'queried'):
        """Mark a PLZ as queried with status and timestamp."""
        if not self.output_config.use_database:
            return
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(f"""
                    UPDATE {self.output_config.plz_table}
                    SET queried = $1::query_status,
                        queried_at = CURRENT_TIMESTAMP
                    WHERE plz = $2
                """, status, plz)
            except Exception as e:
                logging.error(f"Error marking PLZ {plz} as queried: {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                raise e

    async def get_query_status(self) -> Dict[str, int]:
        """Get counts of queried, not_queried, and failed PLZ codes."""
        if not self.output_config.use_database:
            return {'total': 0, 'queried': 0, 'unqueried': 0, 'failed': 0}
        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow(f"""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(*) FILTER (WHERE queried = 'queried') as queried,
                        COUNT(*) FILTER (WHERE queried = 'not_queried') as unqueried,
                        COUNT(*) FILTER (WHERE queried = 'failed') as failed
                    FROM {self.output_config.plz_table}
                    where country_code ='DE'
                """)
                if row:
                    return {
                        'total': row['total'],
                        'queried': row['queried'],
                        'unqueried': row['unqueried'],
                        'failed': row['failed']
                    }
                return {'total': 0, 'queried': 0, 'unqueried': 0, 'failed': 0}
            except Exception as e:
                logging.error(f"Error getting query status: {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                return {'total': 0, 'queried': 0, 'unqueried': 0, 'failed': 0}

    async def close(self):
        """Close database connection."""
        if self.pool:
            try:
                await self.pool.close()
                logging.info("Database connection pool closed successfully")
            except Exception as e:
                logging.warning(f"Error closing database pool: {str(e)}")
                # Don't raise exception during cleanup
    
    async def check_connection(self) -> bool:
        """Check if database connection is healthy."""
        if not self.pool:
            return False
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception as e:
            logging.error(f"Database connection check failed: {str(e)}")
            return False

    async def get_max_import_id(self) -> Optional[int]:
        """Get the maximum import ID from smartdatastagdb.import_log table."""
        if not self.output_config.use_database:
            logging.warning("Database usage is disabled, cannot get max import ID")
            return None
            
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval("SELECT max(importid)+1 FROM smartdatastagdb.import_log")
                logging.info(f"Maximum import ID retrieved: {result}")
                return result
            except Exception as e:
                logging.error(f"Error getting maximum import ID: {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                return None

    async def get_max_import_id_other_butypes(self) -> Optional[int]:
        """Get the maximum import ID from smartdatastagdb.import_log table for other business types."""
        if not self.output_config.use_database:
            logging.warning("Database usage is disabled, cannot get max import ID")
            return None
            
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval("SELECT max(importid)+1 FROM smartdatastagdb.import_log")
                logging.info(f"Maximum import ID retrieved for other_butypes: {result}")
                return result
            except Exception as e:
                logging.error(f"Error getting maximum import ID for other_butypes: {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                return None


class AsyncHasDataClient:
    """Asynchronous client class for HasData operations."""

    def __init__(self, output_config: OutputConfig, creds: dict, hasdata_api_key: str, max_concurrent: int = 50):
        self.api_key = hasdata_api_key
        if not self.api_key:
            raise ValueError("API key required")

        self.api = HasDataAPI(self.api_key)
        self.db = DatabaseManager(output_config, creds)
        self.output_config = output_config
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.default_extract_emails = 0
        self.default_country = "Germany"

    async def connect(self):
        await self.db.connect()
        await self.db.create_tables()

    async def search_places(self, location: str, country: str, keywords: List[str],
                            num_results: int = 3000, extract_emails: int = 0, 
                            timeout_minutes: int = 15) -> Optional[Dict[str, Union[pd.DataFrame, int]]]:
        """Execute a single search with provided keywords and timeout.
        
        Args:
            location: Location string (e.g., "Düsseldorf 40211")
            country: Country name (e.g., "Germany")
            keywords: List of keywords to search for (required)
            num_results: Maximum number of results to return
            extract_emails: Whether to extract emails (0 or 1)
            timeout_minutes: Timeout in minutes
        """
        async with self.semaphore:
            job_id = None
            last_log_time = datetime.now()
            log_interval = 60

            try:
                # Use provided keywords directly (no fallback to QueryType)
                if not keywords or len(keywords) == 0:
                    raise ValueError("keywords parameter is required and cannot be empty")
                
                query = keywords  # Use keywords directly
                job = await self.api.create_job_async(
                    location=location,
                    country=country,
                    query=query,
                    num_results=num_results,
                    extract_emails=extract_emails if extract_emails is not None else self.default_extract_emails
                )

                if not job:
                    logging.error(f"Failed to create job for location {location}")
                    return None

                job_id = job['id']
                logging.info(f"Created job {job_id} for location {location}")

                start_time = datetime.now()
                while True:
                    status = await self.api.get_job_status_async(job_id)
                    if not status:
                        logging.warning(f"Failed to get status for job {job_id}. Retrying...")
                        await asyncio.sleep(5)
                        continue

                    current_status = status.get('status')
                    elapsed_time = datetime.now() - start_time
                    current_time = datetime.now()

                    if elapsed_time.total_seconds() > timeout_minutes * 60:
                        logging.error(f"Job {job_id} for location {location} timed out after {timeout_minutes} minutes")
                        return None

                    if (current_time - last_log_time).total_seconds() >= log_interval:
                        logging.info(f"Job {job_id} ({location}) still running. Status: {current_status}. Elapsed time: {elapsed_time}")
                        last_log_time = current_time

                    if current_status == 'finished':
                        # NEW API: Save job log and get results directly
                        if self.output_config.use_database:
                            await self.db.save_log(job_id, json.dumps(status))

                        results_tuple = await self.api.get_results_async(job_id)
                        if results_tuple is not None:
                            results, raw_json = results_tuple

                            if len(results) > 0 and self.output_config.use_database:
                                await self.db.save_results(job_id, raw_json)

                            credits_used = int(status.get('creditsSpent', 0))  # Fixed field name
                            logging.info(f"Job {job_id} completed for {location}. Credits used: {credits_used}")
                            logging.info(f"Retrieved {len(results)} results")

                            return {
                                'data': results,
                                'job_id': job_id,
                                'credits_used': credits_used
                            }
                        else:
                            logging.error(f"Failed to get results for completed job {job_id} ({location})")
                            return None

                    elif current_status in ['failed', 'finished_with_error']:
                        logging.error(f"Job {job_id} for location {location} failed with status: {current_status}")
                        if 'error' in status:
                            logging.error(f"Error details: {status['error']}")
                        return None

                    await asyncio.sleep(5)

            except Exception as e:
                logging.error(f"Error in search_places for location {location}: {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                return None

    async def process_plz(self, plz: str, city: str, country: str, keywords: List[str]):
        """Process a single PLZ with improved error handling.
        
        Args:
            plz: Postal code
            city: City name
            country: Country name
            keywords: List of keywords to search for (required)
        """
        try:
            await asyncio.sleep(0.1)  # Small delay for API stability
            location = f"{city} {plz}"  # Combine city and PLZ for location

            results = await self.search_places(
                location=location,
                country=country,
                keywords=keywords,
                # num_results=3000,
                num_results=3,
                timeout_minutes=15
            )

            if results and isinstance(results.get('data'), pd.DataFrame):
                if len(results['data']) > 0:
                    logging.info(f"Successfully processed {city} PLZ {plz} with {len(results['data'])} results")
                    if self.output_config.use_database:
                        await self.db.mark_plz_as_queried(plz, 'queried')
                    return results['data']
                else:
                    logging.warning(f"Query for {city} PLZ {plz} returned empty results")
                    if self.output_config.use_database:
                        await self.db.mark_plz_as_queried(plz, 'failed')
                    return None
            else:
                logging.error(f"Query for {city} PLZ {plz} failed or returned invalid results")
                if self.output_config.use_database:
                    await self.db.mark_plz_as_queried(plz, 'failed')
                return None

        except Exception as e:
            logging.error(f"Error processing {city} PLZ {plz}: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            if self.output_config.use_database:
                await self.db.mark_plz_as_queried(plz, 'failed')
            return None

    async def process_multiple_locations(self, plz_list: List[Tuple[str, str]], keywords: List[str], 
                                         country: str = None) -> Dict[str, pd.DataFrame]:
        """Process multiple PLZ codes.
        
        Args:
            plz_list: List of tuples containing (plz, city) pairs
            keywords: List of keywords to search for (required)
            country: Country name (optional, defaults to self.default_country)
        """
        if not plz_list:
            logging.error("No PLZ codes provided")
            return {}
        country = country or self.default_country
        tasks = []

        for plz, city in plz_list:
            if not plz.strip() or not city.strip():
                logging.warning("Skipping empty PLZ or city")
                continue

            logging.info(f"Processing {city} PLZ: {plz}")
            tasks.append(self.process_plz(plz, city, country, keywords))

        completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        results = {}

        for i, result in enumerate(completed_tasks):
            plz, city = plz_list[i]
            if isinstance(result, Exception):
                logging.error(f"Task for {city} PLZ {plz} failed with error: {result}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                continue

            if result is not None:
                results[plz] = result

        return results

    async def close(self):
        """Close all connections."""
        await self.api.close_session()
        await self.db.close()


async def main(creds, hasdata_api_key, keywords: List[str]):
    """Main execution function with improved error handling and monitoring.
    
    Args:
        creds: Database credentials
        hasdata_api_key: HasData API key
        keywords: List of keywords to search for (required, must be provided from DAG level)
    """
    logging.info("Starting with main() execution")
    if not keywords or len(keywords) == 0:
        raise ValueError("keywords parameter is required and cannot be empty. Please provide keywords from DAG level.")
    logging.info(f"Using keywords from DAG: {keywords}")
    # logger.add("hasdata_api_{time}.log", rotation="1 day")
    # logger.add("hasdata_errors_{time}.log", rotation="1 day", level="ERROR")

    stats = {
        'start_time': datetime.now(),
        'total_batches': 0,
        'successful_jobs': 0,
        'failed_jobs': 0,
        'total_results': 0,
        'total_credits': 0
    }

    client = None

    try:
        output_config = OutputConfig(
            use_database=True,
            excel_output_path="hasdata_results.xlsx"
        )

        max_concurrent = 50
        logging.info(f"Setting max concurrent jobs to: {max_concurrent}")

        client = AsyncHasDataClient(output_config, creds=creds,hasdata_api_key=hasdata_api_key, max_concurrent=max_concurrent)
        await client.connect()

        try:
            while True:
                status = await client.db.get_query_status()
                logging.info("\nCurrent PLZ Query Status:")
                logging.info(f"Total PLZ codes: {status['total']}")
                logging.info(f"Successfully queried: {status['queried']}")
                logging.info(f"Failed queries: {status['failed']}")
                logging.info(f"Remaining unqueried: {status['unqueried']}")

                if status['unqueried'] == 0 and status['failed'] == 0:
                    logging.info("All PLZ codes have been processed")
                    break
                
                # :TODO: 
                # As I see in the table, there is only 1 failed PLZ and 0 unqueried PLZs,
                # Which means that the batch size should be 1, but the max_concurrent is 2,
                # This would cause issue with `failed_retry_size = min(batch_size // 2, status['failed'])`
                # cause of batch_size // 2 = 0, means that the failed_retry_size will be 0,
                # which means that the failed_plz_batch will be empty,
                # which means that the plz_batch will be empty,
                # Why the focus is just on the failed or unqueried PLZs?
                # What about the time if there are some new establishment in the queried PLZs?
                
                batch_size = min(
                    max_concurrent,
                    status['unqueried'] + status['failed']
                )

                if batch_size == 0:
                    logging.info("No more PLZ codes to process")
                    break

                failed_retry_size = min(batch_size // 2, status['failed'])
                failed_plz_batch = await client.db.get_plz_batch('failed', failed_retry_size)

                new_plz_size = batch_size - len(failed_plz_batch)
                unqueried_plz_batch = await client.db.get_plz_batch('not_queried', new_plz_size)

                plz_batch = failed_plz_batch + unqueried_plz_batch

                if not plz_batch:
                    logging.warning("Got empty batch despite status showing work remains")
                    break

                logging.info(f"\nProcessing batch {stats['total_batches'] + 1}:")
                logging.info(f"Total PLZs in batch: {len(plz_batch)}")
                logging.info(f"Failed PLZs being retried: {len(failed_plz_batch)}")
                logging.info(f"New PLZs: {len(unqueried_plz_batch)}")

                batch_start = datetime.now()
                try:
                    results = await asyncio.wait_for(
                        client.process_multiple_locations(plz_batch, keywords, country='Germany'),
                        timeout=60 * 30
                    )
                except asyncio.TimeoutError:
                    logging.error("Batch processing timed out after 30 minutes")
                    results = {}

                stats['total_batches'] += 1
                batch_duration = datetime.now() - batch_start

                if results:
                    successful_count = sum(1 for data in results.values() if data is not None)
                    failed_count = len(plz_batch) - successful_count
                    total_results = sum(len(data) for data in results.values() if data is not None)

                    stats['successful_jobs'] += successful_count
                    stats['failed_jobs'] += failed_count
                    stats['total_results'] += total_results

                    logging.info(f"\nBatch {stats['total_batches']} completed in {batch_duration}:")
                    logging.info(f"Successful PLZs: {successful_count}/{len(plz_batch)} ({successful_count/len(plz_batch)*100:.1f}%)")
                    logging.info(f"Results collected: {total_results}")
                    if successful_count > 0:
                        logging.info(f"Average results per successful PLZ: {total_results/successful_count:.1f}")

                    for plz, data in results.items():
                        if data is not None:
                            logging.info(f"PLZ {plz}: {len(data)} results")
                        else:
                            logging.warning(f"PLZ {plz}: Failed to get results")
                else:
                    logging.error("Batch processing failed completely")
                    stats['failed_jobs'] += len(plz_batch)

                delay = 5
                logging.info(f"Waiting {delay:.1f} seconds before next batch...")
                await asyncio.sleep(delay)

        except asyncio.CancelledError:
            logging.warning("Received cancellation request")
            raise
        except Exception as e:
            logging.error(f"Error in processing loop: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise

    except Exception as e:
        logging.error(f"Fatal error in main execution: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        runtime = datetime.now() - stats['start_time']
        logging.info("\nFinal Statistics:")
        logging.info(f"Total runtime: {runtime}")
        logging.info(f"Batches processed: {stats['total_batches']}")
        logging.info(f"Successful jobs: {stats['successful_jobs']}")
        logging.info(f"Failed jobs: {stats['failed_jobs']}")
        logging.info(f"Total results collected: {stats['total_results']}")

        if stats['successful_jobs'] > 0:
            logging.info(f"Average results per successful job: {stats['total_results']/stats['successful_jobs']:.1f}")

        if client is not None:
            await client.close()


def run_hasdata_scraper(creds: dict, hasdata_api_key: str, keywords: List[str]):
    """
    Helper function to run the HasData scraper from Airflow.
    
    Args:
        creds (dict): Database credentials containing:
            - hostname: Database host
            - database: Database name  
            - db_user: Database username
        hasdata_api_key (str): HasData API key
        keywords (List[str]): List of keywords to search for (required, must be provided from DAG level)
    """
    try:
        asyncio.run(main(creds, hasdata_api_key, keywords))
    except Exception as e:
        logging.critical(f"Fatal error in run_hasdata_scraper: {str(e)}")
        logging.critical(f"Traceback: {traceback.format_exc()}")
        raise

def run_get_max_import_id_other_butypes(creds: dict) -> Optional[int]:
    """
    Helper function to get the maximum import ID from the database.
    
    Args:
        creds (dict): Database credentials containing:
            - host: Database host
            - database: Database name  
            - user: Database username
            - password: Database password
    
    Returns:
        Optional[int]: Maximum import ID or None if error
    """
    async def get_max_import_id_async_other_butypes():
        db_manager = None
        try:
            output_config = OutputConfig(use_database=True)
            db_manager = DatabaseManager(output_config, creds)
            await db_manager.connect()
            max_import_id = await db_manager.get_max_import_id_other_butypes()
            return max_import_id
        except Exception as e:
            logging.critical(f"Fatal error in get_max_import_id_async_other_butypes: {str(e)}")
            logging.critical(f"Traceback: {traceback.format_exc()}")
            raise
        finally:
            if db_manager:
                await db_manager.close()
    
    try:
        return asyncio.run(get_max_import_id_async_other_butypes())
    except Exception as e:
        logging.critical(f"Fatal error in run_get_max_import_id_other_butypes: {str(e)}")
        logging.critical(f"Traceback: {traceback.format_exc()}")
        raise

# if __name__ == "__main__":
#     try:
#         # os.environ['CONFIGPATH'] = root_path
#         # Note: When running directly, creds should be passed from Airflow operator
#         # For testing, you can define creds here or pass it as an argument
#         # asyncio.run(main(creds))
#         raise ValueError("This script should be called from Airflow with creds parameter")
#     except KeyboardInterrupt:
#         logging.warning("Process interrupted by user")
#         sys.exit(1)
#     except Exception as e:
#         logging.critical(f"Fatal error: {str(e)}")
#         logging.critical(f"Traceback: {traceback.format_exc()}")
#         sys.exit(1) 