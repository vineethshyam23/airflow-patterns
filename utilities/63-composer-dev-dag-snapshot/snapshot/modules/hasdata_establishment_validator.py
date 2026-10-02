import asyncio
import aiohttp
import os
import sys
import json
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import traceback
import time
from abc import ABC, abstractmethod
from psycopg2 import pool
import psycopg2
import logging
# from shared.ReadConfiguration import ReadConfig
# from root_path import root_path
from tqdm import tqdm
from collections import defaultdict

# os.environ['CONFIGPATH'] = root_path

# Data Classes
@dataclass
class PlaceToRequery:
    """Data class for places that need to be requeried"""
    establishment_id: int
    establishment_name: str
    geo_lat: float
    geo_long: float
    requery_attempts: int
    city: str

@dataclass
class SearchConfig:
    """Configuration for search parameters"""
    zoom_levels: List[str] = field(default_factory=lambda: ['12z', '14z', '13z'])
    # 12z = Street level (most specific) , 
    # 14z = Neighborhood level (broader), 
    # 13z = City district level (broadest)
    language: str = 'en'
    start_offset: str = '0'

    def __post_init__(self):
        if not self.zoom_levels:
            raise ValueError("zoom_levels cannot be empty")
        if not self.language:
            raise ValueError("language cannot be empty")

@dataclass
class OutputConfig:
    """Configuration for output handling"""
    results_table: str = "smartdata_analyticdb.hasdata_query_results_HR"
    source_places_table: str = "smartdata_analyticdb.stamm_e_export_HR"
    status_table: str = "smartdata_analyticdb.places_requery_status_HR"
    related_places_table: str = "smartdata_analyticdb.places_related_findings_HR"

@dataclass
class BatchStats:
    """Statistics for a single processing batch"""
    batch_id: int
    processed: int
    direct_matches: int
    related_places: int
    failed: int
    credits_used: int
    duration_seconds: float
    remaining_total: int

@dataclass
class TotalStats:
    """Aggregated statistics for the entire run"""
    total_processed: int = 0
    total_direct_matches: int = 0
    total_related_places: int = 0
    total_failed: int = 0
    total_credits: int = 0
    start_time: Optional[datetime] = None
    
    @property
    def duration(self) -> float:
        if not self.start_time:
            return 0.0
        return (datetime.now() - self.start_time).total_seconds()
    
    @property
    def success_rate(self) -> float:
        if not self.total_processed:
            return 0.0
        return (self.total_direct_matches / self.total_processed) * 100

@dataclass
class DebugEntry:
    """Stores debug information for a single place processing attempt"""
    debug_id: str
    timestamp: str
    place_name: str
    establishment_id: int
    input_params: Dict
    raw_response: Dict
    processing_result: str
    duration: float
    success: bool

class DebugLogger:
    """Handles debug logging and session management"""
    def __init__(self):
        self.debug_entries = {}
        self.current_batch_entries = []
        
    def create_debug_id(self, place_name: str) -> str:
        """Create unique debug ID for a place processing attempt"""
        timestamp = datetime.now().strftime('%H%M%S')
        return f"DEBUG_{timestamp}_{place_name.replace(' ', '_')}"
    
    def add_entry(self, entry: DebugEntry):
        """Add a debug entry to both session and batch storage"""
        self.debug_entries[entry.debug_id] = entry
        self.current_batch_entries.append(entry.debug_id)
    
    def start_batch(self):
        """Start a new batch of debug entries"""
        self.current_batch_entries = []
    
    def get_batch_summary(self) -> str:
        """Generate summary of current batch with navigation links"""
        summary = ["\nBatch Debug Summary:"]
        summary.append("="*50)
        
        # Group entries by success/failure
        successful = []
        failed = []
        
        for debug_id in self.current_batch_entries:
            entry = self.debug_entries[debug_id]
            if entry.success:
                successful.append(entry)
            else:
                failed.append(entry)
        
        # Add failed entries first
        if failed:
            summary.append("\n🔴 Failed Processes:")
            for entry in failed:
                summary.append(
                    f"→ [{entry.debug_id}] {entry.place_name} - {entry.processing_result} -{entry.input_params}"
                )
        
        # Add successful entries
        # if successful:
        #     summary.append("\n🟢 Successful Processes:")
        #     for entry in successful:
        #         summary.append(
        #             f"→ [{entry.debug_id}] {entry.place_name} - {entry.processing_result} "
        #         )
        
        return "\n".join(summary)

class ProgressLogger:
    """Handles formatted logging output for the Google Maps updater"""
    
    def __init__(self, log_file: str = "gmaps_update_{time}.log"):
        # Create logger instance
        self.logger = logging.getLogger('ProgressLogger')
        self.logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Console handler with simple format
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler with detailed format
        # Replace {time} placeholder with actual timestamp
        actual_log_file = log_file.replace("{time}", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
        file_handler = logging.FileHandler(actual_log_file)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        self.total_stats = TotalStats(start_time=datetime.now())
        
    def log_batch_start(self, batch_id: int, batch_size: int, total_remaining: int):
        """Log the start of a new batch"""
        self.logger.info("\n" + "="*50)
        # self.logger.info(f"Starting Batch {batch_id} | Size: {batch_size} | Remaining: {total_remaining}")
        # logging.debug(f"BATCH_START{{batch_id={batch_id},size={batch_size},remaining={total_remaining}}}")
    
    def log_batch_completion(self, stats: BatchStats):
        """Log batch completion with statistics"""
        # Update total stats
        self.total_stats.total_processed += stats.processed
        self.total_stats.total_direct_matches += stats.direct_matches
        self.total_stats.total_related_places += stats.related_places
        self.total_stats.total_failed += stats.failed
        self.total_stats.total_credits += stats.credits_used
        
        # Calculate completion percentage
        total_places = stats.remaining_total + self.total_stats.total_processed
        completion_pct = (self.total_stats.total_processed / total_places * 100) if total_places > 0 else 100
        
        # Format progress bar
        bar_length = 20
        filled = int(completion_pct / 100 * bar_length)
        bar = "=" * filled + "-" * (bar_length - filled)
        
        # Log batch summary
        # self.logger.info("\nBatch Summary:")
        # self.logger.info(f"Progress [{bar}] {completion_pct:.1f}%")
        # self.logger.info(f"✓ Found: {stats.direct_matches} direct, {stats.related_places} related")
        # self.logger.info(f"✗ Failed: {stats.failed}")
        # self.logger.info(f"Credits: {stats.credits_used} (batch) / {self.total_stats.total_credits} (total)")
        # self.logger.info(f"Time: {stats.duration_seconds:.1f}s")
        # self.logger.info(f"Remaining: {stats.remaining_total}")
        
        logging.debug(
            f"BATCH_STATS{{batch_id={stats.batch_id},"
            f"processed={stats.processed},"
            # f"direct_matches={stats.direct_matches},"
            # f"related_places={stats.related_places},"
            f"failed={stats.failed},"
            f"credits={stats.credits_used},"
            # f"duration={stats.duration_seconds:.1f},"
            f"remaining={stats.remaining_total}}}"
        )

    def log_final_summary(self):
        """Log final execution summary"""
        duration_minutes = self.total_stats.duration / 60
        
        self.logger.info("\n" + "="*50)
        self.logger.info("Execution Summary")
        # self.logger.info("="*50)
        # self.logger.info(f"Total Processed: {self.total_stats.total_processed}")
        # self.logger.info(f"Direct Matches: {self.total_stats.total_direct_matches}")
        # self.logger.info(f"Related Places: {self.total_stats.total_related_places}")
        # self.logger.info(f"Failed: {self.total_stats.total_failed}")
        # self.logger.info(f"Success Rate: {self.total_stats.success_rate:.1f}%")
        # self.logger.info(f"Total Credits: {self.total_stats.total_credits}")
        # self.logger.info(f"Total Duration: {duration_minutes:.1f} minutes")
        
        logging.debug(
            f"FINAL_STATS{{processed={self.total_stats.total_processed},"
            f"direct_matches={self.total_stats.total_direct_matches},"
            f"related_places={self.total_stats.total_related_places},"
            f"failed={self.total_stats.total_failed},"
            f"success_rate={self.total_stats.success_rate:.1f},"
            f"credits={self.total_stats.total_credits},"
            f"duration_minutes={duration_minutes:.1f}}}"
        )

# Abstract Base Classes
class APIClient(ABC):
    """Abstract base class for API clients"""
    @abstractmethod
    async def search_place(self, query: str, location: Optional[str], language: str, start: str = '0') -> Optional[Dict]:
        pass

    @abstractmethod
    async def close(self):
        pass

# class DatabaseConnection(ABC):
#     """Abstract base class for database operations"""
#     @abstractmethod
#     def get_connection(self):
#         pass

#     @abstractmethod
#     def return_connection(self, conn):
#         pass

#     @abstractmethod
#     def close(self):
#         pass

# Error Handler
class ErrorHandler:
    """Centralized error handling"""
    _failed_attempts = {}  # Track failed attempts per request

    @classmethod
    def log_error(cls, error: Exception, context: str):
        logging.error(f"Error in {context}: {str(error)}")
        logging.debug(f"Traceback: {traceback.format_exc()}")

    @classmethod
    async def retry_with_backoff(cls, func, place_name: str = None, max_retries: int = 2, initial_delay: float = 0.1):
        delay = initial_delay
        last_exception = None
        request_id = id(func)  # Unique identifier for this request

        if request_id not in cls._failed_attempts:
            cls._failed_attempts[request_id] = 0

        for attempt in range(max_retries):
            try:
                result = await func()
                if result:  # If success, remove from tracking
                    cls._failed_attempts.pop(request_id, None)
                return result
            except Exception as e:
                last_exception = e
                cls._failed_attempts[request_id] += 1
                
                if cls._failed_attempts[request_id] == 1:
                    place_info = f" for {place_name}" if place_name else ""
                    logging.warning(
                        f"Request failed{place_info}: {str(e)} - "
                        f"Will retry up to {max_retries} times"
                    )
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    place_info = f" for {place_name}" if place_name else ""
                    logging.error(
                        f"All {max_retries} attempts failed{place_info}. "
                        f"Final error: {str(last_exception)}"
                    )
                    raise last_exception

    @classmethod
    def clear_failed_attempts(cls):
        cls._failed_attempts.clear()

# Connection Pool
class ConnectionPool():
    """Manages database connection pool"""
    def __init__(self, config, min_conn: int = 1, max_conn: int = 10):
        try:
            self.pool = pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                dbname=config.get('database'),
                user=config.get('user'),
                password=config.get('password'),
                host=config.get('host')
            )
            logging.debug("Connection pool created successfully")
        except psycopg2.Error as e:
            logging.error("Could not create connection pool. Is VPN activated?")
            logging.error(e.pgerror)
            raise

    def get_connection(self):
        return self.pool.getconn()

    def return_connection(self, conn):
        self.pool.putconn(conn)

    def close(self):
        if hasattr(self, 'pool'):
            self.pool.closeall()

# Google Maps API Implementation
class GoogleMapsAPI(APIClient):
    """Handler for Google Maps API calls"""
    def __init__(self, api_key: str, base_url: str, credits_per_request: int = 5):
        self.api_key = api_key
        self.base_url = base_url
        self.credits_per_request = credits_per_request
        self.session = None
        self.semaphore = asyncio.Semaphore(100)  # Optimal concurrency
        self.connector = aiohttp.TCPConnector(limit=100)

    async def create_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                timeout=aiohttp.ClientTimeout(total=30)
            )

    async def search_place(self, query: str, location: Optional[str], language: str = 'en', start: str = '0') -> Optional[Dict]:
        if self.session is None:
            await self.create_session()
            
        async with self.semaphore:
            return await self._make_api_request(query, location, language, start)

    async def _make_api_request(self, query: str, location: Optional[str], language: str, start: str) -> Optional[Dict]:
        for attempt in range(3):
            try:
                if location:
                    # If we have lat/long and zoom
                    params = {
                        'q': f'"{query}"',
                        'll': f"@{location}",
                        'hl': language,
                        'start': start
                    }
                else:
                    # If we do NOT have lat/long, omit 'll' and 'start'
                    params = {
                        'q': f'"{query}"',
                        'hl': language
                    }

                logging.debug(f"Attempt {attempt+1}: Making request to {self.base_url} with params: {json.dumps(params)}")
                logging.debug(f"Headers: {{'x-api-key': '******'}}")  # Mask API key for security

                async with self.session.get(
                    self.base_url,
                    params=params,
                    headers={'x-api-key': self.api_key}
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Rate limit
                        if attempt < 2:
                            await asyncio.sleep(1 * (attempt + 1))
                            continue
                    elif response.status == 422:
                        error_text = await response.text()
                        logging.error(f"API request failed with status {response.status}: {error_text}")
                        return None
                    else:
                        error_text = await response.text()
                        logging.error(f"API request failed with status {response.status}: {error_text}")
                        return None
                    
            except Exception as e:
                if attempt == 2:
                    logging.error(f"API request failed after 3 attempts: {str(e)}")
                    return None
                await asyncio.sleep(0.5 * (attempt + 1))

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

# Database Manager
class DatabaseManager:
    def __init__(self, output_config: OutputConfig, creds: dict, max_retries: int = 2, min_conn: int = 1, max_conn: int = 10):
        # self.config = ReadConfig()
        self.cred = creds
        self.output_config = output_config
        self.max_retries = max_retries
        self.pool = ConnectionPool(config =self.cred, min_conn =min_conn, max_conn = max_conn)
        self._ensure_indexes()

    def _ensure_indexes(self):
        conn = self.pool.get_connection()
        try:
            with conn.cursor() as cursor:
                # Create results table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.output_config.results_table} (
                        job_id BIGINT PRIMARY KEY,
                        raw_json JSONB,
                        created_at TIMESTAMP,
                        source VARCHAR(50)
                    );
                """)
                
                # Create status table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.output_config.status_table} (
                        establishment_id BIGINT PRIMARY KEY,
                        requeried BOOLEAN DEFAULT FALSE,
                        requery_attempts INTEGER DEFAULT 0,
                        last_attempt_at TIMESTAMP,
                        last_attempt_status TEXT,
                        found_google_place_id VARCHAR(255)
                    );
                """)
                
                # Create related findings table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.output_config.related_places_table} (
                        source_establishment_id BIGINT,
                        found_google_places_id VARCHAR(255),
                        found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        processed BOOLEAN DEFAULT FALSE,
                        PRIMARY KEY (source_establishment_id, found_google_places_id)
                    );
                """)
                
                conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Error creating indexes: {str(e)}")
        finally:
            self.pool.return_connection(conn)

    def get_total_remaining(self) -> int:
        conn = self.pool.get_connection()
        try:
            with conn.cursor() as cursor:
                # First, get stats for logging = 4071 rec
                cursor.execute("""
                    WITH stats AS (
                        SELECT 
                            COUNT(*) as total_places,
                            SUM(CASE WHEN s.requeried = TRUE THEN 1 ELSE 0 END) as completed_via_direct,
                            SUM(CASE 
                                WHEN s.requeried = TRUE 
                                AND s.last_attempt_status LIKE 'Found in related results%' 
                                THEN 1 ELSE 0 END) as completed_via_related
                        FROM smartdata_analyticdb.stamm_e_export_HR e
                        LEFT JOIN smartdata_analyticdb.places_requery_status_HR s 
                            ON e.md_establishment_id = s.establishment_id
                        WHERE e.country = 'HR'
                        AND (e.aggregators_listed  LIKE '%hasdata%')
                    )
                    SELECT * FROM stats;
                """)
                stats = cursor.fetchone()
                if stats:
                    total, direct, related = stats
                    logging.debug(f"Places Status: Total={total}, "
                                 f"Completed Direct={direct}, "
                                 f"Completed via Related={related}, "
                                 f"Remaining={total - direct}")

                # Get actual remaining count - 4071 rec 
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM smartdata_analyticdb.stamm_e_export_HR e
                    LEFT JOIN smartdata_analyticdb.places_requery_status_HR s 
                        ON e.md_establishment_id = s.establishment_id
                    WHERE e.country = 'HR'
                        AND (e.aggregators_listed  LIKE '%hasdata%')
                        AND (
                            s.establishment_id IS NULL
                            OR (
                                NOT s.requeried 
                                AND (s.requery_attempts IS NULL OR s.requery_attempts < 3)
                                AND (s.last_attempt_status NOT LIKE 'Found in related results%%' 
                                    OR s.last_attempt_status IS NULL)
                            )
                        )
                """)
                
                result = cursor.fetchone()
                remaining = result[0] if result else 0
                return remaining
                
        except Exception as e:
            ErrorHandler.log_error(e, "Get total remaining")
            return 0
        finally:
            self.pool.return_connection(conn)

    def get_places_batch(self, batch_size: int = 10) -> List[PlaceToRequery]:
        conn = self.pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT 
                        e.md_establishment_id as establishment_id,
                        e.restaurant_name as establishment_name,
                        e.geo_lat as geo_lat,
                        e.geo_long as geo_long,
                        COALESCE(s.requery_attempts, 0) as requery_attempts,
                        e.city
                    FROM smartdata_analyticdb.stamm_e_export_HR e
                    LEFT JOIN smartdata_analyticdb.places_requery_status_HR s 
                        ON e.md_establishment_id = s.establishment_id
                    WHERE e.country = 'HR'
                        AND (e.aggregators_listed  LIKE '%hasdata%')
                        AND (
                            s.establishment_id IS NULL
                            OR (
                                s.requeried = false 
                                AND s.requery_attempts < 3
                            )
                        )
                    ORDER BY 
                        COALESCE(s.requery_attempts, 0) ASC,
                        s.last_attempt_at ASC NULLS FIRST
                    LIMIT {batch_size}
                """)
                
                results = cursor.fetchall()
                return [PlaceToRequery(*row) for row in results]
        except Exception as e:
            ErrorHandler.log_error(e, "Get places batch")
            return []
        finally:
            self.pool.return_connection(conn)

    #
    # Updated signature: now we receive the actual numeric establishment_id
    #
    def save_result(
        self,
        establishment_id: int,
        request_id: str,
        search_query: str,
        location: str,
        raw_json: str
    ) -> tuple[bool, int]:
        """Save API response and process related places. Returns (success, num_related_places)"""
        conn = self.pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("BEGIN")
                
                cursor.execute("SELECT nextval('smartdata_analyticdb.gmaps_job_id_seq')")
                job_id = cursor.fetchone()[0]
                
                processed_data = self._process_response(raw_json, request_id, search_query, location)
                
                cursor.execute(
                    f"INSERT INTO {self.output_config.results_table} "
                    "(job_id, raw_json, created_at, source) VALUES (%s, %s, CURRENT_TIMESTAMP, 'gmaps_api')",
                    (job_id, json.dumps(processed_data))
                )
                
                # Pass establishment_id to _save_related_places
                related_places_count = self._save_related_places(cursor, processed_data, establishment_id)
                
                conn.commit()
                return True, related_places_count
                
        except Exception as e:
            if conn:
                conn.rollback()
            ErrorHandler.log_error(e, "Save result")
            return False, 0
        finally:
            self.pool.return_connection(conn)

    def _process_response(self, raw_json: str, request_id: str, search_query: str, location: str) -> Dict:
        data = json.loads(raw_json)
        response_data = data.get('response', {})
        
        found_place_ids = set()
        if 'placeResults' in response_data:
            place_result = response_data['placeResults']
            if isinstance(place_result, dict) and 'placeId' in place_result:
                found_place_ids.add(place_result['placeId'])
        
        if 'localResults' in response_data and isinstance(response_data['localResults'], list):
            for result in response_data['localResults']:
                if isinstance(result, dict) and 'placeId' in result:
                    found_place_ids.add(result['placeId'])
        
        data['metadata'] = {
            'api_request_id': request_id,
            'source': 'gmaps_api',
            'search_query': search_query,
            'search_location': location,
            'timestamp': datetime.now().isoformat(),
            'found_place_ids': list(found_place_ids)
        }
        
        return data

    #
    # Updated signature here: we accept `source_establishment_id` (integer)
    #
    def _save_related_places(self, cursor, processed_data: Dict, source_establishment_id: int) -> int:
        found_place_ids = processed_data['metadata']['found_place_ids']
        
        if not found_place_ids:
            return 0

        try:
            # Insert into related_places_table
            cursor.execute(f"""
                INSERT INTO {self.output_config.related_places_table}
                (source_establishment_id, found_google_places_id, found_at, processed)
                SELECT 
                    %s,
                    unnest(%s),
                    CURRENT_TIMESTAMP,
                    FALSE
                ON CONFLICT (source_establishment_id, found_google_places_id) 
                DO NOTHING
                RETURNING found_google_places_id
            """, (source_establishment_id, list(found_place_ids)))
            
            new_relations = cursor.fetchall()
            return len(new_relations)
                
        except Exception as e:
            logging.error(f"Error saving related places: {str(e)}")
            return 0

    def update_place_status(self, establishment_id: int, success: bool, status: str, found_google_place_id: str = None):
        conn = self.pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {self.output_config.status_table}
                    (establishment_id, requeried, requery_attempts, last_attempt_at, last_attempt_status, found_google_place_id)
                    VALUES (%s, %s, 1, NOW(), %s, %s)
                    ON CONFLICT (establishment_id) DO UPDATE 
                    SET 
                        requeried = EXCLUDED.requeried,
                        requery_attempts = {self.output_config.status_table}.requery_attempts + 1,
                        last_attempt_at = EXCLUDED.last_attempt_at,
                        last_attempt_status = EXCLUDED.last_attempt_status,
                        found_google_place_id = COALESCE(EXCLUDED.found_google_place_id, 
                                                         {self.output_config.status_table}.found_google_place_id)
                """, (establishment_id, success, status, found_google_place_id))
                conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            ErrorHandler.log_error(e, "Update place status")
        finally:
            self.pool.return_connection(conn)

    # **
    def get_max_import_id(self) -> int:
        conn = self.pool.get_connection()
        try:
            with conn.cursor() as cursor:
                # Getting max import id from smartdatastagdb.import_log table
                cursor.execute("""
                    SELECT max(importid)+1 FROM smartdatastagdb.import_log;
                """)
                result = cursor.fetchone()
                remaining = result[0] if result else 0
                return remaining
                
        except Exception as e:
            ErrorHandler.log_error(e, "Get max(importID)+1 ")
            return 0
        finally:
            self.pool.return_connection(conn)

    def close(self):
        self.pool.close()

class BatchProgressTracker:
    """Tracks progress of multiple concurrent batches"""
    def __init__(self, progress_logger: ProgressLogger):
        self.progress_logger = progress_logger
        self.batch_progress = defaultdict(lambda: {"total": 0, "completed": 0, "pbar": None})
        self.lock = asyncio.Lock()

    def start_batch(self, batch_id: int, total: int):
        """Initialize new batch with progress bar"""
        pbar = tqdm(
            total=total,
            desc=f"Batch {batch_id}",
            position=batch_id % 3,  # Maximum 3 progress bars simultaneously
            bar_format="{desc:<10} {percentage:3.0f}%|{bar:20}{r_bar}",
            leave=False
        )
        self.batch_progress[batch_id] = {
            "total": total,
            "completed": 0,
            "pbar": pbar
        }

    async def update(self, batch_id: int, completed: int = 1):
        """Update progress of a batch"""
        async with self.lock:
            if batch_id in self.batch_progress:
                self.batch_progress[batch_id]["completed"] += completed
                self.batch_progress[batch_id]["pbar"].update(completed)

    def complete_batch(self, batch_id: int):
        """Complete a batch"""
        if batch_id in self.batch_progress:
            self.batch_progress[batch_id]["pbar"].close()
            del self.batch_progress[batch_id]

# Place Processor
class PlaceProcessor:
    def __init__(
        self, 
        api_client: APIClient,
        repository: DatabaseManager,
        search_config: SearchConfig,
        debug_logger: DebugLogger
    ):
        self.api_client = api_client
        self.repository = repository
        self.search_config = search_config
        self.debug_logger = debug_logger

    def _log_debug_section(self, debug_id: str, section: str, content: Any):
        """Log a debug section with consistent formatting"""
        logging.debug(f"\n[{debug_id}] {section}")
        logging.debug("="*80)
        if isinstance(content, dict):
            logging.debug(json.dumps(content, indent=2))
        else:
            logging.debug(str(content))

    def _extract_place_details(self, results: Dict) -> List[Dict]:
        """Extract relevant details from all found places"""
        places = []
        
        # Check placeResults (primary result)
        if 'placeResults' in results:
            place = results['placeResults']
            if isinstance(place, dict) and 'placeId' in place:
                places.append({
                    'name': place.get('title', 'Unknown'),
                    'place_id': place['placeId'],
                    'address': place.get('address', 'No address'),
                    'result_type': 'primary'
                })
        
        # Check localResults (nearby results)
        if 'localResults' in results and isinstance(results['localResults'], list):
            for place in results['localResults']:
                if isinstance(place, dict) and 'placeId' in place:
                    places.append({
                        'name': place.get('title', 'Unknown'),
                        'place_id': place['placeId'],
                        'address': place.get('address', 'No address'),
                        'result_type': 'local'
                    })
        
        return places

    async def process_place(self, place: PlaceToRequery) -> Optional[Dict]:
        start_time = time.time()
        debug_id = self.debug_logger.create_debug_id(place.establishment_name)
        input_params = None

        try:
            # Determine if coordinates are available
            has_coords = place.geo_lat is not None and place.geo_long is not None

            if has_coords:
                zoom_level = self.search_config.zoom_levels[
                    min(place.requery_attempts, len(self.search_config.zoom_levels) - 1)
                ]
                location = f"{place.geo_lat},{place.geo_long},{zoom_level}"
            else:
                # Fallback: No coordinates available
                location = None
                zoom_level = None  # Ensure zoom_level is defined

            input_params = {
                "name": f"{place.establishment_name} {place.city}",
                "establishment_id": place.establishment_id,
                "location": {
                    "lat": place.geo_lat,
                    "lng": place.geo_long,
                    "zoom": zoom_level
                },
                "attempt": place.requery_attempts + 1,
                "language": self.search_config.language
            }
                    
            self._log_debug_section(debug_id, "Input Parameters", input_params)
            search_query = f"{place.establishment_name} {place.city}"
            results = await ErrorHandler.retry_with_backoff(
                lambda: self.api_client.search_place(
                    search_query,
                    location,
                    self.search_config.language,
                    self.search_config.start_offset
                ),
                place_name=place.establishment_name
            )
            
            if not results:
                error_msg = "API call failed - no response"
                self._log_debug_section(debug_id, "Error", error_msg)
                self.debug_logger.add_entry(DebugEntry(
                    debug_id=debug_id,
                    timestamp=datetime.now().isoformat(),
                    place_name=place.establishment_name,
                    establishment_id=place.establishment_id,
                    input_params=input_params,
                    raw_response={},
                    processing_result=error_msg,
                    duration=time.time() - start_time,
                    success=False
                ))
                return None

            self._log_debug_section(debug_id, "API Response", results)
            
            request_id = results.get('requestMetadata', {}).get('id', '')
            
            # Save results
            success, related_count = self.repository.save_result(
                place.establishment_id,
                request_id,
                place.establishment_name,
                location if has_coords else f"{place.city}",
                json.dumps({'response': results})
            )
            
            if not success:
                error_msg = "Failed to save results"
                self._log_debug_section(debug_id, "Error", error_msg)
                self.debug_logger.add_entry(DebugEntry(
                    debug_id=debug_id,
                    timestamp=datetime.now().isoformat(),
                    place_name=place.establishment_name,
                    establishment_id=place.establishment_id,
                    input_params=input_params,
                    raw_response=results,
                    processing_result=error_msg,
                    duration=time.time() - start_time,
                    success=False
                ))
                return None

            found_places = self._extract_place_details(results)
            
            # Find best match
            best_match = None
            if found_places:
                best_match = next(
                    (p for p in found_places if p['result_type'] == 'primary'),
                    found_places[0]  # Fallback to first result
                )

            duration = time.time() - start_time
            
            if best_match:
                result_msg = f"Found potential match: {best_match['place_id']}"
                self._log_debug_section(debug_id, "Success", {
                    "message": result_msg,
                    "found_places": found_places
                })
                
                self.debug_logger.add_entry(DebugEntry(
                    debug_id=debug_id,
                    timestamp=datetime.now().isoformat(),
                    place_name=place.establishment_name,
                    establishment_id=place.establishment_id,
                    input_params=input_params,
                    raw_response=results,
                    processing_result=result_msg,
                    duration=duration,
                    success=True
                ))
                
                self.repository.update_place_status(
                    place.establishment_id,
                    True,
                    result_msg,
                    found_google_place_id=best_match['place_id']
                )
                
                return {
                    'success': True,
                    'related_places': related_count,
                    'duration': duration
                }
            else:
                result_msg = "No matching places found in results"
                self._log_debug_section(debug_id, "Not Found", {
                    "message": result_msg,
                    "found_places": found_places
                })
                
                self.debug_logger.add_entry(DebugEntry(
                    debug_id=debug_id,
                    timestamp=datetime.now().isoformat(),
                    place_name=place.establishment_name,
                    establishment_id=place.establishment_id,
                    input_params=input_params,
                    raw_response=results,
                    processing_result=result_msg,
                    duration=duration,
                    success=False
                ))
                
                self.repository.update_place_status(
                    place.establishment_id,
                    False,
                    result_msg
                )
                
                return {
                    'success': False,
                    'related_places': related_count,
                    'duration': duration
                }
            
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            self._log_debug_section(debug_id, "Exception", {
                "error": str(e),
                "traceback": traceback.format_exc()
            })
            
            self.debug_logger.add_entry(DebugEntry(
                debug_id=debug_id,
                timestamp=datetime.now().isoformat(),
                place_name=place.establishment_name,
                establishment_id=place.establishment_id,
                input_params=input_params or {},
                raw_response={},
                processing_result=error_msg,
                duration=time.time() - start_time,
                success=False
            ))
            
            self.repository.update_place_status(
                place.establishment_id,
                False,
                error_msg
            )
            
            return None

# Main Client class
class GoogleMapsClient:
    def __init__(
        self,
        api_client: APIClient,
        repository: DatabaseManager,
        processor: PlaceProcessor,
        progress_logger: ProgressLogger,
        debug_logger: DebugLogger,
        batch_size: int = 100,
        max_parallel_batches: int = 1
    ):
        self.api_client = api_client
        self.repository = repository
        self.processor = processor
        self.progress_logger = progress_logger
        self.debug_logger = debug_logger
        self.batch_size = batch_size
        self.batch_count = 0
        self.max_parallel_batches = max_parallel_batches
        self.active_batches = []
        self.progress_tracker = BatchProgressTracker(progress_logger)

    async def process_place_with_progress(self, place: PlaceToRequery, batch_id: int) -> Optional[Dict]:
        result = await self.processor.process_place(place)
        await self.progress_tracker.update(batch_id)
        return result

    async def process_single_batch(self, places: List[PlaceToRequery], batch_id: int, total_remaining: int) -> None:
        try:
            batch_start_time = time.time()
            self.debug_logger.start_batch()
            
            self.progress_logger.log_batch_start(
                batch_id,
                len(places),
                total_remaining
            )
            
            self.progress_tracker.start_batch(batch_id, len(places))
            
            tasks = [self.process_place_with_progress(place, batch_id) for place in places]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful = 0
            failed = 0
            related_places = 0
            
            for result in results:
                if isinstance(result, dict) and result.get('success', False):
                    successful += 1
                    related_places += result.get('related_places', 0)
                else:
                    failed += 1
            
            batch_duration = time.time() - batch_start_time
            
            self.progress_tracker.complete_batch(batch_id)
            
            stats = BatchStats(
                batch_id=batch_id,
                processed=len(places),
                direct_matches=successful,
                related_places=related_places,
                failed=failed,
                credits_used=len(places) * 5,
                duration_seconds=batch_duration,
                remaining_total=total_remaining - len(places)
            )
            
            self.progress_logger.log_batch_completion(stats)
            # logging.info(self.debug_logger.get_batch_summary())

        except Exception as e:
            ErrorHandler.log_error(e, f"Batch {batch_id} processing")
            self.progress_tracker.complete_batch(batch_id)

    async def process_batches(self) -> None:
        try:
            while True:
                self.active_batches = [batch for batch in self.active_batches if not batch.done()]
                
                while len(self.active_batches) < self.max_parallel_batches:
                    total_remaining = self.repository.get_total_remaining()
                    places = self.repository.get_places_batch(self.batch_size)
                    
                    if not places:
                        if not self.active_batches:
                            return
                        break
                    
                    self.batch_count += 1
                    batch_task = asyncio.create_task(
                        self.process_single_batch(
                            places, 
                            self.batch_count, 
                            total_remaining
                        )
                    )
                    self.active_batches.append(batch_task)
                
                await asyncio.sleep(0.1)
        finally:
            for batch_id in list(self.progress_tracker.batch_progress.keys()):
                self.progress_tracker.complete_batch(batch_id)

    async def close(self):
        if self.active_batches:
            await asyncio.gather(*self.active_batches)
        await self.api_client.close()
        self.repository.close()

# Main execution
async def main(cred,hasdata_api_key):
    try:
        output_config = OutputConfig()
        search_config = SearchConfig()
        progress_logger = ProgressLogger()
        debug_logger = DebugLogger()

        # Note: This script uses HasData's direct Google Maps search API, not the job-based API
        # The endpoint '/scrape/google-maps/search' is different from the new job-based endpoints
        api_client = GoogleMapsAPI(
            api_key=hasdata_api_key,
            # os.getenv('HASDATA_API_KEY'),
            base_url='https://api.hasdata.com/scrape/google-maps/search',  # Direct search API endpoint
            credits_per_request=5
        )
        
        repository = DatabaseManager(
            output_config=output_config,
            creds = cred,
            max_retries=2,
            min_conn=5,
            max_conn=20
        )
        
        processor = PlaceProcessor(
            api_client=api_client,
            repository=repository,
            search_config=search_config,
            debug_logger=debug_logger
        )

        client = GoogleMapsClient(
            api_client=api_client,
            repository=repository,
            processor=processor,
            progress_logger=progress_logger,
            debug_logger=debug_logger,
            batch_size=100,
            max_parallel_batches=1
        )
        
        await client.process_batches()
        progress_logger.log_final_summary()
        
    except Exception as e:
        ErrorHandler.log_error(e, "Main execution")
        raise
    finally:
        if 'client' in locals():
            await client.close()

def run_hasdata_validator(creds: dict,hasdata_api_key: str):
    """
    Helper function to run the HasData scraper from Airflow.
    
    Args:
        creds (dict): Database credentials containing:
            - hostname: Database host
            - database: Database name  
            - db_user: Database username
    """
    try:
        asyncio.run(main(creds,hasdata_api_key))
    except Exception as e:
        logging.critical(f"Fatal error in run_hasdata_validator: {str(e)}")
        logging.critical(f"Traceback: {traceback.format_exc()}")
        raise

def run_get_max_import_id(creds: dict) -> Optional[int]:
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
    db_manager = None
    try:
        output_config = OutputConfig()
        db_manager = DatabaseManager(output_config, creds)
        max_import_id = db_manager.get_max_import_id()
        return max_import_id
    except Exception as e:
        logging.critical(f"Fatal error in run_get_max_import_id: {str(e)}")
        logging.critical(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        if db_manager:
            db_manager.close()

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         logging.warning("\nProcess interrupted by user")
#         sys.exit(1)
#     except Exception as e:
#         logging.critical(f"Fatal error: {str(e)}")
#         logging.critical(f"Traceback: {traceback.format_exc()}")
#         logging.info("Restarting in 60 seconds...")
#         time.sleep(20)
 