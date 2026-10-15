import os
import sys
import subprocess
from datetime import datetime
import logging
from typing import Dict, Optional
from enum import Enum
import asyncio
import traceback
from dataclasses import dataclass
import asyncpg
import re

# Fix sys.path to find the shared module
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_script_dir, '..', '..', '..')
project_root = os.path.abspath(project_root)
if project_root not in sys.path:
    sys.path.insert(0, project_root)


@dataclass
class RestaurantGuruConfig:
    """Configuration for Restaurant Guru scraper output handling."""
    use_database: bool = True
    results_table: str = "smartdata_analyticdb.restaurant_guru_raw_germany"
    logs_table: str = "smartdata_analyticdb.restaurant_guru_log_germany"
    scrapy_project_path: str = "/home/airflow/gcs/dags/modules/RestaurantGuru/RestaurantGuru/RestaurantGuru"
    spider_name: str = "rguru_de"


class ScrapyExecutionMode(str, Enum):
    """Enum for Scrapy execution modes."""
    NORMAL = "normal"
    TEST = "test"


class RestaurantGuruDatabaseManager:
    """Simple database manager for Restaurant Guru logs."""
    
    def __init__(self, config: RestaurantGuruConfig, creds: dict):
        self.config = config
        self.creds = creds
        self.pool = None

    async def connect(self):
        """Establish database connection."""
        try:
            logging.info("Connecting to database...")
            self.pool = await asyncpg.create_pool(
                host=self.creds['host'],
                port=self.creds['port'],
                user=self.creds['user'],
                password=self.creds['password'],
                database=self.creds['database'],
                min_size=1,
                max_size=3
            )
            logging.info("Database connection pool created successfully")
        except Exception as e:
            logging.error(f"Error connecting to database: {str(e)}")
            raise

    async def create_tables(self):
        """Create necessary tables if they don't exist."""
        if not self.config.use_database or not self.pool:
            return

        async with self.pool.acquire() as conn:
            try:
                # ðŸ”§ Create results table with job_id as integer
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.config.results_table} (
                        job_id BIGINT NOT NULL,
                        spider_name TEXT NOT NULL,
                        raw_json TEXT NOT NULL,
                        items_count INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (job_id)
                    )
                """)

                # ðŸ”§ Create logs table with job_id as integer
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.config.logs_table} (
                        job_id BIGINT NOT NULL,
                        spider_name TEXT NOT NULL,
                        log_content TEXT NOT NULL,
                        execution_status TEXT NOT NULL,
                        items_scraped INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (job_id)
                    )
                """)
                
                # ðŸ”§ Handle migration from execution_id to job_id in existing tables
                try:
                    # Add job_id column if it doesn't exist
                    await conn.execute(f"""
                        ALTER TABLE {self.config.logs_table} 
                        ADD COLUMN IF NOT EXISTS job_id BIGINT
                    """)
                    
                    # Add items_scraped column if it doesn't exist
                    await conn.execute(f"""
                        ALTER TABLE {self.config.logs_table} 
                        ADD COLUMN IF NOT EXISTS items_scraped INTEGER DEFAULT 0
                    """)
                    
                    # Make execution_id nullable for migration compatibility
                    await conn.execute(f"""
                        ALTER TABLE {self.config.logs_table} 
                        ALTER COLUMN execution_id DROP NOT NULL
                    """)
                    
                    logging.info("ðŸ”§ Table migration steps completed successfully")
                    
                except Exception as e:
                    logging.warning(f"Table migration warning (columns may already exist): {e}")
                    pass
                
                logging.info("Database tables created/verified successfully")
                
            except Exception as e:
                logging.error(f"Error creating tables: {str(e)}")
                raise

    async def save_log(self, job_id: int, spider_name: str, log_content: str, 
                      execution_status: str, items_scraped: int = 0, city_name: str = "all_cities"):
        """Save execution log to database."""
        if not self.config.use_database or not self.pool:
            return
            
        async with self.pool.acquire() as conn:
            try:
                # ðŸ”§ Smart insert: Handle both old and new table structures
                try:
                    # Try new structure (job_id primary key)
                    await conn.execute(f"""
                        INSERT INTO {self.config.logs_table} 
                        (job_id, city_name, spider_name, log_content, execution_status, items_scraped, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
                    """, job_id, city_name, spider_name, log_content, execution_status, items_scraped)
                except Exception as e:
                    # Fallback: Try old structure (execution_id primary key)
                    logging.warning(f"New structure failed, trying fallback: {e}")
                    execution_id_str = str(job_id)  # Convert job_id to string for old structure
                    await conn.execute(f"""
                        INSERT INTO {self.config.logs_table} 
                        (execution_id, city_name, spider_name, log_content, execution_status, items_scraped, created_at, job_id)
                        VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, $7)
                        ON CONFLICT (execution_id) DO UPDATE SET
                        job_id = $7,
                        log_content = $4,
                        execution_status = $5,
                        items_scraped = $6,
                        created_at = CURRENT_TIMESTAMP
                    """, execution_id_str, city_name, spider_name, log_content, execution_status, items_scraped, job_id)
                
                logging.info(f"ðŸ”§ Saved execution log for job_id: {job_id}")
                
            except Exception as e:
                logging.error(f"Error saving log: {str(e)}")
                raise

    async def get_max_import_id(self) -> Optional[int]:
        """Get the maximum import ID from import_log table."""
        if not self.config.use_database or not self.pool:
            return None
            
        async with self.pool.acquire() as conn:
            try:
                # Get max import_id from import_log table and increment by 1
                row = await conn.fetchrow("""
                    SELECT COALESCE(MAX(importid), 0) + 1 as next_import_id 
                    FROM smartdatastagdb.import_log
                """)
                
                next_id = row['next_import_id'] if row else 1
                logging.info(f"Next import ID: {next_id}")
                return next_id
                
            except Exception as e:
                logging.error(f"Error getting max import ID: {str(e)}")
                # Fallback to a simple incrementing number based on timestamp
                from datetime import datetime
                fallback_id = int(datetime.now().strftime('%Y%m%d%H'))  # YYYYMMDDHH format
                logging.warning(f"Using fallback import ID: {fallback_id}")
                return fallback_id

    async def close(self):
        """Close database connection."""
        if self.pool:
            await self.pool.close()
            logging.info("Database connection pool closed successfully")


def extract_items_count(log_content: str) -> int:
    """Extract items scraped count from log content."""
    try:
        # 🚨 DEPLOYMENT CHECK: This will confirm if the enhanced extraction is deployed
        logging.error("🚨🚨🚨 DEPLOYMENT CHECK: ENHANCED EXTRACT_ITEMS_COUNT IS DEPLOYED! 🚨🚨🚨")
        
        # 🚨 MEGA DEBUG: Log what we're searching in
        logging.error(f"🚨 EXTRACT_ITEMS_COUNT DEBUG: Searching in {len(log_content)} characters of log content")
        
        # Look for item_scraped_count in various formats
        # 🚨 REORDERED: Put our reliable custom patterns FIRST to avoid false matches from Scrapy's 0-count logs
        patterns = [
            r"Restaurants processed: (\d+)",                   # 🚨 PRIORITY: Our custom format (most reliable)
            r"Restaurant #(\d+) processed successfully",       # 🚨 PRIORITY: Individual restaurant logs  
            r"Total: (\d+)/\d+",                              # 🚨 PRIORITY: Progress format
            r"'item_scraped_count': (\d+)",                    # Scrapy stats format
            r"item_scraped_count.*?(\d+)",                     # General format
            r"(\d+) items scraped",                            # Text format
            r"Items: (\d+)",                                   # Simple format
            r"Crawled \d+ pages.*?scraped (\d+) items",       # Alternative logstats
            r"scraped (\d+) items \(at (\d+) items/min\)",    # 🚨 MOVED TO END: Scrapy logstats (often shows 0)
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, log_content, re.IGNORECASE)
            if matches:
                # For multiple matches, take the highest number (most recent/complete)
                if isinstance(matches[0], tuple):
                    # Handle patterns with multiple groups
                    count = max([int(match[0]) for match in matches])
                else:
                    count = max([int(match) for match in matches])
                logging.error(f"🚨 FOUND ITEMS COUNT using pattern '{pattern}': {count} (from {len(matches)} matches)")
                return count
        
        # Fallback: Look for any mention of items in the final stats
        scrapy_stats_match = re.search(r"item_scraped_count.*?(\d+)", log_content, re.DOTALL)
        if scrapy_stats_match:
            count = int(scrapy_stats_match.group(1))
            logging.error(f"🚨 FOUND ITEMS COUNT in Scrapy stats: {count}")
            return count
        
        # 🚨 MEGA DEBUG: Show what patterns we tried
        logging.error("🚨 NO ITEMS COUNT PATTERN MATCHED - Tried patterns:")
        for i, pattern in enumerate(patterns):
            logging.error(f"   {i+1}. {pattern}")
        
        # 🚨 MEGA DEBUG: Show last 500 chars of log to see what's there
        logging.error(f"🚨 LAST 500 CHARS OF LOG: ...{log_content[-500:]}")
        
        return 0
    except Exception as e:
        logging.error(f"🚨 ERROR extracting items count: {str(e)}")
        return 0


async def main_restaurant_guru(creds: dict, mode: ScrapyExecutionMode = ScrapyExecutionMode.NORMAL,
                               item_limit: int = None, scrapy_project_path: str = None, max_concurrent: int = 20,
                               start_index: int = None, end_index: int = None, index_based_mode: bool = False,
                               restaurants_per_batch: int = 50000):
    """Main execution function for Restaurant Guru scraping - simplified for original spider design."""
    logging.info("Starting Restaurant Guru scraper execution")
    
    stats = {
        'start_time': datetime.now(),
        'total_items': 0
    }
    
    db_manager = None
    execution_status = 'failed'  # ðŸ”§ Initialize execution_status at function level
    
    try:
        config = RestaurantGuruConfig(
            use_database=True,
            scrapy_project_path=scrapy_project_path or "/home/airflow/gcs/dags/modules/RestaurantGuru/RestaurantGuru/RestaurantGuru"
        )
        
        logging.info(f"Execution mode: {mode}")
        if item_limit:
            logging.info(f"Item limit: {item_limit}")
        
        # 🚀 Processing mode configuration
        if index_based_mode:
            logging.info(f"🔢 INDEX-BASED MODE: Processing restaurants {start_index}-{end_index}")
        else:
            logging.info(f"🌐 INFINITE SCROLL MODE: Restaurant Guru infinite scroll processing")
        
        # Initialize database connection for result storage
        db_manager = RestaurantGuruDatabaseManager(config, creds)
        await db_manager.connect()
        await db_manager.create_tables()
        
        # ðŸ”§ Generate integer job_id instead of string execution_id
        job_id = int(datetime.now().strftime('%Y%m%d%H%M%S'))  # Integer format: YYYYMMDDHHMMSS
        
        # Create logs directory with more robust path handling
        logs_dir = os.path.join(config.scrapy_project_path, 'logs')
        try:
            os.makedirs(logs_dir, exist_ok=True)
            logging.info(f"Ã¢Å“â€¦ Created/verified logs directory: {logs_dir}")
        except Exception as e:
            logging.error(f"Ã¢ÂÅ’ Failed to create logs directory: {e}")
            # Fallback to temp directory
            import tempfile
            logs_dir = tempfile.gettempdir()
            logging.info(f"Ã°Å¸â€â€ž Using fallback logs directory: {logs_dir}")
        
        # Create log file with timestamp
        log_file = os.path.join(logs_dir, f'{config.spider_name}_all_cities_{job_id}.log')
        logging.info(f"Ã°Å¸â€œâ€ž Target log file: {log_file}")
        
        # SIMPLIFIED: Just try to list spiders first to test basic functionality
        test_cmd = ['scrapy', 'list']
        logging.info("Ã°Å¸Â§Âª Testing basic scrapy functionality...")
        try:
            test_result = subprocess.run(test_cmd, cwd=config.scrapy_project_path, capture_output=True, text=True, timeout=30)
            logging.info(f"Ã°Å¸Â§Âª Scrapy list result: return_code={test_result.returncode}")
            logging.info(f"Ã°Å¸Â§Âª Available spiders: {test_result.stdout}")
            if test_result.stderr:
                logging.error(f"Ã°Å¸Â§Âª Scrapy list errors: {test_result.stderr}")
        except Exception as e:
            logging.error(f"Ã°Å¸Â§Âª Failed to test scrapy: {e}")

        # ðŸ™ï¸ MEGA CITY: Build optimized Scrapy command for 40K+ establishments
        restaurant_count = item_limit or 45000  # ðŸ”§ Fixed: Use parameter item_limit instead of config.item_limit
        
        # ðŸš€ MEGA CITY: Conservative settings for 40K+ restaurants (rate limit optimized)
        if restaurant_count >= 30000:  # MEGA CITIES (30K+) - Berlin, Munich
            concurrent_requests = 4          # REDUCED: Conservative for rate limits
            concurrent_per_domain = 2        # REDUCED: Lower domain pressure
            reactor_pool_size = 15           # REDUCED: Fewer threads
            timeout_seconds = 86400          # 24 hours for mega cities
            download_delay = 0.5             # REDUCED: Faster processing (was 1.0)
            retry_times = 2                  # REDUCED: Prevent API waste (was 8!)
            logging.info(f"ðŸ™ï¸ MEGA CITY CONSERVATIVE mode: {restaurant_count} restaurants (rate limit optimized)")
        elif restaurant_count >= 15000:  # LARGE CITIES (15K-30K)
            concurrent_requests = 10
            concurrent_per_domain = 7
            reactor_pool_size = 25
            timeout_seconds = 64800          # 18 hours
            download_delay = 0.15
            retry_times = 2
            logging.info(f"ðŸŒ† LARGE CITY mode: {restaurant_count} restaurants")
        elif restaurant_count >= 10000:  # Large cities (10K-15K)
            concurrent_requests = 8
            concurrent_per_domain = 6
            reactor_pool_size = 20
            timeout_seconds = 43200          # 12 hours
            download_delay = 0.2
            retry_times = 2
            logging.info(f"ðŸ¢ BIG CITY mode: {restaurant_count} restaurants")
        else:  # Smaller cities
            concurrent_requests = 6
            concurrent_per_domain = 4
            reactor_pool_size = 15
            timeout_seconds = 21600          # 6 hours
            download_delay = 0.25
            retry_times = 2
            logging.info(f"ðŸ˜ï¸ STANDARD mode: {restaurant_count} restaurants")
        
        logging.info(f"ðŸš€ OPTIMIZED SETTINGS for {restaurant_count} restaurants:")
        logging.info(f"   ðŸ“¡ Concurrent requests: {concurrent_requests}")
        logging.info(f"   ðŸŒ Per domain: {concurrent_per_domain}")
        logging.info(f"   â° Timeout: {timeout_seconds/3600:.1f} hours")
        logging.info(f"   ðŸ”„ Retry times: {retry_times}")
        
        cmd_parts = [
            'scrapy', 'crawl', config.spider_name,
            '-L', 'INFO',  # Log level
            
            # 🚨 GCP COMPOSER CACHE FIX: Override middleware to prevent old cache issues  
            '-s', 'DOWNLOADER_MIDDLEWARES={"RestaurantGuru.middlewares.ScrapeOpsProxyMiddleware": 610}',
            
            # ðŸ™ï¸ MEGA CITY: Optimized timeout settings
            '-s', f'CLOSESPIDER_TIMEOUT={timeout_seconds}',  # Dynamic timeout based on city size
            '-s', 'DOWNLOAD_TIMEOUT=60',                     # REDUCED: 1 minute (was 2 minutes)
            
            # ðŸš€ MEGA CITY: Enhanced concurrency settings
            '-s', f'CONCURRENT_REQUESTS={concurrent_requests}',           # Dynamic concurrency
            '-s', f'CONCURRENT_REQUESTS_PER_DOMAIN={concurrent_per_domain}',  # Per-domain limits
            '-s', f'REACTOR_THREADPOOL_MAXSIZE={reactor_pool_size}',      # Reactor thread pool
            
            # ðŸ’° API COST OPTIMIZATION: Minimal retries to prevent API waste
            '-s', 'RETRY_TIMES=2',                                      # FIXED: Only 2 retries (was dynamic 8!)
            '-s', 'RETRY_HTTP_CODES=500,502,503,504',                   # REMOVED 429,403 to prevent API waste
            '-s', f'DOWNLOAD_DELAY={download_delay}',                   # Dynamic delay
            
            # ðŸš€ MEGA CITY: Memory & performance optimization
            '-s', 'MEMUSAGE_ENABLED=True',                  # Monitor memory usage
            '-s', 'MEMUSAGE_LIMIT_MB=8192',                 # INCREASED: 8GB memory limit
            '-s', 'MEMUSAGE_WARNING_MB=6144',               # Warning at 6GB
            '-s', 'GC_ENABLED=True',                        # Enable garbage collection
            
            # ðŸ™ï¸ MEGA CITY: Connection optimization
            '-s', 'DNS_TIMEOUT=60',                         # INCREASED: DNS timeout
            '-s', 'DOWNLOAD_WARNSIZE=67108864',             # INCREASED: 64MB download warning
            '-s', 'DOWNLOAD_MAXSIZE=134217728',             # INCREASED: 128MB max download
            
            # ðŸš€ MEGA CITY: Enhanced AutoThrottle for rate limit management
            '-s', 'AUTOTHROTTLE_ENABLED=True',
            '-s', f'AUTOTHROTTLE_START_DELAY={download_delay}',
            '-s', 'AUTOTHROTTLE_MAX_DELAY=30',              # INCREASED: Max 30s delay for mega cities
            '-s', f'AUTOTHROTTLE_TARGET_CONCURRENCY={max(1, concurrent_requests * 0.5)}',  # REDUCED: Lower target concurrency
            '-s', 'AUTOTHROTTLE_DEBUG=True',                # Enable detailed throttle logging
            
            # Spider arguments
            '-a', f'job_id={job_id}',                       # ðŸ”§ Pass job_id as integer to spider
            '-a', f'restaurant_limit={restaurant_count}',   # Pass configurable restaurant limit
            '-a', f'mega_city_mode=true',                   # ðŸ™ï¸ Enable mega city mode
        ]
        
        # 🚀 NEW: Add index-based processing arguments
        if index_based_mode and start_index is not None and end_index is not None:
            cmd_parts.extend([
                '-a', f'index_based_mode=true',
                '-a', f'start_index={start_index}',
                '-a', f'end_index={end_index}',
            ])
            logging.info(f"🔢 Added index-based arguments: {start_index}-{end_index}")
        else:
            # 🎯 BATCH PROCESSING: Set restaurant limit per run
            cmd_parts.extend([
                '-a', f'restaurants_per_batch={restaurants_per_batch}',  # Process restaurants per batch
            ])
            logging.info(f"🎯 Added batch processing: {restaurants_per_batch} restaurants per run")
        
        logging.info(f"Executing Scrapy spider: {config.spider_name}")
        logging.info(f"Command: {' '.join(cmd_parts)}")
        logging.info(f"Working directory: {config.scrapy_project_path}")
        
        # Execute spider with extensive debugging
        items_scraped = 0
        
        # Pre-execution debugging
        logging.info(f"Ã°Å¸â€Â Pre-execution working directory: {os.getcwd()}")
        logging.info(f"Ã°Å¸â€Â Target working directory: {config.scrapy_project_path}")
        logging.info(f"Ã°Å¸â€Â Does target directory exist? {os.path.exists(config.scrapy_project_path)}")
        
        # Check if scrapy command exists
        try:
            scrapy_check = subprocess.run(['which', 'scrapy'], capture_output=True, text=True, timeout=10)
            logging.info(f"Ã°Å¸â€Â Scrapy location: {scrapy_check.stdout.strip()}")
        except Exception as e:
            logging.warning(f"Ã¢Å¡ Ã¯Â¸Â Could not locate scrapy: {e}")
        
        # Check spider file exists
        spider_file = os.path.join(config.scrapy_project_path, 'spiders', 'rguru_de.py')
        logging.info(f"Ã°Å¸â€Â Spider file exists? {os.path.exists(spider_file)}")
        
        # Check cities file from spider's perspective
        cities_file_rel = os.path.join(config.scrapy_project_path, '..', 'city_files', 'cities_normal_de.txt')
        cities_file_abs = os.path.abspath(cities_file_rel)
        logging.info(f"Ã°Å¸â€Â Cities file path: {cities_file_abs}")
        logging.info(f"Ã°Å¸â€Â Cities file exists? {os.path.exists(cities_file_abs)}")
        
        try:
            logging.info("Ã°Å¸Å¡â‚¬ Starting subprocess execution...")
            # ðŸ™ï¸ MEGA CITY: Dynamic subprocess timeout based on restaurant count
            subprocess_timeout_minutes = (timeout_seconds // 60) + 60  # Spider timeout + 1 hour buffer
            
            logging.info(f"ðŸ• Subprocess timeout: {subprocess_timeout_minutes} minutes ({subprocess_timeout_minutes/60:.1f} hours)")
            
            # ðŸš€ LIVE LOGS: Enable real-time log streaming during spider execution
            logging.info("ðŸš€ Starting spider with live log streaming...")
            
            # Use Popen for real-time output streaming
            process = subprocess.Popen(
                cmd_parts,
                cwd=config.scrapy_project_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Merge stderr into stdout
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )
            
            # Stream output in real-time
            stdout_lines = []
            stderr_lines = []
            
            try:
                # Read output line by line in real-time
                for line in iter(process.stdout.readline, ''):
                    if line:
                        line = line.strip()
                        stdout_lines.append(line)
                        
                        # ðŸš€ COMPREHENSIVE LIVE LOGGING: Show all important spider activity
                        
                        # Log all spider activity (filtered for relevance)
                        if any(keyword in line.lower() for keyword in [
                            'spider opened', 'spider closed', 'restaurant', 'checkpoint', 
                            'memory', 'mega city', 'processing', 'error', 'warning',
                            'munich', 'selected', 'marking', 'starting', 'completed',
                            'found', 'extracted', 'saved', 'skipped', 'filtered'
                        ]):
                            logging.info(f"ðŸ•·ï¸ SPIDER: {line}")
                        
                        # Log critical events immediately
                        if any(keyword in line.lower() for keyword in [
                            'error', 'failed', 'exception', 'traceback', 'timeout', 'could not'
                        ]):
                            logging.error(f"ðŸš¨ SPIDER ERROR: {line}")
                            
                        # Log progress updates with more detail
                        if any(keyword in line.lower() for keyword in [
                            'restaurants processed', 'checkpoint saved', 'page', 'restaurant',
                            'total restaurants', 'establishments', 'urls found', 'processing restaurant'
                        ]):
                            logging.info(f"ðŸ“Š SPIDER PROGRESS: {line}")
                            
                        # Log memory and performance info
                        if any(keyword in line.lower() for keyword in [
                            'memory', 'peak memory', 'gc', 'garbage', 'optimization'
                        ]):
                            logging.info(f"ðŸ§  SPIDER MEMORY: {line}")
                            
                        # Log database operations
                        if any(keyword in line.lower() for keyword in [
                            'database', 'connection', 'query', 'saved', 'updated', 'checkpoint'
                        ]):
                            logging.info(f"ðŸ’¾ SPIDER DB: {line}")
                            
                        # Log API and network activity
                        if any(keyword in line.lower() for keyword in [
                            'request', 'response', 'http', 'proxy', 'scrapeops', 'api'
                        ]):
                            logging.info(f"ðŸŒ SPIDER API: {line}")
                
                # Wait for process to complete
                return_code = process.wait(timeout=subprocess_timeout_minutes * 60)
                
                # Create result object for compatibility
                result = type('Result', (), {
                    'returncode': return_code,
                    'stdout': '\n'.join(stdout_lines),
                    'stderr': '\n'.join(stderr_lines)
                })()
                
            except subprocess.TimeoutExpired:
                logging.error(f"â° Spider timeout after {subprocess_timeout_minutes} minutes")
                process.kill()
                result = type('Result', (), {
                    'returncode': -1,
                    'stdout': '\n'.join(stdout_lines),
                    'stderr': 'Process killed due to timeout'
                })()
            except Exception as e:
                logging.error(f"ðŸš¨ Spider execution error: {e}")
                process.kill()
                result = type('Result', (), {
                    'returncode': -1,
                    'stdout': '\n'.join(stdout_lines),
                    'stderr': str(e)
                })()
            logging.info(f" Subprocess completed with return code: {result.returncode}")
            logging.info(f" Subprocess stdout length: {len(result.stdout) if result.stdout else 0}")
            logging.info(f" Subprocess stderr length: {len(result.stderr) if result.stderr else 0}")
            
            # ðŸš€ LIVE LOGS SUMMARY: Show what happened during execution
            logging.info("ðŸš€ LIVE LOGS SUMMARY:")
            logging.info(f"   ðŸ“Š Total log lines processed: {len(stdout_lines)}")
            logging.info(f"   â±ï¸ Execution time: {subprocess_timeout_minutes} minutes max")
            logging.info(f"   ðŸ”„ Return code: {result.returncode}")
            
            # Count different types of events
            error_count = sum(1 for line in stdout_lines if any(keyword in line.lower() for keyword in ['error', 'failed', 'exception']))
            progress_count = sum(1 for line in stdout_lines if any(keyword in line.lower() for keyword in ['restaurant', 'checkpoint', 'page']))
            memory_count = sum(1 for line in stdout_lines if any(keyword in line.lower() for keyword in ['memory', 'gc', 'optimization']))
            
            logging.info(f"   ðŸš¨ Errors/Warnings: {error_count}")
            logging.info(f"   ðŸ“Š Progress events: {progress_count}")
            logging.info(f"   ðŸ§  Memory events: {memory_count}")
            
            if error_count > 0:
                logging.warning(f"âš ï¸ Found {error_count} errors/warnings during execution")
            if progress_count == 0:
                logging.warning("âš ï¸ No progress events detected - spider may not be processing restaurants")
            
            # Log ALL stdout/stderr for debugging - this is critical
            if result.stdout:
                logging.info(f"Ã°Å¸â€Â FULL STDOUT: {result.stdout}")
            else:
                logging.error("Ã°Å¸Å¡Â¨ NO STDOUT - This should never happen with Scrapy!")
                
            if result.stderr:
                logging.error(f"Ã°Å¸â€Â FULL STDERR: {result.stderr}")
            else:
                logging.info("Ã¢â€žÂ¹Ã¯Â¸Â No stderr output")
            
            # Extract from stdout since that's where our live logging captures everything
            log_content = result.stdout or ""
            execution_status = 'completed' if result.returncode == 0 else 'failed'
            items_scraped = extract_items_count(log_content)
            
            # Additional stderr analysis (where Scrapy actually logs)
            if log_content:
                if "cities_normal_de.txt" in log_content:
                    logging.info("Ã¢Å“â€¦ Cities file mentioned in logs")
                if "Number of cities found" in log_content:
                    logging.info("Ã¢Å“â€¦ Cities counting logic executed")
                if "item_scraped_count" in log_content:
                    logging.info("Ã¢Å“â€¦ Items were scraped (found in logs)")
                if "spider opened" in log_content.lower():
                    logging.info("Ã¢Å“â€¦ Spider successfully opened")
                if "spider closed" in log_content.lower():
                    logging.info("Ã¢Å“â€¦ Spider successfully closed")
                if "Processing restaurant" in log_content:
                    logging.info("Ã¢Å“â€¦ Restaurants found and processed")
            else:
                logging.error("Ã°Å¸Å¡Â¨ No stderr output from Scrapy")
            
            # Enhanced debugging for item extraction
            logging.info(f"Ã°Å¸â€Â Extracted items count: {items_scraped}")
            if items_scraped == 0:
                logging.warning("Ã¢Å¡ Ã¯Â¸Â Zero items extracted - checking stderr output")
                if result.stderr:
                    logging.error(f"Ã¢ÂÅ’ Scrapy stderr: {result.stderr}")
                if result.stdout:
                    logging.info(f"Ã°Å¸â€œÂ¤ Scrapy stdout: {result.stdout}")
            
            # Additional debugging: Check if logs directory exists after execution
            if os.path.exists(logs_dir):
                try:
                    log_files = os.listdir(logs_dir)
                    logging.info(f"Ã°Å¸â€œÂ Files in logs directory: {log_files}")
                except Exception as e:
                    logging.error(f"Ã¢ÂÅ’ Error listing logs directory: {e}")
            else:
                logging.error(f"Ã¢ÂÅ’ Logs directory disappeared: {logs_dir}")
                
            # Check current working directory
            cwd = os.getcwd()
            logging.info(f"Ã°Å¸â€œÂ Current working directory after spider: {cwd}")
            
            # Try to find any log files that might have been created elsewhere
            import glob
            possible_logs = glob.glob("**/*rguru*.log", recursive=True)
            if possible_logs:
                logging.info(f"Ã°Å¸â€Â Found potential log files elsewhere: {possible_logs}")
            
            # ðŸ”§ Save execution results to database with job_id
            await db_manager.save_log(job_id, config.spider_name, log_content, execution_status, items_scraped)
            
            if execution_status == 'completed':
                logging.info(f"Ã¢Å“â€¦ Spider execution completed successfully")
                logging.info(f"Ã°Å¸â€œÅ  Items scraped: {items_scraped}")
                stats['total_items'] = items_scraped
            else:
                logging.error(f"Ã¢ÂÅ’ Spider execution failed with return code: {result.returncode}")
                logging.error(f"Error output: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logging.error("Ã¢ÂÅ’ Spider execution timed out after 50 minutes")
            execution_status = 'timeout'
        except Exception as e:
            logging.error(f"Ã¢ÂÅ’ Error executing spider: {str(e)}")
            execution_status = 'error'
            
        except Exception as e:
            logging.error(f"Ã¢Å’ Error in subprocess execution: {str(e)}")
            execution_status = 'error'
            
    except Exception as e:
        logging.error(f"Fatal error in main execution: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        raise
        
    finally:
        if db_manager:
            await db_manager.close()
        
        stats['end_time'] = datetime.now()
        stats['total_runtime'] = stats['end_time'] - stats['start_time']
        
        logging.info("\nðŸ”§ Final Statistics:")
        logging.info(f"Job ID: {job_id}")
        logging.info(f"Total runtime: {stats['total_runtime']}")
        logging.info(f"Total items collected: {stats['total_items']}")
        
        # ðŸ”§ Extract city name from scraper logs (same run)
        city_name = None
        # Ensure log_content is defined for exception handling
        if 'log_content' not in locals():
            log_content = ""
        if log_content:
            # Debug: Show first 500 chars of log content to see what we're working with
            logging.info(f"ðŸ”§ Log content preview: {log_content[:500]}...")
            
            import re
            # Look for city name patterns in the logs from this specific run
            patterns = [
                r'Processing specified city: ([A-Za-z\s\-]+?)(?:\s*\(|,|\n|$)',
                r'Starting single-city processing for: ([A-Za-z\s\-]+?)(?:\s*\(|,|\n|$)',
                r'Selected next city to process: ([A-Za-z\s\-]+?)(?:\s*\(|,|\n|$)',
                r'Processing specified city.*?([A-Za-z\s\-]{3,}?)(?:\s*\(|,|\n|$)',
                r'Starting single-city processing.*?([A-Za-z\s\-]{3,}?)(?:\s*\(|,|\n|$)',
                r'Selected next city.*?([A-Za-z\s\-]{3,}?)(?:\s*\(|,|\n|$)'
            ]
            
            for pattern in patterns:
                city_match = re.search(pattern, log_content, re.IGNORECASE)
                if city_match:
                    city_name = city_match.group(1).strip()
                    # Clean up the city name
                    city_name = re.sub(r'[^\w\s\-]', '', city_name).strip()
                    
                    # Validate city name - reject error messages and invalid patterns
                    if (city_name and 
                        len(city_name) > 2 and 
                        len(city_name) < 50 and  # Reasonable city name length
                        not any(error_word in city_name.lower() for error_word in [
                            'error', 'structure', 'query', 'function', 'result', 'type', 
                            'detail', 'returned', 'expected', 'column', 'context', 'sql'
                        ]) and
                        re.match(r'^[A-Za-z\s\-]+$', city_name)):  # Only letters, spaces, hyphens
                        logging.info(f"ðŸ”§ Extracted city name from scraper logs: '{city_name}' using pattern: {pattern}")
                        break
                    else:
                        logging.warning(f"ðŸ”§ Rejected invalid city name: '{city_name}'")
            
            if not city_name:
                logging.warning("ðŸ”§ Could not extract city name from scraper logs")
                # Show all lines that might contain city info
                lines = log_content.split('\n')
                city_lines = [line for line in lines if any(word in line.lower() for word in ['city', 'processing', 'selected'])]
                if city_lines:
                    logging.info(f"ðŸ”§ Lines that might contain city info: {city_lines[:5]}")
        else:
            logging.warning("ðŸ”§ No log content available to extract city name")
        
        # ðŸ”§ Return job_id and execution details for DAG usage
        return {
            'job_id': job_id,
            'total_items': stats['total_items'],
            'runtime': str(stats['total_runtime']),
            'status': execution_status,
            'execution_start': stats['start_time'].isoformat(),
            'execution_end': stats['end_time'].isoformat(),
            'city_name': city_name  # ðŸ”§ Add city name for conversion task
        }


def run_restaurant_guru_scraper(creds: dict, mode: str = "normal", item_limit: int = None, scrapy_project_path: str = None,
                                start_index: int = None, end_index: int = None, index_based_mode: bool = False,
                                restaurants_per_batch: int = 50000):
    """
    Helper function to run the Restaurant Guru scraper from Airflow.
    
    Args:
        creds (dict): Database credentials
        mode (str): Execution mode (normal, test)
        item_limit (int): Optional limit on items total (for testing)
        scrapy_project_path (str): Path to the Scrapy project directory
        start_index (int): Starting restaurant index for index-based processing
        end_index (int): Ending restaurant index for index-based processing
        index_based_mode (bool): Whether to use index-based processing
        restaurants_per_batch (int): Number of restaurants to process per run (default: 50000 for mega cities)
    
    Returns:
        dict: Execution result with job_id
    """
    try:
        execution_mode = ScrapyExecutionMode(mode)
        result = asyncio.run(main_restaurant_guru(creds, execution_mode, item_limit, scrapy_project_path,
                                                 start_index=start_index, end_index=end_index, index_based_mode=index_based_mode,
                                                 restaurants_per_batch=restaurants_per_batch))
        logging.info(f"§ Scraper execution completed with result: {result}")
        return result  # ðŸ"§ Return the result instead of None
    except Exception as e:
        logging.critical(f"Fatal error in run_restaurant_guru_scraper: {str(e)}")
        logging.critical(f"Traceback: {traceback.format_exc()}")
        raise


def run_get_max_import_id(creds: dict) -> Optional[int]:
    """
    Helper function to get the maximum import ID from the database.
    
    Args:
        creds (dict): Database credentials
    
    Returns:
        Optional[int]: Maximum import ID or None if error
    """
    async def get_max_import_id_async():
        db_manager = None
        try:
            config = RestaurantGuruConfig(use_database=True)
            db_manager = RestaurantGuruDatabaseManager(config, creds)
            await db_manager.connect()
            max_import_id = await db_manager.get_max_import_id()
            return max_import_id
        except Exception as e:
            logging.critical(f"Fatal error in get_max_import_id_async: {str(e)}")
            logging.critical(f"Traceback: {traceback.format_exc()}")
            raise
        finally:
            if db_manager:
                await db_manager.close()
    
    try:
        return asyncio.run(get_max_import_id_async())
    except Exception as e:
        logging.critical(f"Fatal error in run_get_max_import_id: {str(e)}")
        logging.critical(f"Traceback: {traceback.format_exc()}")
        raise