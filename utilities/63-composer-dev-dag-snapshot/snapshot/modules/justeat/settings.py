# Scrapy settings for RestaurantGuru project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "RestaurantGuru"

SPIDER_MODULES = ["RestaurantGuru.spiders"]
NEWSPIDER_MODULE = "RestaurantGuru.spiders"

# Enhanced logging configuration like JustEat for better visibility
LOG_ENABLED = True
LOG_LEVEL = "INFO"  # Changed from DEBUG to INFO for cleaner logs
LOG_FILE_APPEND = False

# Dynamic log file name with timestamp for better tracking like JustEat
import os
from datetime import datetime
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f'rguru_{timestamp}.log'

# Enable stdout logging for Airflow visibility
LOG_STDOUT = True
LOG_STDERR = True

# Additional logging settings for better debugging like JustEat
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# Enable specific loggers for comprehensive debugging but reduce verbosity
LOG_LEVELS = {
    'scrapy': 'INFO',
    'scrapy.core.engine': 'INFO',
    'scrapy.spider': 'INFO',
    'scrapy.utils.log': 'ERROR',
    'RestaurantGuru': 'INFO',
    'psycopg2': 'WARNING',
    'urllib3': 'WARNING',
    'requests': 'WARNING',
}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "RestaurantGuru (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# Following JustEat pattern - very conservative for anti-detection
CONCURRENT_REQUESTS = 1  # Reduced from 32 to 1 (like JustEat)

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 15  # Increased to 15 seconds (JustEat uses 20)
RANDOMIZE_DOWNLOAD_DELAY = True  # Add randomization like JustEat
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 1
#CONCURRENT_REQUESTS_PER_IP = 16

# Add timeout settings like JustEat
DOWNLOAD_TIMEOUT = 120  # 2 minutes timeout like JustEat

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    "RestaurantGuru.middlewares.AntiDetectionSpiderMiddleware": 543,
}
# Activate the middleware
CRAWLBASE_ENABLED = False

# The ProxyCrawl API token you wish to use, either normal of javascript token
CRAWLBASE_TOKEN = 'REDACTED'

# ScrapeOps Proxy Settings
SCRAPEOPS_API_KEY = 'REDACTED'
SCRAPEOPS_PROXY_ENABLED = False  # TEMPORARILY DISABLED - proxy getting 503 errors
SCRAPEOPS_CONNECTION_TIMEOUT = 6000  # Increased from 30 to 60 seconds
SCRAPEOPS_MAX_RETRIES = 5  # Increased from 3 to 5 retries

# ScrapeOps Headers Settings (to avoid CAPTCHA)
SCRAPEOPS_HEADERS_ENABLED = True  # Keep headers for anti-detection

# Anti-Detection Spider Middleware Settings - More conservative based on JustEat
ANTIDETECTION_MIN_DELAY = 10.0  # Minimum delay between requests (seconds) - increased
ANTIDETECTION_MAX_DELAY = 30.0  # Maximum delay between requests (seconds) - increased
ANTIDETECTION_RETRY_503 = True  # Whether to retry 503 errors
ANTIDETECTION_MAX_503_RETRIES = 2  # Max retries for 503 errors before aggressive slowdown - reduced

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'RestaurantGuru.middlewares.SessionMaintenanceMiddleware': 600,  # Session maintenance first
    'RestaurantGuru.middlewares.ScrapeOpsHeadersMiddleware': 610,  # Headers for anti-detection
    # 'RestaurantGuru.middlewares.ScrapeOpsProxyMiddleware': 620,    # COMPLETELY DISABLED - getting 503 errors
    # 'RestaurantGuru.middlewares.ScrapeOpsAPIMiddleware': 630,   # Disabled - API having issues with Restaurant Guru
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "RestaurantGuru.pipelines.TimestampPipeline": 100,
    'RestaurantGuru.pipelines.PostgresPipeline': 300,
}

# Postgres database with target table for JSON
POSTGRES_HOST             = 'REDACTED'
POSTGRES_USER             = 'REDACTED' 
POSTGRES_PASSWORD         = 'REDACTED'
POSTGRES_DATABASE         = 'postgres'
POSTGRES_PORT             = 5432
POSTGRES_TABLE_NAME       = 'smartdatastagdb.jsonimport'
POSTGRES_SPIDERFIELD_NAME = 'spider'
POSTGRES_JSONFIELD_NAME   = 'jsondata'

# Enable retry middleware like JustEat
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429, 403, 407]  # More error codes like JustEat

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay - more conservative like JustEat
AUTOTHROTTLE_START_DELAY = 5  # Increased from 1 to 5 seconds
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 120  # Increased from 60 to 120 seconds like JustEat
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 0.5  # More conservative (JustEat uses 1.0)
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = True

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# For testing: also save to local files
# FEEDS disabled - data goes directly to database
# FEEDS = {
#     'rguru_data.json': {
#         'format': 'json',
#         'encoding': 'utf8',
#         'store_empty': False,
#         'indent': 2,
#     },
#     'rguru_data.csv': {
#         'format': 'csv',
#         'encoding': 'utf8',
#         'store_empty': False,
#     },
# }
