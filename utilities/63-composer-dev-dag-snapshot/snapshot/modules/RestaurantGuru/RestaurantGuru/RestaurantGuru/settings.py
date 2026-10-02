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

LOG_ENABLED = True
LOG_LEVEL = "DEBUG"  # comment out before deployment
# LOG_LEVEL = "INFO"
LOG_FILE_APPEND = False

# LOG_FILE = 'rguru.log'  # comment out before deployment
LOG_STDOUT = True

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "RestaurantGuru (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 1

# Timeout settings for slow proxy responses
DOWNLOAD_TIMEOUT = 60  # Increased from default 30s to handle slow ScrapeOps responses
#CONCURRENT_REQUESTS_PER_IP = 16

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
#SPIDER_MIDDLEWARES = {
#    "RestaurantGuru.middlewares.RestaurantguruSpiderMiddleware": 543,
#}
# ScrapeOps API Configuration
SCRAPEOPS_PROXY_ENABLED = True
SCRAPEOPS_API_KEY = 'REDACTED'
SCRAPEOPS_CONNECTION_TIMEOUT = 30  # Connection test timeout in seconds
SCRAPEOPS_MAX_RETRIES = 3  # Maximum retry attempts for connection test

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'RestaurantGuru.middlewares.ScrapeOpsAPIMiddleware': 610
    #'RestaurantGuru.middlewares.ScrapeOpsProxyMiddleware': 610  # HTTP Proxy method - had 403 issues
    #'scrapy_crawlbase.CrawlbaseMiddleware': 610  # Disabled
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# Note: PostgresPipeline disabled - database operations handled by wrapper script
ITEM_PIPELINES = {
    "RestaurantGuru.pipelines.TimestampPipeline": 100,
    'RestaurantGuru.pipelines.PostgresPipeline': 300,  # Re-enabled with working host
}

# Postgres database with target table for JSON
# DEV - Using same host as stored procedure (Airflow environment connectivity)
POSTGRES_HOST             = 'REDACTED'
POSTGRES_USER             = 'REDACTED' 
POSTGRES_PASSWORD         = 'REDACTED'
POSTGRES_DATABASE         = 'postgres'
POSTGRES_PORT             = 5432
POSTGRES_TABLE_NAME       = 'smartdata_analyticdb.restaurant_guru_raw_germany'
POSTGRES_SPIDERFIELD_NAME = 'spider_name'
POSTGRES_JSONFIELD_NAME   = 'raw_json'

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 1
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
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
