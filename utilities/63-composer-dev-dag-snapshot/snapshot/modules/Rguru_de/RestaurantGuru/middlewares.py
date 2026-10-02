# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy import Request
import requests
import logging

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter

logger = logging.getLogger(__name__)


class RestaurantguruSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class RestaurantguruDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ScrapeOpsProxyMiddleware:
    """
    ScrapeOps Proxy Middleware for Scrapy
    Implements HTTP proxy with authentication for ScrapeOps residential proxy service
    WITH FALLBACK: Automatically disables on authentication failures
    """
    def __init__(self, proxy_enabled=True, api_key='REDACTED'):
        self.proxy_enabled = proxy_enabled
        self.api_key = api_key
        self.proxies = {
            "http": f"http://scrapeops:{api_key}@residential-proxy.scrapeops.io:8181",
            "https": f"http://scrapeops:{api_key}@residential-proxy.scrapeops.io:8181"
        }
        self.proxy_working = True
        self.failure_count = 0
        self.max_failures = 3
        
        # Test the proxy connection on initialization
        if self.proxy_enabled:
            self._test_proxy_connection()
    
    @classmethod
    def from_crawler(cls, crawler):
        """Initialize middleware from crawler settings"""
        proxy_enabled = crawler.settings.getbool('SCRAPEOPS_RESIDENTIAL_PROXY_ENABLED', False)
        api_key = crawler.settings.get('SCRAPEOPS_API_KEY', '0a0bd6f5-e1c0-49c3-b8f5-1ec732a2e3b6')
        middleware = cls(proxy_enabled=proxy_enabled, api_key=api_key)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def _test_proxy_connection(self):
        """Test proxy connection during initialization"""
        try:
            logger.info("Testing ScrapeOps residential proxy connection...")
            response = requests.get(
                'https://quotes.toscrape.com/', 
                proxies=self.proxies, 
                verify=False, 
                timeout=10
            )
            if response.status_code == 200:
                logger.info("✅ ScrapeOps residential proxy connection successful!")
                logger.info(f"Response length: {len(response.text)} characters")
                self.proxy_working = True
            elif response.status_code == 401:
                logger.error("❌ ScrapeOps residential proxy: 401 Unauthorized - API key may not have residential proxy access")
                logger.warning("⚠️ Disabling residential proxy middleware - will use API proxy instead")
                self.proxy_working = False
                self.proxy_enabled = False
            else:
                logger.warning(f"⚠️ ScrapeOps proxy test returned status: {response.status_code}")
        except Exception as e:
            error_str = str(e)
            if '401' in error_str or 'Unauthorized' in error_str:
                logger.error(f"❌ ScrapeOps residential proxy authentication failed: {error_str}")
                logger.warning("⚠️ Disabling residential proxy middleware - API key lacks residential proxy permissions")
                self.proxy_working = False
                self.proxy_enabled = False
            else:
                logger.error(f"❌ ScrapeOps residential proxy connection failed: {error_str}")
    
    def process_request(self, request, spider):
        """Process each request through ScrapeOps proxy"""
        # Skip if proxy is disabled or not working
        if not self.proxy_enabled or not self.proxy_working:
            return None
        
        # Don't proxy requests that are already going through ScrapeOps API
        if 'proxy.scrapeops.io' in request.url:
            return None
            
        # Set proxy for the request
        proxy_url = self.proxies.get(request.url.split('://')[0], self.proxies['http'])
        request.meta['proxy'] = proxy_url
        
        # Disable SSL verification for proxy requests
        request.meta['dont_cache'] = True
        request.meta['using_residential_proxy'] = True
        
        logger.debug(f"🌐 Routing request through ScrapeOps residential proxy: {request.url}")
        return None
    
    def process_response(self, request, response, spider):
        """Process response from ScrapeOps proxy"""
        if request.meta.get('using_residential_proxy', False):
            if response.status == 200:
                logger.debug(f"✅ ScrapeOps residential proxy response successful: {request.url}")
                # Reset failure count on success
                self.failure_count = 0
            elif response.status == 401:
                logger.error(f"⚠️ ScrapeOps residential proxy 401 Unauthorized: {request.url}")
                self._handle_auth_failure()
            else:
                logger.warning(f"⚠️ ScrapeOps residential proxy response status {response.status}: {request.url}")
        
        return response
    
    def process_exception(self, request, exception, spider):
        """Handle proxy-related exceptions with fallback"""
        exception_str = str(exception)
        
        if request.meta.get('using_residential_proxy', False):
            if '401' in exception_str or 'Unauthorized' in exception_str:
                logger.error(f"❌ ScrapeOps residential proxy authentication error for {request.url}: {exception_str}")
                self._handle_auth_failure()
            else:
                logger.error(f"❌ ScrapeOps residential proxy exception for {request.url}: {exception_str}")
                self.failure_count += 1
                
                if self.failure_count >= self.max_failures:
                    logger.warning(f"⚠️ Too many residential proxy failures ({self.failure_count}). Disabling residential proxy.")
                    self.proxy_working = False
                    self.proxy_enabled = False
        
        return None
    
    def _handle_auth_failure(self):
        """Handle authentication failures by disabling the proxy"""
        logger.error("❌ Authentication failure detected - disabling residential proxy")
        logger.info("ℹ️ Requests will now use API proxy or direct connection")
        self.proxy_working = False
        self.proxy_enabled = False
    
    def spider_opened(self, spider):
        """Called when spider opens"""
        if self.proxy_enabled and self.proxy_working:
            spider.logger.info("🌐 ScrapeOps Residential Proxy Middleware activated for spider: %s" % spider.name)
        else:
            spider.logger.info("⚠️ ScrapeOps Residential Proxy Middleware disabled for spider: %s" % spider.name)


class ScrapeOpsAPIMiddleware:
    """
    ScrapeOps API Middleware for Scrapy
    Uses ScrapeOps API-based proxy service
    WITH FALLBACK: Automatically disables on authentication failures and falls back to direct requests
    """
    
    def __init__(self, api_key='REDACTED', proxy_enabled=True, 
                 connection_timeout=30, max_retries=3):
        self.api_key = api_key
        self.proxy_enabled = proxy_enabled
        self.api_url = 'https://proxy.scrapeops.io/v1/'
        self.connection_timeout = connection_timeout
        self.max_retries = max_retries
        self.api_working = True
        self.failure_count = 0
        self.max_failures = 5
        
        # Test the API connection on initialization
        if self.proxy_enabled:
            self._test_api_connection()
    
    def _test_api_connection(self):
        """Test ScrapeOps API connection during initialization with retry logic"""
        if not self.proxy_enabled:
            logger.info("ScrapeOps API proxy disabled, skipping connection test")
            return
            
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Testing ScrapeOps API connection (attempt {attempt + 1}/{self.max_retries})...")
                test_response = requests.get(
                    self.api_url,
                    params={
                        'api_key': self.api_key,
                        'url': 'https://quotes.toscrape.com/',
                    },
                    timeout=self.connection_timeout
                )
                if test_response.status_code == 200:
                    logger.info("✅ ScrapeOps API connection test successful")
                    logger.info(f"Response length: {len(test_response.content)} bytes")
                    self.api_working = True
                    return  # Success, exit retry loop
                elif test_response.status_code == 401:
                    logger.error("❌ ScrapeOps API: 401 Unauthorized - Invalid API key")
                    logger.warning("⚠️ Disabling ScrapeOps API - will try direct requests")
                    self.api_working = False
                    self.proxy_enabled = False
                    return
                else:
                    logger.warning(f"⚠️ ScrapeOps API test returned status: {test_response.status_code}")
                    if attempt < self.max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logger.info(f"Retrying in {wait_time} seconds...")
                        import time
                        time.sleep(wait_time)
            except Exception as e:
                error_str = str(e)
                if '401' in error_str or 'Unauthorized' in error_str:
                    logger.error(f"❌ ScrapeOps API authentication failed: {error_str}")
                    logger.warning("⚠️ Disabling ScrapeOps API - API key authentication failed")
                    self.api_working = False
                    self.proxy_enabled = False
                    return
                
                logger.error(f"❌ ScrapeOps API connection test failed (attempt {attempt + 1}): {error_str}")
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    import time
                    time.sleep(wait_time)
                else:
                    logger.warning("⚠️ All ScrapeOps API connection attempts failed. Disabling API proxy - will use direct requests.")
                    self.api_working = False
                    self.proxy_enabled = False
    
    @classmethod
    def from_crawler(cls, crawler):
        """Create middleware instance from crawler settings"""
        api_key = crawler.settings.get('SCRAPEOPS_API_KEY', '0a0bd6f5-e1c0-49c3-b8f5-1ec732a2e3b6')
        proxy_enabled = crawler.settings.getbool('SCRAPEOPS_API_ENABLED', True)
        connection_timeout = crawler.settings.getint('SCRAPEOPS_CONNECTION_TIMEOUT', 30)
        max_retries = crawler.settings.getint('SCRAPEOPS_MAX_RETRIES', 3)
        return cls(api_key=api_key, proxy_enabled=proxy_enabled, 
                  connection_timeout=connection_timeout, max_retries=max_retries)
    
    def process_request(self, request, spider):
        """Process each request through ScrapeOps API or fallback to direct"""
        # Skip if API is disabled or not working
        if not self.proxy_enabled or not self.api_working:
            logger.debug(f"📡 Making direct request (API disabled): {request.url}")
            return None
        
        # Skip processing if the request is already going to ScrapeOps API
        if 'proxy.scrapeops.io' in request.url:
            return None
        
        # Build the ScrapeOps API URL with parameters
        import urllib.parse
        api_params = {
            'api_key': self.api_key,
            'url': request.url,
        }
        
        # 🎭 Add session rotation and country targeting from request.meta (for bypass)
        if 'scrapeops_session' in request.meta:
            api_params['session'] = request.meta['scrapeops_session']
            logger.info(f"🎭 Using session: {request.meta['scrapeops_session']}")
        
        if 'scrapeops_country' in request.meta:
            api_params['country'] = request.meta['scrapeops_country']
            logger.info(f"🇩🇪 Using country: {request.meta['scrapeops_country']}")
        
        # Add residential proxy if specified in meta
        if request.meta.get('scrapeops_residential', False):
            api_params['residential'] = 'true'
            logger.info(f"🏠 Using residential proxy")
        
        # Create the full API URL with query parameters
        query_string = urllib.parse.urlencode(api_params)
        api_url_with_params = f"{self.api_url}?{query_string}"
        
        # Create new request with ScrapeOps API URL
        new_request = request.replace(
            url=api_url_with_params,
            method='GET'
        )
        
        # Store the original URL for logging and fallback
        new_request.meta['original_url'] = request.url
        new_request.meta['using_scrapeops_api'] = True
        
        logger.debug(f"🌐 Routing request through ScrapeOps API: {request.url}")
        return new_request
    
    def process_response(self, request, response, spider):
        """Process response from ScrapeOps API"""
        if request.meta.get('using_scrapeops_api', False):
            if response.status == 200:
                logger.debug(f"✅ ScrapeOps API response successful for: {request.meta.get('original_url', 'unknown')}")
                # Reset failure count on success
                self.failure_count = 0
            elif response.status == 401:
                logger.error(f"⚠️ ScrapeOps API 401 Unauthorized for: {request.meta.get('original_url', 'unknown')}")
                self._handle_auth_failure()
            else:
                logger.warning(f"⚠️ ScrapeOps API response status {response.status} for: {request.meta.get('original_url', 'unknown')}")
        
        return response
    
    def process_exception(self, request, exception, spider):
        """Handle API-related exceptions with fallback to direct requests"""
        exception_str = str(exception)
        original_url = request.meta.get('original_url', 'unknown')
        
        if request.meta.get('using_scrapeops_api', False):
            if '401' in exception_str or 'Unauthorized' in exception_str:
                logger.error(f"❌ ScrapeOps API authentication error for {original_url}: {exception_str}")
                self._handle_auth_failure()
                
                # Return a new request without proxy for immediate retry
                logger.info(f"🔄 Retrying with direct request: {original_url}")
                return Request(
                    url=original_url,
                    callback=request.callback,
                    errback=request.errback,
                    meta=request.meta.copy(),
                    headers=request.headers,
                    dont_filter=True
                )
            else:
                logger.error(f"❌ ScrapeOps API exception for {original_url}: {exception_str}")
                self.failure_count += 1
                
                if self.failure_count >= self.max_failures:
                    logger.warning(f"⚠️ Too many API failures ({self.failure_count}). Disabling ScrapeOps API.")
                    self.api_working = False
                    self.proxy_enabled = False
                    
                    # Return a new request without proxy for immediate retry
                    logger.info(f"🔄 Retrying with direct request: {original_url}")
                    return Request(
                        url=original_url,
                        callback=request.callback,
                        errback=request.errback,
                        meta=request.meta.copy(),
                        headers=request.headers,
                        dont_filter=True
                    )
        
        return None
    
    def _handle_auth_failure(self):
        """Handle authentication failures by disabling the API"""
        logger.error("❌ Authentication failure detected - disabling ScrapeOps API")
        logger.info("ℹ️ Future requests will use direct connection")
        self.api_working = False
        self.proxy_enabled = False
    
    def spider_opened(self, spider):
        """Called when spider opens"""
        if self.proxy_enabled and self.api_working:
            spider.logger.info("🌐 ScrapeOps API Middleware activated for spider: %s" % spider.name)
        else:
            spider.logger.info("⚠️ ScrapeOps API Middleware disabled for spider: %s" % spider.name)
