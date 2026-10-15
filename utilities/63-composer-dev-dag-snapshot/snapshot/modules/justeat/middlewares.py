# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.http import Request
import requests
import logging
import random
import json
import time

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
    """
    def __init__(self, proxy_enabled=True):
        self.proxy_enabled = proxy_enabled
        self.proxies = {
            "http": "http://scrapeops:dataops@example.com:8181",
            "https": "http://scrapeops:dataops@example.com:8181"
        }
        
        # Test the proxy connection on initialization
        if self.proxy_enabled:
            self._test_proxy_connection()
    
    @classmethod
    def from_crawler(cls, crawler):
        """Initialize middleware from crawler settings"""
        proxy_enabled = crawler.settings.getbool('SCRAPEOPS_PROXY_ENABLED', True)
        middleware = cls(proxy_enabled=proxy_enabled)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def _test_proxy_connection(self):
        """Test proxy connection during initialization"""
        try:
            logger.info("Testing ScrapeOps proxy connection...")
            response = requests.get(
                'https://quotes.toscrape.com/', 
                proxies=self.proxies, 
                verify=False, 
                timeout=10
            )
            if response.status_code == 200:
                logger.info("✅ ScrapeOps proxy connection successful!")
                logger.info(f"Response length: {len(response.text)} characters")
            else:
                logger.warning(f"⚠️ ScrapeOps proxy test returned status: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ ScrapeOps proxy connection failed: {str(e)}")
    
    def process_request(self, request, spider):
        """Process each request through ScrapeOps proxy"""
        if not self.proxy_enabled:
            return None
            
        # Set proxy for the request
        proxy_url = self.proxies.get(request.url.split('://')[0], self.proxies['http'])
        request.meta['proxy'] = proxy_url
        
        # Disable SSL verification for proxy requests
        request.meta['dont_cache'] = True
        
        logger.debug(f"🌐 Routing request through ScrapeOps proxy: {request.url}")
        return None
    
    def process_response(self, request, response, spider):
        """Process response from ScrapeOps proxy"""
        if response.status == 200:
            logger.debug(f"✅ ScrapeOps proxy response successful: {request.url}")
        else:
            logger.warning(f"⚠️ ScrapeOps proxy response status {response.status}: {request.url}")
        
        return response
    
    def process_exception(self, request, exception, spider):
        """Handle proxy-related exceptions"""
        logger.error(f"❌ ScrapeOps proxy exception for {request.url}: {str(exception)}")
        return None
    
    def spider_opened(self, spider):
        """Called when spider opens"""
        if self.proxy_enabled:
            spider.logger.info("🌐 ScrapeOps Proxy Middleware activated for spider: %s" % spider.name)
        else:
            spider.logger.info("⚠️ ScrapeOps Proxy Middleware disabled for spider: %s" % spider.name)


class ScrapeOpsAPIMiddleware:
    """
    ScrapeOps API Middleware for Scrapy
    Uses ScrapeOps API-based proxy service
    """
    
    def __init__(self, api_key='REDACTED', proxy_enabled=True, 
                 connection_timeout=30, max_retries=3):
        self.api_key = api_key
        self.proxy_enabled = proxy_enabled
        self.api_url = 'https://proxy.scrapeops.io/v1/'
        self.connection_timeout = connection_timeout
        self.max_retries = max_retries
        
        # Test the API connection on initialization
        if self.proxy_enabled:
            self._test_api_connection()
    
    def _test_api_connection(self):
        """Test ScrapeOps API connection during initialization with retry logic"""
        if not self.proxy_enabled:
            logger.info("ScrapeOps proxy disabled, skipping connection test")
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
                    return  # Success, exit retry loop
                else:
                    logger.warning(f"⚠️ ScrapeOps API test returned status: {test_response.status_code}")
                    if attempt < self.max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logger.info(f"Retrying in {wait_time} seconds...")
                        import time
                        time.sleep(wait_time)
            except Exception as e:
                logger.error(f"❌ ScrapeOps API connection test failed (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    import time
                    time.sleep(wait_time)
                else:
                    logger.warning("⚠️ All ScrapeOps connection attempts failed. Spider will continue without proxy validation.")
    
    @classmethod
    def from_crawler(cls, crawler):
        """Create middleware instance from crawler settings"""
        api_key = crawler.settings.get('SCRAPEOPS_API_KEY', 'b1f91187-34d7-4d3d-a7f8-f43e943172d4')
        proxy_enabled = crawler.settings.getbool('SCRAPEOPS_PROXY_ENABLED', True)
        connection_timeout = crawler.settings.getint('SCRAPEOPS_CONNECTION_TIMEOUT', 30)
        max_retries = crawler.settings.getint('SCRAPEOPS_MAX_RETRIES', 3)
        return cls(api_key=api_key, proxy_enabled=proxy_enabled, 
                  connection_timeout=connection_timeout, max_retries=max_retries)
    
    def process_request(self, request, spider):
        """Process each request through ScrapeOps API"""
        if not self.proxy_enabled:
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
        
        # Create the full API URL with query parameters
        query_string = urllib.parse.urlencode(api_params)
        api_url_with_params = f"{self.api_url}?{query_string}"
        
        # Create new request with ScrapeOps API URL
        new_request = request.replace(
            url=api_url_with_params,
            method='GET'
        )
        
        # Store the original URL for logging
        new_request.meta['original_url'] = request.url
        
        logger.debug(f"🌐 Routing request through ScrapeOps API: {request.url}")
        return new_request
    
    def process_response(self, request, response, spider):
        """Process response from ScrapeOps API"""
        if hasattr(request.meta, 'scrapeops_params'):
            if response.status == 200:
                logger.debug(f"✅ ScrapeOps API response successful for: {request.meta.get('original_url', 'unknown')}")
            else:
                logger.warning(f"⚠️ ScrapeOps API response status {response.status} for: {request.meta.get('original_url', 'unknown')}")
        
        return response
    
    def process_exception(self, request, exception, spider):
        """Handle API-related exceptions"""
        original_url = request.meta.get('original_url', 'unknown')
        logger.error(f"❌ ScrapeOps API exception for {original_url}: {str(exception)}")
        return None
    
    def spider_opened(self, spider):
        """Called when spider opens"""
        if self.proxy_enabled:
            spider.logger.info("🌐 ScrapeOps API Middleware activated for spider: %s" % spider.name)
        else:
            spider.logger.info("⚠️ ScrapeOps API Middleware disabled for spider: %s" % spider.name)


class ScrapeOpsHeadersMiddleware:
    """
    ScrapeOps Headers Middleware for Scrapy
    Rotates browser headers to avoid CAPTCHA and bot detection
    """
    
    def __init__(self, api_key='REDACTED', headers_enabled=True):
        self.api_key = api_key
        self.headers_enabled = headers_enabled
        self.headers_api_url = 'http://headers.scrapeops.io/v1/browser-headers'
        self.user_agents_api_url = 'http://headers.scrapeops.io/v1/user-agents'
        self.browser_headers_list = []
        self.user_agents_list = []
        
        # Fetch headers on initialization
        if self.headers_enabled:
            self._fetch_headers()
    
    @classmethod
    def from_crawler(cls, crawler):
        """Create middleware instance from crawler settings"""
        api_key = crawler.settings.get('SCRAPEOPS_API_KEY', '0a0bd6f5-e1c0-49c3-b8f5-1ec732a2e3b6')
        headers_enabled = crawler.settings.getbool('SCRAPEOPS_HEADERS_ENABLED', True)
        return cls(api_key=api_key, headers_enabled=headers_enabled)
    
    def _fetch_headers(self):
        """Fetch browser headers and user agents from ScrapeOps API"""
        try:
            # Fetch browser headers
            headers_response = requests.get(f"{self.headers_api_url}?api_key={self.api_key}", timeout=10)
            if headers_response.status_code == 200:
                headers_data = headers_response.json()
                self.browser_headers_list = headers_data.get('result', [])
                logger.info(f"✅ Fetched {len(self.browser_headers_list)} browser header sets from ScrapeOps")
            else:
                logger.warning(f"⚠️ Failed to fetch browser headers: {headers_response.status_code}")
            
            # Fetch user agents as fallback
            ua_response = requests.get(f"{self.user_agents_api_url}?api_key={self.api_key}", timeout=10)
            if ua_response.status_code == 200:
                ua_data = ua_response.json()
                self.user_agents_list = ua_data.get('result', [])
                logger.info(f"✅ Fetched {len(self.user_agents_list)} user agents from ScrapeOps")
            else:
                logger.warning(f"⚠️ Failed to fetch user agents: {ua_response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Error fetching headers from ScrapeOps: {str(e)}")
            # Use fallback headers if API fails
            self._use_fallback_headers()
    
    def _use_fallback_headers(self):
        """Use fallback headers if ScrapeOps API is unavailable"""
        self.browser_headers_list = [
            {
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "sec-ch-ua": '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-site": "none",
                "sec-fetch-mode": "navigate",
                "sec-fetch-user": "?1",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "en-US,en;q=0.9"
            },
            {
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "en-US,en;q=0.5"
            },
            {
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "sec-ch-ua": '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
                "sec-fetch-site": "none",
                "sec-fetch-mode": "navigate",
                "sec-fetch-user": "?1",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "en-US,en;q=0.9"
            }
        ]
        logger.info(f"🔄 Using {len(self.browser_headers_list)} fallback browser headers")
    
    def _get_random_headers(self):
        """Get random browser headers from the list"""
        if self.browser_headers_list:
            return random.choice(self.browser_headers_list)
        elif self.user_agents_list:
            # Fallback to just user agent if browser headers not available
            return {"User-Agent": random.choice(self.user_agents_list)}
        else:
            # Final fallback
            return {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36"}
    
    def process_request(self, request, spider):
        """Add random browser headers to each request"""
        if not self.headers_enabled:
            return None
        
        # Get random headers
        random_headers = self._get_random_headers()
        
        # Apply headers to the request
        for header_name, header_value in random_headers.items():
            # Convert header names to proper case (e.g., 'user-agent' -> 'User-Agent')
            proper_header_name = '-'.join(word.capitalize() for word in header_name.split('-'))
            request.headers[proper_header_name] = header_value
        
        logger.debug(f"🎭 Applied random headers to request: {request.url}")
        logger.debug(f"🎭 User-Agent: {random_headers.get('user-agent', 'N/A')}")
        
        return None
    
    def spider_opened(self, spider):
        """Called when spider opens"""
        if self.headers_enabled:
            spider.logger.info("🎭 ScrapeOps Headers Middleware activated for spider: %s" % spider.name)
            spider.logger.info(f"🎭 Loaded {len(self.browser_headers_list)} browser header sets")
        else:
            spider.logger.info("ScrapeOps Headers Middleware disabled for spider: %s" % spider.name)


class SessionMaintenanceMiddleware:
    """
    Middleware to maintain session-like behavior and add proper referrer headers
    """
    
    def __init__(self):
        self.last_url = None
    
    def process_request(self, request, spider):
        """Add referrer header to maintain session-like behavior"""
        # Add referrer header if we have a previous URL
        if self.last_url and self.last_url != request.url:
            request.headers['Referer'] = self.last_url
            spider.logger.debug(f"🔗 Added referrer: {self.last_url}")
        
        # Update last URL for next request
        self.last_url = request.url
        
        return None
    
    def spider_opened(self, spider):
        """Called when spider opens"""
        spider.logger.info("🔗 Session Maintenance Middleware activated for spider: %s" % spider.name)


class AntiDetectionSpiderMiddleware:
    """
    Spider middleware for anti-detection measures based on official Scrapy documentation.
    Implements proper spider middleware methods to handle requests and responses.
    """
    
    def __init__(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        self.logger = logging.getLogger(__name__)
        
        # Anti-detection settings
        self.min_delay = self.settings.getfloat('ANTIDETECTION_MIN_DELAY', 2.0)
        self.max_delay = self.settings.getfloat('ANTIDETECTION_MAX_DELAY', 8.0)
        self.retry_503_enabled = self.settings.getbool('ANTIDETECTION_RETRY_503', True)
        self.max_503_retries = self.settings.getint('ANTIDETECTION_MAX_503_RETRIES', 3)
        
        # Track 503 errors per domain
        self.domain_503_count = {}
        self.last_request_time = {}
        
        self.logger.info(f"🛡️ AntiDetectionSpiderMiddleware initialized with delays {self.min_delay}-{self.max_delay}s")
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)
    
    def process_spider_input(self, response, spider):
        """
        Process responses coming into the spider.
        Handle 503 errors and implement anti-detection logic.
        """
        domain = response.url.split('/')[2] if '://' in response.url else 'unknown'
        
        # Track 503 errors
        if response.status == 503:
            self.domain_503_count[domain] = self.domain_503_count.get(domain, 0) + 1
            self.logger.warning(f"⚠️ 503 error #{self.domain_503_count[domain]} for domain {domain}: {response.url}")
            
            # If too many 503 errors, slow down significantly
            if self.domain_503_count[domain] >= 3:
                self.logger.warning(f"🐌 Domain {domain} has {self.domain_503_count[domain]} 503 errors, implementing aggressive slowdown")
                spider.custom_settings = spider.custom_settings or {}
                spider.custom_settings.update({
                    'DOWNLOAD_DELAY': 10,
                    'RANDOMIZE_DOWNLOAD_DELAY': 5.0,
                    'CONCURRENT_REQUESTS': 1,
                    'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
                })
        
        # Reset 503 count on successful response
        elif response.status == 200:
            if domain in self.domain_503_count:
                self.logger.info(f"✅ 200 response for {domain}, resetting 503 count")
                del self.domain_503_count[domain]
        
        return None
    
    def process_spider_output(self, response, result, spider):
        """
        Process the output from the spider (requests and items).
        Add anti-detection measures to outgoing requests.
        """
        processed_results = []
        
        for item_or_request in result:
            if isinstance(item_or_request, Request):
                # Process request with anti-detection measures
                processed_request = self.get_processed_request(item_or_request, response)
                if processed_request:
                    processed_results.append(processed_request)
            else:
                # Pass through items unchanged
                processed_results.append(item_or_request)
        
        return processed_results
    
    def get_processed_request(self, request, response):
        """
        Apply anti-detection measures to outgoing requests.
        Based on BaseSpiderMiddleware pattern from official docs.
        """
        domain = request.url.split('/')[2] if '://' in request.url else 'unknown'
        
        # Implement intelligent delays based on domain health
        current_time = time.time()
        if domain in self.last_request_time:
            time_since_last = current_time - self.last_request_time[domain]
            
            # Calculate required delay based on 503 error count
            error_count = self.domain_503_count.get(domain, 0)
            if error_count > 0:
                # Exponential backoff for domains with errors
                required_delay = self.min_delay * (2 ** min(error_count, 4))  # Cap at 2^4 = 16x
                required_delay = min(required_delay, self.max_delay * 2)  # Don't exceed 2x max delay
            else:
                # Normal random delay for healthy domains
                required_delay = random.uniform(self.min_delay, self.max_delay)
            
            if time_since_last < required_delay:
                sleep_time = required_delay - time_since_last
                self.logger.debug(f"⏱️ Sleeping {sleep_time:.1f}s for {domain} (errors: {error_count})")
                time.sleep(sleep_time)
        
        self.last_request_time[domain] = time.time()
        
        # Add anti-detection headers
        request.headers.setdefault('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
        request.headers.setdefault('Accept-Language', 'en-US,en;q=0.5')
        request.headers.setdefault('Accept-Encoding', 'gzip, deflate')
        request.headers.setdefault('DNT', '1')
        request.headers.setdefault('Connection', 'keep-alive')
        request.headers.setdefault('Upgrade-Insecure-Requests', '1')
        
        # Add cache control to avoid cached responses
        request.headers['Cache-Control'] = 'no-cache'
        request.headers['Pragma'] = 'no-cache'
        
        # Set priority based on error count (lower priority for problematic domains)
        error_count = self.domain_503_count.get(domain, 0)
        if error_count > 0:
            request.priority = max(0, request.priority - (error_count * 10))
            self.logger.debug(f"🔽 Lowered priority to {request.priority} for {domain} (errors: {error_count})")
        
        return request
    
    def process_spider_exception(self, response, exception, spider):
        """
        Handle exceptions that occur during spider processing.
        """
        domain = response.url.split('/')[2] if '://' in response.url else 'unknown'
        self.logger.error(f"💥 Spider exception for {domain}: {exception}")
        
        # Track exceptions as potential blocking indicators
        self.domain_503_count[domain] = self.domain_503_count.get(domain, 0) + 1
        
        return None
