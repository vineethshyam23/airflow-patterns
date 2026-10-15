# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
import scrapy
import logging
from urllib.parse import urlencode

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
        # that it doesn't have a response associated.

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
    Simplified ScrapeOps Proxy Middleware based on working middleware from other project
    """
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.scrapeops_api_key = settings.get('SCRAPEOPS_API_KEY', '0a0bd6f5-e1c0-49c3-b8f5-1ec732a2e3b6')
        self.scrapeops_endpoint = 'https://proxy.scrapeops.io/v1/?'
        self.scrapeops_proxy_active = settings.get('SCRAPEOPS_PROXY_ENABLED', True)

    def _scrapeops_proxy_enabled(self):
        """Check if ScrapeOps proxy is enabled"""
        if self.scrapeops_api_key is None or self.scrapeops_api_key == '' or self.scrapeops_proxy_active == False:
            return False
        return True

    def _get_scrapeops_url(self, request):
        """Generate ScrapeOps URL with retry-specific configurations for CAPTCHA avoidance"""
        # 🚨 ENHANCED PROXY ROTATION: Different configurations for retries
        retry_attempt = getattr(request, 'meta', {}).get('retry_times', 0)
        
        # Base configuration - most reliable
        payload = {
            'api_key': self.scrapeops_api_key, 
            'url': request.url,
            'render_js': 'true',      # Always enable JS for Restaurant Guru
            'residential': 'true',    # Use residential proxies
            'keep_headers': 'true',   # Keep original headers
            'premium': 'true',        # Use premium proxies
            'captcha_solver': 'true', # Enable CAPTCHA solving
            'stealth_mode': 'true',   # Enable stealth mode
        }
        
        # 🔄 RETRY-SPECIFIC CONFIGURATIONS: Different settings for each retry
        if retry_attempt == 0:
            # First attempt: Balanced approach
            payload.update({
                'wait': '15000',           # 15 second wait
                'country': 'DE'            # German proxy first
            })
        elif retry_attempt == 1:
            # Second attempt: More aggressive anti-detection
            payload.update({
                'wait': '20000',           # 20 second wait
                'country': 'AT',           # Austrian proxy
                'ultra_premium': 'true',   # Ultra premium proxies
                'session': 'true'          # Session persistence
            })
        else:
            # Third+ attempt: Maximum anti-detection
            payload.update({
                'wait': '25000',           # 25 second wait
                'country': 'CH',           # Swiss proxy
                'ultra_premium': 'true',   # Ultra premium proxies
                'session': 'true',         # Session persistence
                'fingerprint_randomize': 'true',  # Randomize fingerprint
                'timezone_randomize': 'true'      # Randomize timezone
            })
        
        proxy_url = self.scrapeops_endpoint + urlencode(payload)
        return proxy_url

    @staticmethod
    def _replace_response_url(response):
        """Replace response URL with the final URL from ScrapeOps"""
        real_url = response.headers.get('Sops-Final-Url', def_val=response.url)
        if isinstance(real_url, bytes):
            real_url = real_url.decode(response.headers.encoding or 'utf-8')
        return response.replace(url=real_url)
    
    def process_request(self, request, spider):
        """Process request through ScrapeOps API with retry-aware configuration"""
        if not self._scrapeops_proxy_enabled() or self.scrapeops_endpoint in request.url:
            return None
        
        # 🔄 LOG RETRY CONFIGURATION: Show which proxy config is being used
        retry_attempt = getattr(request, 'meta', {}).get('retry_times', 0)
        if retry_attempt > 0:
            spider.logger.warning(f"🔄 CAPTCHA RETRY #{retry_attempt}: Using enhanced ScrapeOps configuration for {request.url}")
        
        scrapeops_url = self._get_scrapeops_url(request)
        new_request = request.replace(
            cls=scrapy.Request, url=scrapeops_url, meta=request.meta)
        
        spider.logger.info(f"🌐 USING ScrapeOps for: {request.url}")
        return new_request

    def process_response(self, request, response, spider):
        """Process response from ScrapeOps"""
        new_response = self._replace_response_url(response)
        if response.status == 200:
            spider.logger.debug(f"✅ ScrapeOps response successful: {request.url}")
        else:
            spider.logger.warning(f"⚠️ ScrapeOps response status {response.status}: {request.url}")
        return new_response

    def process_exception(self, request, exception, spider):
        """Handle ScrapeOps exceptions"""
        spider.logger.error(f"❌ ScrapeOps exception for {request.url}: {str(exception)}")
        return None
