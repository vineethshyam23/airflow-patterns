import logging
import traceback
from json.decoder import JSONDecodeError

from scrapy.http.response.html import HtmlResponse

from scrapy import Spider

from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy_proxycrawl.request import ProxyCrawlRequest

import re
from math import ceil
import json

logger = logging.getLogger(__name__)


class RestGuruSpider(Spider):
    name = "rguru_cities_pl"

    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "DOWNLOAD_TIMEOUT": 30.0,
        "DOWNLOAD_DELAY": 0,
        "COOKIES_ENABLED": False,
        "PROXYCRAWL_ENABLED": True,
    }

    def start_requests(self):
        url = 'https://restaurantguru.com/cities-Poland-c'
        yield ProxyCrawlRequest(
                url,
                user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                callback=self.parse,
                device="desktop",
                country="PL",
                page_wait=1000,
                ajax_wait=True,
                dont_filter=True,
        )

    def parse(self, response):
        try:
            for letter in response.xpath(".//div[@class='cities_block']/div/a"):
                agg_url_letter = letter.xpath(".//@href").get()
                yield {
                   'agg_letter_url' : agg_url_letter
                }
                yield ProxyCrawlRequest(
                        agg_url_letter,
                        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                        callback=self.get_pagination_pages,
                        device="desktop",
                        country="PL",
                        page_wait=1000,
                        ajax_wait=True,
                        dont_filter=True,
                    )
        
        except JSONDecodeError:
            traceback.print_exc()
            logger.warning("JSONDecodeError at URL %s", response.url)
        except Exception:
            traceback.print_exc()
            logger.error(response.url)

    def get_pagination_pages(self,response):
        
        # In jedem Fall ist eine Seite vorhanden
        yield ProxyCrawlRequest(
                            response.url,
                            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                            callback=self.parse_letter,
                            device="desktop",
                            country="PL",
                            page_wait=1000,
                            ajax_wait=True,
                            dont_filter=True,
                        )
        
        # Xpath für die evl. pagination Links
        pag_resp = response.xpath('.//div[@class="pagination"]/a[@class="number"]/@href').getall()
        
        # Bei mehr als 500 Städten pro Seite sind die Städte auf mehrere Pages aufgeteilt (nur über Inspektor sichtbar)
        if pag_resp != None:
            for url in pag_resp:
                yield ProxyCrawlRequest(
                            url,
                            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                            callback=self.parse_letter,
                            device="desktop",
                            country="PL",
                            page_wait=1000,
                            ajax_wait=True,
                            dont_filter=True,
                        )

    def parse_letter(self, response):

        letter_city = response.xpath(".//div[@class='cities scrolled-container']/ul/li/a")

        with open (r"./city_files/cities_pl.txt", "a") as city_file:

            for city in letter_city:    
                #city_name = city.xpath('./span/text()').extract_first()
                url_city = city.xpath('./@href').extract_first()
                city_url_form = re.search('(?<=https:\/\/restaurantguru\.com\/).*',url_city)

                city_file.write(f'{city_url_form.group(0)}\n')
                