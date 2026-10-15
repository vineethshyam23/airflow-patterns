import logging
import traceback
from json.decoder import JSONDecodeError
import os
from scrapy.http.response.html import HtmlResponse
from scrapy import Spider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
import re
from math import ceil
import json

logger = logging.getLogger(__name__)


class RestGuruSpider(Spider):
    name = "rguru_cities_de"

    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "DOWNLOAD_TIMEOUT": 30.0,
        "DOWNLOAD_DELAY": 0,
        "COOKIES_ENABLED": False,
        "SCRAPEOPS_PROXY_ENABLED": True,
    }

    os.makedirs('city_files', exist_ok=True)
            
    def start_requests(self):

        url = 'https://de.restaurantguru.com/cities-Germany-c'  

        yield Request(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'},
                callback=self.parse,
                dont_filter=True,
        )

    def parse(self, response):
        try:
            for letter in response.xpath(".//div[@class='cities_block']/div/a"):
                agg_url_letter = letter.xpath(".//@href").get()
                yield {
                   'agg_letter_url' : agg_url_letter
                }
                yield Request(
                        agg_url_letter,
                        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'},
                        callback=self.get_pagination_pages,
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
        yield Request(
                            response.url,
                            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'},
                            callback=self.parse_letter,
                            dont_filter=True,
                        )
        
        # Xpath für die evl. pagination Links
        pag_resp = response.xpath('.//div[@class="pagination"]/a[@class="number"]/@href').getall()
        
        # Bei mehr als 500 Städten pro Seite sind die Städte auf mehrere Pages aufgeteilt (nur über Inspektor sichtbar)
        if pag_resp != None:
            for url in pag_resp:
                yield Request(
                            url,
                            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'},
                            callback=self.parse_letter,
                            dont_filter=True,
                        )

    def parse_letter(self, response):
       #letter_city = response.xpath(".//div[@class='cities scrolled-container']/ul/li/a")
        letter_city = response.xpath(".//div[@class='cities scrolled-container']/ul/li")
        with open (r"./city_files/cities_normal_de.txt", "a") as file_normalcity, open (r"./city_files/cities_mega_de.txt", "a") as file_megacity:
           for cityinfo in letter_city:  

                url_city = cityinfo.xpath('./a/@href').extract_first()
                city_url_form = re.search(r'(?<=https://de\.restaurantguru\.com/).*',url_city)
                num_est = cityinfo.xpath("./span[@class='city-cnt']/text()").extract_first()            

                if int(num_est) >= 10000:                                
                    file_megacity.write(f'{city_url_form.group(0)}\n')                   
                else:                             
                   file_normalcity.write(f'{city_url_form.group(0)}\n')
       


                