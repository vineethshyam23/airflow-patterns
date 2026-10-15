import logging
import traceback
from math import ceil
import os
from typing import Any, Optional, Callable, NoReturn
from scrapy import Selector, Spider, signals
from scrapy.crawler import Crawler
from scrapy.http import Response
from scrapy_crawlbase import CrawlbaseRequest as ProxyCrawlRequest

logger = logging.getLogger(__name__)


class RestGuruSpider(Spider):
    name = "rguru_cities"

    custom_settings = {
        "CONCURRENT_REQUESTS": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        "DOWNLOAD_TIMEOUT": 30.0,
        "DOWNLOAD_DELAY": 0,
        "COOKIES_ENABLED": False,
        "PROXYCRAWL_ENABLED": True,
    }

    os.makedirs('city_files', exist_ok=True)

    def __init__(self, name: Optional[str] = None, country: str = "", **kwargs: Any):
        if not country:
            raise AttributeError(
                f"Spider {self.name} got no country paramter!"
                " Start the spider with -a country=COUNTRY"
                "Where COUNTRY={de, pl, hr}"
            )
        assert country in ["de", "pl", "hr"]
        self.country = country
        country_dict = {
            'de': 'Germany',
            'pl': 'Poland',
            'hr': 'Croatia'}
        self.country_en = country_dict.get(self.country)

        super().__init__(name, **kwargs)

    @classmethod
    def from_crawler(cls, crawler: Crawler, *args: Any, **kwargs: Any) -> "RestGuruSpider":
        spider = super(RestGuruSpider, cls).from_crawler(crawler, *args, **kwargs)

        # connect signals to perform actions if spider opens or closes
        # This is for managing the files where the city names should be
        # written to
        crawler.signals.connect(spider.spider_opend, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)

        return spider

    def start_requests(self):

        url = f"https://de.restaurantguru.com/cities-{self.country_en}-c"
        yield self._create_request(url, self.parse)

    def parse(self, response: Response):
        try:
            for letter in response.xpath(".//div[@class='cities_block']"):
                agg_url_letter = letter.xpath(
                    ".//a[@class='show_all show_all_btn']/@href"
                ).get()
                if agg_url_letter:
                    yield {"agg_letter_url": agg_url_letter}
                    yield self._create_request(
                        agg_url_letter, callback=self.get_pagination_pages
                    )
                    continue
                self.get_cities_without_see_more(letter)

        except Exception:
            traceback.print_exc()
            logger.error(response.url)

    def get_pagination_pages(self, response):

        # In jedem Fall ist eine Seite vorhanden
        yield self._create_request(response.url, self.parse_letter)

        restaurant_count = response.xpath(
            './/div[@class="restaurants_count"]/text()'
        ).get()

        # Since the paginations links not allwasy includes all pages, only use
        # them as a backup
        if not restaurant_count:
            # Xpath für die evl. pagination Links
            pag_resp = response.xpath(
                './/div[@class="pagination"]/a[@class="number"]/@href'
            ).getall()

            # Bei mehr als 500 Städten pro Seite sind die Städte auf mehrere Pages aufgeteilt (nur über Inspektor sichtbar)
            if pag_resp != None:
                for url in pag_resp:
                    yield self._create_request(url, self.parse_letter)
        else:
            # calculate the number of pages under the assumption of 500
            # restaurants per page
            page_count = ceil(int(restaurant_count.split("/")[1].strip()) / 500)
            for i in range(2, page_count + 1):
                yield self._create_request(f"{response.url}/{i}", self.parse_letter)

    def parse_letter(self, response: Response):
        # letter_city = response.xpath(".//div[@class='cities scrolled-container']/ul/li/a")
        letter_city = response.xpath(".//div[@class='cities scrolled-container']/ul/li")
        logger.debug(f"Letter: {response.url}")
        for cityinfo in letter_city:

            city_url = cityinfo.xpath("./a/@href").extract_first()
            if not city_url:
                continue
            city_name = city_url.split("/")[-1]

            num_est = cityinfo.xpath("./span[@class='city-cnt']/text()").extract_first()

            if int(num_est) >= 10000:
                self.file_megacity.write(f"{city_name}\n")
            else:
                self.file_normalcity.write(f"{city_name}\n")

    def _create_request(self, url: str, callback: Callable) -> ProxyCrawlRequest:
        return ProxyCrawlRequest(
            url,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            callback=callback,
            device="desktop",
            # country=self.country.upper(),
            page_wait=1000,
            ajax_wait=False,
            dont_filter=True,
        )

    def get_cities_without_see_more(self, letter_tag: Selector) -> None:
        letter_whithou_see_all_cities = letter_tag.xpath(".//li//a")
        if letter_whithou_see_all_cities:
            for city_tag in letter_whithou_see_all_cities:
                city_restaurant_count_tag = city_tag.xpath(".//span/text()").get()

                city_url = city_tag.xpath("./@href").get()

                if not city_restaurant_count_tag or not city_url:
                    logger.warning(
                        "The url %s was likly restructured!",
                        letter_tag.response.url if letter_tag.response else "Unknown",
                    )
                    continue

                city_restaurant_count = int(
                    city_restaurant_count_tag.split("/")[1].strip()
                )
                city_name = city_url.split("/")[-1]

                if city_restaurant_count > 10000:
                    self.file_megacity.write(f"{city_name}\n")
                else:
                    self.file_normalcity.write(f"{city_name}\n")

    def spider_closed(self, spider: Spider) -> None:
        self.file_normalcity.close()
        self.file_megacity.close()

    def spider_opend(self, spider: Spider) -> None:
        self.file_megacity = open(
            f"./city_files/cities_mega_{self.country}.txt", "w", encoding="utf8"
        )
        self.file_normalcity = open(
            f"./city_files/cities_normal_{self.country}.txt", "w", encoding="utf8"
        )