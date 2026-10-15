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
    name = "rguru_pl"

    custom_settings = {
        "CONCURRENT_REQUESTS": 3,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "DOWNLOAD_TIMEOUT": 30.0,
        "DOWNLOAD_DELAY": 0,
        "COOKIES_ENABLED": False,
        "PROXYCRAWL_ENABLED": True,
    }

    def start_requests(self):
        
        with open(r'city_files/cities_pl.txt','r') as cities_file:
            for line in cities_file: 
                   
                yield ProxyCrawlRequest(
                    url = f'https://restaurantguru.com/restaurant-{line}-t1',
                    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    callback=self.parse,
                    device="desktop",
                    #country="DE",
                    page_wait=1000,
                    ajax_wait=True,
                    dont_filter=True,
                )

    def parse(self, response):
        try:
            num_tag = response.xpath('.//div[@class="restaurant_row show words_review_link   "]/div/div[2]/div[2]/div[1]/div/div[1]/text()').get()
            self.log(f'NumberTag: {num_tag}', logging.DEBUG)
            
            num_est = re.search('(?<=of )\d{1,5}', num_tag)
            self.log(f'Establishment Number: {num_est}', logging.DEBUG)

            num_pages = ceil(int(num_est.group(0))/20)
            self.log(f'Number of Pages: {num_pages}', logging.DEBUG)

            for page in range(1,num_pages):
                
                total_url = f'{response.url}/{page}?skip_geo=1'

                yield ProxyCrawlRequest(
                    url = total_url,
                    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    callback=self.parse_estabs,
                    device="desktop",
                    #country="DE",
                    page_wait=1000,
                    ajax_wait=True,
                    dont_filter=True,
                )

        except JSONDecodeError:
            traceback.print_exc()
            self.log("JSONDecodeError at URL %s", response.url, logging.ERROR)
        except Exception:
            traceback.print_exc()
            self.log(response.url, logging.ERROR)


    def parse_estabs(self, response):
        for est_url in response.xpath('.//div[@class="info_header"]/div/a/@href').getall():
            yield ProxyCrawlRequest(
                    url = est_url,
                    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    callback=self.parse_estab_page,
                    device="desktop",
                    #country="DE",
                    page_wait=1000,
                    ajax_wait=True,
                    dont_filter=True,
                )


    def parse_estab_page(self, response):
        
        try:
            # google rating
            google_response = response.xpath('.//div[@class="right"]/div[@class="google_stars"]/div')

            if google_response != None and google_response != []:

                google_rating = re.search('\d+',google_response.attrib['style']).group(0)

                if google_rating != 100:
                    google_rating = round(float(f"0.{google_rating}")*5, 1)
                else: google_rating = 5
            else: google_rating = None
            
            # foursqure
            foursquare_response = response.xpath('.//a[@class="row foursquare rating_list_right"]/div[@class="right"]/div/text()').get()
            if foursquare_response != None:
                foursquare = f"{foursquare_response}/10"
            else: foursquare = None

            # yelp rating
            yelp_response = response.xpath('.//a[@class="row yelp rating_list_right"]/div[@class="right"]/div/@class').get()
            if yelp_response != None and yelp_response != 'no_rating':
                yelp = re.search('\d.?\d?',re.sub('_','.',yelp_response)).group(0)
            else: yelp = None

            # michelin
            michelin_response = response.xpath('.//div[@class="right michelin"]/div/div[1]/@class').get()
            #logger.debug(f"Response Type: {michelin_response}")
            if michelin_response != None and michelin_response != 'michelin_selection':
                michelin = re.search('\d',michelin_response).group(0)
            else: michelin = None

            # trip
            trip_response = response.xpath('.//div[@class="row trip rating_list_right"]/div[@class="right"]/div/div/@class').get()
            if trip_response != None:
                trip = re.search('\d.?\d?',re.sub('_','.',trip_response)).group(0)
            else: trip = None

            # url
            link_response = response.xpath('.//div[@class="website"]/div[2]/a/text()').get()
            if link_response != None:
                link = f"https://{link_response}/"
            else: link = None

            # last review dates
            last_reviews_response = response.xpath('.//div[@class="o_review"]/div[@class="overflow"]/div[1]/div[1]/span[@class="grey"]/text()').getall()
            if last_reviews_response != None:
                last_review_dates = []
                # cut down review_date string via regex
                for i in last_reviews_response:
                    last_review_dates.append(re.search('(\d{1,2}|(one|ein)) \w+', i).group(0))
            else: last_review_dates = None

            #may be closed
            closed = False
            if response.xpath('.//div[@class="wrapper_title"]/div[@class="closed_info_block"]').get() != None:
                closed = True

            #meta_json
            meta_json = response.xpath('.//script[@type="application/ld+json"]').extract_first()
            if meta_json != None and not closed:
                meta_json = (meta_json.replace('</script>','')).replace('<script type=\"application/ld+json\">','') 
                meta_json = json.loads(meta_json)            
                menu_url = meta_json.get('hasMenu')        
                address_meta = meta_json.get('address')               
                country = address_meta.get('addressCountry')
                city = address_meta.get('addressLocality')
                address_region = address_meta.get('addressRegion')
                street = address_meta.get('streetAddress')
                opening_hours = meta_json.get('openingHours')
                aggregate_rating = meta_json.get('aggregateRating')
                phone = meta_json.get('telephone')
                geo_cordinates = meta_json.get('geo')
                latitude = geo_cordinates.get('latitude')
                longitude = geo_cordinates.get('longitude')
                date_published_on_rg = meta_json.get('datePublished')
                type = meta_json.get('@type')
                serves_cuisine = meta_json.get('servesCuisine')
                price_range = meta_json.get('priceRange')


                yield {
                    '_from_url' : response.url,
                    'title' : response.xpath('.//div[@class="wrapper_title "]/div/h1/a/text()').get(),
                    'address-meta' : address_meta,
                    'country' : country,
                    'city' : city,
                    'address_region' : address_region,
                    'street' : street,
                    'latitude' : latitude,
                    'longitude': longitude,
                    'phone' : phone,
                    'intern_link' : response.xpath('.//div[@class="website"]/div[2]/a/@href').get(),
                    'url': link,
                    'type_tags' : response.xpath('.//div[@id="ranks"]/div/div/a/span/text()').getall(),
                    'establishment_type' : type,
                    'price_range_euro' : response.xpath('.//span[@class="hint"]/span/span[@class="nowrap"]/span/text()').get(),
                    'price_range' : price_range,
                    'menu_url' : menu_url,
                    'cuisine_type' : serves_cuisine,
                    'specials' : response.xpath('.//div[@class="features_block"]/div[2]/span/text()').getall(),
                    'opening_hours' : opening_hours,
                    'aggregate_rating' : aggregate_rating,
                    'rating_google' : google_rating,
                    'rating_yelp' : yelp,
                    'foursquare' : foursquare,
                    'michelin' : michelin,
                    'trip' : trip,
                    'facebook': response.xpath('.//div[@class="right"]/div[@class="facebook_rate"]/span/text()').get(),
                    'date_published_on_rg' : date_published_on_rg,
                    'closed_permanent' : response.xpath('.//div[@class="wrapper_title "]/div[@class="closed_info_block"]/text()').get(),
                    'last_review_dates' : last_review_dates
                
                }
        
        
        except JSONDecodeError:
            traceback.print_exc()
            self.log("JSONDecodeError at URL %s", response.url, logging.ERROR)
        except Exception:
            traceback.print_exc()
            self.log(response.url, logging.ERROR)
