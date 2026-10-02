import logging
import traceback
from json.decoder import JSONDecodeError
from scrapy import Spider, Selector
from scrapy.http import Response
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy_crawlbase import CrawlbaseRequest as ProxyCrawlRequest
import re
from math import ceil
import json
from typing import Optional, Any, cast, Tuple, Dict, List

class RestGuruSpider(Spider):
    name = "rguru_mega_de"

    filter_and_prioritize = False  # for just parsing all establishments, like the normal cities 
    # filter_and_prioritize = True  # for applying filters and prioritizing (errorprone)

    rest_slugs = []

    custom_settings = {
        "CONCURRENT_REQUESTS": 30,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "DOWNLOAD_TIMEOUT": 30.0,
        "DOWNLOAD_DELAY": 0,
        "COOKIES_ENABLED": False,
        "PROXYCRAWL_ENABLED": True,
    }

    def start_requests(self):
        
        if self.filter_and_prioritize:
            requests_callback_function = self.filter_mega
        else:
            requests_callback_function = self.parse

        # Einlesen der txt mit city slugs
        cities_file_path = r'city_files/cities_mega_de.txt'
        with open(cities_file_path, "r") as cities_file:
            lines = list(cities_file)

            self.log(f"Number of cities found in {cities_file_path}: {len(lines)}", logging.INFO)

            for line in lines[1:2]:  # this is for testing, comment out before deploying
            # for line in lines:    
                self.log(f"Crawling urls for city: {line}", logging.INFO)
                
                yield ProxyCrawlRequest(
                    url = f'https://de.restaurantguru.com/restaurant-{line}-t1',
                    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    callback=requests_callback_function,
                    device="desktop",
                    country="DE",
                    page_wait=1000,
                    ajax_wait=True,
                    dont_filter=True,
                    meta={"city" : line}
                )

    # Funktion zum Zählen der Estabs pro Stadt
    def count_estabs(self, response):
        self.log(f'URL: {response.url}',logging.INFO)
        success = False
        try:
            # try to find a class wrap_top_title, try to extract the number from there: 
            num_text = response.xpath('//div[contains(@class, "wrap_top_title")]//text()').getall()
            # Join all extracted text into one string
            num_text_clean = " ".join(num_text).strip()
            self.log(f"wrap_top_title contents: {num_text_clean}", logging.DEBUG)
            # Extract all numbers (handles spaces, commas, dots)
            matches = re.findall(r'\d{1,3}(?:[ \xa0,.]?\d{3})*|\d+', num_text_clean)
            self.log(f"number matches: {';'.join(matches)}", logging.DEBUG)
            # Convert matches to integers (removing spaces/non-numeric chars)
            matches_int = [int(re.sub(r'[ \xa0,.]', '', m)) for m in matches]
            # Get the largest number
            num_est = max(matches_int)
            succes = True
        except Exception as e:
            self.log(f'Could not extract the est number based on wrap_top_title : {e}', logging.WARNING)

        if not success:
            self.log(f'Trying based on static xpath expression', logging.INFO)
            try:
                num_tag = (
                        response.xpath('//*[@id="content"]/div[1]/div[2]/div[1]/span/text()')
                        .get()
                        .strip()
                        )
                self.log(f"NumberTag: {num_tag}", logging.DEBUG)
                num_est = int("".join(num_tag.split("\xa0")[1:]))
                success = True
            except Exception as e:
                self.log(f'Could not est number based on static xpath expression: {e}', logging.WARNING)
        
        if success:
            self.log(f"Total number of restaurants: {num_est}", logging.INFO)
        else:
            num_est = 0
            self.log(f"No Establishments found for {response.url}", logging.WARNING)
            # with open("responselog.html", "w") as out:
                    #    out.write(response.text)

        return num_est

    # Berechnung der Anzahl Seiten mit Restaurants auf basis der Estab-Anzahl (immer 20 pro Seite)
    def num_of_pages(self, response):
        estab_number = self.count_estabs(response)
        num_pages = ceil(estab_number/20)

        return num_pages
    
    # Prüfen, ob Mega City vorliegt (>10k Estabs) und anwenden der Filterkriterien
    def filter_mega(self, response):
        city = response.meta["city"]

        #filter für establishment types
        #type_sets = [{"1" : "", "2" : ""},{"1" : "fast-food-", "2" : "-t11"},{"1" : "pub-and-bar-", "2" : "-t2"},{"1" : "club-", "2" : "-t10"},{"1" : "cafe", "2" : "-t6"}]
        type_sets = [5,6,10,11,12]

        estab_number = self.count_estabs(response)
        self.log(f"{estab_number} found for city {city}", logging.INFO)

        if estab_number <= 10000:
            self.log(f"For MegaCity {city}: less then 10k Establishments ({estab_number}) were found, will parse all", logging.INFO)
            self.parse(response, response.url, '?skip_geo=1')
        else:
            self.log(f"For MegaCity {city}: more than 10k Estab ({estab_number}), parsing only types: {type_sets}", logging.INFO)
            for set in type_sets[0:1]:  # this is for testing, comment out before deploying
            # for set in type_sets:
                self.log(f"For MegaCity {city} parsing type: {set}", logging.INFO)
                total_url = f'https://de.restaurantguru.com/restaurant-{city}-t1?set={set}&skip_geo=1'
                
                yield ProxyCrawlRequest(
                    url = total_url,
                    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    callback=self.parse_mega,
                    device="desktop",
                    country="DE",
                    page_wait=1000,
                    ajax_wait=True,
                    dont_filter=True,
                    meta={"city" : city, "set" : set}
                )

    # Anwenden zweiter Filterebene (Price-Range), dann parse der Estab-Pages
    def parse_mega(self,response):
        self.log(f"Apply type filter: URL: {response.url}", logging.INFO)

        price_filter = [1,2,3,4]
        city = response.meta["city"]
        set = response.meta["set"]
        base_url = f'https://de.restaurantguru.com/restaurant-{city}-t1'

        num_estab = self.count_estabs(response)

        if num_estab <= 10000:
            self.log(f"In Megacity {city} type {set}, less then 10k establishments ({num_estab} are found, will parse all)", logging.INFO)
            self.parse(response, base_url , f'?set={set}&skip_geo=1&search=1')
            self.log(f"URL: {response.url}", logging.INFO)

        else:
            self.log(f"In Megacity {city} type {set} still over 10k establishments ({num_estab}), parsing only price ids: {price_filter}", logging.INFO)
            for price_id in price_filter[0:1]:  # this is for testing, comment out before deploying
            # for price_id in price_filter:
                self.log(f"In Megacity {city} type {set}, parsing price_id: {price_id}", logging.INFO)
                search_query = f'?set={set}&price_scroe={price_id}&skip_geo=1'
                total_url = base_url + search_query
                
                kwargs = dict(request_url=base_url, search_query=search_query)

                yield ProxyCrawlRequest(
                    url = total_url,
                    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    callback=self.parse,
                    device="desktop",
                    country="DE",
                    page_wait=1000,
                    ajax_wait=True,
                    dont_filter=True,
                    cb_kwargs=kwargs
                )
    
    # Parse der einzelnen City Urls und Iteration über Pages
    def parse(self, response, request_url=None, search_query=None):
        try:
            num_pages = self.num_of_pages(response)
            if self.filter_and_prioritize:
                self.log(f"{num_pages} to parse for URL: {response.url} q: {search_query}", logging.INFO)
            else:
                self.log(f"{num_pages} to parse for URL: {response.url}", logging.INFO)

            for page in range(1, 3):  # this is for testing, comment out before deploying
            # for page in range(1,num_pages + 1):
                
                if self.filter_and_prioritize:
                    total_url = f'{request_url}/{page}{search_query}'
                else:
                    total_url = f"{response.url}/{page}?skip_geo=1"
                self.log(f'ESTAB_URL: {total_url}', logging.INFO)

                yield ProxyCrawlRequest(
                    url = total_url,
                    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    callback=self.parse_estabs,
                    device="desktop",
                    country="DE",
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

    # Parse der Establishments pro Page und Duplicate Filter
    def parse_estabs(self, response):
        for est_url in response.xpath('.//div[@class="info_header"]/div/a/@href').getall():

            rest_slug = est_url.split(".com/")[1]

            if rest_slug not in self.rest_slugs:
                self.rest_slugs.append(rest_slug)
                yield ProxyCrawlRequest(
                        url = est_url,
                        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                        callback=self.parse_estab_page,
                        device="desktop",
                        country="DE",
                        page_wait=1000,
                        ajax_wait=True,
                        dont_filter=True,
                    )
            else:
                self.log(f"Skipping Estab due to duplicate ({rest_slug})", logging.INFO)

    # Final Parse, um Daten von der Estab-Page zu extrahieren
    def parse_estab_page(self, response):
        def get_rating(provider: str) -> Optional[Tuple[float, float]]:
            rating_row = response.xpath(
                f'.//a[@class="row {provider} rating_list_right"] | .//div[@class="row {provider} rating_list_right"]'
            )
            rating_tag = rating_row.xpath(
                f'./div[@class="left"]//span[@class="agency-count"]/text()'
            ).get()
            if not rating_tag:
                return None

            rating, max_rating = rating_tag.replace(",", ".").split("/")
            rating = float(rating[1:])
            max_rating = float(max_rating[:-1])
            return (rating, max_rating)
        
        try:
            self.log(f"Beginn Scraping of Restaurant: {response.url}", logging.DEBUG)
            # google rating
            google_response = get_rating("google")
            if google_response:
                google_rating = google_response[0]
            else:
                google_rating = None
            
            # foursqure
            foursquare_response = get_rating("foursqure")
            if foursquare_response != None:
                foursquare = f"{foursquare_response[0]}/10"
            else:
                foursquare = None

            # yelp rating
            yelp_response = get_rating("yelp")
            if yelp_response != None:
                yelp = str(yelp_response[0])
            else:
                yelp = None

            # michelin
            michelin_response = response.xpath(
                './/div[@class="right michelin michelin-flex"]/div/div[1]/@class'
            ).get()
            if michelin_response != None and michelin_response != "michelin_selection":
                match = re.search(r"\d", michelin_response)
                if match:
                    michelin = match.group(0)
                else:
                    michelin = None
            else:
                michelin = None

            # trip
            trip_response = get_rating("trip")
            if trip_response:
                trip = trip_response[0]
            else:
                trip = None

            # facebook
            facebook_response = get_rating("facebook")
            if facebook_response:
                facebook = facebook_response[0]
            else:
                facebook = None

            # url
            link_response = response.xpath('.//div[@class="website"]/div[2]/a/text()').get()
            if link_response != None:
                link = f"https://{link_response}/"
            else: link = None

            # last review dates
            last_reviews_response = response.xpath(
                './/div[@class="o_review"]//span[@class="grey"]/text()'
            ).getall()
            if last_reviews_response:

                last_review_dates = []
                # cut down review_date string via regex
                for i in last_reviews_response:
                    match = re.search(r"(\d{1,2}|(one|ein)) \w+", i)
                    if match:
                        last_review_dates.append(match.group(0))
                    else:
                        last_review_dates.append(
                            None
                        )  # or handle it differently if needed
            else:
                last_review_dates = None

            #may be closed
            closed = False
            if response.xpath('.//div[@class="wrapper_title"]/div[@class="closed_info_block"]').get() != None:
                closed = True

            # price range euro
            price_range_euro_tag = response.xpath(
                './/span[@class="hint"]/span/span[@class="nowrap"]/span/text() | .//span[@class="hint"]/span/span[@class="nowrap"]/text()'
            ).getall()
            if price_range_euro_tag:
                price_range_euro = [tag for tag in price_range_euro_tag if tag.strip()][
                    0
                ]
            else:
                price_range_euro = None

            #meta_json
            meta_json = response.xpath(
                './/script[@type="application/ld+json"]'
            ).extract_first()
            if meta_json != None and not closed:
                meta_json = (meta_json.replace("</script>", "")).replace(
                    '<script type="application/ld+json">', ""
                )
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
                _type = meta_json.get('@type')
                serves_cuisine = meta_json.get('servesCuisine')
                price_range = meta_json.get('priceRange')
            else:
                side_info_json = self._get_infos_from_side(response)

                menu_url = None  # Needs more crawling depth
                country = side_info_json["country"]
                city = side_info_json["city"]
                address_region = side_info_json["addressRegion"]
                street = side_info_json["street"]
                opening_hours = side_info_json["openingHours"]
                aggregate_rating = None  # Not Available
                phone = side_info_json["telephone"]
                latitude = side_info_json["latitude"]
                longitude = side_info_json["longitude"]
                date_published_on_rg = None  # Not Available
                _type = None  # Not Available
                serves_cuisine = side_info_json["servesCuisine"]
                price_range = side_info_json["priceRange"]
                address_meta = None  # No Meta data available in this case


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
                'establishment_type' : _type,
                'price_range_euro' : price_range_euro,
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
                "facebook": facebook,
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
    
    def _get_infos_from_side(self, response: Response) -> Dict[str, Any]:
        side_infos: Dict[str, Any] = {}

        # coordinates
        coordinates_tag = response.xpath('.//a[@class="direction_link"]/@href').get()
        if coordinates_tag:
            match = re.search(
                r"destination=([-+]?\d*\.\d+),([-+]?\d*\.\d+)", coordinates_tag
            )
            if match:
                side_infos["latitude"] = match.group(1)
                side_infos["longitude"] = match.group(2)
            else:
                side_infos["latitude"] = None
                side_infos["longitude"] = None
        else:
            side_infos["latitude"] = None
            side_infos["longitude"] = None

        # cuisine
        cusine_tags = response.xpath(
            './/div[@class="cuisine_hidden"]/span/text()'
        ).getall()
        side_infos["servesCuisine"] = cusine_tags

        # phone
        phone_tag = response.xpath('.//a[@class="call"]/@href').get()
        if phone_tag:
            match = re.search(r"\+\d+", phone_tag)
            if match:
                side_infos["telephone"] = match.group(0)
            else:
                side_infos["telephone"] = None
        else:
            side_infos["telephone"] = None

        # address
        address_tag = cast(
            Optional[str],
            response.xpath('.//div[@class="address"]/div[2]/text()').get(),
        )
        if address_tag:
            address = [
                address_part.strip() for address_part in address_tag.strip().split(",")
            ]
            try:
                side_infos["street"] = address[0]
                side_infos["city"] = address[1]
                side_infos["addressRegion"] = address[2]
                side_infos["country"] = address[3]
            except IndexError:
                self.log(
                    f"Address {address} for url has less then 4 comma seperated parts {response.url}",
                    logging.WARNING
                )
                side_infos["street"] = None
                side_infos["city"] = None
                side_infos["addressRegion"] = None
                side_infos["country"] = None

        else:
            side_infos["street"] = None
            side_infos["city"] = None
            side_infos["addressRegion"] = None
            side_infos["country"] = None

        # has Menu
        # TODO: Not implemented yet. Needs one more crawling depth to work

        # Openung hours
        opening_hours_tags = cast(
            Optional[List[Selector]],
            response.xpath('.//table[@class="schedule-table"]//tr'),
        )

        if opening_hours_tags:
            side_infos["openingHours"] = []
            for tag in opening_hours_tags:
                day = tag.xpath('.//span[@class="short-day"]/text()').get()
                hours = tag.xpath("./td[2]/text()").getall()
                side_infos["openingHours"].extend(
                    [f"{day} {hour}" for hour in hours if re.match(r"\d", hour)]
                )
        else:
            side_infos["openingHours"] = None

        # aggregateRating does not exisit without the meta_json

        # price Range:
        side_infos["priceRange"] = response.xpath('.//span[@class="cost"]/text()').get()

        return side_infos
