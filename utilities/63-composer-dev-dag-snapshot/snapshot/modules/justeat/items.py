# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JusteatItem(scrapy.Item):
    # Restaurant basic information
    restaurant_id = scrapy.Field()
    name = scrapy.Field()
    slug = scrapy.Field()
    
    # Address information
    address_line1 = scrapy.Field()
    address_line2 = scrapy.Field()
    city = scrapy.Field()
    postcode = scrapy.Field()
    country = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    
    # Contact information
    phone = scrapy.Field()
    website = scrapy.Field()
    
    # Rating and reviews
    rating = scrapy.Field()
    rating_count = scrapy.Field()
    rating_out_of_five = scrapy.Field()
    
    # Cuisine and categories
    cuisines = scrapy.Field()
    categories = scrapy.Field()
    
    # Restaurant status and features
    is_open = scrapy.Field()
    is_test_restaurant = scrapy.Field()
    is_brand = scrapy.Field()
    is_chain = scrapy.Field()
    
    # Delivery information
    delivery_time = scrapy.Field()
    minimum_order = scrapy.Field()
    delivery_fee = scrapy.Field()
    free_delivery_threshold = scrapy.Field()
    
    # Menu information
    menu_url = scrapy.Field()
    #menu_page_html = scrapy.Field()
    #menu_sections = scrapy.Field()
    
    # Additional metadata
    zipcode = scrapy.Field()
    rest_scraped_at = scrapy.Field()
    rest_source_url = scrapy.Field()
    #menu_scraped_at = scrapy.Field()
    menu_source_url = scrapy.Field()
    
    # Error tracking
    rest_error_message = scrapy.Field()
    rest_processing_status = scrapy.Field()
    menu_error_message = scrapy.Field()
    menu_url_extraction_status = scrapy.Field()
    
    # Pipeline fields (added to fix KeyError)
    _spider = scrapy.Field()
    _timestamp = scrapy.Field()
    _from_url = scrapy.Field()
    
    # Request tracking
    request_id = scrapy.Field()
