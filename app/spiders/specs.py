import json
from datetime import date

import scrapy
from decouple import config

from app.constants import PRODUCTS_DIR
from app.utils import parse_latest_date


class SpecsSpider(scrapy.Spider):
    referer_link = config("referer_link")

    def __init__(self, category):
        super().__init__()
        parse_date = parse_latest_date(PRODUCTS_DIR)
        self.products_json = f"{PRODUCTS_DIR}/{parse_date}/{category}-list.json"

    def start_requests(self):
        with open(self.products_json) as products_json:
            products = json.load(products_json)
            for product in products:
                url = product["shopLink"]
                yield scrapy.Request(
                    url=url, cb_kwargs={"product": product}, callback=self.parse_product
                )

    def parse_product(self, response, product):
        """
        Parse product specs

        dl - is the block
        dt - is the header
        dd - is the body

        dd may be the root of other dls
        """
        details = {}
        blocks = response.xpath('//dl[contains(@class,"specifications-list__spec")]')
        for block in blocks:
            characteristic = block.xpath("dt/span/text()").get()
            value = block.xpath("dd/text()").get()

            details[characteristic] = value

        yield {
            "id": product["id"],
            "title": product["title"],
            "url": product["shopLink"],
            "brand": product["brand"],
            "rating": product["adjustedRating"],
            "reviews_quantity": product["reviewsQuantity"],
            "price": product["unitSalePrice"],
            "category_id": product["categoryId"],
            "category_name": product["categoryName"],
            "parsed_details": details,
        }


class SmartphoneSpecsSpider(SpecsSpider):
    name = "smartphones-details"
    category = "smartphones"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"{PRODUCTS_DIR}/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category)


class ComputerSpecsSpider(SpecsSpider):
    name = "computers-details"
    category = "computers"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"{PRODUCTS_DIR}/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category)
