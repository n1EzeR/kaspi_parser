import copy
import json
from datetime import date

import scrapy
from decouple import config

from app.constants import HEADER_DEFAULT


class ListSpider(scrapy.Spider):
    page = 1
    url = config("API_LIST_URL")
    referer_link = config("referer_link")

    def __init__(self, category, stop_if_no_reviews=True):
        super().__init__()
        self.url = self.url.format(category=category, page="{}")
        self.headers = copy.deepcopy(HEADER_DEFAULT)
        self.headers["Referer"] = self.referer_link.format(category)
        self.category = category
        self.stop_if_no_reviews = stop_if_no_reviews

    def start_requests(self):
        url = self.url.format(self.page)
        yield scrapy.Request(
            url, callback=self.parse_products, headers=self.headers, dont_filter=True
        )

    def parse_products(self, response):
        total_reviews = 0
        products = json.loads(response.text)
        for product in products["data"]:
            total_reviews += product["reviewsQuantity"]
            yield {
                "source_id": product["id"],
                "title": product["title"],
                "url": product["shopLink"],
                "brand": product["brand"],
                "rating": product["adjustedRating"],
                "reviews_quantity": product["reviewsQuantity"],
                "price_unit": product["unitPrice"],
                "price_sale": product["unitSalePrice"],
                "category_id": product["categoryId"],
                "category_name": self.category,
            }

        if products["data"]:
            if total_reviews == 0 and self.stop_if_no_reviews:
                return

            self.page += 1
            url = self.url.format(self.page)

            yield scrapy.Request(
                url=url,
                headers=self.headers,
                callback=self.parse_products,
                dont_filter=True,
            )


class SmartphonesSpider(ListSpider):
    name = "smartphones-list"
    category_link = "smartphones"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link, stop_if_no_reviews=False)


class ComputersSpider(ListSpider):
    name = "computers-list"
    category_link = "computers"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link, stop_if_no_reviews=False)


class BeautySpider(ListSpider):
    name = "beauty-list"
    category_link = "beauty%20care%20equipment"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class PerfumeSpider(ListSpider):
    name = "perfumes-list"
    category_link = "perfumes"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class BooksSpider(ListSpider):
    name = "books-list"
    category_link = "books"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class CarAudioSpider(ListSpider):
    name = "car-audio-list"
    category_link = "car%20audio"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class CarElectronicsSpider(ListSpider):
    name = "car-electronics-list"
    category_link = "car%20electronics"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class HeadphonesSpider(ListSpider):
    name = "headphones-list"
    category_link = "headphones"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class BigHomeAppliancesSpider(ListSpider):
    name = "big-home-appl-list"
    category_link = "big%20home%20appliances"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class SmallHomeAppliancesSpider(ListSpider):
    name = "small-home-appl-list"
    category_link = "small%20home%20appliances"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class ClimateEquipmentSpider(ListSpider):
    name = "climate-equipment-list"
    category_link = "climate%20equipment"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class KitchenAppliancesSpider(ListSpider):
    name = "kitchen-home-appl-list"
    category_link = "kitchen%20appliances"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class MemoryCardsSpider(ListSpider):
    name = "memory-cards-list"
    category_link = "memory%20cards"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class PortableSpeakersSpider(ListSpider):
    name = "portable-speakers-list"
    category_link = "portable%20speakers"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class PowerBanksSpider(ListSpider):
    name = "power-banks-list"
    category_link = "power%20banks"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class TiresSpider(ListSpider):
    name = "tires-list"
    category_link = "tires"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class WatchesSpider(ListSpider):
    name = "watches-list"
    category_link = "smart%20watches"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)


class WearablesSpider(ListSpider):
    name = "wearables-list"
    category_link = "wearables"
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_URI": f"../data/products/{date.today()}/{name}.json",
    }

    def __init__(self):
        super().__init__(self.category_link)
