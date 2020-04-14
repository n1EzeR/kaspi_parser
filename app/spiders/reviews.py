import json
import logging
import os
from datetime import date

import scrapy
from decouple import config

from app.constants import HEADER_REVIEWS, PRODUCTS_DIR, REVIEWS_DIR, REVIEWS_PER_REQUEST
from app.utils import parse_latest_date


class ReviewsSpider(scrapy.Spider):
    url = config("API_REVIEWS_URL")

    def __init__(self, category):
        super().__init__()

        parse_date = parse_latest_date(PRODUCTS_DIR)
        self.parse_list = f"{PRODUCTS_DIR}/{parse_date}/{category}-list.json"

        self.output_dir = f"{REVIEWS_DIR}/{date.today()}/{category}"
        os.makedirs(self.output_dir, exist_ok=True)

        self.log(f"Parser for {category} reviews has been started.")

    def start_requests(self):
        with open(self.parse_list) as products_json:
            products = json.load(products_json)
            for product in products:
                yield scrapy.Request(
                    url=self.url.format(product["id"], REVIEWS_PER_REQUEST),
                    headers=HEADER_REVIEWS,
                    callback=self.parse_reviews,
                    cb_kwargs={
                        "product_id": product["id"],
                        "actual_reviews_quantity": product["reviewsQuantity"],
                    },
                )

    def parse_reviews(self, response, product_id, actual_reviews_quantity):
        reviews_json = response.body_as_unicode()
        with open(f"{self.output_dir}/{product_id}.json", "w") as f:
            f.write(reviews_json)

        reviews = json.loads(reviews_json)
        parsed_reviews_quantity = len(reviews["data"])

        if actual_reviews_quantity != parsed_reviews_quantity:
            self.logger.warning(
                f"Product with id={product_id} received {parsed_reviews_quantity} reviews, "
                f"but has {actual_reviews_quantity}"
            )


class ComputersSpider(ReviewsSpider):
    name = "computers-reviews"
    category = "computers"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class BeautySpider(ReviewsSpider):
    name = "beauty-reviews"
    category = "beauty"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class BigHomeAppSpider(ReviewsSpider):
    name = "bha-reviews"
    category = "big-home-appl"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class SmallHomeAppSpider(ReviewsSpider):
    name = "sha-reviews"
    category = "small-home-appl"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class KitchenHomeAppSpider(ReviewsSpider):
    name = "kha-reviews"
    category = "kitchen-home-appl"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class ClimateEquipmentSpider(ReviewsSpider):
    name = "climate-reviews"
    category = "climate-equipment"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class BooksSpider(ReviewsSpider):
    name = "books-reviews"
    category = "books"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class HeadphonesSpider(ReviewsSpider):
    name = "headphones-reviews"
    category = "headphones"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class PerfumesSpider(ReviewsSpider):
    name = "perfumes-reviews"
    category = "perfumes"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class CarAudioSpider(ReviewsSpider):
    name = "car-audio-reviews"
    category = "car-audio"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class CarElectronicsSpider(ReviewsSpider):
    name = "car-electronics-reviews"
    category = "car-electronics"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class MemoryCardsSpider(ReviewsSpider):
    name = "memory-cards-reviews"
    category = "memory-cards"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class PowerBanksSpider(ReviewsSpider):
    name = "power-banks-reviews"
    category = "power-banks"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class TiresSpider(ReviewsSpider):
    name = "tires-reviews"
    category = "tires"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class WatchesSpider(ReviewsSpider):
    name = "watches-reviews"
    category = "watches"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class WearablesSpider(ReviewsSpider):
    name = "wearables-reviews"
    category = "wearables"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class SmartphonesSpider(ReviewsSpider):
    name = "smartphones-reviews"
    category = "smartphones"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)


class PortableSpeakersSpider(ReviewsSpider):
    name = "portable-speakers-reviews"
    category = "portable-speakers"
    custom_settings = {"LOG_FILE": f"logs/{date.today()}{name}.log"}
    logging.getLogger().addHandler(logging.StreamHandler())

    def __init__(self):
        super().__init__(self.category)
