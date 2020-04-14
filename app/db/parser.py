import json
import os
import re
from datetime import date, datetime

import orjson

from app import utils
from app.constants import DB_DUMPS_DIR, PRODUCTS_DIR, REVIEWS_DIR, SPECS_DIR
from app.db import schemas
from app.db.session import LocalSession
from app.utils import perf_logger

PRODUCTS_DIR = f"../{PRODUCTS_DIR}"
REVIEWS_DIR = f"../{REVIEWS_DIR}"
SPECS_DIR = f"../{SPECS_DIR}/all"

DB_DUMPS_DIR = f"../{DB_DUMPS_DIR}/{date.today()}"
os.makedirs(DB_DUMPS_DIR, exist_ok=True)


@perf_logger
def parse_categories():
    date_latest = utils.parse_latest_date(f"{PRODUCTS_DIR}")
    products_latest = f"{PRODUCTS_DIR}/{date_latest}"

    categories = os.listdir(products_latest)
    categories = [c for c in categories if not c.startswith(".")]

    categories_unique = set()
    for category in categories:
        with open(f"{products_latest}/{category}") as f:
            products = json.load(f)
            categories_unique = categories_unique.union(
                {(p["category_name"], p["category_id"]) for p in products}
            )
    categories_sorted = sorted(categories_unique, key=lambda x: (x[0], x[1]))
    categories_dict = [{"name": c[0], "source_id": c[1]} for c in categories_sorted]

    with open(f"{DB_DUMPS_DIR}/categories.json", "w") as f:
        json.dump(categories_dict, f)


@perf_logger
def parse_products():
    date_latest = utils.parse_latest_date(PRODUCTS_DIR)
    products_latest = f"{PRODUCTS_DIR}/{date_latest}"

    categories = os.listdir(products_latest)
    categories = [c for c in categories if not c.startswith(".")]

    products = []
    for category in categories:
        with open(f"{products_latest}/{category}") as f:
            category_products = json.loads(f.read())
            products.extend([schemas.Product(**p).dict() for p in category_products])

    with open(f"{DB_DUMPS_DIR}/products.json", "w") as f:
        f.write(json.dumps(products, ensure_ascii=False))


@perf_logger
def parse_reviews():
    date_latest = utils.parse_latest_date(REVIEWS_DIR)
    reviews_latest = f"{REVIEWS_DIR}/{date_latest}"

    categories = os.listdir(reviews_latest)
    categories = [c for c in categories if not c.startswith(".")]

    reviews = []
    for category in categories:
        products = os.listdir(f"{reviews_latest}/{category}")
        products = [p for p in products if not p.startswith(".")]

        for product in products:
            product_id = product[: product.index(".json")]
            with open(f"{reviews_latest}/{category}/{product}") as f:
                raw_reviews = json.load(f)["data"]
                for review in raw_reviews:
                    review_dict = {
                        "product_id": product_id,
                        "source_id": review["id"],
                        "date": datetime.strptime(review["date"], "%d.%m.%Y"),
                        "rating": review["rating"],
                        "comment_plus": review["comment"]["plus"],
                        "comment_minus": review["comment"]["minus"],
                        "comment_text": review["comment"]["text"],
                    }

                    review_rating = review["feedback"]["reviewsRating"]
                    if review_rating:
                        match = re.search("(\d+)\s+из\s+(\d+)", review_rating)
                        approved, rated = match.groups()
                        review_dict["review_approved"] = approved
                        review_dict["review_rated"] = rated

                    review_db = schemas.Review(**review_dict)
                    reviews.append(review_db.dict())

    with open(f"{DB_DUMPS_DIR}/reviews.json", "wb") as f:
        f.write(orjson.dumps(reviews))


@perf_logger
def parse_specs():
    products = os.listdir(SPECS_DIR)
    products = [p for p in products if not p.startswith(".")]
    dumped_products = _get_dumped_products()

    db_specs = []
    for product in products:
        with open(f"{SPECS_DIR}/{product}") as f:
            product = json.load(f)
            if int(product["id"]) not in dumped_products:
                continue

            specs = _parse_product_specs(product)
            db_specs.append(specs)

    with open(f"{DB_DUMPS_DIR}/specs.json", "w") as f:
        json.dump(db_specs, f, ensure_ascii=False)


def _parse_product_specs(product):
    details = _cleaned_dict(product["parsed_details"])
    return {
        "product_id": product["id"],
        "type": details.get("Тип"),
        "cpu": details.get("Процессор"),
        "hertz": details.get("Частота процессора", details.get("Производительность")),
        "cores": details.get(
            "Количество ядер процессора", details.get("Производительность")
        ),
        "gpu": details.get("Видеопроцессор", details.get("Видеокарта")),
        "ram": details.get(
            "Размер оперативной памяти", details.get("Оперативная память")
        ),
        "ssd": details.get(
            "Твердотельный диск (SSD)", details.get("Тип жесткого диска")
        ),
        "drive_size": details.get(
            "Объем жесткого диска", details.get("Общий объем накопителей")
        ),
        "camera": details.get("Фотокамера"),
        "battery": details.get("Аккумулятор"),
        "extra": json.dumps(details, ensure_ascii=False),
    }


def _cleaned_dict(d: dict):
    return {k: v.strip().replace("&nbsp", " ") for k, v in d.items()}


def _get_dumped_products():
    db_session = LocalSession()
    fetch_all = db_session.select_all("SELECT source_id FROM product")
    return {x[0] for x in fetch_all}


if __name__ == "__main__":
    parse_categories()
    parse_products()
    parse_reviews()
    parse_specs()
