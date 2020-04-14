import json
import logging
from typing import List

from app import utils
from app.constants import DB_DUMPS_DIR
from app.db.session import LocalSession
from app.utils import perf_logger

LOGGER = logging.getLogger(__name__)


def dump_into_tables():
    db = LocalSession()
    queries = db.insert_queries

    source_files = ("categories.json", "products.json", "reviews.json", "specs.json")
    queries = (
        queries.bulk_insert_categories.sql,
        queries.bulk_insert_products.sql,
        queries.bulk_insert_reviews.sql,
        queries.bulk_insert_specs.sql,
    )

    for source_file, query in zip(source_files, queries):
        insert_from_source(source_file, query)


@perf_logger
def insert_from_source(source_file, query):
    db = LocalSession()
    date = utils.parse_latest_date(DB_DUMPS_DIR)

    with open(f"{DB_DUMPS_DIR}/{date}/{source_file}") as f:
        data: List[dict] = json.load(f)

    db.bulk_insert_dicts(data, query)
