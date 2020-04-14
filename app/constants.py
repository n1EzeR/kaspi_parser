from decouple import config

DB_DUMPS_DIR = "../data/db_dumps"
PRODUCTS_DIR = "../data/products"
REVIEWS_DIR = "../data/reviews"
SPECS_DIR = "../data/details"
SQL_DIR = "sql"

REVIEWS_PER_REQUEST = 4000

HEADER_DEFAULT = {
    "Accept": "application/json, text/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ru,en;q=0.9,kk;q=0.8,es;q=0.7,ba;q=0.6",
    "Connection": "keep-alive",
    "Host": config("host"),
    "Referrer Policy": "no-referrer-when-downgrade",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) "
    "AppleWebKit/604.1.38 (KHTML, like Gecko) "
    "Version/11.0 Mobile/15A372 Safari/604.1",
}

HEADER_REVIEWS = {
    "Accept": "application/json, text/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ru,en;q=0.9,kk;q=0.8,es;q=0.7,ba;q=0.6",
    "Connection": "keep-alive",
    "Host": config("host"),
    "Cache-Control": "no-cache, no-store, max-age=0",
}
