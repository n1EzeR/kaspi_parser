import json
import logging
import os

import psycopg2 as ps
from decouple import config

from spiders.utils import _get_latest_date_in_dir


class ProductsRepository:
    logging.basicConfig(level=logging.INFO)
    LOGGER = logging.getLogger()
    allowed_categories = {'computers-list.json', 'smartphones-list.json'}

    def __init__(self):
        self.con = ps.connect(dbname=config('DB_NAME'), user=config('DB_USERNAME'),
                              password=config('DB_PASS'), host=config('DB_HOST'))
        self.insert_products()

    def init_products(self):
        try:
            with self.con:
                cur = self.con.cursor()
                cur.execute('''
                            CREATE TABLE IF NOT EXISTS PRODUCTS(
                            id                  SERIAL PRIMARY KEY,
                            details_id          int,
                            category_id         int,
                            FOREIGN KEY (details_id) REFERENCES DETAILS (id) on delete set null ,
                            FOREIGN KEY (category_id) REFERENCES CATEGORIES(id) on delete set null 
                            )''')
                self.LOGGER.info("Products was created successfully")
        except ps.Error as exc:
            self.LOGGER.error(f'Could not create products table:{ exc }')

    def get_detail_by_url(self, url):
        query = "select id from DETAILS where url = %s"
        try:
            with self.con:
                cur = self.con.cursor()
                cur.execute(query, [url])
                detail_id = cur.fetchone()
                if (detail_id is None): return detail_id
                return detail_id[0]
        except ps.Error as exc:
            self.LOGGER.warning(f'Could not get details by url: { exc }')

    def get_category_by_name(self, name):
        query = "select id from CATEGORIES where name = %s"
        try:
            with self.con:
                cur = self.con.cursor()
                cur.execute(query, [name])
                category_id = cur.fetchone()
                if (category_id is None): return category_id
                return category_id[0]
        except ps.Error as exc:
            self.LOGGER.warning(f'Could not get category by name: { exc }')

    def insert_products(self):
        # x = os.chdir('../data/products/')
        try:
            last_date = _get_latest_date_in_dir('../data/products')
            data = os.listdir('../data/products/' + last_date)
        except ps.Error as exc:
            self.LOGGER.warning(f'Could not find data in products: { exc }')
        print(last_date)
        for file_name in data:
            with open(file_name, 'r', encoding='utf-8') as f:
                if (file_name in self.allowed_categories):
                    products_data = json.load(f)
                    for products in products_data:
                        product_url = products['shopLink']
                        product_name = products['categoryName']
                        details_id = self.get_detail_by_url(product_url)
                        category_id = self.get_category_by_name(product_name)

                        print(details_id)
                        with self.con:
                            cur = self.con.cursor()
                            try:
                                cur.execute("INSERT INTO PRODUCTS(details_id, category_id) VALUES (%s, %s)",
                                            [details_id, category_id])
                                self.con.commit()
                            except ps.Error as exc:
                                self.LOGGER.warning(f'Could not insert data into table: { exc }')
