import json
import psycopg2 as ps
import os
import ast
import logging
from decouple import config
from idna import unicode

from spiders.utils import _get_latest_date_in_dir


class ProductsRepository:

    logging.basicConfig(filename= 'db.log', level=logging.DEBUG)
    def __init__(self):
        self.con = ps.connect(dbname=config('DB_NAME'), user=config('DB_USERNAME'),
                              password=config("DB_PASS"), host=config('DB_HOST'))
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
                logging.info("Products was created successfully")
        except ps.Error as e:
            logging.error('Could not create products table: ', e)
    def get_detail_by_url(self, url):
        postgreSQL_select_Query = "select id from DETAILS where url = %s"
        try:
            with self.con:
                cur = self.con.cursor()
                cur.execute(postgreSQL_select_Query, [url])
                detail_id = cur.fetchone()
                if (detail_id == None): return detail_id
                return detail_id[0]
        except ps.Error as e:
            logging.error('Could not create products table: ', e)
    def get_category_by_name(self, name):
        postgreSQL_select_Query = "select id from CATEGORIES where name = %s"
        try:
            with self.con:
                cur = self.con.cursor()
                cur.execute(postgreSQL_select_Query, [name])
                category_id = cur.fetchone()
                if (category_id == None): return category_id
                return category_id[0]
        except ps.Error as e:
            logging.error('Could not create products table: ', e)




    def insert_products(self):
        x = os.chdir('../data/products/')
        last_date = _get_latest_date_in_dir(x)
        data = os.listdir(os.chdir(last_date))
        print(data)
        for file_name in data:
            with open(file_name, 'r', encoding='utf-8') as f:
                if (file_name == 'computers-list.json' or file_name == 'smartphones-list.json'):
                    products_data = json.load(f)
                    print(file_name)
                    for products in products_data:
                        url_of_product = products['shopLink']
                        name_of_product = products['categoryName']
                        details_id = self.get_detail_by_url(url_of_product)
                        category_id = self.get_category_by_name(name_of_product)

                        print(details_id)
                        with self.con:
                            cur = self.con.cursor()
                            try:
                                cur.execute("INSERT INTO PRODUCTS(details_id, category_id) VALUES (%s, %s)",
                                            [details_id, category_id])
                                self.con.commit()
                            except ps.Error as e:
                                logging.error('Could not insert data in categories table')
                        f.close()

