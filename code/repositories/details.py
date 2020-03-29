import ast
import os

import psycopg2 as ps
from decouple import config


class DetailsRepository:
    def __init__(self):
        self.con = ps.connect(dbname=config('DB_NAME'), user=config('DB_USERNAME'),
                              password=config('DB_PASS'), host=config('DB_HOST'))
        self.init_details()
        self.insert_details()

    def init_details(self):
        try:
            with self.con:
                cur = self.con.cursor()
                cur.execute(
                    '''
                    CREATE TABLE IF NOT EXISTS DETAILS(
                    id                  SERIAL PRIMARY KEY,
                    title               varchar(255),
                    url                 varchar(255),
                    brand               varchar(255),
                    rating              INT,
                    reviews_quantity    INT,
                    price               INT,
                    type                varchar(255) default null,
                    cpu                 varchar(255) default null ,
                    gpu                 varchar(255) default null,
                    series              varchar(255) default null,
                    ram                 varchar(255) default null,
                    camera              varchar(255) default null  
                    )
                    ''')
                print('Categories was created successfully')
        except ps.Error as e:
            print('Could not create categories table: ', e)

    def get_db_data(self, details):
        details_db_data = {
            'title': '',
            'url': '',
            'brand': '',
            'rating': 0,
            'reviews_quantity': 0,
            'price': 0,
            'cpu': '',
            'gpu': '',
            'series': '',
            'ram': '',
            'camera': ''
        }
        details_db_data['title'] = details.get('title', '')
        details_db_data['url'] = details.get('url', '')
        details_db_data['brand'] = details.get('brand', '')
        details_db_data['rating'] = details.get('rating', '')
        details_db_data['reviews_quantity'] = details.get('reviews_quantity', '')
        details_db_data['price'] = details.get('price', '')
        if ("parsed_details" in details):
            details_db_data['type'] = details["parsed_details"].get('Тип', '')
            details_db_data['cpu'] = details["parsed_details"].get('Процессор', '')
            details_db_data['series'] = details["parsed_details"].get('Серия', '')
            details_db_data['ram'] = details["parsed_details"].get('Объем оперативной памяти', '')
            details_db_data['camera'] = details["parsed_details"].get('Фотокамера', '')
        else:
            details_db_data['type'] = details.get('Тип', '')
            details_db_data['cpu'] = details.get('Процессор', '')
            details_db_data['cpu'] = details.get('Видеопроцессор', '')

        return details_db_data

    def insert_details(self):
        x = os.chdir('../data/details/')
        data = os.listdir(x)
        for file_name in data:
            with open(file_name, 'r') as f:
                details_data = ast.literal_eval(f.read())
                for details in details_data:
                    with self.con:
                        cur = self.con.cursor()
                        db_data = self.get_db_data(details)
                        try:
                            cur.execute('''
                                INSERT INTO DETAILS(title,url,brand,rating,reviews_quantity,
                                price,type,cpu,gpu,series,ram,camera)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ''', [db_data['title'], db_data['url'], db_data['brand'], db_data['rating'],
                                  db_data['reviews_quantity'], db_data['price'], db_data['type'],
                                  db_data['cpu'], db_data['gpu'], db_data['series'], db_data['ram'],
                                  db_data['camera']
                                  ])
                            self.con.commit()
                        except ps.Error as e:
                            print('Could not insert data in categories table')
                    f.close()
