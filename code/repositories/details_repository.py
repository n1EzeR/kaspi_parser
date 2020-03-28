import json

import psycopg2 as ps
import os
from decouple import config
import ast
from spiders.utils import _get_latest_date_in_dir

class DetailsRepository:
    def __init__(self):
        self.con = ps.connect(dbname= config('DB_NAME'), user=config("DB_USERNAME"),
                              password=config("DB_PASS"), host=config("DB_HOST"))
        # self.init_details()
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

    def get_values(self, details):
        dict = {
            "title" : '',
            "url" : '',
            "brand" : '',
            "rating" : 0,
            "reviews_quantity" : 0,
            "price" : 0,
            "cpu" : "",
            "gpu" : "",
            "series":"",
            "ram":"",
            "camera": ""
        }

        if ("title" in details):
            dict['title'] = details['title']
        if ("url" in details):
            dict['url'] = details['url']
        if ("brand" in details):
            dict['brand'] = details['brand']
        if ("rating" in details):
            dict['rating'] = details['rating']
        if ("reviews_quantity" in details):
            dict['reviews_quantity'] = details['reviews_quantity']
        if ("price" in details):
            dict['price'] = details['price']
        if ("parsed_details" in details):
            if ("Тип" in details["parsed_details"]):
                dict['type'] = details["parsed_details"]['Тип']
            if ("Процессор" in details["parsed_details"]):
                dict['cpu'] = details["parsed_details"]['Процессор']
            if ("Серия" in details["parsed_details"]):
                dict['series'] = details["parsed_details"]['Серия']
            if ("Объем оперативной памяти" in details["parsed_details"]):
                dict['ram'] = details["parsed_details"]['Объем оперативной памяти']
            if ("Фотокамера" in details["parsed_details"]):
                dict['camera'] = details["parsed_details"]['Фотокамера']
        else:
            if ("Тип" in details):
                dict['type'] = details['Тип']
            if ("Процессор" in details):
                dict['cpu'] = details['Процессор']
            if ("Видеопроцессор" in details):
                dict['cpu'] = details['Видеопроцессор']

        return dict
    def insert_details(self):
        x = os.chdir('../data/details/')
        data = os.listdir(x)
        for file_name in data:
            with open(file_name, 'r') as f:
                details_data = ast.literal_eval(f.read())
                for details in details_data:
                    with self.con:
                        cur = self.con.cursor()
                        dict = self.get_values(details)
                        try:
                            cur.execute('''
                                INSERT INTO DETAILS(title,url,brand,rating,reviews_quantity,
                                price,type,cpu,gpu,series,ram,camera)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ''',  [dict['title'], dict['url'], dict['brand'], dict['rating'],
                                   dict['reviews_quantity'], dict['price'], dict['type'],
                                   dict['cpu'], dict['gpu'], dict['series'], dict['ram'],
                                   dict['camera']
                                   ])


                            self.con.commit()
                        except ps.Error as e:
                            print('Could not insert data in categories table')
                    f.close()






