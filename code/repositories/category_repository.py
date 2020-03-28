import json

import psycopg2 as ps
import os
from decouple import config
from spiders.utils import _get_latest_date_in_dir

class CategoryRepository:
    def __init__(self):
        self.con = ps.connect(dbname= config('DB_NAME'), user=config("DB_USERNAME"),
                              password=config("DB_PASS"), host=config("DB_HOST"))
        # self.init_categories()
        self.insert_categories()

    def init_categories(self):
        try:
            with self.con:
                cur = self.con.cursor()
                cur.execute(
                    '''
                    CREATE TABLE IF NOT EXISTS CATEGORIES(
                    id        SERIAL PRIMARY KEY,
                    name      varchar(255)
                    )
                    ''')
                print('Categories was created successfully')
        except ps.Error as e:
            print('Could not create categories table: ', e)


    def insert_categories(self):
        x = os.chdir('../data/products/')
        last_date = _get_latest_date_in_dir(x)
        data = os.listdir(os.chdir(last_date))
        print(data)
        for file_name in data:
            with open(file_name, 'r') as f:
                dict = json.loads(f.readlines()[1][:-2])
                category_name = dict['categoryName']
                # insert into category this data
                with self.con:
                    cur = self.con.cursor()
                    try:
                        cur.execute("INSERT INTO CATEGORIES(name) VALUES (%s)", [category_name])
                        self.con.commit()
                    except ps.Error as e:
                        print('Could not insert data in categories table')
                f.close()






