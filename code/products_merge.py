import os;
import shutil
from datetime import datetime

today_date = datetime.today().strftime('%Y-%m-%d')
path = '../data/products/' + today_date + '/'
main = "../data/products/all_products.json";
arr = os.listdir('../data/products/' + today_date)
with open(main, "wb") as wfd:
    for file in arr:
        file = path + file
        with open(file, "rb") as fd:
            shutil.copyfileobj(fd, wfd)



