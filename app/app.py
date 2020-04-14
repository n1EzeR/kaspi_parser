import logging

from scrapy.crawler import CrawlerProcess

from app.spiders import products, reviews, specs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def start_parse():
    start_products()
    start_reviews()
    start_details()


def start_details():
    process = CrawlerProcess()
    process.crawl(specs.SmartphoneSpecsSpider)
    process.crawl(specs.ComputerSpecsSpider)
    process.start()


def start_products():
    process = CrawlerProcess()
    process.crawl(products.ComputersSpider)
    process.crawl(products.SmartphonesSpider)
    process.crawl(products.PerfumeSpider)
    process.crawl(products.BeautySpider)
    process.crawl(products.BooksSpider)
    process.crawl(products.CarAudioSpider)
    process.crawl(products.CarElectronicsSpider)
    process.crawl(products.HeadphonesSpider)
    process.crawl(products.WatchesSpider)
    process.crawl(products.WearablesSpider)
    process.crawl(products.BigHomeAppliancesSpider)
    process.crawl(products.SmallHomeAppliancesSpider)
    process.crawl(products.ClimateEquipmentSpider)
    process.crawl(products.KitchenAppliancesSpider)
    process.crawl(products.MemoryCardsSpider)
    process.crawl(products.PortableSpeakersSpider)
    process.crawl(products.PowerBanksSpider)
    process.crawl(products.TiresSpider)
    process.start()


def start_reviews():
    process = CrawlerProcess()
    process.crawl(reviews.SmartphonesSpider)
    process.crawl(reviews.ComputersSpider)
    process.crawl(reviews.PerfumesSpider)
    process.crawl(reviews.BeautySpider)
    process.crawl(reviews.BooksSpider)
    process.crawl(reviews.CarAudioSpider)
    process.crawl(reviews.CarElectronicsSpider)
    process.crawl(reviews.HeadphonesSpider)
    process.crawl(reviews.WatchesSpider)
    process.crawl(reviews.WearablesSpider)
    process.crawl(reviews.BigHomeAppSpider)
    process.crawl(reviews.SmallHomeAppSpider)
    process.crawl(reviews.ClimateEquipmentSpider)
    process.crawl(reviews.KitchenHomeAppSpider)
    process.crawl(reviews.MemoryCardsSpider)
    process.crawl(reviews.PortableSpeakersSpider)
    process.crawl(reviews.PowerBanksSpider)
    process.crawl(reviews.TiresSpider)
    process.start()


if __name__ == "__main__":
    start_parse()
