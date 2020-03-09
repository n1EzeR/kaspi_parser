from scrapy.crawler import CrawlerProcess
from spiders import spider_products as products

def start():
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


if __name__ == '__main__':
    start()
