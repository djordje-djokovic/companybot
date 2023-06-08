import scrapy, os, json, pytesseract
from scrapy.crawler import CrawlerProcess
from scrapy.loader import ItemLoader
from scrapy.utils.project import get_project_settings
from bots.companieshouse_bot import CompaniesHouseBot
from bots.config import POPPLER_PATH

import unittest


def test_companieshouse(data):
    # handle completed requests here
    path = './data/test/07101408__MADE.COM DESIGN LTD.json'
    json_test = data

    with open(path) as file:
        # Load the JSON data from the file
        json_assert = json.load(file)

    assert json_assert['officer'] == json_test['officer']
    assert json_assert['insolvency'] == json_test['insolvency']
    assert json_assert['shareholding'] == json_test['shareholding']
    assert json_assert['incorporation'] == json_test['incorporation']
    assert json_assert['appointment'] == json_test['appointment']
    assert json_assert['sics'] == json_test['sics']
    assert json_assert['persons'].keys() == json_test['persons'].keys() # we only check the keys since the occupation list is not always appended in same order

# test MADE.COM
class TestMultiply(unittest.TestCase):
    def test_companieshouse(self):

        settings = get_project_settings()
        process = CrawlerProcess(settings)

        # settings

        company_id = '07101408'
        process.crawl(CompaniesHouseBot, company_id=company_id, poppler_path=POPPLER_PATH, write_enabled=False, callback_finish=test_companieshouse)
        process.start()  # the script will block here until the crawling is finished