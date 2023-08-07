from io import StringIO
from pathlib import Path
# from pdfminer.layout import LAParams
# from pdfminer.high_level import extract_text_to_fp, extract_pages, extract_text
import pytesseract, re, json, math, scrapy, requests, os, psycopg2
from datetime import datetime
from pdf2image import convert_from_path, convert_from_bytes
from fuzzywuzzy import fuzz, process
from sortedcontainers import SortedDict
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as BraveService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from scrapy.utils.project import get_project_settings
from scrapy import signals
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.loader import ItemLoader

from twisted.python.failure import Failure
from twisted.internet import reactor, defer

from uuid import uuid5, NAMESPACE_DNS
from .common import PendingStatus, DataSource, get_profile_uuid, logger, is_organization, get_persons
from .config import TESSDATA_PATH, TESSERACT_PATH, DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, POPPLER_PATH, BRAVE_PATH # these imports are required for setup

# pytesseract segmentation modes (--psm)
# Page segmentation modes:
#   0    Orientation and script detection (OSD) only.
#   1    Automatic page segmentation with OSD.
#   2    Automatic page segmentation, but no OSD, or OCR.
#   3    Fully automatic page segmentation, but no OSD. (Default)
#   4    Assume a single column of text of variable sizes.
#   5    Assume a single uniform block of vertically aligned text.
#   6    Assume a single uniform block of text.
#   7    Treat the image as a single text line.
#   8    Treat the image as a single word.
#   9    Treat the image as a single word in a circle.
#  10    Treat the image as a single character.
#  11    Sparse text. Find as much text as possible in no particular order.
#  12    Sparse text with OSD.
#  13    Raw line. Treat the image as a single text line,
#                         bypassing hacks that are Tesseract-specific

max_request_retries = 5
session = requests.Session()
# Create a retry instance
retry = Retry(total=max_request_retries, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
# Create an HTTP adapter with the retry settings
adapter = HTTPAdapter(max_retries=retry)
# Mount the adapter to the session
session.mount('http://', adapter)
session.mount('https://', adapter)


class CompaniesHouseErrorCodes():

    document_not_readable = {
    "error": {
        "code": 100,
        "message": "Document electronically not readable"
        }
    }

    company_not_found = {
    "error": {
        "code": 110,
        "message": "Company not found"
        }
    }


class CompaniesHouseBot(scrapy.Spider):

    name = 'find-and-update.company-information.service.gov.uk'
    allowed_domains = ['find-and-update.company-information.service.gov.uk']
    special_characters = '_—"?#¬|:;,=!%$£*&'
    __version__ = 'CompaniesHouseBot 0.9'
    def __init__(self, company_id, crunchbase_company_name, uuid, poppler_path, is_write_db=False, is_write_file=False, callback_finish=None):
        self.company_id = company_id
        self.crunchbase_company_name = crunchbase_company_name
        self.uuid = uuid
        self.registered_office_address = None
        self.company_status = None
        self.company_type = None
        self.incorporated_on = None
        self.sic = None
        self.is_write_file = is_write_file
        self.is_write_db = is_write_db
        self.page_number_filing = 1
        self.poppler_path = poppler_path
        #html urls
        self.base_url = f"https://find-and-update.company-information.service.gov.uk"
        self.company_url = f"{self.base_url}/company/{company_id}"
        self.filing_history_url = f"{self.company_url}/filing-history"
        self.officers_url = f"{self.company_url}/officers"
        self.persons_with_significant_control_url = f"{self.company_url}/persons-with-significant-control"
        self.insolvency_url = f"{self.company_url}/insolvency"
        self.filing_dict = {}
        self.callback_finish = callback_finish

        self.data = {}
        self.data['properties'] = {}
        self.data['cards'] = {}
        self.data['source'] = DataSource.companieshouse.name
        self.data['properties']['parsing_date'] = datetime.now().strftime('%Y-%m-%d')

        self.data['cards']['officer'] = {}
        self.data['cards']['insolvency'] = {}
        self.data['cards']['shareholding'] = {}
        self.data['cards']['incorporation'] = {}
        self.data['cards']['incorporation']['items'] = []
        self.data['cards']['appointment'] = {}

        self.parse_group_num = 5 # parse_company_info, parse_officers, parse_insolvency, parse_filing, parse_appointments
        self.parse_group_count = 0
        self.parse_appointments_count = 0

        self.conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        self.conn.autocommit = False
    @staticmethod
    def get_data_from_pending(uuids='*', uuids_parent='*', category_groups_list='*', country_codes='*', fr=datetime.max, to=datetime.max, force=False):

        assert type(uuids) == list or uuids == '*'
        assert type(uuids_parent) == list or uuids_parent == '*'
        assert type(category_groups_list) == list or category_groups_list == '*'
        assert type(country_codes) == list or country_codes == '*'
        assert type(fr) == datetime
        assert type(to) == datetime

        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )

        cursor = conn.cursor()

        uuids_parent_str = "'%'" if uuids_parent == '*' else ', '.join([f"'{item}'" for item in uuids_parent])
        uuids_str = "'%'" if uuids == '*' else ', '.join([f"'{item}'" for item in uuids])
        category_groups_list_str = "'%'" if category_groups_list == '*' else ', '.join([f"'%{item}%'" for item in category_groups_list])
        country_codes_str = "'%'" if country_codes == '*' else ', '.join([f"'{item}'" for item in country_codes])

        pending = f"" if force else f" and pending.status = '{PendingStatus.pending.name}' "

        query = f"SELECT pending.*, \"data\".data as crunchbase_data " \
            f"FROM pending " \
            f"INNER JOIN \"data\" ON pending.uuid = \"data\".uuid AND \"data\".source = '{DataSource.crunchbase.name}' " \
            f"WHERE pending.source = '{DataSource.companieshouse.name}' and " \
            f"pending.founded_on >= '{fr.strftime('%Y-%m-%dT%H:%M:%S')}' and " \
            f"pending.founded_on <= '{to.strftime('%Y-%m-%dT%H:%M:%S')}' and " \
            f"pending.category_groups_list::text ILIKE ANY (ARRAY[{category_groups_list_str}]) and " \
            f"pending.country_code::text ILIKE ANY (ARRAY[{country_codes_str}]) and " \
            f"pending.uuid_parent::text LIKE ANY (ARRAY[{uuids_parent_str}]) and " \
            f"pending.uuid::text LIKE ANY (ARRAY[{uuids_str}])" \
            f"{pending}" \
            f"ORDER BY pending.name COLLATE \"C\" ASC"

        # print(query)
        cursor.execute(query)
        rows = cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        results = [dict(zip(columns, row)) for row in rows]

        return results

    '''
    Returns the maximum ratio score for the closest matched name. This can be used to check if one of the founders 
    from the crunchbase list is in the companies house officers list 


    # names1 = ['John Smith', 'Jane Doe', 'Michael Brown']
    names1 = ['Bla Bla', 'Jane Dou']
    names2 = ['Jon Smith', 'Jane Dough', 'Mike Brown']

    matching_score = calculate_matching_score(names1, names2)
    print(matching_score)

    '''
    @staticmethod
    def calculate_matching_score(names1, names2):
        total_names1 = len(names1)
        total_names2 = len(names2)
        max_score = 0

        # Check if there is a perfect match
        if set(names1).intersection(names2):
            return 1

        # Iterate over names1 and find the closest match
        for name1 in names1:
            max_name_score = 0
            for name2 in names2:
                name_score = fuzz.ratio(name1, name2)
                if name_score > max_name_score:
                    max_name_score = name_score
            if max_name_score > max_score:
                max_score = max_name_score

        # Calculate matching score
        return max_score / 100  # Normalize score to range from 0 to 1


    '''
    Searches for a company name in companies house based on two criteria. search only by name might not be sufficient. if loops through max_search_results to match a company AND one of 
    its founders (supplied by crunchbase) to identify a company. If founders are not available it uses founding date to 
    confirm match.   
    '''
    @staticmethod
    def search(crunchbase_company_name, crunchbase_founder_names, crunchbase_founded_on, max_search_results = 10,
               score=0.85, brave_path=r'C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe'):

        # https://find-and-update.company-information.service.gov.uk/search?q=ANDFACTS+LIMITED
        crunchbase_company_name = crunchbase_company_name.strip()
        search_name = crunchbase_company_name.replace(' ', '+')
        url_search = f'https://find-and-update.company-information.service.gov.uk/search/companies?q={search_name}'
        r_search = session.get(url=url_search)
        html_search = etree.HTML(r_search.content)
        i = 0
        ul_element = html_search.xpath("//ul[@id='results']")

        try:
            if ul_element:
                for li_element in ul_element[0].iter('li'): #company_href in html_search.xpath("//li[@class='type-company']/h3/a/@href"):

                    company_href = li_element.find('.//a').get('href')
                    i += 1
                    if i > max_search_results:
                        break

                    id_company = company_href.split('/')[-1]
                    url_company = f'https://find-and-update.company-information.service.gov.uk/company/{id_company}'
                    url_officers = f'https://find-and-update.company-information.service.gov.uk/company/{id_company}/officers'
                    companieshouse_company_name = li_element.find('.//a').text.strip()

                    # check if companies house previous names are a match
                    matchin_previous_names_element = li_element.xpath('//p[contains(text(), "Matching previous names")]')
                    companieshouse_company_name_prev = None
                    if matchin_previous_names_element:
                        companieshouse_company_name_prev = matchin_previous_names_element[0].find('span').text.strip()

                    r_company = session.get(url=url_company)
                    html_company = etree.HTML(r_company.content)

                    crunchbase_founded_on_dt = datetime.strptime(crunchbase_founded_on, '%Y-%m-%d')
                    companieshouse_founded_on_dt = crunchbase_founded_on_dt
                    if html_company.xpath("//dd[@id='company-creation-date']/text()"):
                        companieshouse_founded_on_dt = datetime.strptime(CompaniesHouseBot.to_date(html_company.xpath("//dd[@id='company-creation-date']/text()")[0]),'%Y-%m-%d')


                    is_match_founded_fuzzy = abs((crunchbase_founded_on_dt - companieshouse_founded_on_dt).days) <= 365

                    match_company_fuzzy = fuzz.ratio(companieshouse_company_name.lower(), crunchbase_company_name.lower()) / 100
                    match_company_fuzzy = max(match_company_fuzzy, fuzz.ratio(companieshouse_company_name.lower(), crunchbase_company_name.lower() +  ' ltd') / 100)
                    match_company_fuzzy = max(match_company_fuzzy, fuzz.ratio(companieshouse_company_name.lower(), crunchbase_company_name.lower() + ' limited') / 100)
                    match_company_fuzzy = max(match_company_fuzzy, fuzz.ratio(companieshouse_company_name.lower(), crunchbase_company_name.lower().replace('limited', 'ltd')) / 100)
                    match_company_fuzzy = max(match_company_fuzzy, fuzz.ratio(companieshouse_company_name.lower(), crunchbase_company_name.lower().replace('ltd', 'limited')) / 100)

                    if companieshouse_company_name_prev:
                        match_company_fuzzy = max(match_company_fuzzy, fuzz.ratio(companieshouse_company_name_prev.lower(), crunchbase_company_name.lower()) / 100)
                        match_company_fuzzy = max(match_company_fuzzy, fuzz.ratio(companieshouse_company_name_prev.lower(), crunchbase_company_name.lower() + ' ltd') / 100)
                        match_company_fuzzy = max(match_company_fuzzy, fuzz.ratio(companieshouse_company_name_prev.lower(), crunchbase_company_name.lower() + ' limited') / 100)
                        match_company_fuzzy = max(match_company_fuzzy, fuzz.ratio(companieshouse_company_name_prev.lower(), crunchbase_company_name.lower().replace( 'limited', 'ltd')) / 100)
                        match_company_fuzzy = max(match_company_fuzzy, fuzz.ratio(companieshouse_company_name_prev.lower(), crunchbase_company_name.lower().replace( 'ltd', 'limited')) / 100)

                    is_match_company_fuzzy_weak = match_company_fuzzy >= 0.5
                    is_match_company_fuzzy_strong = match_company_fuzzy >= 0.75

                    is_match_company_name_exact = companieshouse_company_name.lower() == crunchbase_company_name.strip().lower() or \
                                                    companieshouse_company_name.lower() == crunchbase_company_name.strip().lower().replace('limited', 'ltd')  or \
                                                    companieshouse_company_name.lower() == crunchbase_company_name.strip().lower().replace('ltd', 'limited')

                    companieshouse_officers = []
                    msg = {'company_name_CB': crunchbase_company_name.upper(),
                           'company_name_CH': companieshouse_company_name.upper(),
                           'founded_on_CB': crunchbase_founded_on_dt.strftime('%Y-%m-%d'),
                           'founded_on_CH': companieshouse_founded_on_dt.strftime('%Y-%m-%d'),
                           'founders_CB': crunchbase_founder_names,
                           'officers_CH': companieshouse_officers,
                           'url_CH': url_company
                           }

                    # match by founder names
                    if crunchbase_founder_names and is_match_company_fuzzy_weak:
                        r_officers = session.get(url=url_officers)
                        html_officers = etree.HTML(r_officers.content)
                        companieshouse_officers = []

                        j = 1
                        for officer_element in html_officers.xpath('//div[@class="appointments-list"]/*'):
                            officer_name = CompaniesHouseBot.strip(html_officers.xpath(f"//div[@class='appointment-{j}']/h2/span/a/text()")[0]).title()
                            officer_string = etree.tostring(officer_element, encoding="unicode")
                            if not '<dt>Registration number</dt>' in officer_string:
                                # if it contains a registration_element, it is not an officer but a company. this does not work all the time as sometimes companies do not havea registration element
                                if is_organization(officer_name) == False:
                                    profile_name_split = officer_name.split(',')
                                    profile_name = profile_name_split[1].strip().split(' ')[0].strip() + ' ' + profile_name_split[0]
                                    profile_name = profile_name.title()
                                else:
                                    profile_name = officer_name #could still be a company it seems
                                companieshouse_officers.append(profile_name.title())
                            j += 1

                        msg['officers_CH'] = companieshouse_officers

                        officers_lower = [x.lower() for x in companieshouse_officers]
                        founders_lower = [x.lower() for x in crunchbase_founder_names]
                        matching_score = CompaniesHouseBot.calculate_matching_score(officers_lower, founders_lower)
                        if matching_score >= score:
                            logger.info(f'company: {crunchbase_company_name} successful match by FOUNDER NAME and DATE {json.dumps(msg)}')
                            return id_company, url_company
                        else:
                            logger.info(f'company: {crunchbase_company_name} unsuccessful match by FOUNDER NAME and DATE {json.dumps(msg)}')
                    # match by founding date
                    elif is_match_founded_fuzzy and is_match_company_fuzzy_strong:
                        logger.info(f'company: {crunchbase_company_name} successful match by COMPANY NAME and DATE {json.dumps(msg)}')
                        return id_company, url_company
                    elif is_match_company_name_exact:
                        logger.info(f'company: {crunchbase_company_name} successful match by COMPANY NAME {json.dumps(msg)}')
                        return id_company, url_company
                    else:
                        logger.info(f'company: {crunchbase_company_name} unsuccessful match by COMPANY NAME and DATE {json.dumps(msg)}')

        except Exception as ex:
            logger.error(f'company: {crunchbase_company_name} {str(ex)}')

        return None

    def get_requests(self):
        yield scrapy.Request(self.company_url, self.parse_company_info)
        yield scrapy.Request(self.officers_url, self.parse_officers)
        yield scrapy.Request(self.insolvency_url, self.parse_insolvency, errback=self.parse_insolvency)
        yield scrapy.Request(self.filing_history_url, self.parse_filing)

    def start_requests(self):
        #companieshouse requests
        yield from self.get_requests()


    def closed(self, reason):
        pass

    @staticmethod
    def to_date(dt_string):
        if dt_string is not None:
            return datetime.strptime(dt_string, '%d %B %Y').date().strftime('%Y-%m-%d')
        else:
            return None

    @staticmethod
    def to_date_short(dt_string):
        if dt_string is not None:
            return datetime.strptime(dt_string, '%d %b %Y').date().strftime('%Y-%m-%d')
        else:
            return None

    @staticmethod
    def to_date_dob(dt_string):
        if dt_string is not None:
            return datetime.strptime(dt_string, '%B %Y').date().strftime('%Y-%m-%d')
        else:
            return None

    @staticmethod
    def strip(string):
        if string is None:
            return None
        else:
            return string.strip()

    def parse_insolvency(self, response):

        logger.info(f'company: {self.crunchbase_company_name} url: {response.request.url}')

        insolvency_dict = {}

        if isinstance(response, Failure):
            pass # not a problem if endpoint cant be found
            # print('parse_insolvency could not find endpoint')
        else:
            # insolvency_dict['company_name'] = self.strip(response.xpath(f'//p[@class="heading-xlarge"]/text()').get())
            # insolvency_dict['company_id'] = self.strip(response.xpath(f'//p[@id="company-number"]/strong/text()').get())
            insolvency_dict['url'] = response.url
            # insolvency_dict['key'] = 'insolvency'
            insolvency_dict['items'] = []
            for i in range(1, 10):
                case_dict = {}
                case = response.xpath(f'//p[@class="heading-medium "][@id="case-{i}"]/text()').get()
                if case is not None:
                    case_dict['case'] = case
                    dt = self.strip(response.xpath(f'//dd[@id="administration-started-on_date_{i}"]/text()').get())
                    case_dict['administration_started'] = self.to_date(dt)
                    practitioners = []
                    for j in range(1, 10):
                        role = self.strip(response.xpath(f'//dt[@id="case_{i}_practitioner_{j}_role"]/text()').get())
                        if role is not None:
                            name = self.strip(response.xpath(f'//dd[@id="case_{i}_practitioner_{j}_name"]/text()').get())
                            address = self.strip(response.xpath(f'//span[@id="case_{i}_practitioner_{j}_address"]/text()').get())
                            name_without_titles = self.remove_titles(name.strip())
                            practitioners.append({'name': name_without_titles, 'address': address, 'role': role})
                        else:
                            break
                    case_dict['practitioners'] = practitioners
                    insolvency_dict['items'].append(case_dict)
                else:
                    break

        # yield {"key": 'insolvency', 'value': insolvency_dict}
        self.data['cards']['insolvency'] = insolvency_dict

        self.parse_group_count += 1

        logger.info(f'company: {self.crunchbase_company_name} completed. count: {self.parse_group_count}')
        logger.debug(f'company: {self.crunchbase_company_name} completed. data: {json.dumps(insolvency_dict)}')

        if self.parse_group_count == self.parse_group_num:
            self.finished(self.data)
            if self.callback_finish: self.callback_finish(self.data)

    def parse_company_info(self, response):

        logger.info(f'company: {self.crunchbase_company_name} url: {response.request.url}')

        company_dict = {}
        company_dict['company_name'] = self.strip(response.xpath(f'//p[@class="heading-xlarge"]/text()').get()).title()
        company_dict['uuid'] = self.uuid
        company_dict['company_id'] = self.strip(response.xpath(f'//p[@id="company-number"]/strong/text()').get())
        company_dict['url'] = response.url
        company_dict['key'] = 'company'
        company_dict['address'] = self.strip(response.xpath(f'//dd[@class="text data"]/text()').get())
        company_dict['status'] = self.strip(response.xpath(f'//dd[@class="text data"][@id="company-status"]/text()').get())
        company_dict['type'] = self.strip(response.xpath(f'//dd[@class="text data"][@id="company-type"]/text()').get())
        dt = self.strip(response.xpath(f'//dd[@class="data"][@id="company-creation-date"]/text()').get())
        company_dict['incorporated_on'] = self.to_date(dt)
        company_dict['company_name'] = self.strip(response.xpath(f'//p[@class="heading-xlarge"]/text()').get()).title()
        sics = []
        for i in range(0, 10):
            sic = response.xpath(f'//span[@id="sic{i}"]/text()').get()
            if sic is None:
                break
            else:
                sics.append(self.strip(sic))
        company_dict['sics'] = sics

        # yield {"key": 'company', 'value': company_dict}
        self.data['properties'].update(company_dict)

        self.parse_group_count += 1
        logger.info(f'company: {self.crunchbase_company_name} completed. count: {self.parse_group_count}')
        logger.debug(f'company: {self.crunchbase_company_name} completed. data: {json.dumps(company_dict)}')
        if self.parse_group_count == self.parse_group_num:
            self.finished(self.data)
            if self.callback_finish: self.callback_finish(self.data)

    def parse_officers(self, response):

        logger.info(f'company: {self.crunchbase_company_name} url: {response.request.url}')

        # print('parse_officers')
        # url = 'https://find-and-update.company-information.service.gov.uk/company/07101408/officers'
        # lst = response.xpath('//div[@class="appointments-list"]')
        # officer_response = response.xpath('//div[@class="appointment-1"]')
        company_dict = {}

        # company_dict['company_name'] = self.strip(response.xpath(f'//p[@class="heading-xlarge"]/text()').get())
        # company_dict['company_id'] = self.strip(response.xpath(f'//p[@id="company-number"]/strong/text()').get())
        company_dict['url'] = response.url
        # company_dict['key'] = 'officer'

        officer_list = []

        i = 1

        requested_url = []
        allowed_i = [] # we need to filter since some names could be appearing more than once in the page with the same url. this will breka the parse_appointments logic
        for selector in response.xpath('//div[@class="appointments-list"]/*'):
            url_relative = self.strip(response.xpath(f'//span[@id="officer-name-{i}"]/a[@class="govuk-link"]/@href').get())
            url = self.base_url + url_relative
            if url not in requested_url:
                allowed_i.append(i)
                requested_url.append(url)
            i += 1

        self.parse_appointments_num = len(requested_url)

        i = 1
        for selector in response.xpath('//div[@class="appointments-list"]/*'):
            if i in allowed_i:
                officer_dict = {}
                name_without_titles = self.remove_titles(self.strip(response.xpath(f'//span[@id="officer-name-{i}"]/a/text()').get()).title())
                officer_dict['name'] = name_without_titles
                url_relative = self.strip(response.xpath(f'//span[@id="officer-name-{i}"]/a[@class="govuk-link"]/@href').get())
                officer_dict['appointments_url'] = url = self.base_url + url_relative

                yield scrapy.Request(url, callback=self.parse_appointments, meta={'name': officer_dict['name']})
                requested_url.append(url)

                # out_dict['appointments']['items'] = items
                officer_dict['address'] = self.strip(response.xpath(f'//dd[@class="data"][@id="officer-address-value-{i}"]/text()').get())
                officer_dict['status'] = self.strip(response.xpath(f'//span[@id="officer-status-tag-{i}"][@class="status-tag font-xsmall"]/text()').get())
                officer_dict['role'] = self.strip(response.xpath(f'//dd[@id="officer-role-{i}"][@class="data"]/text()').get())

                dt = self.strip(response.xpath(f'//dd[@id="officer-appointed-on-{i}"][@class="data"]/text()').get())
                officer_dict['appointed_on'] = self.to_date(dt)

                dt = self.strip(response.xpath(f'//dd[@id="officer-resigned-on-{i}"][@class="data"]/text()').get())
                officer_dict['resigned_on'] = self.to_date(dt)

                dt = self.strip(response.xpath(f'//dd[@id="officer-date-of-birth-{i}"][@class="data"]/text()').get())
                officer_dict['date_of_birth'] = self.to_date_dob(dt)

                officer_dict['nationality'] = self.strip(response.xpath(f'//dd[@id="officer-nationality-{i}"][@class="data"]/text()').get())
                officer_dict['residence'] = self.strip(response.xpath(f'//dd[@id="officer-country-of-residence-{i}"][@class="data"]/text()').get())
                officer_dict['occupation'] = self.strip(response.xpath(f'//dd[@id="officer-occupation-{i}"][@class="data"]/text()').get())
                officer_list.append(officer_dict)
            i += 1

        officer_list_sorted = sorted(officer_list, key=lambda x: x['name'])
        company_dict['items'] = officer_list_sorted

        # yield {"key": 'officer', 'value': company_dict}
        self.data['cards']['officer'] = company_dict

        self.parse_group_count += 1
        logger.info(f'company: {self.crunchbase_company_name} completed. count: {self.parse_group_count}')
        logger.debug(f'company: {self.crunchbase_company_name} completed. data: {json.dumps(company_dict)}')
        if self.parse_group_count == self.parse_group_num:
            self.finished(self.data)
            if self.callback_finish: self.callback_finish(self.data)

    def write(self, uuid, name, json_rest_api):

        try:
            cursor = self.conn.cursor()

            table_name = 'data'
            dt = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            data_str = json.dumps(json_rest_api)

            d = {'uuid': uuid, 'uuid_parent': uuid, 'name': name, 'source': DataSource.companieshouse.name, 'version':self.__version__, 'created_at': dt, 'updated_at': dt, 'data': data_str}
            columns = d.keys()
            values = tuple(d.values())

            # write data to db
            query_data = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (uuid, source) DO UPDATE SET data = EXCLUDED.data, version = EXCLUDED.version, updated_at = EXCLUDED.updated_at"
            # query_data = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ('{values[0]}', '{values[1]}', '{values[2]}', '{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}')"
            # print(f'query_data 1: {query_data}')
            cursor.execute(query_data, values)
            # print(f'query_data 2: {query_data}')

            # update pending table
            query_pending = f"UPDATE pending SET status = '{PendingStatus.completed.name}', updated_at = '{dt}'  WHERE uuid = '{uuid}' AND source = '{DataSource.companieshouse.name}'"

            # print(f'query_pending 1: {query_data}')
            cursor.execute(query_pending)
            # print(f'query_pending 2: {query_data}')

            # write pending table for linkedin. this is done so that we immidiately have pending record for linked in officers
            # when a new linkedin record is written if a record is already existing in
            # pending table DO NOTHING ON CONFLICT
            columns_companieshouse = ['uuid', 'uuid_parent', 'name', 'legal_name', 'category_groups_list', 'founded_on', 'source',
                                       'status', 'version', 'created_at', 'updated_at']

            source = DataSource.linkedin.name
            status = PendingStatus.pending.name
            version = ''
            created_at = dt
            updated_at = dt

            persons = get_persons(self.data)
            # convert to usable format (input into LinkedInBot)

            logger.info(f'company: {self.crunchbase_company_name} get_persons')
            logger.debug(f'company: {self.crunchbase_company_name} get_persons. data: {json.dumps(persons)}')

            for item in persons['items']:
                profile_name = item['name'].title()
                full_name = item['full_name'].title()

                date_of_birth = item['date_of_birth']
                # add born on to guid
                uuid_profile_str = get_profile_uuid(full_name, uuid)
                uuid_profile = uuid5(NAMESPACE_DNS, uuid_profile_str)
                occupations = item['occupation']

                values_pending_companieshouse = [str(uuid_profile), uuid, profile_name, full_name, occupations, date_of_birth, source, status,
                                                  version, created_at, updated_at]

                query_pending_linkedin = f"INSERT INTO pending ({', '.join(columns_companieshouse)}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
                                                f"ON CONFLICT DO NOTHING"

                cursor.execute(query_pending_linkedin, values_pending_companieshouse)

        except psycopg2.Error as e:
            logger.error(f"company: {self.crunchbase_company_name} write executing queries: {str(e)}")
            self.conn.rollback()
        except Exception as e:
            logger.error(f"company: {self.crunchbase_company_name} write: {str(e)}")
        else:
            self.conn.commit()
            logger.info(f'company: {self.crunchbase_company_name} writing data successful from source: {DataSource.companieshouse.name} status: {PendingStatus.completed.name} company: {name}')
        finally:
            cursor.close()


    def write_to_file(self):
        # sic = self.data['sics'][0]
        company_id = self.data['properties']['company_id']
        company_name = self.data['properties']['company_name']
        file_name = f'{company_id}__{company_name}.json'
        dir = f'../data/companieshouse'
        path = f'{dir}/{file_name}'
        # print('CLOSED', str(path))
        Path(dir).mkdir(parents=True, exist_ok=True)
        os.path.isfile(path)

        with open(path, 'w') as fp:
            json.dump(self.data, fp, indent=4)


    # joins officers and shareholders to get a list of all unique physical persons
    def finished(self, data):
        # self.get_persons(data)
        # get unique shareholder persons
        logger.info(f'company: {self.crunchbase_company_name} parse_finished')
        # print('parse_before_write', json.dumps(self.data))
        if self.is_write_file:
            self.write_to_file()

        if self.is_write_db:
            self.write(self.uuid, self.crunchbase_company_name, self.data)

        # print('parse_after_write')

    def parse_appointments(self, response):

        officer_name = self.strip(response.xpath('//h1[@id="officer-name"][@class="heading-xlarge"]/text()').get()).title()
        logger.info(f'company: {self.crunchbase_company_name} officer: {officer_name} [{self.parse_appointments_count}/{self.parse_appointments_num}] url: {response.request.url}')

        appointment_dict = {}
        appointment_dict['name'] = response.meta.get('name').title()
        appointment_dict['officer_name'] = officer_name
        appointment_dict['url'] = self.strip(response.url)
        appointment_dict['key'] = 'appointment'
        dt = self.strip(response.xpath('//dd[@id="officer-date-of-birth-value"][@class="data"]/text()').get())
        appointment_dict['date_of_birth'] = self.to_date_dob(dt)

        appointment_list = []

        i = 1
        for selector in response.xpath('//div[@class="appointments-list"]/*'):
            d = {}
            company_name = self.strip(response.xpath(f'//h2[@id="company-name-{i}"]/a/text()').get())
            company_id = self.strip(company_name[company_name.find('(')+1:company_name.find(')')])
            d['company_name'] = self.strip(company_name[:company_name.find('(')]).title()
            d['company_id'] = company_id

            # out_dict['appointments']['items'] = items
            d['company_status'] = self.strip(response.xpath(f'//dd[@id="company-status-value-{i}"][@class="data"]/text()').get())
            d['address'] = self.strip(response.xpath(f'//dd[@id="correspondence-address-value-{i}"][@class="data"]/text()').get())
            d['role'] = self.strip(response.xpath(f'//dd[@id="appointment-type-value{i}"][@class="data"]/text()').get())
            d['status'] = self.strip(response.xpath(f'//span[@id="{company_id}-appointment-status-tag-{i}"][@class="status-tag font-xsmall"]/text()').get())

            dt = self.strip(response.xpath(f'//dd[@id="appointed-value{i}"][@class="data"]/text()').get())
            d['appointed_on'] = self.to_date(dt)

            dt = self.strip(response.xpath(f'//dd[@id="resigned-value-{i}"][@class="data"]/text()').get())
            d['resigned_on'] = self.to_date(dt)

            d['nationality'] = self.strip(response.xpath(f'//dd[@id="nationality-value{i}"][@class="data"]/text()').get())
            d['residence'] = self.strip(response.xpath(f'//dd[@id="country-of-residence-value{i}"][@class="data"]/text()').get())
            d['occupation'] = self.strip(response.xpath(f'//dd[@id="occupation-value-{i}"][@class="data"]/text()').get())

            appointment_list.append(d)

            i += 1

        appointment_dict['items'] = appointment_list

        if len(self.data['cards']['appointment']) == 0:
            self.data['cards']['appointment']['items'] = []

        self.data['cards']['appointment']['items'].append(appointment_dict)

        self.parse_appointments_count += 1

        logger.info(
            f'company: {self.crunchbase_company_name} completed: {self.crunchbase_company_name} [{self.parse_appointments_count}/{self.parse_appointments_num}] url: {response.request.url}')

        if self.parse_appointments_count == self.parse_appointments_num:
            self.data['cards']['appointment']['items'] = sorted(self.data['cards']['appointment']['items'], key=lambda x: x['name'])
            self.parse_group_count += 1
            logger.info(f'company: {self.crunchbase_company_name} completed. count: {self.parse_group_count}')
            if self.parse_group_count == self.parse_group_num:
                self.finished(self.data)
                if self.callback_finish: self.callback_finish(self.data)


        # print(officer_dict)
        # yield {"key": 'appointment', 'value': officer_dict}
    def parse_filing(self, response):

        logger.info(f'company: {self.crunchbase_company_name} page: {self.page_number_filing} url: {response.request.url}')

        if not self.filing_dict:
            # self.filing_dict['company_name'] = self.strip(response.xpath(f'//p[@class="heading-xlarge"]/text()').get())
            # self.filing_dict['company_id'] = self.strip(response.xpath(f'//p[@id="company-number"]/strong/text()').get())
            self.filing_dict['url'] = response.url
            # self.filing_dict['key'] = 'filing'
            self.filing_dict['items'] = []

        rows = response.xpath(f'//table[@id="fhTable"][@class="full-width-table"]/tr[*]')
        if len(rows) <= 1: #means the table is not existing for this page and we return the stored data
            # since this is a recursive function. this is the last time it is called
            self.parse_group_count += 1
            logger.info(f'company: {self.crunchbase_company_name} page: {self.page_number_filing} completed. count: {self.parse_group_count}')
            logger.debug(f'company: {self.crunchbase_company_name} page: {self.page_number_filing} data: {json.dumps(self.filing_dict)}')
            if self.parse_group_count == self.parse_group_num:
                self.finished(self.data)
                if self.callback_finish: self.callback_finish(self.data)

            return

        # print('parse_filing_page:', self.page_number)
        i = 1
        try:
            for row in rows:
                url_relative = row.xpath(
                    f'//table[@id="fhTable"][@class="full-width-table"]/tr[{i}]/td[4]/div/a/@href').get()
                if url_relative:
                    url = self.base_url + url_relative
                    description = self.strip(row.xpath(f'//table[@id="fhTable"][@class="full-width-table"]/tr[{i}]/td[3]/strong/text()').get())
                    dt = self.strip(row.xpath(f'//table[@id="fhTable"][@class="full-width-table"]/tr[{i}]/td[1]/text()').get())
                    received_dt = self.to_date_short(dt)
                    # print(description, url)
                    if description:
                        if 'confirmation statement' in description.lower():
                            d = self.strip(row.xpath(f'//table[@id="fhTable"][@class="full-width-table"]/tr[{i}]/td[3]').get())
                            d = d[d.find('made on') + 8:]
                            d = d[:d.find('\n')]

                            if 'with no updates' in d:
                                update = False
                                filing_dt = self.to_date(d[:d.find('with no updates')-1])
                            elif 'with updates' in d:
                                update = True
                                filing_dt = self.to_date(d[:d.find('with updates')-1])
                            else:
                                raise ValueError('updates not found in confirmation statement string')

                            confirmation_statement = self.parse_confirmation_statement_ocr(url, filing_dt, self.poppler_path)
                            self.data['cards']['shareholding'][received_dt] = {}
                            self.data['cards']['shareholding'][received_dt]['source'] = 'Confirmation Statement'
                            self.data['cards']['shareholding'][received_dt]['url'] = url
                            self.data['cards']['shareholding'][received_dt]['received_date'] = received_dt
                            self.data['cards']['shareholding'][received_dt]['filing_date'] = filing_dt
                            self.data['cards']['shareholding'][received_dt]['update'] = update
                            self.data['cards']['shareholding'][received_dt]['items'] = confirmation_statement['FULL DETAILS OF SHAREHOLDERS']

                        elif 'annual return' in description.lower():
                            d = self.strip(row.xpath(f'//table[@id="fhTable"][@class="full-width-table"]/tr[{i}]/td[3]').get())
                            d = d[d.find('made up to') + 11:]
                            d = d[:d.find('\n')]

                            if 'with' in d:
                                filing_dt = self.to_date(d[:d.find('with') - 1])
                            else:
                                raise ValueError('updates not found in confirmation statement string')

                            update = True
                            annual_return_dict = self.parse_annual_return_ocr(url, filing_dt, self.poppler_path)
                            self.data['cards']['shareholding'][received_dt] = {}
                            self.data['cards']['shareholding'][received_dt]['source'] = 'Annual Return'
                            self.data['cards']['shareholding'][received_dt]['url'] = url
                            self.data['cards']['shareholding'][received_dt]['filing_date'] = filing_dt
                            self.data['cards']['shareholding'][received_dt]['received_date'] = received_dt
                            self.data['cards']['shareholding'][received_dt]['update'] = update
                            self.data['cards']['shareholding'][received_dt]['items'] = annual_return_dict['FULL DETAILS OF SHAREHOLDERS']

                        elif 'incorporation' in description.lower():
                            update = True
                            incorporation = self.parse_incorporation_ocr(url, received_dt, self.poppler_path)
                            if 'error' not in incorporation:
                                self.data['cards']['incorporation']['url'] = url
                                self.data['cards']['incorporation']['received_date'] = received_dt
                                self.data['cards']['incorporation']['update'] = update
                                self.data['cards']['incorporation']['items'] = incorporation['INITIAL SHAREHOLDINGS']
                                self.data['cards']['shareholding'][received_dt] = {}
                                self.data['cards']['shareholding'][received_dt]['source'] = 'Incorporation'
                                self.data['cards']['shareholding'][received_dt]['url'] = url
                                self.data['cards']['shareholding'][received_dt]['filing_date'] = received_dt
                                self.data['cards']['shareholding'][received_dt]['received_date'] = received_dt
                                self.data['cards']['shareholding'][received_dt]['update'] = update
                                self.data['cards']['shareholding'][received_dt]['items'] = incorporation['INITIAL SHAREHOLDINGS']
                            else:
                                self.data['cards']['incorporation']['url'] = url
                                self.data['cards']['incorporation']['error'] = incorporation['error']
                i += 1

        except Exception as ex:
            logger.error(f'company: {self.crunchbase_company_name} page: {self.page_number_filing} {str(line)} {str(lst)} {str(ex)}')
            raise ValueError(str(ex), description, url)

        self.page_number_filing +=1
        next_page_url = f'https://find-and-update.company-information.service.gov.uk/company/{self.company_id}/filing-history?page={self.page_number_filing}'  #response.xpath(f'//ul[@class="pager"]/li[{self.page_number}]/a/@href').get()
        yield scrapy.Request(next_page_url, self.parse_filing)

    def parse_confirmation_statement_ocr(self, url, dt, poppler_path):
        # output_string = StringIO()
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__07101408__20141210.pdf'
        # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__07110878__20171222.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__07637003__20130517.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__08070525__20140516.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__08458210__20140322.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__08905651__20160221.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__09147492__20150725.pdf'
        # # Convert PDF to image
        # pages = convert_from_path(p)

        logger.info(f'company: {self.crunchbase_company_name} page: {self.page_number_filing} date: {str(dt)} url: {url}')

        r = session.get(url=url)
        pages = convert_from_bytes(r.content, poppler_path=poppler_path)

        # Read text from images using OCR

        # COMPANY INFORMATION
        pass
        # STATEMENT OF CAPITAL (SHARE CAPITAL)
        pass
        # STATEMENT OF CAPITAL (TOTALS)
        pass
        # FULL DETAILS OF SHAREHOLDERS
        pass
        # PERSON WITH SIGNIFICANT CONTROL (PSC)
        pass
        # CONFIRMATION STATEMENT
        pass
        # AUTHORISATION

        sections = {'COMPANY INFORMATION': [], 'STATEMENT OF CAPITAL (SHARE CAPITAL)':[],
                    'STATEMENT OF CAPITAL (TOTALS)': [], 'FULL DETAILS OF SHAREHOLDERS': [],
                    'PERSON WITH SIGNIFICANT CONTROL (PSC)': [], 'CONFIRMATION STATEMENT': [],
                    'AUTHORISATION': []
                    }

        key = 'COMPANY INFORMATION'

        for img in pages:
            page = pytesseract.image_to_string(img, config='--psm 4').split('\n')

            for line in page:

                # COMPANY INFORMATION
                pass
                # OFFICERS OF THE COMPANY
                if 'STATEMENT OF CAPITAL (SHARE CAPITAL)'.lower() in line.lower():
                    key = 'STATEMENT OF CAPITAL (SHARE CAPITAL)'
                # STATEMENT OF CAPITAL (TOTALS)
                if 'STATEMENT OF CAPITAL (TOTALS)'.lower() in line.lower():
                    key = 'STATEMENT OF CAPITAL (TOTALS)'
                # FULL DETAILS OF SHAREHOLDERS
                if 'FULL DETAILS OF SHAREHOLDERS'.lower() in line.lower():
                    key = 'FULL DETAILS OF SHAREHOLDERS'
                # AUTHORISATION
                if 'PERSON WITH SIGNIFICANT CONTROL (PSC)'.lower() in line.lower():
                    key = 'PERSON WITH SIGNIFICANT CONTROL (PSC)'
                # AUTHORISATION
                if line.startswith('Confirmation Statement'):
                    key = 'CONFIRMATION STATEMENT'
                # AUTHORISATION
                if line.startswith('Authorisation'):
                    key = 'AUTHORISATION'
                sections[key].append(line)

        # COMPANY INFORMATION ARE not parsed from this document since its information is available on website. the only additional data that this document posseses are historical addresses which are not important at this stage
        # OFFICERS OF THE COMPANY are not parsed from this document since its full history is available on website
        # STATEMENT OF CAPITAL (SHARE CAPITAL) are not parsed as most information can be deducted from FULL DETAILS OF SHAREHOLDERS
        # STATEMENT OF CAPITAL (TOTALS)': [] are not parsed as most information can be deducted from FULL DETAILS OF SHAREHOLDERS

        content = {
                    'FULL DETAILS OF SHAREHOLDERS': []
                    }

        # this defines an empty page
        if len(sections['FULL DETAILS OF SHAREHOLDERS']) <= 5:
            logger.info(f'company: {self.crunchbase_company_name} page: {self.page_number_filing} completed. date: {str(dt)} url: {url}')
            return content


        try:
            for key, lst in sections.items():

                i = 0
                for line in lst:

                    if key == 'FULL DETAILS OF SHAREHOLDERS':

                        # some data might still have : in the name. such as Shareholding: --> remove it

                        first_colon = max(line.find(":"), line.find(";")) #ocr can mistake ; for :
                        shareholding_line = line[first_colon + 1:] if first_colon != -1 else line

                        if 'shares held as at the date' in shareholding_line:
                            shareholding_line = shareholding_line.split('shares held as at the date')[0].strip()

                            match = re.search(r"\d+", shareholding_line)
                            shares = int(match.group())  # Get the matched number
                            start_pos = match.start()  # Starting position of the number
                            end_pos = match.end()  # Ending position of the number
                            share_type = shareholding_line[end_pos:].strip()

                            # print('shares held as at the date', str(shareholding_line), str(shares), str(share_type))

                            #find shareholder

                            j = 1
                            shareholder = ''
                            searching = True
                            while searching: #look ten lines ahead to find the next shareholder

                                next_line = lst[i + j].strip()
                                if next_line.lower().startswith('name'):
                                    first_colon = max(next_line.find(":"), next_line.find(";"))
                                    next_line = next_line[first_colon + 1:] if first_colon != -1 else next_line

                                    shareholder = next_line
                                    searching = False
                                j += 1

                            if shareholder == '':
                                raise ValueError('shareholder not found')
                            is_company = is_organization(shareholder)
                            name_without_titles = self.remove_titles(shareholder.strip())
                            content['FULL DETAILS OF SHAREHOLDERS'].append({'name': name_without_titles, 'share_type': share_type.strip(), 'shares': shares, 'is_company': is_company})
                    i += 1
        except Exception as ex:
            logger.error(f'company: {self.crunchbase_company_name} page: {self.page_number_filing} {str(line)} {str(lst)} {str(ex)}')
        logger.info(f'company: {self.crunchbase_company_name} page: {self.page_number_filing} completed. {str(dt)} {url}')
        return content

    def parse_annual_return_ocr(self, url, dt, poppler_path):
        # output_string = StringIO()
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__07101408__20141210.pdf'
        # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__07110878__20171222.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__07637003__20130517.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__08070525__20140516.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__08458210__20140322.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__08905651__20160221.pdf'
        # # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\annual_return\\annual_return_AR01__09147492__20150725.pdf'
        # # Convert PDF to image
        # pages = convert_from_path(p)

        logger.info(f'company: {self.crunchbase_company_name} date: {str(dt)} url: {url}')

        r = session.get(url=url)
        pages = convert_from_bytes(r.content, poppler_path=poppler_path)

        # Read text from images using OCR

        sections = {'COMPANY INFORMATION': [], 'OFFICERS OF THE COMPANY':[], 'STATEMENT OF CAPITAL (SHARE CAPITAL)':[],
                    'STATEMENT OF CAPITAL (TOTALS)': [], 'FULL DETAILS OF SHAREHOLDERS': [], 'AUTHORISATION': [],
                    }

        key = 'COMPANY INFORMATION'

        for img in pages:
            page = pytesseract.image_to_string(img, config='--psm 4').split('\n')

            for line in page:

                # COMPANY INFORMATION
                pass
                # OFFICERS OF THE COMPANY
                if 'OFFICERS OF THE COMPANY'.lower() in line.lower():
                    key = 'OFFICERS OF THE COMPANY'
                # STATEMENT OF CAPITAL (SHARE CAPITAL)
                if 'STATEMENT OF CAPITAL (SHARE CAPITAL)'.lower() in line.lower():
                    key = 'STATEMENT OF CAPITAL (SHARE CAPITAL)'
                # STATEMENT OF CAPITAL (TOTALS)
                if 'STATEMENT OF CAPITAL (TOTALS)'.lower() in line.lower():
                    key = 'STATEMENT OF CAPITAL (TOTALS)'
                # FULL DETAILS OF SHAREHOLDERS
                if 'FULL DETAILS OF SHAREHOLDERS'.lower() in line.lower():
                    key = 'FULL DETAILS OF SHAREHOLDERS'
                # AUTHORISATION
                if line.lower().startswith('AUTHORISATION'.lower()):
                    key = 'AUTHORISATION'
                sections[key].append(line)

        # COMPANY INFORMATION ARE not parsed from this document since its information is available on website. the only additional data that this document posseses are historical addresses which are not important at this stage
        # OFFICERS OF THE COMPANY are not parsed from this document since its full history is available on website
        # STATEMENT OF CAPITAL (SHARE CAPITAL) are not parsed as most information can be deducted from FULL DETAILS OF SHAREHOLDERS
        # STATEMENT OF CAPITAL (TOTALS)': [] are not parsed as most information can be deducted from FULL DETAILS OF SHAREHOLDERS

        content = {
                    'FULL DETAILS OF SHAREHOLDERS': []
                    }

        for key, lst in sections.items():

            i = 0
            for line in lst:

                if key == 'FULL DETAILS OF SHAREHOLDERS':

                    # some data might still have : in the name. such as Shareholding: --> remove it

                    first_colon = max(line.find(":"), line.find(";")) #ocr can mistake ; for :
                    shareholding_line = line[first_colon + 1:] if first_colon != -1 else line

                    if 'shares held as at' in shareholding_line:
                        shareholding_line = shareholding_line.split('shares held as at')[0].strip()
                        first_number = re.search(r"\d", shareholding_line).start()
                        shareholding = shareholding_line[first_number:].split(' ')
                        shares = int(shareholding[0])
                        share_type = ' '.join(shareholding[1:])

                        #find shareholder

                        j = 1
                        shareholder = ''
                        searching = True
                        while searching: #look ten lines ahead to find the next shareholder

                            next_line = lst[i + j].strip()
                            if next_line.lower().startswith('name'):
                                first_colon = max(next_line.find(":"), next_line.find(";"))
                                next_line = next_line[first_colon + 1:] if first_colon != -1 else next_line

                                shareholder = next_line
                                searching = False
                            j += 1

                        if shareholder == '':
                            raise ValueError('shareholder not found')

                        name_without_titles = self.remove_titles(shareholder.strip())
                        is_company = is_organization(shareholder)
                        content['FULL DETAILS OF SHAREHOLDERS'].append({'name': name_without_titles, 'share_type': share_type.strip(), 'shares': shares, 'is_company': is_company})
                i += 1

        return content

    # gets a substring of a string after a number of symbols. i.e. Shareholder x: XXXX would return XXXX.
    # You can supply multiple symbols which ocr can parse wrongly. i.e. ocr could parse Shareholder ; XXXX and it will still work if you pass both : and ;

    def parse_initial_shareholdings_line(self, line, keys):
        keys_sorted = []
        find_index_sorted = []

        sum = 0
        for key in keys:
            f = line.find(key)
            find_index_sorted.append(f)
            sum += f

        if sum == len(keys) * -1:
            return {'': line}, keys

        reverse_key_value_list = [(y, x) for y, x in sorted(zip(find_index_sorted, keys), reverse=True)]

        found_key_value = {}
        remain_list = []
        l = line
        for find_index, key in reverse_key_value_list:
            if find_index > -1:
                value = l[find_index + len(key):]
                value = self.remove_special_characters(value, self.special_characters)
                l = l[:find_index]
                found_key_value.update({key.replace(' ', '_'): value.upper()})
            else:
                remain_list.append(key)

        if len(l):
            l = self.remove_special_characters(l, self.special_characters)
            if len(l):
                found_key_value[''] = l.upper()
        return found_key_value, remain_list


    def remove_special_characters(self, line, special_characters):
        return re.sub(f'[{special_characters}]', ' ', line).strip()  # remove special characters

    def parse_incorporation_ocr(self, url, dt, poppler_path):
        def set_out_dict(out_dict):
            if 'nominal_value_of' in out_dict:
                out_dict['nominal_value'] = out_dict.pop('nominal_value_of')

            for key, value in out_dict.items():
                if key == 'number_of_shares':
                    out_dict[key] = int(value)
                elif key in ['nominal_value', 'amount_paid', 'amount_unpaid']:
                    try:
                        out_dict[key] = float(value.lower().replace('each', '').replace('share', '').strip())
                    except:
                        out_dict[key] = None

        output_string = StringIO()
        # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\incorporation\\incorporation__07488363__20110111.pdf'
        # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\incorporation\\incorporation__7637003__20110517.pdf'
        # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\incorporation\\incorporation__8070525__20120516.pdf'
        # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\incorporation\\incorporation__8905651__20140221.pdf'
        # p = 'C:\\projects\\uni\\sources\\companieshouse\\data\\test\\incorporation\\incorporation__9147492__20140725.pdf'
        # pages = convert_from_path(p)

        logger.info(f'company: {self.crunchbase_company_name} page: {self.page_number_filing} date: {str(dt)} url: {url}')

        # Convert PDF to image
        r = session.get(url=url)
        pages = convert_from_bytes(r.content, poppler_path=poppler_path)

        # Read text from images using OCR

        sections = {'INITIAL SHAREHOLDINGS': []}

        key = 'COMPANY INFORMATION'
        ###### psm 4 is the best structure
        class Found(Exception):
            pass

        other_titles = ['PROPOSED OFFICERS', 'STATEMENT OF CAPITAL', 'PERSONS WITH SIGNIFICANT CONTROL',
                        'INDIVIDUAL PERSON WITH SIGNIFICANT CONTROL', 'STATEMENT OF COMPLIANCE']

        key = ''


        try:
            page_num = 1
            is_electronic_document = False
            for img in pages:
                page = pytesseract.image_to_string(img, config='--psm 6').split('\n')

                for line in page:
                    # OFFICERS OF THE COMPANY
                    # print('parse_incorporation', line)

                    if page_num <= 3:
                        if 'electronically filed document' in line.lower():
                            is_electronic_document = True

                    if 'INITIAL SHAREHOLDINGS'.lower() in line.lower():
                        key = 'INITIAL SHAREHOLDINGS'
                    # STATEMENT OF CAPITAL (SHARE CAPITAL)

                    if key == 'INITIAL SHAREHOLDINGS':
                        for item in other_titles:
                            if item.lower() in line.lower():
                                raise Found

                        sections[key].append(line)
                page_num += 1

        except Found:
            # found initial shareholdings
            pass

        if is_electronic_document == False:
            content = CompaniesHouseErrorCodes.document_not_readable
            return content

        # COMPANY INFORMATION ARE not parsed from this document since its information is available on website. the only additional data that this document posseses are historical addresses which are not important at this stage
        # OFFICERS OF THE COMPANY are not parsed from this document since its full history is available on website

        content = {'INITIAL SHAREHOLDINGS': []}
        name = None
        skip_next = False
        address = None
        key_list = ['name', 'address',  'class of share', 'number of shares', 'currency', 'amount unpaid', 'amount paid', 'nominal value of']
        out_dict = {}


        try:
            for key, lst in sections.items():

                i = 0
                adress_parsing_started = False
                line_lower_prev = ''
                for line in lst:
                    # print(f'parse_incorporation: 4', line)
                    if key == 'INITIAL SHAREHOLDINGS':
                        if skip_next == False:
                            line_lower = line.lower()

                            if 'name' in line_lower:
                                key_list = ['name', 'address', 'class of share', 'number of shares', 'currency', 'amount unpaid', 'amount paid', 'nominal value of']
                                if out_dict:
                                    content['INITIAL SHAREHOLDINGS'].append(out_dict)
                                out_dict = {}

                            out_parse, key_list = self.parse_initial_shareholdings_line(line_lower, key_list)

                            if 'address' in out_parse:
                                adress_parsing_started = True
                                a = out_parse['address']
                                a = self.remove_special_characters(a, self.special_characters)  # '"|?/_¬~#—-=:;,!%$£*&')
                                address = [a]

                            elif '' in out_parse and address is not None:
                                address_addition = out_parse[''].replace('each share', '')
                                if address_addition.startswith('electronically'):
                                    pass
                                else:
                                    address_addition = self.remove_special_characters(address_addition, self.special_characters)
                                    if address_addition:
                                        address.append(address_addition)
                                        out_dict['address'] = address
                                if '' in out_parse: del out_parse['']
                            # elif '' in out_parse and len(out_parse) > 1:
                            #     raise ValueError('problem detected.')
                            elif '' not in out_parse:
                                if 'nominal_value_of' in out_parse and out_parse['nominal_value_of'] == '':
                                    #check next line
                                    out_parse['nominal_value_of'] = lst[i + 1]
                                    skip_next = True
                                    i += 1
                                    if '' in out_parse: del out_parse['']
                                    set_out_dict(out_parse)
                                    out_dict.update(out_parse)
                                    continue

                            if '' in out_parse: del out_parse['']

                            if 'name' in line_lower_prev and adress_parsing_started == False:
                                out_dict['name'] += ' ' + line_lower.upper()

                            set_out_dict(out_parse)
                            out_dict.update(out_parse)

                            line_lower_prev = line_lower

                        skip_next = False
                    i += 1
                content['INITIAL SHAREHOLDINGS'].append(out_dict)
        except Exception as ex:
            # found initial shareholdings
            raise ValueError(ex)

        # change names to same as in shareholders dictionary
        for item in content['INITIAL SHAREHOLDINGS']:
            item['name'] = self.remove_titles(item['name'].strip())
            if 'class_of_share' in item:
                item['share_type'] = item.pop('class_of_share')
            if 'number_of_shares' in item:
                item['shares'] = item.pop('number_of_shares')
            is_company = is_organization(item['name'])
            item['is_company'] = is_company
        return content
        # print(json.dumps(content, sort_keys = True, indent = 4))

    @staticmethod
    def remove_titles(name,
                      title_identifiers=['MR', 'MR.', 'MRS', 'MRS.', 'MISS', 'MISS.', 'MS', 'MS.', 'DR', 'DR.', 'PROF',
                                         'PROF.', 'SIR', 'SIR.', 'LORD', 'LORD.',
                                         'LADY', 'LADY.', 'PHD', 'PHD.', 'REV', 'REV.', 'FR', 'FR.', 'BARON', 'BARON.',
                                         'BARONESS', 'BARONESS.', 'SULTAN', 'SULTAN.',
                                         'PRINCE', 'PRINCE.', 'PRINCESS', 'PRINCESS.', 'DUKE', 'DUKE.', 'DUCHESS',
                                         'DUCHESS.', 'EARL', 'EARL.', 'COUNTESS', 'COUNTESS.',
                                         'VISCOUNT', 'VISCOUNT.', 'VISCOUNTESS', 'VISCOUNTESS.', 'AMB', 'AMB.', 'ADM',
                                         'ADM.', 'CAPT', 'CAPT.', 'COL', 'COL.', 'CMDR',
                                         'CMDR.', 'LT', 'LT.', 'MAJ', 'MAJ.', 'SGT', 'SGT.', 'PVT', 'PVT.', 'REV',
                                         'REV.', 'FR', 'FR.', 'BROTHER', 'BROTHER.', 'SISTER',
                                         'SISTER.', 'FATHER', 'FATHER.', 'MOTHER', 'MOTHER.', 'RABBI', 'RABBI.',
                                         'SHEIKH', 'SHEIKH.', 'AYATOLLAH', 'AYATOLLAH.', 'PRESIDENT',
                                         'PRESIDENT.', 'PRIME MINISTER', 'PRIME MINISTER.', 'KING', 'KING.', 'QUEEN',
                                         'QUEEN.']):
        # Prepare pattern (escape each value to handle special regex characters, join with '|')
        pattern = '|'.join(re.escape(value) for value in title_identifiers)

        # Create full pattern with custom "word boundaries", case insensitive
        full_pattern = r'(?:(?<=\W)|^)(' + pattern + r')(?:[.]?(?=\W)|$)'
        full_pattern = re.compile(full_pattern, re.IGNORECASE)

        # Replace all occurrences of the organization identifiers with an empty string
        cleaned_name = re.sub(full_pattern, '', name)

        # Remove any leading or trailing whitespaces from the cleaned name
        cleaned_name = cleaned_name.strip()

        return cleaned_name


def run_companieshouse_bot(uuids_filter='*', category_groups_list_filter='*', country_code_filter='*',
                           from_filter=datetime.min, to_filter=datetime.max,
                           force=False, callback_finish=None):
    run_companieshouse_bot_defer(uuids_filter, category_groups_list_filter, country_code_filter, from_filter, to_filter, force, callback_finish)
    reactor.run()
'''
if uuids are not supplied, all companies that are pending in pending table will be scraped
if force is True, then pending status completed will be ignored and scraped again 
'''
@defer.inlineCallbacks
def run_companieshouse_bot_defer(uuids_filter='*', category_groups_list_filter='*', country_code_filter='*',
                                 from_filter=datetime.min, to_filter=datetime.max,
                                 force=False, callback_finish=None):


    # companies house crawler
    company_id = '07101408'
    # Set the log level to suppress warning messages
    settings = get_project_settings()

    data = CompaniesHouseBot.get_data_from_pending(uuids_filter, '*', category_groups_list_filter, country_code_filter, from_filter, to_filter, force)

    # data = [data[8]]

    runner = CrawlerRunner(settings)
    for row in data:
        try:
            if row['legal_name'] == '' or row['legal_name'] is None:
                crunchbase_company_name = row['name']
            else:
                crunchbase_company_name = row['legal_name']

            uuid = row['crunchbase_data']['properties']['uuid']
            crunchbase_founded_on = row['crunchbase_data']['properties']['founded_on']['value']
            crunchbase_founder_names = []

            if 'founder_identifiers' in row['crunchbase_data']['properties']:
                if row['crunchbase_data']['properties']['founder_identifiers'] != []:
                    crunchbase_founder_names = [x['value'] for x in row['crunchbase_data']['properties']['founder_identifiers']]
            elif 'founders' in row['crunchbase_data']['properties']:
                if row['crunchbase_data']['cards']['founders'] != []:
                    raise NotImplementedError()

            msg = {'company_name_CB': crunchbase_company_name.upper(),
                   'founded_on_CB': crunchbase_founded_on,
                   'founders_CB': crunchbase_founder_names,
                   }

            logger.info(f'company: {crunchbase_company_name} searching for: {json.dumps(msg)}')

            search_results = CompaniesHouseBot.search(crunchbase_company_name, crunchbase_founder_names, crunchbase_founded_on, 10, 0.85, BRAVE_PATH)

            if search_results:
                company_id, company_url = search_results
                msg['url'] = company_url

                logger.info(f'company: {crunchbase_company_name} found and starting to crawl: {json.dumps(msg)}.')
                # process = CrawlerProcess(settings)
                # process.crawl(CompaniesHouseBot, company_id=company_id, crunchbase_company_name=row['name'], uuid=uuid, poppler_path=POPPLER_PATH, is_write_db=True, is_write_file=False, callback_finish=companieshouse_finished)
                # process.start() # the script will block here until the crawling is finished
                # process.stop()
                args = {'company_id':company_id, 'crunchbase_company_name': row['name'], 'uuid':uuid,
                        'poppler_path':POPPLER_PATH, 'is_write_db':True, 'is_write_file':False,
                        'callback_finish':callback_finish}

                yield runner.crawl(CompaniesHouseBot, **args)

            else:
                logger.warning(f'company: {crunchbase_company_name} not found {json.dumps(msg)}.')
                json_rest_api = CompaniesHouseErrorCodes.company_not_found
                write_failed(uuid, crunchbase_company_name, json_rest_api)

        except Exception as ex:
            logger.error(f'company: {crunchbase_company_name} {str(ex)}')

    reactor.stop()

def write_failed(uuid, crunchbase_company_name, json_rest_api):

    conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )

    try:
        cursor = conn.cursor()

        dt = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        data_str = json.dumps(json_rest_api)

        d = {'uuid': uuid, 'uuid_parent': uuid, 'name': crunchbase_company_name, 'source': DataSource.companieshouse.name,
             'version': CompaniesHouseBot.__version__, 'created_at': dt, 'updated_at': dt, 'data': data_str}
        columns = d.keys()
        values = tuple(d.values())

        table_name = 'data'
        # write data to db
        query_data = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (uuid, source) DO UPDATE SET data = EXCLUDED.data, version = EXCLUDED.version, updated_at = EXCLUDED.updated_at"
        # query_data = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ('{values[0]}', '{values[1]}', '{values[2]}', '{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}')"
        # print(f'query_data 1: {query_data}')
        cursor.execute(query_data, values)

        # update pending table
        query_pending = f"UPDATE pending SET status = '{PendingStatus.completed.name}', updated_at = '{dt}'  WHERE uuid = '{uuid}' AND source = '{DataSource.companieshouse.name}'"

        # print(f'query_pending 1: {query_data}')
        cursor.execute(query_pending)

    except psycopg2.Error as e:
        logger.error(f"company: {crunchbase_company_name} write executing queries: {str(e)}")
        conn.rollback()
    except Exception as e:
        logger.error(f"company: {crunchbase_company_name} write: {str(e)}")
    else:
        conn.commit()
        logger.info(
        f'company: {crunchbase_company_name} writing failed data successful from source: {DataSource.companieshouse.name} status: {PendingStatus.completed.name} company: {crunchbase_company_name}')
    finally:
        cursor.close()

def run_companieshouse_bot_by_company_id(company_house_id='07101408', callback_finish=None):
    # companies house crawler
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(CompaniesHouseBot, company_id=company_house_id, poppler_path=POPPLER_PATH, write_enabled=True, callback_finish=callback_finish)
    process.start() # the script will block here until the crawling is finished

