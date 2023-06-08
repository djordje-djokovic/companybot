import os, time, json, scrapy, psycopg2
from datetime import datetime

from bs4 import BeautifulSoup
from geotext import GeoText
from fuzzywuzzy import fuzz

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as BraveService
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType

from .config import LINKEDIN_EMAIL, LINKEDIN_PWD, BRAVE_PATH, DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
from .common import PendingStatus, DataSource, get_logger
from .companieshouse_bot import CompaniesHouseBot

class LinkedInErrorCodes():

    profile_not_found = {
    "error": {
        "code": 300,
        "message": "Profile not found"
        }
    }

    profile_and_organization_not_matched_name_matched = {
    "error": {
        "code": 310,
        "message": "Profile and organization not matched. Name was matched"
        }
    }

    profile_companieshouse_linkedin_not_not_matched = {
    "error": {
        "code": 320,
        "message": "Companies House and LinkedIn name not matched"
        }
    }

    occupation_not_matched = {
    "error": {
        "code": 330,
        "message": "Occupation does not match"
        }
    }

    def __init__(self):
        pass

# linkedin bot we do not use scrapy since we want to download profiles in a fully controller manner one by one.
# i could not make that work in scrapy
class LinkedInBot():
    __version__ = 'LinkedInBot 0.9'
    def __init__(self, user_email=LINKEDIN_EMAIL, user_pwd=LINKEDIN_PWD,
                 brave_path=r'C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe',
                 callback_profile=None, callback_company=None, callback_finish=None, logger=None):
        # Set the path to the Brave browser executable
        self.logger = logger
        if self.logger is None:
            self.logger = get_logger('LinkedInBot')

        self.user_email = user_email
        self.user_pwd = user_pwd
        self.brave_path = brave_path
        self.callback_profile = callback_profile
        self.callback_company = callback_company
        self.callback_finish = callback_finish
        self.companies = []

        self.conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        self.conn.autocommit = False

    def login(self, user_email, user_pwd):
        # Launch Brave browser using Selenium
        self.logger.info('Logging in')
        options = webdriver.ChromeOptions()
        options.add_argument('--start-maximized')
        # options.add_argument('--no-startup-window')
        options.binary_location = self.brave_path

        self.driver = webdriver.Chrome(service=BraveService(ChromeDriverManager(chrome_type=ChromeType.BRAVE).install()),
                                  options=options)


        # Opening linkedIn's login page
        self.driver.get("https://linkedin.com/uas/login")

        # self.logger.info(f'Login before: {str(datetime.now())}')
        # waiting for the page to load
        wait = WebDriverWait(self.driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        # self.logger.info(f'Login after: {str(datetime.now())}')

        # entering username
        username = self.driver.find_element(By.ID, "username")

        # In case of an error, try changing the element
        # tag used here.

        # Enter Your Email Address
        username.send_keys(user_email)

        # entering password
        pword = self.driver.find_element(By.ID, "password")
        # In case of an error, try changing the element
        # tag used here.

        # Enter Your Password
        pword.send_keys(user_pwd)

        # Clicking on the log in button
        # Format (syntax) of writing XPath -->
        # //tagname[@attribute='value']
        self.driver.find_element(By.XPATH, "//button[@type='submit']").click()
        # In case of an error, try changing the
        # # XPath used here.
        time.sleep(20)
        self.logger.info('Logging in completed.')

    @staticmethod
    def get_data_from_pending(uuids_filter='*', uuids_parent_filter='*', force=False):

        assert type(uuids_filter) == list or uuids_filter == '*'
        assert type(uuids_parent_filter) == list or uuids_parent_filter == '*'

        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )

        cursor = conn.cursor()

        uuids_str = ', '.join([f"'{item}'" for item in uuids_filter]) if type(uuids_filter) == list else uuids_filter.replace('*', "'%'")
        uuids_parent_str = ', '.join([f"'{item}'" for item in uuids_parent_filter]) if type(uuids_parent_filter) == list else uuids_parent_filter.replace('*', "'%'")

        if force:
            status = f"pending.uuid::text LIKE ANY (ARRAY[{uuids_str}]) and pending.uuid_parent::text LIKE ANY (ARRAY[{uuids_parent_str}]) and"
        else:
            status = f"pending.uuid::text LIKE ANY (ARRAY[{uuids_str}]) and pending.status = '{PendingStatus.pending.name}' and"

        query = f"SELECT pending.*, \"data\".data as companieshouse_data " \
            f"FROM pending " \
            f"INNER JOIN \"data\" ON pending.uuid_parent = \"data\".uuid AND \"data\".source = '{DataSource.companieshouse.name}' " \
            f"WHERE {status} pending.source = '{DataSource.linkedin.name}' " \
            f"ORDER BY pending.name COLLATE \"C\", pending.uuid_parent ASC"
        # print(query)
        cursor.execute(query)
        rows = cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0] for desc in cursor.description]

        # Transform the result set into a list of dictionaries
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        conn.close()

        return result

    def run(self, uuids_filter='*', uuids_parent_filter='*', occupations_filter=['Founder', 'Director', 'Shareholder', 'Secretary'], force=False):

        self.login(self.user_email, self.user_pwd)

        self.uuids_filter = uuids_filter
        self.uuids_parent_filter = uuids_parent_filter
        self.occupations_filter = occupations_filter

        data = self.get_data_from_pending(uuids_filter, uuids_parent_filter, force)

        if not data:
            self.logger.error('LinkedinBot::run no data found in pending')

        profiles_by_company = {}
        for row in data:
            company_name = row['companieshouse_data']['properties']['company_name']
            if company_name not in profiles_by_company:
                profiles_by_company[company_name] = []
            profiles_by_company[company_name].append(row)

        for company_name, rows in profiles_by_company.items():
            company_dict = {'properties': {}, 'cards': {}, 'source': DataSource.linkedin.name}
            company_dict['properties']['parsing_date'] = datetime.now().strftime('%Y-%m-%d')

            company_item = {}
            company_item['persons'] = [] # profiles

            for row in rows:
                uuid = row['uuid']
                uuid_parent = row['uuid_parent']
                company_dict['properties']['uuid'] = uuid_parent
                full_company_name = company_dict['properties']['companieshouse_company_name'] = row['companieshouse_data']['properties']['company_name']
                company_id = company_dict['properties']['companieshouse_id'] = row['companieshouse_data']['properties']['company_id']

                profile_item = {'parsing_date': datetime.now().strftime('%Y-%m-%d')}
                occupations = row['category_groups_list']

                name = row['name']
                full_name = row['legal_name']

                company_name_short = self.min_case(full_company_name)
                search_name = name + ' ' + company_name_short

                if set(occupations) & set(occupations_filter):  # check if lists have a superset of occupations
                    # full_name = item['full_name']
                    # profile_name_split = full_name.split(',')
                    # profile_name = profile_name_split[1].strip().split(' ')[0].strip() + ' ' + profile_name_split[0]
                    # profile_name = profile_name.title()
                    profile_url = self.search(search_name)
                    info = self.get_info(uuid, uuid_parent, company_id, name, full_name, full_company_name, occupations, profile_url)
                    profile_item.update(info)

                    if profile_url is None:
                        profile_item.update(LinkedInErrorCodes.profile_not_found)
                        self.logger.warning(str(profile_item))
                    else:

                        experience = self.get_experience(profile_url)
                        profile_item.update(info)
                        info_name = info['name']

                        self.logger.info(f'Attempting to parse: linkedin_name:{info_name} search_name:{search_name} full_name:{full_name} profile_url:{profile_url}')

                        # there are two things that can go wrong. when we search a profile name in linked in we will get almost
                        # always a list of matches returned where the profiles are completely different persons.
                        # we check names against companies house names with a fuzzy match
                        if fuzz.token_sort_ratio(info['name'], name) >= 80:
                            # secondly we need to check if the profile has aefined the company we are searching in the profile and check for this
                            if self.fuzzy_organization_match(experience, company_name_short):
                                education = self.get_education(profile_url)

                                profile_item['experience'] = experience
                                profile_item['education'] = education

                                self.logger.info(f'Successfully matched Profile and Organization: {search_name} {info_name} {str(occupations)} {profile_url}')
                                if self.callback_profile: self.callback_profile(profile_item)

                            else:
                                profile_item.update(LinkedInErrorCodes.profile_and_organization_not_matched_name_matched)
                                self.logger.warning(f'Unuccessfully matched Profile and Organization although Name was matched: linkedin_name:{info_name} search_name:{search_name} full_name:{full_name} profile_url:{profile_url}')
                        else:
                            profile_item.update(LinkedInErrorCodes.profile_companieshouse_linkedin_not_not_matched)
                            self.logger.warning(f'Unuccessfully matched Companies House name and LinkedIn name: linkedin_name:{info_name} search_name:{search_name} full_name:{full_name} profile_url:{profile_url}')

                    company_item['persons'].append(profile_item)
                    self.write(uuid, uuid_parent, name, profile_item)

                else:
                    # we do not need to write profile item because it was never requested
                    self.logger.info(f'Ignoring Profile since Occupation does not match: {search_name} {full_name} {str(occupations)} not in {str(occupations_filter)}')

            # time.sleep(5)

            self.remove_duplicates(company_item['persons'])

            company_dict['cards'] = company_item
            if self.callback_company: self.callback_company(company_dict)
            self.companies.append(company_dict)

        if self.callback_finish: self.callback_finish(self.companies)


    def write(self, uuid, uuid_parent, name, json_rest_api):

        try:
            cursor = self.conn.cursor()

            table_name = 'data'
            dt = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            data_str = json.dumps(json_rest_api)

            d = {'uuid': uuid, 'uuid_parent': uuid_parent, 'name': name, 'source': DataSource.linkedin.name, 'version':self.__version__, 'created_at': dt, 'updated_at': dt, 'data': data_str}
            columns = d.keys()
            values = tuple(d.values())

            # write data to db
            query_data = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (uuid, source) DO UPDATE SET data = EXCLUDED.data, version = EXCLUDED.version, updated_at = EXCLUDED.updated_at"
            # query_data = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ('{values[0]}', '{values[1]}', '{values[2]}', '{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}')"
            # print(f'query_data 1: {query_data}')
            cursor.execute(query_data, values)
            # print(f'query_data 2: {query_data}')

            # update pending table
            query_pending = f"UPDATE pending SET status = '{PendingStatus.completed.name}', updated_at = '{dt}'  WHERE uuid = '{uuid}' AND source = '{DataSource.linkedin.name}'"

            # print(f'query_pending 1: {query_data}')
            cursor.execute(query_pending)
            # print(f'query_pending 2: {query_data}')

        except psycopg2.Error as e:
            self.logger.error(f"write executing queries: {str(e)}")
            self.conn.rollback()
        except Exception as e:
            self.logger.error(str(e))
        else:
            self.conn.commit()
            self.logger.info(f'write data successful: source: {DataSource.companieshouse.name} status: {PendingStatus.completed.name} profile: {name}')
        finally:
            cursor.close()

    def run_from_dict(self, profiles_by_company_id, occupations_filter):
        raise NotImplementedError()

    # find indices with same linked in name
    def remove_duplicates(self, profile_item):
        l = profile_item
        same_indices = []

        for i, item in enumerate(l):
            indices = [j for j, x in enumerate(l) if x['name'] == item['name']]
            if len(indices) > 1 and indices not in same_indices:
                same_indices.append(indices)

        #     print(same_indices)

        # find the company house name that most closely matches linked in names and remove the other duplicates
        remove_indices = []
        for same_index in same_indices:

            max_score = 0
            for same_sub_index in same_index:
                score = fuzz.token_sort_ratio(l[same_sub_index]['name'], l[same_sub_index]['full_name'])
                if score >= max_score:
                    keep_index = same_sub_index
                    max_score = score
            remove_index = same_index.copy()
            remove_index.remove(keep_index)
            remove_indices.extend(remove_index)
        #         print('keep index', keep_index, 'from', same_index, 'remove index', remove_index)

        # remove duplicates
        remove_indices.sort(reverse=True)  # Sort in descending order to avoid index shifting

        for index in remove_indices:
            l.pop(index)

        return l

    def scroll_down(self):
        # Wait for page to load
        time.sleep(5)

        last_height = self.driver.execute_script("return document.body.scrollHeight")

        while True:
            # Scroll down to the bottom of the page
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait for the page to load
            time.sleep(2)

            # Calculate the new page height and compare with the previous height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def search(self, profile_name):

        # find user by name
        self.logger.info(f'{profile_name}')
        self.driver.get('https://www.linkedin.com/search/results/people/')
        search = self.driver.find_element(By.CLASS_NAME, 'search-global-typeahead__input')
        search.send_keys(profile_name)
        search.send_keys(Keys.RETURN)

        # Switch to the new window and wait for it to fully load
        wait = WebDriverWait(self.driver, 10)
        wait.until(EC.title_contains(profile_name))
        # wait.until(EC.presence_of_element_located((By.ID, 'element-id')))

        src = self.driver.page_source
        # Now using beautiful soup
        soup = BeautifulSoup(src, 'lxml')

        # soup.find_all('a', {'class': 'app-aware-link'})
        list_element = soup.find_all('li', {'class': 'reusable-search__result-container'})
        # take first element in list
        if list_element:
            link = list_element[0].find('a', {'class': 'app-aware-link'})['href'].split('?')[0]
            return link
        return None


    # search dictionary to see if the organization is existing
    def min_case(self, str_full, n_max=5, separator=' '):
        str_split = str_full.strip().split(separator)
        new_str = ''
        for s in str_split:
            if new_str == '':
                new_str = s
            else:
                new_str = new_str + separator + s
            n_case = len(new_str)
            if n_case >= n_max:
                return new_str
        return new_str

    '''    
    x = 'made.com design ltd'
    y = 'made.com design limited'
    x_min = min_case(x)
    y_min = min_case(y)
    print('comparing x vs y:', x, y, fuzz.partial_ratio(x, y))
    print('comparing x_min vs y_min:', x_min, y_min, fuzz.partial_ratio(x_min, y_min))
    comparing x vs y: made.com design ltd made.com design limited 89
    comparing x_min vs y_min: made.com made.com 100
    '''
    def fuzzy_organization_match(self, d, match_value, min_case_num=5, ratio_limit=80):
        match_value = self.min_case(match_value, min_case_num).lower()
        for x in d:
            organization = self.min_case(x['organization'], min_case_num).lower()
            ratio = fuzz.token_sort_ratio(organization, match_value)
            #         ratio = fuzz.ratio(x['Organization'].lower(), match_value.lower())
            # print(organization, match_value, ratio)
            if ratio >= ratio_limit:  # threshold for matching
                return True

        return False

    def parse_date(self, date_str):
        try:
            return datetime.strptime(date_str, '%b %Y')
        except ValueError:
            try:
                return datetime.strptime(date_str, '%Y')
            except ValueError:
                raise ValueError(f"Invalid date string: '{date_str}'")

    def set_period(self, span, d, d_item):
        date_range = span.get_text().split('·')[0]
        if '-' in date_range:
            fr, to = date_range.split('-')
            fr_dt = self.parse_date(fr.strip())
        else:
            fr_dt = self.parse_date(date_range.strip())
            to = fr_dt.replace(month=12, day=31).strftime('%b %Y')

        if to.strip().lower() == 'present':
            to_dt = datetime.now()
        else:
            to_dt = self.parse_date(to.strip())

        if d['from'] == '':
            fr_dt_min = datetime.max
        else:
            fr_dt_min = datetime.strptime(d['from'], '%Y-%m-%d')

        if d['to'] == '':
            to_dt_max = datetime.min
        else:
            to_dt_max = datetime.strptime(d['to'], '%Y-%m-%d')

        fr_dt_min = min(fr_dt, fr_dt_min)
        to_dt_max = max(to_dt, to_dt_max)
        period_minmax = to_dt_max - fr_dt_min

        d['from'] = fr_dt_min.strftime('%Y-%m-%d')
        d['to'] = to_dt_max.strftime('%Y-%m-%d')
        d['days'] = period_minmax.days

        period = to_dt - fr_dt
        d_item['from'] = fr_dt.strftime('%Y-%m-%d')
        d_item['to'] = to_dt.strftime('%Y-%m-%d')
        d_item['days'] = period.days

    def set_location(self, span, d, d_item):
        location_text = span.get_text()
        location = GeoText(location_text)
        d_item['country'] = "" if len(location.country_mentions) == 0 else list(location.country_mentions.keys())[0]
        d_item['city'] = "" if len(location.cities) == 0 else location.cities[0]

    def set_item(self, span, name, d):
        d[name] = span.get_text().split('·')[0].strip()

    def set_dict(self, s, s_type_prev, parse_type, d, d_item):
        s_class = s.parent.get('class')
        #     print(s, s_class)
        if 'mr1' in s_class and 't-bold' in s_class:  # this indicates a Position or Organization (education)
            if parse_type.lower() == 'experience':
                s_type = 'position'
                self.set_item(s, s_type, d_item)
            elif parse_type.lower() == 'education':
                s_type = 'organization'
                self.set_item(s, s_type, d)
            else:
                raise NotImplementedError(str(s))

        elif 't-14' in s_class and 't-normal' in s_class and 't-black--light' in s_class:  # indicates Period or Location
            if s_type_prev.lower() == 'period':
                s_type = 'location'
                self.set_location(s, d, d_item)
            else:
                s_type = 'period'
                self.set_period(s, d, d_item)

        elif 't-14' in s_class and 't-normal' in s_class and 't_black--light' not in s_class and 'display-flex' not in s_class:  # this indicates an Organization (experience) or Degree
            if parse_type.lower() == 'experience':
                s_type = 'organization'
                self.set_item(s, s_type, d)
            elif parse_type.lower() == 'education':
                s_type = 'degree'
                self.set_item(s, s_type, d_item)
            else:
                raise NotImplementedError(str(s))

        elif 'display-flex' in s_class:  # indicated Description
            s_type = 'description'
            self.set_item(s, s_type, d_item)

        else:
            s_type = s_type_prev
            self.logger.debug(f'Following item is not set: {str(s_class)} {str(s)}')

        return s_type

    def parse_list(self, html_lst, parse_type):
        #     keys = ['Position', 'Organization', 'Period', 'Description'] # parse experience
        #     keys = ['Organization', 'Degree', 'Period', 'Description'] # parse education
        lst = []
        # extract text elements
        element_num = 0
        for item in html_lst:
            d = {}
            d['items'] = []

            d['organization'] = ''
            d['from'] = ''
            d['to'] = ''
            d['days'] = ''

            span = item.find_all('span', attrs={'aria-hidden': 'true'})
            soup_list = BeautifulSoup(str(html_lst[element_num]), 'html.parser')

            if soup_list.find('ul', {'class': 'pvs-list'}) is None or soup_list.find('ul', {'class': 'pvs-list'}).find(
                    'ul', {'class': 'pvs-list'}) is None:
                soup_sublist = [None]
            else:
                soup_sublist = soup_list.find('ul', {'class': 'pvs-list'}).find('ul', {'class': 'pvs-list'}).find_all(
                    'li', recursive=False)

            subelement_num = 0

            for sublist in soup_sublist:
                #             print(element_num, subelement_num)
                d_item = {}

                if parse_type.lower() == 'experience':
                    d_item['position'] = ''
                elif parse_type.lower() == 'education':
                    d_item['degree'] = ''
                else:
                    raise NotImplementedError()

                d_item['from'] = ''
                d_item['to'] = ''
                d_item['days'] = ''
                d_item['country'] = ''
                d_item['city'] = ''
                d_item['description'] = ''

                if len(soup_sublist) > 1:
                    span_sublist = soup_sublist[subelement_num].find_all('span', attrs={'aria-hidden': 'true'})

                subelement_num += 1

                if len(soup_sublist) == 1:
                    s_type = ''
                    for s in span:
                        s_type = self.set_dict(s, s_type, parse_type, d, d_item)


                elif len(soup_sublist) >= 2:
                    if parse_type == 'experience':
                        d_item['position'] = span_sublist[0].get_text().split('·')[0].strip()
                        d['organization'] = span[0].get_text().split('·')[0].strip()
                    elif parse_type == 'education':
                        d_item['degree'] = span_sublist[1].get_text().split('·')[0].strip()
                        d['organization'] = span[0].get_text().split('·')[0].strip()
                    else:
                        raise NotImplementedError()

                    s_type = ''
                    # period
                    for i in range(1, len(span_sublist)):
                        s = span_sublist[i]
                        s_type = self.set_dict(s, s_type, parse_type, d, d_item)

                #                 print(s.get_text(), s_class)

                d['items'].append(d_item)

            #         d[keys[0]] = span[0].get_text()
            #         d[keys[1]] = span[1].get_text().split('·')[0].strip()

            lst.append(d)

            self.logger.debug(json.dumps(d, indent=4))

            element_num += 1

        return lst

    def get_info(self, uuid, uuid_parent, company_id, name, full_name, company_name_companieshouse, occupations, profile_url):
        info = {}
        info['uuid'] = uuid
        info['parent_uuid'] = uuid_parent
        info['companieshouse_id'] = company_id
        info['name'] = name
        info['full_name'] = full_name
        info['linkedin_name'] = None
        info['companieshouse_company_name'] = company_name_companieshouse
        info['occupations'] = occupations
        info['linkedin_url'] = profile_url

        if profile_url is None:
            return info

        self.driver.get(profile_url)
        # scroll_down(driver)
        src = self.driver.page_source
        # Now using beautiful soup
        soup = BeautifulSoup(src, 'lxml')

        # Extracting the HTML of the complete introduction box
        # that contains the name, company name, and the location
        intro = soup.find_all('div', {'class': 'pv-text-details__left-panel'})

        # In case of an error, try changing the tags used here.

        name_loc = intro[0].find("h1")

        # Extracting the Name
        linkedin_name = name_loc.get_text().strip()
        # strip() is used to remove any extra blank spaces

        works_at_loc = intro[0].find("div", {'class': 'text-body-medium'})

        # this gives us the HTML of the tag in which the Company Name is present
        # Extracting the Company Name
        works_at = works_at_loc.get_text().strip()

        location_loc = intro[1].find("span", {'class': 'text-body-small'})

        # Ectracting the Location
        # The 2nd element in the location_loc variable has the location
        location_text = location_loc.get_text().strip()

        location = GeoText(location_text)
        info['linkedin_name'] = linkedin_name
        info['country'] = "" if len(location.country_mentions) == 0 else list(location.country_mentions.keys())[0]
        info['city'] = "" if len(location.cities) == 0 else location.cities[0]

        return info

    def get_experience(self, profile_url):
        self.driver.get(profile_url + '/details/experience/')
        self.scroll_down()
        src = self.driver.page_source
        # Now using beautiful soup
        soup = BeautifulSoup(src, 'lxml')

        #recursive false is needed otherwise sublists will be extracted as well
        experience_html_list = soup.find('ul', {'class': 'pvs-list'}).find_all('li', recursive=False)

        experience_list = self.parse_list(experience_html_list, 'experience')

        return experience_list

    def get_education(self, profile_url):

        self.driver.get(profile_url + '/details/education/')
        self.scroll_down()
        src = self.driver.page_source
        # Now using beautiful soup
        soup = BeautifulSoup(src, 'lxml')

        # recursive false is needed otherwise sublists will be extracted as well
        education_html_list = soup.find('ul', {'class': 'pvs-list'}).find_all('li', recursive=False)

        education_list = self.parse_list(education_html_list, 'education')

        return education_list

'''
    profiles_by_companieshouse_id = [{'company_id': '07101408',
                                       'company_name': 'MADE.COM DESIGN LTD',
                                       'items': [
                                           # {'name': 'ANGELINI-HURLL, Rogan James', 'occupation': ['Director']},
                                           # {'name': 'CALLEDE, Julien', 'occupation': ['Director', 'Shareholder']},
                                           # {'name': 'CHAINIEUX, Philippe', 'occupation': ['Director', 'Shareholder']},
                                           # {'name': 'CLARK, John Robert Morton', 'occupation': ['Secretary', 'Shareholder']},
                                           # {'name': 'EVANS, Adrian Baynham', 'occupation': ['Director']},
                                           {'name': 'GOTHARD, Ben Winston', 'occupation': ['Secretary', 'Shareholder']},
                                           # {'name': 'HOBERMAN, Brent Shawzin', 'occupation': ['Director']},
                                           # {'name': 'HUNT, John Francis Weston', 'occupation': ['Director']},
                                           # {'name': 'KWOK, Win', 'occupation': ['Shareholder']},
                                           # {'name': 'LEWIS, John Patrick', 'occupation': ['Director']},
                                           {'name': 'LI, Ning Lucas Gabriel',
                                            'occupation': ['Director', 'Founder', 'Shareholder']},
                                           # {'name': 'MACINTOSH, Chloe', 'occupation': ['Shareholder']},
                                           # {'name': 'MCCULLOCH, George William', 'occupation': ['Director']},
                                           # {'name': 'REID, Stephen Graham', 'occupation': ['Secretary']},
                                           # {'name': 'ROEN, Carson', 'occupation': ['Shareholder']},
                                           # {'name': 'ROEN, Carston', 'occupation': ['Shareholder']},
                                           # {'name': 'SIMONCINI, Marc', 'occupation': ['Director']},
                                           # {'name': 'SKIPPER, Andrew', 'occupation': ['Shareholder']},
                                           # {'name': 'SKIPPER, Andy', 'occupation': ['Shareholder']},
                                           # {'name': 'THOMPSON, Nicola', 'occupation': ['Director']},
                                           # {'name': 'TOMLINS, Lisa Gan', 'occupation': ['Director', 'Secretary']},
                                           # {'name': 'TYLER, Laura', 'occupation': ['Director', 'Secretary']},
                                           # {'name': 'VANEK, David', 'occupation': ['Secretary']}
                                       ]}]


'''
def run_linkedin_bot_by_dict(profiles_by_companieshouse_id,
                     occupations_filter=['Founder', 'Director'], force=False,
                     callback_profile=None, callback_company=None, callback_finish=None, logger=None):

    linkedin_bot = LinkedInBot(user_email=LINKEDIN_EMAIL, user_pwd=LINKEDIN_PWD,
                               brave_path=BRAVE_PATH, callback_profile=callback_profile,
                               callback_company=callback_company, callback_finish=callback_finish, logger=logger)

    linkedin_bot.run_from_dict(profiles_by_company_id=profiles_by_companieshouse_id, occupations_filter=occupations_filter)


def run_linkedin_bot(uuids_filter='*', uuids_parent_filter='*',
                     occupations_filter=['Founder', 'Director'], force=False,
                     callback_profile=None, callback_company=None, callback_finish=None, logger=None):

    linkedin_bot = LinkedInBot(user_email=LINKEDIN_EMAIL, user_pwd=LINKEDIN_PWD,
                               brave_path=BRAVE_PATH, callback_profile=callback_profile,
                               callback_company=callback_company, callback_finish=callback_finish, logger=logger)

    linkedin_bot.run(uuids_filter=uuids_filter, uuids_parent_filter=uuids_parent_filter,
                     occupations_filter=occupations_filter, force=force)