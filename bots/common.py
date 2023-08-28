from fuzzywuzzy import fuzz
from .config import DB_NAME, DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, CRUNCHBASE_DIR, POPPLER_PATH, BRAVE_PATH, CRUNCHBASE_KEY
import time
from datetime import datetime
from dateutil import parser as date_parser
from enum import Enum
import re, psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import csv, numpy as np
import scrapy, os, json, logging, requests

USER_AGENTS = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36']

TITLE_NAMES = ['ADM', 'AMB', 'AYATOLLAH', 'BARON', 'BARONESS', 'BROTHER', 'CAPT', 'CMDR', 'COL', 'COUNTESS',
                                 'DR', 'DUCHESS', 'DUKE', 'EARL', 'FATHER', 'FR', 'KING', 'LADY', 'LORD', 'LT', 'MAJ', 'MISS',
                                 'MOTHER', 'MR', 'MRS', 'MS', 'PHD', 'PRESIDENT', 'PRIME MINISTER', 'PRINCE', 'PRINCESS', 'PROF',
                                 'PVT', 'QUEEN', 'RABBI', 'REV', 'SGT', 'SHEIKH', 'SIR', 'SISTER', 'SULTAN', 'VISCOUNT', 'VISCOUNTESS']

ORGANIZATION_SHORTCUTS = ['A/S', 'AB', 'ADV', 'AG', 'AS', 'ASBL', 'BV', 'BVBA', 'BT', 'CO', 'CO.', 'EIS', 'EIRL', 'EARL', 'EI', 'ETI',
                          'EURL', 'EV', 'GAEC', 'GCS', 'GIE', 'GMBH', 'Gbr', 'INC', 'INC.', 'KG', 'KGaA', 'KK',
                          'Kd', 'Kft', 'Kkt', 'LDA', 'LLC', 'LLLP', 'LLP', 'LP', 'LTD', 'M.B.', 'ME', 'NV',
                          'Nyrt', 'OG', 'OOO', 'PLC', 'PT', 'PTE', 'PTE LTD', 'PTY', 'PVT', 'PartG',
                          'QSC', 'Rt', 'SCI', 'SPA', 'SA', 'SAOC', 'SAOG', 'SAPA', 'SARL', 'SAS', 'SASU', 'SC', 'SCA',
                          'SCM', 'SCP', 'SRL', 'SCRL', 'SCS', 'SCSP', 'SDN', 'SE', 'SELARL', 'SERL', 'SL', 'SLL', 'SLNE', 'SLU',
                          'SNC', 'SP.ZO.O', 'SPRL', 'SRL', 'SRO', 'STH', 'UA', 'ULC', 'VOF', 'VAG', 'VC', 'VCC', 'VCT', 'VZW', 'Zrt', 'eG',
                          'eU', 'mbH', 'АО', 'ООО']


ORGANIZATION_NAMES = ['&', 'ACTIVITY', 'ADVISORY', 'ALPHA', 'ASSOCIATE', 'ASSOCIATES', 'ASSOCIATI', 'CAPITAL', 'CAPITALS', 'COMPANY', 'COMPANIES', 'CONSULTANCY',
                      'COLLEGE', 'COLLEGES', 'COFUND', 'CORP', 'CORPS', 'CORPORATION', 'CORPORATIONS',
                      'COUNCIL', 'COUNCILS', 'DIVERSIFIED', 'ENTREPRENEUR', 'ENTREPRENEURS', 'EQUITY', 'EQUITIES', 'FACTORY', 'FIRST',
                      'FACTORIES', 'FOUNDER', 'FOUNDERS', 'FOUNDATION', 'FOUNDATIONS', 'FUND', 'FUNDS', 'FUNDING',
                      'GROUP', 'GROUPS', 'GROWTH', 'HARDWARE', 'HOLDING', 'HOLDINGS', 'INNOVATION', 'INFORMATION',
                      'INVEST', 'INVESTMENT', 'INVESTMENTS', 'LAB', 'LABS', 'LEADERSHIP',
                      'LIMITED', 'MANAGEMENT', 'NOMINEE', 'NOMINEES', 'OPPORTUNITY', 'OPPORTUNITIES', 'ORGANICZONA',
                      'PARTNER', 'PARTNERS', 'PARTNERSHIP', 'SCIENCE', 'SCIENCES', 'SCHOOL', 'SHARES', 'SOLUTION', 'SOFTWARE',
                      'SOLUTIONS', 'STARTUP', 'STRATEGIES', 'STRATEGY',
                      'SUPPORT', 'TECHNOLOGY', 'TECHNOLOGIES', 'TRADING', 'TRUST',
                      'TRUSTEE', 'TRUSTEES', 'UNIVERSITY', 'UNIVERSITIES', 'VENTURE', 'VENTURES']

class PendingStatus(Enum):
    pending = 0
    completed = 1

class DataSource(Enum):
    crunchbase = 0
    companieshouse = 1
    linkedin = 2

def split_csv(csv_string):
    reader = csv.reader([csv_string])
    rows = next(reader)
    return rows


def remove_titles(name):
    name = name.replace('.', ' ').strip()
    # Prepare pattern (escape each value to handle special regex characters, join with '|')
    pattern = '|'.join(re.escape(value) for value in TITLE_NAMES)

    # Create full pattern with custom "word boundaries", case insensitive
    full_pattern = r'(?:(?<=\W)|^)(' + pattern + r')(?:[.]?(?=\W)|$)'
    full_pattern = re.compile(full_pattern, re.IGNORECASE)

    # Replace all occurrences of the organization identifiers with an empty string
    cleaned_name = re.sub(full_pattern, '', name)

    # Remove any leading or trailing whitespaces from the cleaned name
    cleaned_name = cleaned_name.strip()

    return cleaned_name

def is_organization(name):
    # Combine the original and new lists
    identifiers = ORGANIZATION_NAMES + ORGANIZATION_SHORTCUTS

    # Prepare pattern (escape each value to handle special regex characters, join with '|')
    pattern = '|'.join(re.escape(value) for value in identifiers)

    # Create full pattern with custom "word boundaries", case insensitive
    full_pattern = r'(?:(?<=\W)|^)(' + pattern + r')(?:(?=\W)|$)'
    full_pattern = re.compile(full_pattern, re.IGNORECASE)

    return bool(re.search(full_pattern, name.replace(",", '').replace('.', '').replace('/', '').replace("\\", '').replace("|", '')))

def get_category_group_list():
    filename = f'{CRUNCHBASE_DIR}/bulk_export/category_groups.csv'
    with open(filename, 'r', encoding='utf-8') as fp:

        i = 0
        category_group_set = []
        for line in fp.readlines():
            if i == 0:
                header = split_csv(line)
                category_name = 'category_groups_list' #'name'
                category_name_idx = header.index(category_name)
            else:
                category = split_csv(line)[category_name_idx].split(',')
                category_group_set.extend(category)
            i += 1
        return sorted(set(category_group_set))
def get_category_list():
    filename = f'{CRUNCHBASE_DIR}/bulk_export/category_groups.csv'
    with open(filename, 'r', encoding='utf-8') as fp:

        i = 0
        category_set = set()
        for line in fp.readlines():
            if i == 0:
                header = split_csv(line)
                category_name = 'name'  # 'name'
                category_name_idx = header.index(category_name)
            else:
                category = split_csv(line)[category_name_idx].strip('"').strip('\n').strip()
                category_set.add(category)
            i += 1
        return sorted(category_set)

def is_guid(string):
    pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    return re.match(pattern, string) is not None

# the organisations file is not a perfect csv file. it has some line breaks that need to be handled. we just ignore line breaks
from datetime import datetime, timedelta

def get_organizations_from_crunchbase_csv(uuids_filter, category_groups_list_filter, country_code_filter,
                                          from_filter=datetime.min, to_filter=datetime.max):
    filter = {}

    assert type(uuids_filter) == list or uuids_filter == '*'
    assert type(category_groups_list_filter) == list or category_groups_list_filter == '*'
    assert type(country_code_filter) == list or country_code_filter == '*'

    if uuids_filter != '*': filter['uuid'] = uuids_filter
    if category_groups_list_filter != '*': filter['category_groups_list'] = category_groups_list_filter
    if country_code_filter != '*': filter['country_code'] = country_code_filter

    logger.info(f"Fetching organizations from Crunchbase CSV with filters {str(filter)}")
    filename = f'{CRUNCHBASE_DIR}/bulk_export/organizations.csv'
    prev_progress = 0
    with open(filename, 'r', encoding='utf-8') as fp:
        i = 0
        values = []
        organizations_list = []
        lines_total = len(fp.readlines()) - 1  # Subtract 1 to exclude the header line
        fp.seek(0)  # Reset file pointer to the beginning

        for line in fp.readlines():
            try:
                if i == 0:
                    header = split_csv(line)
                else:
                    values = split_csv(line)

                    if values and is_guid(values[0]):
                        matched = np.zeros(len(filter))
                        j = 0
                        for filter_key, filter_value_list in filter.items():
                            key_idx = header.index(filter_key)
                            if len(values) == len(header):
                                value = values[key_idx].strip('"').strip('\n').strip()
                                for filter_value in filter_value_list:
                                    if filter_value in value:
                                        matched[j] = 1
                                        j += 1
                                        break
                                else:
                                    j += 1
                                    continue
                            else:
                                break

                        if len(filter) == np.sum(matched):

                            founded_on_idx = header.index('founded_on')
                            founded_on_str = values[founded_on_idx].strip('"').strip('\n').strip()

                            if founded_on_str:
                                founded_on = date_parser.parse(founded_on_str)
                                if from_filter <= founded_on <= to_filter:  # Apply date range filter
                                    organizations_list.append(dict(zip(header, values)))
                            elif from_filter==datetime.min and to_filter==datetime.max:
                                organizations_list.append(dict(zip(header, values)))

            except Exception as ex:
                logger.error(f'Error: {str(ex)}')

            i += 1
            progress = i / lines_total * 100
            if int(progress) > int(prev_progress):
                print(f'Progress: {progress:.0f}%', end='\r')
                prev_progress = progress

    print('\n')

    logger.info(f'Organizations found: {len(organizations_list)} {str(filter)}')
    clean_list_of_dictionaries(organizations_list)
    sorted_organizations_list = sorted(organizations_list, key=lambda x: x['name'])
    logger.info("Organizations fetched successfully.")
    return sorted_organizations_list


def create_database():
    conn = psycopg2.connect(
        host=DB_HOST,
        database="postgres",  # Connect to the default "postgres" database
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
    database_exists = cursor.fetchone()

    if not database_exists:
        cursor.execute(f"CREATE DATABASE {DB_NAME}")

    cursor.close()
    conn.close()

def create_organizations_table(drop_existing=False):
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port = DB_PORT
    )

    cursor = conn.cursor()

    table_name = 'crunchbase_organizations'

    if drop_existing:
        drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_table_query)
        logger.info(f'Dropped postgreSQL table {table_name}')

    create_table_query = """
    CREATE TABLE IF NOT EXISTS {} (
        uuid UUID,
        name TEXT,
        type TEXT,
        permalink TEXT,
        cb_url TEXT,
        rank INTEGER,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        legal_name TEXT,
        roles TEXT,
        domain TEXT,
        homepage_url TEXT,
        country_code TEXT,
        state_code TEXT,
        region TEXT,
        city TEXT,
        address TEXT,
        postal_code TEXT,
        status TEXT,
        short_description TEXT,
        category_list TEXT,
        category_groups_list TEXT,
        num_funding_rounds INTEGER,
        total_funding_usd BIGINT,
        total_funding BIGINT,
        total_funding_currency_code TEXT,
        founded_on DATE,
        last_funding_on DATE,
        closed_on DATE,
        employee_count TEXT,
        email TEXT,
        phone TEXT,
        facebook_url TEXT,
        linkedin_url TEXT,
        twitter_url TEXT,
        logo_url TEXT,
        alias1 TEXT,
        alias2 TEXT,
        alias3 TEXT,
        primary_role TEXT,
        num_exits INTEGER,
        CONSTRAINT crunchbase_organizations_pkey PRIMARY KEY (uuid)
    )
    """.format(table_name)
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f'Created postgreSQL table if existing {table_name}')

# pending table will contain all entries that need further processing by crunchbase REST API, companies house spider or linked in spider
def create_pending_table(drop_existing=False):
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cursor = conn.cursor()

    table_name = 'pending'

    if drop_existing:
        drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_table_query)
        logger.info(f'Dropped postgreSQL table {table_name}')

    create_table_query = """
        CREATE TABLE IF NOT EXISTS {} (
            uuid UUID NOT NULL,
            uuid_parent UUID NOT NULL,
            name TEXT NOT NULL,
            legal_name TEXT,
            country_code TEXT,
            category_groups_list TEXT[] NOT NULL,
            founded_on TIMESTAMP,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            version TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            CONSTRAINT pending_pkey PRIMARY KEY (uuid, source)
        )
        """.format(table_name)
    cursor.execute(create_table_query)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f'Created postgreSQL table if exisiting {table_name}')

def create_data_table(drop_existing=False):
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cursor = conn.cursor()

    table_name = 'data'

    if drop_existing:
        drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_table_query)
        logger.info(f'Dropped postgreSQL table {table_name}')

    create_table_query = """
            CREATE TABLE IF NOT EXISTS {} (
                uuid UUID,
                uuid_parent UUID,
                name TEXT,
                source TEXT,
                version TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                data JSON,
                CONSTRAINT data_pkey PRIMARY KEY (uuid, source)
            )
            """.format(table_name)
    cursor.execute(create_table_query)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f'Created postgreSQL table if existing {table_name}')

def clean_list_of_dictionaries(data):
    for d in data:
        for key, value in d.items():
            if value == '':
                d[key] = None

# writes all organizations from csv file to database. we use it to create for example a GBR subset of data
def write_organizations_from_csv(organizations, logging=None):
    # write to organizations table
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    data = organizations
    table_name = 'crunchbase_organizations'
    cursor = conn.cursor()

    columns = data[0].keys()
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"

    # Extract values from the list of dictionaries
    values = [tuple(d.values()) for d in data]

    # Execute the bulk insert query
    execute_values(cursor, query, values)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f'Writing organizations from csv successful: {len(data)}')

'''
This function queries the crunchbase_organizations table and writes them into pending table. wildcard '*' is supported.
otherwise list of category_groups is expected. Function is used to create a dataset for bots to scrape data for specific category groups.

uuids: List of uuids. wildcard * is supported
category_groups_list: List of categories. wildcard * is supported
source:     Expects a DataSource enum, i.e. crunchbase, companieshouse, linkedin
status:     Expects a PendingStatus enum. i.e. pending, completed
from_filter:    Date from which companies have been founded to query original csv crunchbase dataset
to_filter:      Date until companies have been founded to query original csv crunchbase dataset
force:      Used to force the status update. if it is set to false or true and current record does not exist in pending table, 
            a new record will be written. if it is set to false and a current record exists in the table, status will not be updated.
            if it is true and record exists, record status will be updated
'''
def write_organizations_pending(uuids, category_groups_list, country_codes, source, status=PendingStatus.pending,
                                fr=datetime.min, to=datetime.max, force=False):
    # write to organizations table
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cursor = conn.cursor()

    if uuids == '*':
        uuids_str = "'%'"
    else:
        uuids_str = ', '.join([f"'{item}'" for item in uuids])

    if category_groups_list == '*':
        category_groups_list_str = "'%'"
    else:
        category_groups_list_str = ', '.join([f"'%{item}%'" for item in category_groups_list])

    if country_codes == '*':
        country_codes_str = "'%'"
    else:
        country_codes_str = ', '.join([f"'{item}'" for item in country_codes])

    query = f"SELECT uuid, name, legal_name, country_code, category_groups_list, founded_on " \
            f"FROM crunchbase_organizations " \
            f"WHERE founded_on >= '{fr.strftime('%Y-%m-%dT%H:%M:%S')}' and " \
            f"founded_on <= '{to.strftime('%Y-%m-%dT%H:%M:%S')}' and " \
            f"category_groups_list::text ILIKE ANY (ARRAY[{category_groups_list_str}]) and " \
            f"country_code::text ILIKE ANY (ARRAY[{country_codes_str}]) and " \
            f"uuid::text ILIKE ANY (ARRAY[{uuids_str}])"

    cursor.execute(query)
    rows = cursor.fetchall()
    dt = datetime.utcnow()
    data = []

    for row in rows:
        uuid = row[0]
        name = row[1]
        legal_name = row[2]
        country_code = row[3]
        category_group_list = [element.strip() for element in row[4].split(',')]
        category_group_list_str = "{" + ",".join(category_group_list) + "}"

        founded_on = row[5]
        version = ''
        created_at = dt
        updated_at = dt
        d = {'uuid': uuid, 'uuid_parent': uuid, 'name': name, 'legal_name': legal_name, 'country_code': country_code, 'category_groups_list': category_group_list_str,
             'founded_on': founded_on, 'source': source.name, 'status': status.name, 'version': version,
             'created_at': created_at, 'updated_at': updated_at}
        data.append(d)

    # insert organizations into pending table. if force = True set state to pending. if force = False only add non existing records and don't change state
    table_name = 'pending'
    if data:
        columns = data[0].keys()
        if force:
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT (uuid, source) DO UPDATE SET status = EXCLUDED.status, version = EXCLUDED.version, updated_at = EXCLUDED.updated_at"
        else:
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"

        # Extract values from the list of dictionaries
        values = [tuple(d.values()) for d in data]

        # Execute the bulk insert query
        execute_values(cursor, query, values)

        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f'Writing organization pending successful: source: {source.name} status: {status} rows: {len(data)}')
    else:
        logger.info(f'Writing organization pending has data to write for {source.name} {str(category_groups_list)}.')


### find differences between crunchbase and companieshouse data
### if diff is false, the same and matching records will be returned
def get_data_diff(diff=True):
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cursor = conn.cursor()

    if diff == True:
        sign = '<'
    else:
        sign = '='

    sql = f"SELECT t4.uuid, t4.name, t4.source, t4.category_groups_list \
            FROM(SELECT t1.uuid, t2.name, t2.source \
                FROM (SELECT uuid \
                    FROM data \
                    WHERE source in ('companieshouse', 'crunchbase') \
                    GROUP BY uuid \
                    HAVING COUNT(DISTINCT source) {sign} (SELECT COUNT(DISTINCT source) FROM data WHERE source in ('companieshouse', 'crunchbase')) \
                ) AS t1 \
                JOIN (SELECT uuid, name, source FROM data) AS t2 \
                ON t1.uuid = t2.uuid) as t3 \
            JOIN (SELECT uuid, name, source, category_groups_list FROM pending) AS t4 \
            ON t3.uuid = t4.uuid and t3.source = t4.source"

    cursor.execute(sql)
    rows = cursor.fetchall()
    # Get the column names from the cursor description
    columns = [desc[0] for desc in cursor.description]

    # Transform the result set into a list of dictionaries
    result = [dict(zip(columns, row)) for row in rows]
    cursor.close()
    conn.close()

    return result
    # WHERE 'Hardware' = ANY (category_groups_list);"

# get uuids by name from crunchbase_organizations
def get_uuids_from_crunchbase_organizations(names=['elliptic', 'isize']):

    conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )

    cursor = conn.cursor()

    names_str = ', '.join([f"'%{item}%'" for item in names])
    query = f"SELECT * FROM crunchbase_organizations WHERE name ILIKE ANY (ARRAY[{names_str}])"

    cursor.execute(query)
    rows = cursor.fetchall()

    # Get the column names from the cursor description
    columns = [desc[0] for desc in cursor.description]

    # Transform the result set into a list of dictionaries
    result = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()
    return result

def get_profile_uuid(name, company_uuid):
    return name.lower() + '|' + str(company_uuid)

'''
uuids: company uuids
'''
def get_data(uuids='*'):

    conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )

    cursor = conn.cursor()

    if uuids == '*':
        uuids_str = "'%'"
    else:
        uuids_str = ', '.join([f"'{item}'" for item in uuids])

    query = "SELECT t3.uuid, t3.name, t3.crunchbase_data, t3.companieshouse_data, t4.linkedin_data " \
            "FROM " \
            "(" \
                "SELECT t1.uuid, t2.name, t1.data as crunchbase_data, t2.data as companieshouse_data " \
                "FROM " \
                "(" \
                    "SELECT uuid, data " \
                    "FROM data " \
                    f"WHERE source = 'crunchbase' AND uuid::text LIKE ANY (ARRAY[{uuids_str}])" \
                ") AS t1 " \
                "INNER JOIN " \
                "(" \
                    "SELECT uuid, name, data " \
                    "FROM data " \
                    "WHERE source = 'companieshouse'" \
                ") AS t2 " \
                "ON t1.uuid = t2.uuid" \
            ") AS t3 " \
            "INNER JOIN " \
            "(" \
                "SELECT uuid_parent, json_build_object('source', 'linkedin', 'properties', json_build_object(), 'cards', json_build_object('persons', json_agg(data))) as linkedin_data " \
                "FROM data " \
                "WHERE source = 'linkedin' " \
                "GROUP BY uuid_parent" \
            ") AS t4 " \
            "ON t3.uuid = t4.uuid_parent "

    cursor.execute(query)
    rows = cursor.fetchall()

    # Get the column names from the cursor description
    columns = [desc[0] for desc in cursor.description]

    # Transform the result set into a list of dictionaries
    result = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()
    return result

class CustomFormatter(logging.Formatter):
    def __init__(self):
        template = "{:<19} | {:<10} | {:<10} | {:<20} | {:<35} | {}"
        self.header = template.format("timestamp", "levelname", "name", "module", "funcname", "message")
        fmt = "%(asctime)s | %(levelname)10s | %(name)10s | %(module)20s | %(funcName)35s | %(message)s (%(filename)s:%(lineno)d)"
        super().__init__(fmt, datefmt="%Y-%m-%dT%H:%M:%S")
        self.first_record = True

    def format(self, record):
        # Prepend the column header for the first record
        if self.first_record:
            self.first_record = False
            return f"{self.header}\n{super().format(record)}"

        message = super().format(record)
        return message

# Set the custom formatter for the file handler
def get_logger(name):
    # Create a logger
    logger = logging.getLogger('companybot')
    logger.setLevel(logging.DEBUG)

    # Create a console handler and set its log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a file handler and set its log level
    os.makedirs('log', exist_ok=True)
    utc_str = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    log_name = f'log\\companybot_{utc_str}.log'
    file_handler = logging.FileHandler(log_name, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)

    # Define a log format
    formatter = CustomFormatter()

    # Set the log format for the handlers
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    ## Test the logger
    # logger.debug('Debug message')
    # logger.info('Info message')
    # logger.warning('Warning message')
    # logger.error('Error message')
    # print(os.path.exists(log_name))
    return logger


def bulk_export_crunchbase(download_path="C:\\Users\\Djordje\\Downloads\\crunchbase_bulk_export.tar.gz"):
    # URL of the file to be downloaded
    url = f"https://api.crunchbase.com/bulk/v4/bulk_export.tar.gz?user_key={CRUNCHBASE_KEY}"
    # Path where the file will be extracted
    extraction_path = "./data/crunchbase/bulk_export"

    # Download the file
    response = requests.get(url, stream=True)

    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024 #1 Kibibyte
    total_kb = total_size // block_size

    with open(download_path, 'wb') as file:
        for data in response.iter_content(block_size):
            file.write(data)
            downloaded_kb = file.tell() // block_size
            print(f'Downloaded {downloaded_kb} of {total_kb} KB', end='\r')

    # Check if the downloaded file is a tar.gz file
    if (tarfile.is_tarfile(download_path)):
        # If it is, extract it
        with tarfile.open(download_path, 'r:gz') as tar:
            members = tar.getmembers()
            for i, member in enumerate(members):
                tar.extract(member, path=extraction_path)
                print(f'Extracted {i + 1} of {len(members)} files', end='\r')
    else:
        print(f"{download_path} is not a .tar.gz file")

    # Delete the downloaded tar.gz file after extraction
    os.remove(download_path)


def node_keys_crunchbase(download_path="C:\\Users\\Djordje\\Downloads\\crunchbase_node_keys.tar.gz"):
    # URL of the file to be downloaded
    url = f"https://api.crunchbase.com/node_keys/v4/node_keys.tar.gz?user_key={CRUNCHBASE_KEY}"
    # Path where the file will be extracted
    extraction_path = "./data/crunchbase/node_keys"

    # Download the file
    response = requests.get(url, stream=True)

    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024 #1 Kibibyte
    total_kb = total_size // block_size

    with open(download_path, 'wb') as file:
        for data in response.iter_content(block_size):
            file.write(data)
            downloaded_kb = file.tell() // block_size
            print(f'Downloaded {downloaded_kb} of {total_kb} KB', end='\r')

    # Check if the downloaded file is a tar.gz file
    if (tarfile.is_tarfile(download_path)):
        # If it is, extract it
        with tarfile.open(download_path, 'r:gz') as tar:
            members = tar.getmembers()
            for i, member in enumerate(members):
                tar.extract(member, path=extraction_path)
                print(f'Extracted {i + 1} of {len(members)} files', end='\r')
    else:
        print(f"{download_path} is not a .tar.gz file")

    # Delete the downloaded tar.gz file after extraction
    os.remove(download_path)


def initialize(uuids_filter ='*', category_groups_list_filter ='*', country_code_filter ='*',
               from_filter=datetime.min, to_filter=datetime.max,
               download_crunchbase_csv=True, drop_tables=False, write_organizations=False, pending_force=False):
    """
    Sets up and populates a database with specific Crunchbase companies based on a provided filter.

    :param filter: A dictionary with keys to filter the organizations. Defaults to only include
                   'Artificial Intelligence' companies from 'GBR' (Great Britain).
    :type filter: dict
    :param downloa  d_crunchbase_csv: If True, the function will download the raw csv data from Crunchbase.
                                    This data is used to initially populate Crunchbase companies with uuid.
                                    Defaults to True.
    :type download_crunchbase_csv: bool
    :param drop_tables: If True, existing database tables will be dropped and recreated. Use with caution!
                        Defaults to False.
    :type drop_tables: bool

    :return: None

    The function performs the following steps:
    0. Create database if it doesn not exist
    1. If download_crunchbase_csv is True, downloads and extracts Crunchbase data.
    2. Creates the necessary database tables.
    3. Gets a list of organizations as per the filter from the downloaded Crunchbase data.
    4. Writes these organizations into the Crunchbase organizations table in the database.
    5. Writes these organizations to the pending table with the type set to Crunchbase,
       for a defined date range and with status set to pending.
    6. Writes these organizations to the pending table with the type set to Companies House,
       for a defined date range and with status set to pending.
    """

    if download_crunchbase_csv:
        node_keys_crunchbase()
        bulk_export_crunchbase()

    create_database()
    create_organizations_table(drop_tables)
    create_pending_table(drop_tables)
    create_data_table(drop_tables)

    if write_organizations:
        # get list of organizatins as list of dictionaries. this is the initial step used to fill the database with subset of crunchbase orgnizations.
        # we for example just record GBR organizations in crunchbase_organizations table
        # organizations = get_organizations_from_crunchbase_csv({'category_groups_list': ['Artificial Intelligence'], 'country_code': ['GBR']})
        organizations = get_organizations_from_crunchbase_csv(uuids_filter, category_groups_list_filter, country_code_filter, from_filter, to_filter)
        # write the organizations from crunchbase csv file into database. we use that usually to create a subset
        write_organizations_from_csv(organizations, logging=logging)

        write_organizations_pending(uuids=uuids_filter, category_groups_list=category_groups_list_filter, country_codes=country_code_filter,
                                    source=DataSource.crunchbase,
                                    status=PendingStatus.pending,
                                    fr=from_filter,
                                    to=to_filter,
                                    force=pending_force)


from fuzzywuzzy import fuzz
import itertools

def get_aligned_name(name):
    split_name = name.split(',')
    aligned_name = ' '.join([n.strip() for n in split_name[::-1]])
    return aligned_name.strip()

def get_profile_name(name):
    name_split = name.split(' ')
    if len(name_split) == 0:
        return name_split[0].strip()
    else:
        return (name_split[0] + ' ' + name_split[-1]).strip()

def get_persons(data, ratio=75, ratio_dob=55):

    unique_shareholders = get_unique_shareholders(data['cards']['shareholding'], ratio=ratio)
    unique_officers = get_unique_officers(data['cards']['officer']['items'], ratio=ratio, ratio_dob=ratio_dob)
    unique_founders = get_unique_founders(data['cards']['incorporation']['items'], ratio=ratio)

    persons = unique_shareholders + unique_officers + unique_founders
    unique_persons = get_unique_persons(persons, ratio, ratio_dob)

    out = {}
    out['company_id'] = data['properties']['company_id']
    out['company_name'] = data['properties']['company_name']
    out['items'] = []
    for person in unique_persons:
        out['items'].append(person)

    return out


def get_name_combinations(name, length, ignore_single_characters=True):
    #     print(name, length)
    # Break the name into individual words and remove single character words
    if ignore_single_characters:
        name_parts = [part for part in name.lower().split() if len(part.replace('.', '')) > 1]
    else:
        name_parts = name.lower().split()

    # Create combinations of the given length
    return list(itertools.combinations(name_parts, length))


def match_names(name1, name2, ratio=80):
    # Get permutations for both names

    name1_parts = name1.lower().split()
    name2_parts = name2.lower().split()

    # If both names have only one part, check for an exact match
    if len(name1_parts) == 1 and len(name2_parts) == 1:
        return name1_parts[0] == name2_parts[0]

    # If one of the names has two parts with one of them having only one character,
    # check if it exactly matches one of the tuples of the other name
    if len(name1_parts) == 2 and any(len(part) == 1 for part in name1_parts):
        return any(
            set(name1_parts) == set(perm) for perm in get_name_combinations(name2, 2, ignore_single_characters=False))
    if len(name2_parts) == 2 and any(len(part) == 1 for part in name2_parts):
        return any(
            set(name2_parts) == set(perm) for perm in get_name_combinations(name1, 2, ignore_single_characters=False))

    name1_combinations = get_name_combinations(name1, 2)
    name2_combinations = get_name_combinations(name2, 2)

    #     print(name1_combinations, name2_combinations)
    # Loop through each permutation pair and apply fuzzy matching
    for perm1 in name1_combinations:
        for perm2 in name2_combinations:
            name_1 = ' '.join(perm1)
            name_2 = ' '.join(perm2)
            r = fuzz.token_set_ratio(name_1, name_2)
            #             print(perm1, perm2, name_1, name_2, r)
            # We join the tuples back into strings for comparison using token_set_ratio
            if r >= ratio:
                # If any pair of permutations has a high match ratio, consider the names a match
                return True

    # If no pairs of permutations have a high match ratio, the names don't match
    return False


def get_unique_founders(people, ratio=80):
    people_copy = people.copy()  # Create a copy of the original list
    unique_people = []
    while people_copy:
        person = people_copy.pop(0)  # Start with the first person

        # If the name is a company name, skip it and continue with the next person
        if is_organization(person["name"]):
            continue

        # Separate the rest into duplicates and non-duplicates of this person
        duplicates = []
        non_duplicates = []
        for other_person in people_copy:
            similar_n = match_names(person["name"], other_person["name"], ratio=ratio)
            if similar_n:
                duplicates.append(other_person)
            else:
                non_duplicates.append(other_person)

        # Merge the person and duplicates into a single record
        merged_person = {"name": "", "profile_name": "", "occupation": ['Founder'], "date_of_birth": None}

        for p in [person] + duplicates:

            if len(p["name"]) > len(merged_person["name"]):  # Keep the longest name

                name = p['name'].replace('-', ' ')
                profile_name = get_profile_name(name)

                merged_person["name"] = name.title()
                merged_person["profile_name"] = profile_name.title()

        unique_people.append(merged_person)
        people_copy = non_duplicates  # Continue with the remaining people
    unique_people = sorted(unique_people, key=lambda x: x['name'])
    return unique_people


def get_unique_shareholders(people, ratio=80):
    people_copy = []
    for key, value in people.copy().items():
        if value['items']: people_copy.extend(value['items'])

    unique_people = []
    while people_copy:
        person = people_copy.pop(0)  # Start with the first person

        # If the name is a company name, skip it and continue with the next person
        if is_organization(person["name"]):
            continue

        # Separate the rest into duplicates and non-duplicates of this person
        duplicates = []
        non_duplicates = []
        for other_person in people_copy:
            similar_n = match_names(person["name"], other_person["name"], ratio=ratio)
            #             print(f'{person["name"]} | {other_person["name"]} | {similar_n}')
            if similar_n:
                duplicates.append(other_person)
            else:
                non_duplicates.append(other_person)

        # Merge the person and duplicates into a single record
        merged_person = {"name": "", "profile_name": "", "occupation": ['Shareholder'],
                         "date_of_birth": None}
        for p in [person] + duplicates:

            if len(p["name"]) > len(merged_person["name"]):  # Keep the longest name
                name = p['name'].replace('-', ' ')
                profile_name = get_profile_name(name)

                merged_person["name"] = name.title()
                merged_person["profile_name"] = profile_name.title()

        unique_people.append(merged_person)
        people_copy = non_duplicates  # Continue with the remaining people
    unique_people = sorted(unique_people, key=lambda x: x['name'])
    return unique_people


def get_unique_officers(people, ratio=80, ratio_dob=60, occupations_name='role'):
    people_copy = people.copy()  # Create a copy of the original list
    unique_people = []
    while people_copy:
        person = people_copy.pop(0)  # Start with the first person

        # If the name is a company name, skip it and continue with the next person
        if is_organization(person["name"]):
            continue

        # Separate the rest into duplicates and non-duplicates of this person
        duplicates = []
        non_duplicates = []
        for other_person in people_copy:
            similar_n = match_names(person["name"], other_person["name"], ratio=ratio)
            similar_dob_n = match_names(person["name"], other_person["name"], ratio=ratio_dob)
            similar_dob = False
            if person["date_of_birth"] or other_person["date_of_birth"]:
                similar_dob = person["date_of_birth"] == other_person["date_of_birth"]
            #             print(f'{person["name"]} | {other_person["name"]} | {similar_n} {similar_dob_n and similar_dob}')
            if similar_n or (similar_dob_n and similar_dob):
                duplicates.append(other_person)
            else:
                non_duplicates.append(other_person)

        # Merge the person and duplicates into a single record
        merged_person = {"name": "", "profile_name": "", "occupation": [], "date_of_birth": None}

        for p in [person] + duplicates:

            if len(p['name']) > len(merged_person["name"]):  # Keep the longest name
                name = p['name'].title()
                profile_name = get_profile_name(name)
                merged_person["name"] = name
                merged_person["profile_name"] = profile_name.title()

            if p[occupations_name] not in merged_person["occupation"]:  # Combine the roles
                if type(p[occupations_name]) == str:
                    merged_person["occupation"].append(p[occupations_name])
                else:
                    merged_person["occupation"].extend(p[occupations_name])

            if "date_of_birth" in p and p["date_of_birth"]:  # Retain the birth date if available
                merged_person["date_of_birth"] = p["date_of_birth"]

        merged_person["occupation"] = list(sorted(set(merged_person["occupation"])))

        unique_people.append(merged_person)
        people_copy = non_duplicates  # Continue with the remaining people
    unique_people = sorted(unique_people, key=lambda x: x['name'])
    return unique_people

def get_user_agent(email):
    index = hash(email) % len(USER_AGENTS)
    return USER_AGENTS[index]

def get_unique_persons(data, ratio, ratio_dob):
    return get_unique_officers(data, ratio, ratio_dob, occupations_name='occupation')

def get_data_from_pending(source, uuids='*', uuids_parent='*', category_groups_list='*', country_codes='*', fr=datetime.max, to=datetime.max, force=False):

    # uuids_str = ', '.join([f"'{item}'" for item in uuids]) if type(
    #     uuids) == list else uuids.replace('*', "'%'")

    assert type(uuids_parent) == list or uuids_parent == '*'
    assert type(uuids) == list or uuids == '*'
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

    uuids_parent_str = "'%'" if uuids_parent == '*' else ', '.join([f"'{item}'" for item in uuids_parent])
    uuids_str = "'%'" if uuids == '*' else ', '.join([f"'{item}'" for item in uuids])
    category_groups_list_str = "'%'" if category_groups_list == '*' else ', '.join([f"'%{item}%'" for item in category_groups_list])
    country_codes_str = "'%'" if country_codes == '*' else ', '.join([f"'{item}'" for item in country_codes])

    pending = f"" if force else f" and pending.status = '{PendingStatus.pending.name}' "

    cursor = conn.cursor()
    query = f"SELECT uuid, uuid_parent, name, legal_name, country_code, category_groups_list, founded_on " \
            f"FROM pending " \
            f"WHERE source = '{source}' and " \
            f"founded_on >= '{fr.strftime('%Y-%m-%dT%H:%M:%S')}' and " \
            f"founded_on <= '{to.strftime('%Y-%m-%dT%H:%M:%S')}' and " \
            f"category_groups_list::text ILIKE ANY (ARRAY[{category_groups_list_str}]) and " \
            f"country_code::text ILIKE ANY (ARRAY[{country_codes_str}]) and " \
            f"uuid_parent::text LIKE ANY (ARRAY[{uuids_parent_str}]) and " \
            f"uuid::text LIKE ANY (ARRAY[{uuids_str}])" \
            f"{pending}" \
            f"ORDER BY name ASC"

    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    cursor.close()
    results = [dict(zip(columns, row)) for row in rows]

    return results

logger = get_logger('CompanyBot')
logger.propagate = False

