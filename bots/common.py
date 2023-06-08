from fuzzywuzzy import fuzz
from .config import DB_NAME, DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, CRUNCHBASE_DIR, POPPLER_PATH, BRAVE_PATH
import time
from datetime import datetime
from enum import Enum
import re, psycopg2
from psycopg2.extras import execute_values
import csv, numpy as np
import scrapy, os, json, logging

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
def get_organizations_from_crunchbase_csv(filter = {'category_groups_list': ['Information Technology'], 'country_code': ['GBR']}):
    filename = f'{CRUNCHBASE_DIR}/bulk_export/organizations.csv'

    with open(filename, 'r', encoding='utf-8') as fp:

        i = 0
        values = []
        organizations_list = []
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
                            organizations_list.append(dict(zip(header, values)))
            except Exception as ex:
                print(f'error: {str(ex)}')
            i += 1
        print(f'organizations found: {len(organizations_list)} {str(filter)}')
        clean_list_of_dictionaries(organizations_list)
        return sorted(organizations_list, key=lambda x: x['name'])

def create_organizations_table():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port = DB_PORT
    )

    cursor = conn.cursor()

    table_name = 'crunchbase_organizations'
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

# pending table will contain all entries that need further processing by crunchbase REST API, companies house spider or linked in spider
def create_pending_table():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cursor = conn.cursor()

    table_name = 'pending'
    create_table_query = """
        CREATE TABLE IF NOT EXISTS {} (
            uuid UUID NOT NULL,
            uuid_parent UUID NOT NULL,
            name TEXT NOT NULL,
            legal_name TEXT,
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
def create_data_table():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cursor = conn.cursor()

    table_name = 'data'
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

def clean_list_of_dictionaries(data):
    for d in data:
        for key, value in d.items():
            if value == '':
                d[key] = None

# writes all organizations from csv file to database. we use it to create for example a GBR subset of data
def write_organizations_from_csv(organizations):
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
    print(f'write_organizations_from_csv successful: {len(data)}')

'''
This function queries the crunchbase_organizations table and writes them into pending table. wildcard '*' is supported.
otherwise list of category_groups is expected. Function is used to create a dataset for bots to scrape data for specific category groups.

uuids: List of uuids. wildcard * is supported
category_groups_list: List of categories. wildcard * is supported
source:     Expects a DataSource enum, i.e. crunchbase, companieshouse, linkedin
status:     Expects a PendingStatus enum. i.e. pending, completed
from_dt:    Date from which companies have been founded to query original csv crunchbase dataset
to_dt:      Date until companies have been founded to query original csv crunchbase dataset
force:      Used to force the status update. if it is set to false or true and current record does not exist in pending table, 
            a new record will be written. if it is set to false and a current record exists in the table, status will not be updated.
            if it is true and record exists, record status will be updated
'''
def write_organizations_pending(uuids, category_groups_list, source, status=PendingStatus.pending, from_dt=datetime.min, to_dt=datetime.max, force=False):
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

    query = f"SELECT uuid, name, legal_name, category_groups_list, founded_on " \
            f"FROM crunchbase_organizations " \
            f"WHERE founded_on >= '{from_dt.strftime('%Y-%m-%dT%H:%M:%S')}' and " \
            f"founded_on <= '{to_dt.strftime('%Y-%m-%dT%H:%M:%S')}' and " \
            f"category_groups_list::text ILIKE ANY (ARRAY[{category_groups_list_str}]) and " \
            f"uuid::text ILIKE ANY (ARRAY[{uuids_str}])"

    cursor.execute(query)
    rows = cursor.fetchall()
    dt = datetime.utcnow()
    data  = []
    for row in rows:
        uuid = row[0]
        name = row[1]
        legal_name = row[2]
        category_group_list = [element.strip() for element in row[3].split(',')]
        category_group_list_str = "{" + ",".join(category_group_list) + "}"

        founded_on = row[4]
        version = ''
        created_at = dt
        updated_at = dt
        d = {'uuid': uuid, 'uuid_parent': uuid, 'name': name, 'legal_name': legal_name, 'category_groups_list': category_group_list_str,
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
        print(f'write_organizations_pending successful: source: {source.name} status: {status} rows: {len(data)}')
    else:
        print(f'write_organizations_pending no data to write for {source.name} {str(category_groups_list)}.')


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

def get_profile_uuid(full_name, company_uuid):
    return full_name.lower() + '|' + str(company_uuid)

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
        template = "{:<19} | {:<10} | {:<10} | {:<12} | {:<12} | {}"
        self.header = template.format("timestamp", "levelname", "name", "module", "funcname", "message")
        fmt = "%(asctime)s | %(levelname)10s | %(name)10s | %(module)12s | %(funcName)12s | %(message)s (%(filename)s:%(lineno)d)"
        super().__init__(fmt, datefmt="%Y-%m-%dT%H:%M:%S")
        self.first_record = True

    def format(self, record):
        # Prepend the column header for the first record
        if self.first_record:
            self.first_record = False
            return f"{self.header}\n{super().format(record)}"
        return super().format(record)

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
    file_handler = logging.FileHandler(log_name)
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