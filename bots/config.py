import os
import pytesseract
from dotenv import load_dotenv
from pathlib import Path

# Specify the path to the .env file located one folder up
env_path = Path('..') / '.env'

# Load environment variables from the specified .env file
load_dotenv(dotenv_path=env_path)

# Load variables from the environment
CRUNCHBASE_DIR = './data/crunchbase'
CRUNCHBASE_KEY = os.getenv('CRUNCHBASE_KEY')

POPPLER_PATH = os.getenv('POPPLER_PATH')
TESSDATA_PATH = os.getenv('TESSDATA_PATH')
TESSERACT_PATH = pytesseract.pytesseract.tesseract_cmd = os.getenv('TESSERACT_PATH')

BRAVE_PATH = os.getenv('BRAVE_PATH')

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = int(os.getenv('DB_PORT'))

LINKEDIN_EMAIL = os.getenv('LINKEDIN_EMAIL')
LINKEDIN_PWD = os.getenv('LINKEDIN_PWD')

# CATEGORY_LIST_GROUPS and other non-sensitive variables can stay in your Python script as they are not sensitive data
CATEGORY_LIST_GROUPS = [
    'Administrative Services', 'Advertising', 'Agriculture and Farming', 'Apps', 'Artificial Intelligence',
    'Biotechnology', 'Clothing and Apparel', 'Commerce and Shopping', 'Community and Lifestyle', 'Consumer Electronics',
    'Consumer Goods', 'Content and Publishing', 'Data and Analytics', 'Design', 'Education', 'Energy', 'Events',
    'Financial Services', 'Food and Beverage', 'Gaming', 'Government and Military', 'Hardware', 'Health Care',
    'Information Technology', 'Internet Services', 'Manufacturing', 'Media and Entertainment',
    'Messaging and Telecommunications', 'Mobile', 'Music and Audio', 'Natural Resources', 'Navigation and Mapping', 'Other',
    'Payments', 'Platforms', 'Privacy and Security', 'Professional Services', 'Real Estate', 'Sales and Marketing',
    'Science and Engineering', 'Software', 'Sports', 'Sustainability', 'Transportation', 'Travel and Tourism', 'Video'
]


# organizations found: 14902 {'category_groups_list': ['Administrative Services'], 'country_code': ['GBR']}
# organizations found: 12306 {'category_groups_list': ['Advertising'], 'country_code': ['GBR']}
# organizations found: 1896 {'category_groups_list': ['Agriculture and Farming'], 'country_code': ['GBR']}
# organizations found: 5569 {'category_groups_list': ['Apps'], 'country_code': ['GBR']}
# organizations found: 2749 {'category_groups_list': ['Artificial Intelligence'], 'country_code': ['GBR']}
# organizations found: 2670 {'category_groups_list': ['Biotechnology'], 'country_code': ['GBR']}
# organizations found: 4066 {'category_groups_list': ['Clothing and Apparel'], 'country_code': ['GBR']}
# organizations found: 29233 {'category_groups_list': ['Commerce and Shopping'], 'country_code': ['GBR']}
# organizations found: 10357 {'category_groups_list': ['Community and Lifestyle'], 'country_code': ['GBR']}
# organizations found: 7704 {'category_groups_list': ['Consumer Electronics'], 'country_code': ['GBR']}
# organizations found: 7990 {'category_groups_list': ['Consumer Goods'], 'country_code': ['GBR']}
# organizations found: 9899 {'category_groups_list': ['Content and Publishing'], 'country_code': ['GBR']}
# organizations found: 9620 {'category_groups_list': ['Data and Analytics'], 'country_code': ['GBR']}
# organizations found: 23731 {'category_groups_list': ['Design'], 'country_code': ['GBR']}
# organizations found: 14318 {'category_groups_list': ['Education'], 'country_code': ['GBR']}
# organizations found: 6205 {'category_groups_list': ['Energy'], 'country_code': ['GBR']}
# organizations found: 6727 {'category_groups_list': ['Events'], 'country_code': ['GBR']}
# organizations found: 26169 {'category_groups_list': ['Financial Services'], 'country_code': ['GBR']}
# organizations found: 8684 {'category_groups_list': ['Food and Beverage'], 'country_code': ['GBR']}
# organizations found: 2020 {'category_groups_list': ['Gaming'], 'country_code': ['GBR']}
# organizations found: 2994 {'category_groups_list': ['Government and Military'], 'country_code': ['GBR']}
# organizations found: 17697 {'category_groups_list': ['Hardware'], 'country_code': ['GBR']}
# organizations found: 19502 {'category_groups_list': ['Health Care'], 'country_code': ['GBR']}
# organizations found: 27748 {'category_groups_list': ['Information Technology'], 'country_code': ['GBR']}
# organizations found: 20490 {'category_groups_list': ['Internet Services'], 'country_code': ['GBR']}
# organizations found: 29298 {'category_groups_list': ['Manufacturing'], 'country_code': ['GBR']}
# organizations found: 32680 {'category_groups_list': ['Media and Entertainment'], 'country_code': ['GBR']}
# organizations found: 1762 {'category_groups_list': ['Messaging and Telecommunications'], 'country_code': ['GBR']}
# organizations found: 6057 {'category_groups_list': ['Mobile'], 'country_code': ['GBR']}
# organizations found: 3640 {'category_groups_list': ['Music and Audio'], 'country_code': ['GBR']}
# organizations found: 4150 {'category_groups_list': ['Natural Resources'], 'country_code': ['GBR']}
# organizations found: 698 {'category_groups_list': ['Navigation and Mapping'], 'country_code': ['GBR']}
# organizations found: 37910 {'category_groups_list': ['Other'], 'country_code': ['GBR']}
# organizations found: 2590 {'category_groups_list': ['Payments'], 'country_code': ['GBR']}
# organizations found: 1207 {'category_groups_list': ['Platforms'], 'country_code': ['GBR']}
# organizations found: 5830 {'category_groups_list': ['Privacy and Security'], 'country_code': ['GBR']}
# organizations found: 42590 {'category_groups_list': ['Professional Services'], 'country_code': ['GBR']}
# organizations found: 27784 {'category_groups_list': ['Real Estate'], 'country_code': ['GBR']}
# organizations found: 27107 {'category_groups_list': ['Sales and Marketing'], 'country_code': ['GBR']}
# organizations found: 17365 {'category_groups_list': ['Science and Engineering'], 'country_code': ['GBR']}
# organizations found: 38249 {'category_groups_list': ['Software'], 'country_code': ['GBR']}
# organizations found: 7302 {'category_groups_list': ['Sports'], 'country_code': ['GBR']}
# organizations found: 6008 {'category_groups_list': ['Sustainability'], 'country_code': ['GBR']}
# organizations found: 15679 {'category_groups_list': ['Transportation'], 'country_code': ['GBR']}
# organizations found: 5979 {'category_groups_list': ['Travel and Tourism'], 'country_code': ['GBR']}
# organizations found: 4965 {'category_groups_list': ['Video'], 'country_code': ['GBR']}
