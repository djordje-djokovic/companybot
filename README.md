[//]: # (    {)

[//]: # (        'author': 'Dr. Djordje Djokovic',)

[//]: # (        'text': '“I am not a product of my circumstances. I am a product of my decisions.” ',)

[//]: # (        'tags': ['entrepreneurship', 'high technology', 'commercialization'])

[//]: # (    })

# CompanyBot [UNDER DEVELOPMENT]

**Note: This project is still under active development and not safe to run!**

CompanyBot is an advanced data aggregation project designed to consolidate information about companies and entrepreneurs from multiple sources into a single, comprehensive database. This unified database serves as a powerful resource for in-depth statistical analysis and research.

## Bots

The project currently includes three specialized bots:

1. **Crunchbase Bot** [`crunchbase_bot`]
2. **Companies House Bot** [`companieshouse_bot`]
3. **LinkedIn Bot** [`linkedin_bot`]

Each bot is tailored to extract specific data from its respective source, ensuring a wide range of relevant information is captured.


### Companies House Bot (Partially Implemented)

The `companieshouse_bot` leverages the `scrapy` library to systematically extract data from the official Companies House website (https://find-and-update.company-information.service.gov.uk/). This bot provides crucial information about UK-registered companies.

#### Scraping Process
The bot navigates through various pages and documents using a provided `companyhouse_id`. It employs several parsing methods to extract different types of information:

1. `parse_company_info`: 
   Extracts general company information from the main page, including:
   - Official company name
   - Companies House ID
   - Incorporation date
   - Company type
   - Registered office address
   - SIC code (nature of business)

2. `parse_officers`: 
   Retrieves a comprehensive list of company officers, including:
   - Full names of secretaries and directors
   - Appointment dates
   - Resignation dates (if applicable)

3. `parse_filing`: 
   Analyzes annual company filings and other important documents. Key documents parsed include:
   - Incorporation documents (to extract founder names)
   - Confirmation Statements / Annual Returns (for shareholder information, ownership details, and capital structure)
   
   Note: Some documents like Capital, Accounts, and Charges are currently not parsed but may be included in future updates.

4. `parse_insolvency`: 
   Extracts information related to company insolvency, when applicable. This data is available for a subset of companies.

### Crunchbase Bot (Almost Completed)

The `crunchbase_bot` utilizes the official Crunchbase API, which requires university research access. This bot is crucial for gathering comprehensive company data from one of the most extensive startup and company databases available.

#### API Structure
The Crunchbase API employs a structure of fields and cards:
- A card is a collection of related fields
- The company itself is represented as a card with its own set of fields

#### Data Retrieval
The bot queries two main types of data:

1. **Company Fields**: 
   Full list of fields queried:
   ```
   acquirer_identifier, aliases, categories, category_groups, closed_on, company_type, contact_email, created_at, delisted_on, demo_days, description, diversity_spotlights, entity_def_id, equity_funding_total, exited_on, facebook, facet_ids, founded_on, founder_identifiers, funding_stage, funding_total, funds_total, hub_tags, identifier, image_id, image_url, investor_identifiers, investor_stage, investor_type, ipo_status, last_equity_funding_total, last_equity_funding_type, last_funding_at, last_funding_total, last_funding_type, last_key_employee_change_date, last_layoff_date, layout_id, legal_name, linkedin, listed_stock_symbol, location_group_identifiers, location_identifiers, name, num_acquisitions, num_alumni, num_articles, num_current_advisor_positions, num_current_positions, num_diversity_spotlight_investments, num_employees_enum, num_enrollments, num_event_appearances, num_exits, num_exits_ipo, num_founder_alumni, num_founders, num_funding_rounds, num_funds, num_investments, num_investors, num_lead_investments, num_lead_investors, num_past_positions, num_portfolio_organizations, num_sub_organizations, operating_status, override_layout_id, owner_identifier, permalink, permalink_aliases, phone_number, program_application_deadline, program_duration, program_type, rank_delta_d30, rank_delta_d7, rank_delta_d90, rank_org, rank_principal, revenue_range, school_method, school_program, school_type, short_description, status, stock_exchange_symbol, stock_symbol, twitter, updated_at, uuid, valuation, valuation_date, website, website_url, went_public_on
   ```

2. **Cards**:
   Full list of cards queried:
   ```
   acquiree_acquisitions, acquirer_acquisitions, child_organizations, child_ownerships, event_appearances, fields, founders, headquarters_address, investors, ipos, jobs, key_employee_changes, layoffs, parent_organization, parent_ownership, participated_funding_rounds, participated_funds, participated_investments, press_references, raised_funding_rounds, raised_funds, raised_investments
   ```

#### Data Storage
All retrieved data is consolidated into a single dictionary and stored as a JSON object in the database, allowing for flexible querying and analysis.

### LinkedIn Bot (Partially Implemented)

The `linkedin_bot` employs custom parsing techniques using Python's `beautifulsoup` library and Selenium for web automation. This bot is designed to extract professional information from LinkedIn profiles.

#### Authentication
The bot requires valid LinkedIn credentials to log in and access profile information.

#### Profile Identification
The bot can identify profiles in two ways:
1. Direct URL input
2. Search functionality using profile name and company name, employing fuzzy matching to locate the correct profile

#### Data Extraction
The `linkedin_bot` extracts three main categories of information:

1. **General Information**:
   - Full name
   - Current position
   - Location
   - Industry

2. **Employment History**:
   For each position:
   - Organization name
   - Job title
   - Employment duration (start and end dates)
   - Job description (when available)

3. **Education History**:
   For each educational experience:
   - Institution name
   - Degree obtained
   - Field of study
   - Duration of study (start and end dates)

#### Data Storage
All extracted information is structured and stored in the database, allowing for integration with data from other bots and comprehensive analysis.

## Getting Started

1. **Install PostgreSQL** and create a database (e.g., 'uni'). Configure the database settings in `./bots/config.py`.

2. **Run the bots** using the main script. You can use command-line arguments to control the behavior of the bots.

3. **Setup .env file**

To keep sensitive information secure, such as API keys, database credentials, and file paths, the application uses environment variables stored in a `.env` file. This file should be placed in the **root directory** of your project.

The `.env` file contains key-value pairs for sensitive configurations. Below is an example of how the `.env` file should look:

```plaintext
# API keys and sensitive credentials
CRUNCHBASE_KEY=XXX
POPPLER_PATH=C:\\Program Files (x86)\\poppler-23.01.0\\Library\\bin
TESSDATA_PATH=C:\\Users\\USER\\miniconda3\\share\\tessdata
TESSERACT_PATH=C:\\Program Files\\Tesseract-OCR\\tesseract.exe

BRAVE_PATH=C:\\Program Files\\BraveSoftware\\Brave-Browser\\Application\\brave.exe

# Database configuration
DB_HOST=localhost
DB_NAME=uni
DB_USER=postgres
DB_PASSWORD=XXX
DB_PORT=5433

# LinkedIn login credentials
LINKEDIN_EMAIL=your-email@example.com
LINKEDIN_PWD=yourpassword
```

## Usage

Run the main script with desired command-line arguments:

```
python main.py [arguments]
```

### Command-Line Arguments

- `--uuids-company-filter`: Company UUIDs filter (default: "*")
- `--uuids-profile-filter`: LinkedIn UUIDs profile filter (default: "*")
- `--category-groups-list-filter`: Company category group filter (default: "Artificial Intelligence")
- `--country-code-filter`: Company country code filter (default: "GBR")
- `--from-filter`: Founding start date filter
- `--to-filter`: Founding end date filter
- `--initialize-run`: Run initialization process (default: "false")
- `--initialize-drop-tables`: Drop existing database tables (default: "false")
- `--initialize-download-csv`: Download raw CSV data from Crunchbase (default: "false")
- `--initialize-write-organizations`: Write organizations from Crunchbase CSV file (default: "false")
- `--initialize-pending-force`: Force reset pending state (default: "false")
- `--crunchbase-run`: Run the Crunchbase bot (default: "false")
- `--crunchbase-force`: Force updating Crunchbase data (default: "false")
- `--companieshouse-run`: Run the Companies House bot (default: "false")
- `--companieshouse-force`: Force updating Companies House data (default: "false")
- `--linkedin-run`: Run the LinkedIn bot (default: "false")
- `--linkedin-force`: Force updating LinkedIn data (default: "false")
- `--linkedin-occupations-filter`: LinkedIn occupations filter (default: "Founder, Director Shareholder")

### Example Output Data

The bots store the consolidated data in the PostgreSQL database as JSON objects. Below is an example of the kind of data you can expect:

```json
{
    "company": {
        "name": "Tech Innovators Ltd",
        "founded_on": "2015-06-01",
        "description": "A leading company in artificial intelligence solutions.",
        "website_url": "https://techinnovators.example.com",
        "categories": ["Artificial Intelligence", "Software"],
        "funding_total": 5000000,
        "valuation": 15000000,
        "facebook": "https://facebook.com/techinnovators",
        "linkedin": "https://linkedin.com/company/techinnovators",
        "investors": [
            {
                "name": "Investor A",
                "investment_amount": 2000000,
                "date": "2016-08-15"
            },
            {
                "name": "Investor B",
                "investment_amount": 3000000,
                "date": "2018-11-20"
            }
        ],
        "funding_rounds": [
            {
                "round": "Series A",
                "amount": 2000000,
                "date": "2016-08-15",
                "investors": ["Investor A"]
            },
            {
                "round": "Series B",
                "amount": 3000000,
                "date": "2018-11-20",
                "investors": ["Investor B"]
            }
        ],
        "acquisitions": [
            {
                "acquired_company": "Startup XYZ",
                "date": "2019-05-10",
                "amount": 500000
            }
        ],
        "officers": [
            {
                "name": "Jane Doe",
                "position": "Director",
                "appointed_on": "2015-06-01"
            },
            {
                "name": "John Smith",
                "position": "Secretary",
                "appointed_on": "2015-06-01"
            }
        ],
        "linkedin_profiles": [
            {
                "name": "Alice Johnson",
                "current_position": "CTO at Tech Innovators Ltd",
                "employment_history": [
                    {
                        "organization": "Previous Company",
                        "position": "Senior Developer",
                        "time_range": "2012-01 to 2015-05"
                    }
                ],
                "education_history": [
                    {
                        "institution": "University of Technology",
                        "degree": "M.Sc. Computer Science",
                        "time_range": "2008-09 to 2010-06"
                    }
                ]
            }
        ],
        "companyhouse_info": {
            "companyhouse_id": "07101408",
            "incorporation_date": "2015-06-01",
            "company_type": "Private Limited Company",
            "address": "123 Innovation Drive, Tech City, TC1 2AB",
            "sic_code": "62020",
            "officers": [
                {
                    "name": "Jane Doe",
                    "position": "Director",
                    "appointed_on": "2015-06-01"
                },
                {
                    "name": "John Smith",
                    "position": "Secretary",
                    "appointed_on": "2015-06-01"
                }
            ],
            "filings": [
                {
                    "type": "Incorporation",
                    "date": "2015-06-01",
                    "document_url": "https://companieshouse.gov.uk/document/123456"
                },
                {
                    "type": "Confirmation Statement",
                    "date": "2023-04-15",
                    "document_url": "https://companieshouse.gov.uk/document/789012"
                }
            ],
            "insolvency_info": null
        }
    }
}
```

## Caution

When using the `--initialize-force` or `--initialize-drop-tables` options, exercise caution as they will reset the database state and may result in repopulating all records.
