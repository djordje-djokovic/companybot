[//]: # (    {)

[//]: # (        'author': 'Dr. Djordje Djokovic',)

[//]: # (        'text': '“I may not have gone where I intended to go, but I think I ...”',)

[//]: # (        'tags': ['entrepreneurship', 'high technology', 'commercialization'])

[//]: # (    })

# CompanyBot
This project focuses on consolidating data from various sources about companies and entrepreneurs into a singular, comprehensive database. This assembled database is designed to serve as a valuable resource for statistical analysis. The bots that have been implemented within this project include:


- **Crunchbase Bot** [`crunchbase_bot`]
- **Companies House Bot** [companieshouse_bot] https://find-and-update.company-information.service.gov.uk/
- **LinkedIn Bot** [linkedin_bot]

### Crunchbase Bot
`crunchbase_bot` uses the official crunchbase api to which we have university research access. 
The access needs to be periodically renewed. Current expiry date is October 2023. 

The crunchbase api supports the concept of fields and cards. a card is a collection of fields but the company 
itself is kind of a card that has fields. so we query the company fields and all available cards for a company:

**fields**:
*acquirer_identifier,aliases,categories,category_groups,closed_on,company_type,contact_email,created_at,delisted_on,demo_days,description,diversity_spotlights,entity_def_id,equity_funding_total,exited_on,facebook,facet_ids,founded_on,founder_identifiers,funding_stage,funding_total,funds_total,hub_tags,identifier,image_id,image_url,investor_identifiers,investor_stage,investor_type,ipo_status,last_equity_funding_total,last_equity_funding_type,last_funding_at,last_funding_total,last_funding_type,last_key_employee_change_date,last_layoff_date,layout_id,legal_name,linkedin,listed_stock_symbol,location_group_identifiers,location_identifiers,name,num_acquisitions,num_alumni,num_articles,num_current_advisor_positions,num_current_positions,num_diversity_spotlight_investments,num_employees_enum,num_enrollments,num_event_appearances,num_exits,num_exits_ipo,num_founder_alumni,num_founders,num_funding_rounds,num_funds,num_investments,num_investors,num_lead_investments,num_lead_investors,num_past_positions,num_portfolio_organizations,num_sub_organizations,operating_status,override_layout_id,owner_identifier,permalink,permalink_aliases,phone_number,program_application_deadline,program_duration,program_type,rank_delta_d30,rank_delta_d7,rank_delta_d90,rank_org,rank_principal,revenue_range,school_method,school_program,school_type,short_description,status,stock_exchange_symbol,stock_symbol,twitter,updated_at,uuid,valuation,valuation_date,website,website_url,went_public_on*

**cards**:
*acquiree_acquisitions,acquirer_acquisitions,child_organizations,child_ownerships,event_appearances,fields,founders,headquarters_address,investors,ipos,jobs,key_employee_changes,layoffs,parent_organization,parent_ownership,participated_funding_rounds,participated_funds,participated_investments,press_references,raised_funding_rounds,raised_funds,raised_investments*

All these values will be added to a single dictionary and stored as a json in the database

### Companies House Bot
`companieshouse_bot` uses `scrapy` library to systematically scrape a company. It navigates through all relevant links automatically by just providing it a `companyhouse_id`. 
Following information is retrieved:
- `parse_company_info` parses general company information from the main page such as:
  - *official company name*
  - *company house id*
  - *incoporation date*
  - *company type*
  - *company address*
  - *sic code* (nature of business)

- `parse_officers` parses the complete list of officers and their full names including their appointment and resignation dates:
  - *secretaries*
  - *directors*
  
- `parse_filing` parses the annual company filings that contains a full history of relevant company documents which are in pdf format such as:

  - Incoorporation (parsed - to get founder names)
  - Confirmation Statements/ Annual Returns (parsed - to get shareholders and their ownerships and also capital structure)
  - Capital (currently not parsed. It is also part of the confirmation statements which we parse)
  - Officers (currently not parsed. We parse the officers directly from the website)
  - Accounts (currently not parsed)
  - Charges (currently not parsed)
   
- `parse_insolvency` parses information on company insolvency. Only few companies have information on this.

### LinkedIn Bot
`linkedin_bot` uses custom parsing using python `beautifulsoup` and selenium to automate login and browsing. It requires 
the bot to login as a LinkedIn user. It either requires a LinkedIn url to parse a profile or allows to search by profile
name and company name and uses fuzzy matching logic to find the profile url.

The `linkedin_bot` retrieves **general information** about a profile such as *name* and *current position*. 

It also retrieves the **employment history** such as *organization names*, *positions* held and *time ranges* worked.

Furthermore it retrieves the **education history** such as *organization names*, *degree* acquired and *time ranges* studied.


## Getting started
In order to start using the bots following steps are needed: 
1. **Install PostgreSQL** and create a database e.g. `uni`. You can change the configs in `./bots/config.py`. Please setup in the config the database host, username, password and port. The tables in the database are automatically created.
2. 

## Running the bots

You can run a spider using the `scrapy crawl` command, such as:

    $ scrapy crawl toscrape

## Updating entries