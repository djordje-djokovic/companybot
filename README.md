# CompanyBot
This is a project to scrape company and entrepreneur data from:
- crunchbase using crunchbase api
- companies house https://find-and-update.company-information.service.gov.uk/
- linkedin



## Extracted data

This project extracts company information such incorporation and insolvency data as well as data on shareholders over time.

[//]: # (    {)

[//]: # (        'author': 'Djordje Djokovic',)

[//]: # (        'text': '“I may not have gone where I intended to go, but I think I ...”',)

[//]: # (        'tags': ['entrepreneurship', 'high technology', 'commercialization'])

[//]: # (    })


## Bots

This project contains one spider and you can list it using the `list`
command:

    $ scrapy list
    toscrape
    

The companybot extracts all the data from multiple sources and combines it to a single JSON file.


## Running the bots

You can run a spider using the `scrapy crawl` command, such as:

    $ scrapy crawl toscrape
