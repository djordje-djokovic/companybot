from bots.crunchbase_bot import run_crunchbase_bot
from bots.linkedin_bot import run_linkedin_bot, run_linkedin_bot_by_dict
from bots.common import write_organizations_pending, DataSource, PendingStatus, get_data, get_logger
from datetime import datetime
import json, logging, os

from bots.config import DB_NAME, DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, CRUNCHBASE_DIR, POPPLER_PATH, BRAVE_PATH
from bots.companieshouse_bot import run_companieshouse_bot, run_companieshouse_bot_by_company_id

def companieshouse_finished(data):
    # handle completed requests here
    print(json.dumps(data))

def linkedin_finish(companies):
    print('linkedin_finish', json.dumps(companies))

def linkedin_company(company):
    print('linkedin_company', json.dumps(company))

def linkedin_profile(profile):
    print('linkedin_profile', json.dumps(profile))

def write_profiles_linkedin():
    pass



if __name__ == '__main__':
    logger = get_logger('CompanyBot')

    # create_organizations_table()
    # create_pending_table()
    # create_data_table()

    # for f in CATEGORY_LIST_GROUPS:
    #     filter = {'category_groups_list': [f], 'country_code': ['GBR']}
    #     get_organizations_from_crunchbase_csv(filter)

    # # get list of organizatins as list of dictionarie. this is the initial step used to fill the database with subset of crunchbase orgnizations. we for example just record GBR organizations in crunchbase_organizations table
    # # organizations = get_organizations_from_crunchbase_csv({'category_groups_list': ['Artificial Intelligence'], 'country_code': ['GBR']})
    # organizations = get_organizations_from_crunchbase_csv({'country_code': ['GBR']})
    # # write the organizations from crunchbase csv file into database. we use that usually to create a subset
    # write_organizations_from_csv(organizations)

    uuids_company_filter = ['5fd4dcb9-77ec-8903-5300-dd4c3da76670',
             'a7d2f427-66ba-476a-81cc-171a5d806b22']

    # # write organizations to pending table with type = crunchbase.
    #
    # write_organizations_pending(uuids_company_filter, category_groups_list = '*',
    #                             source=DataSource.crunchbase,
    #                             status=PendingStatus.pending,
    #                             from_dt=datetime.strptime('2012-01-01', '%Y-%m-%d'),
    #                             to_dt=datetime.strptime('2018-01-01', '%Y-%m-%d'),
    #                             force=True)

    # # write organizations to pending table with type = companies house
    # write_organizations_pending(category_groups_list=['Artificial Intelligence'],
    #                             source=DataSource.companieshouse,
    #                             status=PendingStatus.pending,
    #                             from_dt=datetime.strptime('2012-01-01', '%Y-%m-%d'),
    #                             to_dt=datetime.strptime('2018-01-01', '%Y-%m-%d'),
    #                             force=False)

    # # query pending database to get all organizations for crunchbase that need downloading. use crunchbase api to create json type entries into databse. we do not want to use csv data since it is not as verbose as rest api data
    # run_crunchbase_bot(uuids_filter=uuids_company_filter, force=True, logger=logger)
    #
    # # query pending database to get all organizations for companies house that need scraping. use companies house spider for this. use legal name from crunchbase instead of name if it exists as a name input
    # run_companieshouse_bot(uuids_filter=uuids_company_filter, force=True, callback_finish=companieshouse_finished, logger=logger)

    occupations_filter=['Founder', 'Director']
    # query pending database to get all profiles for linkedin that need scraping. use linkedin spider for this and persons table from companies house organizations data
    uuids_profile_filter = '*' #["4111be90-b975-56c0-adfd-6c3541418f89", "d6fbd190-b8bd-5b65-bb31-4a02ad5bfd4f"]

    run_linkedin_bot(uuids_filter=uuids_profile_filter, uuids_parent_filter=uuids_company_filter,
                     occupations_filter=occupations_filter, force=True,
                     callback_company=linkedin_company, callback_profile=linkedin_profile, callback_finish=linkedin_finish,
                     logger=logger)

    # # # run bot based on dictionary
    # profiles_by_companieshouse_id = [{'company_id': '07101408',
    #                                    'company_name': 'MADE.COM DESIGN LTD',
    #                                    'items': [
    #                                        # {'name': 'ANGELINI-HURLL, Rogan James', 'occupation': ['Director']},
    #                                        # {'name': 'CALLEDE, Julien', 'occupation': ['Director', 'Shareholder']},
    #                                        # {'name': 'CHAINIEUX, Philippe', 'occupation': ['Director', 'Shareholder']},
    #                                        # {'name': 'CLARK, John Robert Morton', 'occupation': ['Secretary', 'Shareholder']},
    #                                        # {'name': 'EVANS, Adrian Baynham', 'occupation': ['Director']},
    #                                        {'name': 'GOTHARD, Ben Winston', 'occupation': ['Secretary', 'Shareholder']},
    #                                        # {'name': 'HOBERMAN, Brent Shawzin', 'occupation': ['Director']},
    #                                        # {'name': 'HUNT, John Francis Weston', 'occupation': ['Director']},
    #                                        # {'name': 'KWOK, Win', 'occupation': ['Shareholder']},
    #                                        # {'name': 'LEWIS, John Patrick', 'occupation': ['Director']},
    #                                        {'name': 'LI, Ning Lucas Gabriel',
    #                                         'occupation': ['Director', 'Founder', 'Shareholder']},
    #                                        # {'name': 'MACINTOSH, Chloe', 'occupation': ['Shareholder']},
    #                                        # {'name': 'MCCULLOCH, George William', 'occupation': ['Director']},
    #                                        # {'name': 'REID, Stephen Graham', 'occupation': ['Secretary']},
    #                                        # {'name': 'ROEN, Carson', 'occupation': ['Shareholder']},
    #                                        # {'name': 'ROEN, Carston', 'occupation': ['Shareholder']},
    #                                        # {'name': 'SIMONCINI, Marc', 'occupation': ['Director']},
    #                                        # {'name': 'SKIPPER, Andrew', 'occupation': ['Shareholder']},
    #                                        # {'name': 'SKIPPER, Andy', 'occupation': ['Shareholder']},
    #                                        # {'name': 'THOMPSON, Nicola', 'occupation': ['Director']},
    #                                        # {'name': 'TOMLINS, Lisa Gan', 'occupation': ['Director', 'Secretary']},
    #                                        # {'name': 'TYLER, Laura', 'occupation': ['Director', 'Secretary']},
    #                                        # {'name': 'VANEK, David', 'occupation': ['Secretary']}
    #                                    ]}]
    #
    # run_linkedin_bot_by_dict(profiles_by_companieshouse_id,
    #                      occupations_filter=occupations_filter, force=force,
    #                      callback_company=linkedin_company, callback_profile=linkedin_profile, callback_finish=linkedin_finish,
    #                      logger=logger)

    d = get_data(uuids=uuids_company_filter)
    print(json.dumps(d))