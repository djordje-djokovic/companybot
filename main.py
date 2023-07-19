from bots.crunchbase_bot import run_crunchbase_bot
from bots.linkedin_bot import run_linkedin_bot, run_linkedin_bot_by_dict
from bots.common import write_organizations_pending, DataSource, PendingStatus, get_data, get_logger, initialize, logger
from datetime import datetime
from dateutil import parser as date_parser
import json, logging, os, argparse

from bots.config import DB_NAME, DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, CRUNCHBASE_DIR, POPPLER_PATH, BRAVE_PATH, CRUNCHBASE_KEY
from bots.companieshouse_bot import run_companieshouse_bot, run_companieshouse_bot_by_company_id

def companieshouse_finished(data):
    # handle completed requests here
    print(json.dumps(data))

def linkedin_finish(companies):
    # print('linkedin_finish', json.dumps(companies))
    print('linkedin_finish')

def linkedin_company(company):
    # print('linkedin_company', json.dumps(company))
    print('linkedin_company')

def linkedin_profile(profile):
    # print('linkedin_profile', json.dumps(profile))
    print('linkedin_profile')


def main(
            uuids_filter='*', uuids_profile_filter='*',
            category_groups_list_filter='*', country_code_filter='*',
            from_filter=datetime.min, to_filter=datetime.max,
            initialize_run=True, initialize_drop_tables=False, initialize_download_csv=False, initialize_write_organizations=False,
            crunchbase_run=False, crunchbase_force=False,
            companieshouse_run=False, companieshouse_force=False,
            linkedin_run=False, linkedin_force=False, linkedin_occupations_filter=['Founder', 'Director', 'Shareholder']):

    if initialize_run:
        initialize(uuids_filter=uuids_filter,
                   category_groups_list_filter=category_groups_list_filter, country_code_filter=country_code_filter,
                   from_filter=from_filter, to_filter=to_filter,
                   drop_tables=initialize_drop_tables, download_crunchbase_csv=initialize_download_csv, write_organizations=initialize_write_organizations
                   )

    if crunchbase_run:
        # Query pending database to get all organizations for Crunchbase that need downloading. Use Crunchbase API
        # to create JSON type entries into the database. We do not want to use CSV data since it is not as verbose as REST API data.
        run_crunchbase_bot(uuids_filter=uuids_filter,
                           category_groups_list_filter=category_groups_list_filter, country_code_filter=country_code_filter,
                           from_filter=from_filter, to_filter=to_filter,
                           force=crunchbase_force)

    if companieshouse_run:
        # Query pending database to get all organizations for Companies House that need scraping.
        # Use Companies House spider for this. Use legal name from Crunchbase instead of name if it exists as a name input.
        run_companieshouse_bot(uuids_filter=uuids_filter,
                               category_groups_list_filter=category_groups_list_filter, country_code_filter=country_code_filter,
                               from_filter=from_filter, to_filter=to_filter,
                               force=companieshouse_force,
                               callback_finish=companieshouse_finished)

    if linkedin_run:
        # Query pending database to get all profiles for LinkedIn that need scraping.
        # Use LinkedIn spider for this and persons table from Companies House organizations data.
        run_linkedin_bot(uuids_profile_filter=uuids_profile_filter, uuids_filter=uuids_filter,
                         category_groups_list_filter=category_groups_list_filter, country_code_filter=country_code_filter,
                         from_filter=from_filter, to_filter=to_filter,
                         occupations_filter=linkedin_occupations_filter,
                         force=linkedin_force,
                         callback_company=linkedin_company, callback_profile=linkedin_profile,
                         callback_finish=linkedin_finish)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CompanyBot Command Line Arguments')

    parser.add_argument('--uuids-company-filter', nargs='+', default=['*'], help='Coumpany UUIDs filter')
    parser.add_argument('--uuids-profile-filter', nargs='+', default=['*'], help='Linkedin UUIDs profile filter')
    parser.add_argument('--category-groups-list-filter', nargs='+', default=['Artificial Intelligence'], help='Company category group filter')
    parser.add_argument('--country-code-filter', nargs='+', default=['GBR'], help='Company country code filter')
    parser.add_argument('--from-filter', type=date_parser.parse, default=datetime.min, help='Founding start date filter')
    parser.add_argument('--to-filter', type=date_parser.parse, default=datetime.max, help='Founding end date filter')

    parser.add_argument('--initialize-run', choices=['true', 'false'], default='false', help='Run initialization process to setup database and tables. '
                                                                                             'WARNING! When using the --initialize-force option, exercise caution as it will '
                                                                                             'overwrite the pending table state to True. This action will repopulate all '
                                                                                             'selected records by the bots once again.')
    parser.add_argument('--initialize-drop-tables', choices=['true', 'false'], default='false', help='Drop existing database tables')
    parser.add_argument('--initialize-download-csv', choices=['true', 'false'], default='false', help='Download raw CSV data from Crunchbase')
    parser.add_argument('--initialize-write-organizations', choices=['true', 'false'], default='false', help='Write organizations from Crunchbase csv file')
    parser.add_argument('--initialize-pending-force', choices=['true', 'false'], default='false', help='Specify if the pending state will be force reset. WARNING! When using the --initialize-force option, exercise caution as it will '
                                                                                             'overwrite the pending table state to True. This action will repopulate all '
                                                                                             'selected records by the bots once again.')

    parser.add_argument('--crunchbase-run', choices=['true', 'false'], default='false', help='Run the Crunchbase bot')
    parser.add_argument('--crunchbase-force', choices=['true', 'false'], default='false', help='Force updating Crunchbase data')

    parser.add_argument('--companieshouse-run', choices=['true', 'false'], default='false', help='Run the Companies House bot')
    parser.add_argument('--companieshouse-force', choices=['true', 'false'], default='false', help='Force updating Companies House data')

    parser.add_argument('--linkedin-run', choices=['true', 'false'], default='false', help='Run the LinkedIn bot')
    parser.add_argument('--linkedin-force', choices=['true', 'false'], default='false', help='Force updating LinkedIn data')
    parser.add_argument('--linkedin-occupations-filter', nargs='+', default=['Founder', 'Director', 'Shareholder'], help='LinkedIn occupations filter. Defines occupations for which profiles should be scraped.')

    args = parser.parse_args()

    uuids_company_filter = '*' if args.uuids_company_filter[0] == '*' else args.uuids_company_filter
    uuids_profile_filter = '*' if args.uuids_profile_filter[0] == '*' else args.uuids_profile_filter
    category_groups_list_filter = '*' if args.category_groups_list_filter[0] == '*' else args.category_groups_list_filter
    country_code_filter = '*' if args.country_code_filter[0] == '*' else args.country_code_filter

    # Convert string values to boolean
    args_dict = vars(args)
    for key, value in args_dict.items():
        if isinstance(value, str):
            args_dict[key] = value.lower() == 'true'

    if args.initialize_run and args.initialize_pending_force:
        confirmation = input('Are you sure you want to force reseting the pending state for the records? This would result in bots repopulating all companies again. (y/n): ')
        if confirmation.lower() != 'y':
            args.initialize_pending_force = False

    if args.initialize_run and args.initialize_drop_tables:
        confirmation = input('Are you sure you want to drop the existing database tables? (y/n): ')
        if confirmation.lower() != 'y':
            args.initialize_drop_tables = False


    logger.info("CompanyBot Command Line Arguments:")
    for arg in vars(args):
        arg_value = getattr(args, arg)
        logger.info(f"CompanyBot Command Line Argument {arg}: {arg_value}")

    main(
        uuids_filter=uuids_company_filter,
        uuids_profile_filter=uuids_profile_filter,
        category_groups_list_filter=category_groups_list_filter,
        country_code_filter=country_code_filter,
        from_filter=args.from_filter,
        to_filter=args.to_filter,
        initialize_run=args.initialize_run,
        initialize_drop_tables=args.initialize_drop_tables,
        initialize_download_csv=args.initialize_download_csv,
        initialize_write_organizations=args.initialize_write_organizations,
        crunchbase_run=args.crunchbase_run,
        crunchbase_force=args.crunchbase_force,
        companieshouse_run=args.companieshouse_run,
        companieshouse_force=args.companieshouse_force,
        linkedin_run=args.linkedin_run,
        linkedin_force=args.linkedin_force,
        linkedin_occupations_filter=args.linkedin_occupations_filter
    )

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