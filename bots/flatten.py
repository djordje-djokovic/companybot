'''
flattens the database data into longest format
'''
import sys
import pandas as pd
sys.path.append('C:\\Projects\\uni\\sources\\companybot\\')
from bots.common import get_persons, get_companieshouse_data_with_unique_names
import copy

'''
input are companies coming from get_data function from bots.common
'''
def flatten_data(companies):
    dfs = []
    for company in companies:
        # companieshouse data
        unique_entities = get_persons(data=company['companieshouse_data'], ignore_organization=False)
        companieshouse_sample_unique = get_companieshouse_data_with_unique_names(unique_entities,
                                                                                 company['companieshouse_data'])
        company_uuid = companieshouse_sample_unique['properties']['uuid']
        company_name = companieshouse_sample_unique['properties']['company_name']
        df_companieshouse = flatten_companieshouse_data(companieshouse_sample_unique)
        dfs.append(df_companieshouse)

        # crunchbase data
        df_crunchbase_properties = flatten_crunchbase_properties(company['crunchbase_data'], company_name, company_uuid)
        dfs.append(df_crunchbase_properties)
        df_crunchbase_investments = flatten_crunchbase_investments(company['crunchbase_data'], company_name,
                                                                   company_uuid)
        dfs.append(df_crunchbase_investments)

        # linkedin data
        linkedin_profiles = company['linkedin_data']['cards']['persons']
        for linkedin_profile in linkedin_profiles:
            linkedin_data = flatten_linkedin(linkedin_profile)
            dfs.append(linkedin_data)
    concatenated_df = pd.concat(dfs, ignore_index=True, sort=False, axis=0)
    return concatenated_df

def flatten_companieshouse_data(companieshouse_data_unique):
    def create_dict(uuid, name, parent_uuid, parent_name, group_id, from_date, to_date, variable, value):
        return {
            'uuid': uuid,
            'name': name,
            'source': 'companieshouse',
            'parent_uuid': parent_uuid,
            'parent_name': parent_name,
            'group_id': group_id,
            'from_date': from_date,
            'to_date': to_date,
            'variable': variable,
            'value': value}

    data = companieshouse_data_unique
    last_filing_date = None
    unique_uuids = set()
    entries = []

    parent_name = data['properties']['company_name']
    parent_uuid = data['properties']['uuid']

    insolvency_date = None
    if data['cards']['insolvency']['items']:
        raise NotImplementedError('we also need to record dissolved date which is not part of insolvency')
    elif 'dissolved_on' in data['properties'] and data['properties']['dissolved_on'] is not None:
        entries.append(create_dict(parent_uuid, parent_name, None, None, None, data['properties']['incorporated_on'],
                                   data['properties']['dissolved_on'], 'is_incorporated', True))
        entries.append(create_dict(parent_uuid, parent_name, None, None, None, data['properties']['incorporated_on'],
                                   data['properties']['dissolved_on'], 'is_dissolved', True))
    else:
        entries.append(
            create_dict(parent_uuid, parent_name, None, None, None, data['properties']['incorporated_on'], None,
                        'is_incorporated', True))

    for received_date, item in data['cards']['shareholding'].items():

        # print(received_date, item)
        filing_date = from_date = item['filing_date']
        total_shares = 0

        for x in item['items']:
            total_shares += x['shares']

        # add total_shares
        entries.append(
            create_dict(parent_uuid, parent_name, None, None, None, filing_date, last_filing_date, 'total_shares',
                        total_shares))
        unique_shareholders = set()
        i_unique_shareholding = 0
        for shareholder in item['items']:
            uuid = shareholder['uuid']
            name = shareholder['name']
            shares = shareholder['shares']
            share_type = shareholder['share_type']

            if uuid not in unique_uuids:
                unique_uuids.add(uuid)
                i_unique_shareholding += 1
                # add is_organization
                if shareholder['is_organization']:
                    entries.append(create_dict(uuid, name, parent_uuid, parent_name, None,
                                               data['cards']['incorporation']['received_date'], None, 'is_organization',
                                               True))
                else:
                    entries.append(create_dict(uuid, name, parent_uuid, parent_name, None,
                                               data['cards']['incorporation']['received_date'], None, 'is_organization',
                                               False))

            if i_unique_shareholding == 0:
                group_id = 'shareholding'
            else:
                group_id = uuid  # f'shareholding_{i_unique_shareholding}'

            entries.append(create_dict(uuid, name, parent_uuid, parent_name, group_id, filing_date, last_filing_date,
                                       'shareholding', shares / total_shares))
            entries.append(
                create_dict(uuid, name, parent_uuid, parent_name, group_id, filing_date, last_filing_date, 'share_type',
                            share_type))
            entries.append(create_dict(uuid, name, parent_uuid, parent_name, group_id, filing_date, last_filing_date,
                                       'share_number', shares))

    for officer in data['cards']['officer']['items']:
        uuid = officer['uuid']
        name = officer['name']
        from_date = officer['appointed_on']
        to_date = officer['resigned_on']

        if from_date is None:
            raise NotImplementedError()
        if to_date is None:
            to_date = None

        if officer['role'] == 'Director':
            entries.append(
                create_dict(uuid, name, parent_uuid, parent_name, None, from_date, to_date, 'is_director', True))
        elif officer['role'] == 'Secretary':
            entries.append(
                create_dict(uuid, name, parent_uuid, parent_name, None, from_date, to_date, 'is_secretary', True))
        else:
            raise NotImplementedError()

    for founder in data['cards']['incorporation']['items']:
        uuid = founder['uuid']
        name = founder['name']
        from_date = data['cards']['incorporation']['received_date']
        to_date = None

        entries.append(
            create_dict(uuid, name, parent_uuid, parent_name, None, from_date, to_date, 'is_founder', True))
    df = pd.DataFrame(entries)
    return df

def flatten_crunchbase_properties(crunchbase_sample, company_uuid, company_name):
    # Provided dataset
    data = crunchbase_sample['cards']['fields']

    # Function to flatten nested dictionaries for category_groups and categories
    def flatten_categories(data, key):
        return [{'group_id': cat['uuid'], 'value': cat['value']} for cat in data.get(key, [])]

    # Initialize a list to hold all the rows
    rows = []

    # Function to add rows to list
    def add_row(rows, id_value, name_value, variable, value, group_id=None):
        rows.append({
            'uuid': id_value,
            'name': name_value,
            'source': 'crunchbase',
            'parent_uuid': None,
            'parent_name': None,
            'group_id': group_id,
            'variable': variable,
            'from_date': data['founded_on']['value'],
            'to_date': None,
            'value': value
        })

    # Extracting data from the provided dictionary and adding rows to list
    id_value = company_uuid  # data['identifier']['uuid']
    name_value = company_name  # data['identifier']['value']

    variables_to_extract = {
        'contact_email': data.get('contact_email', None),
        'website_url': data.get('website_url', None),
        'twitter': data.get('twitter', {}).get('value', None),
        'linkedin': data.get('linkedin', {}).get('value', None),
        'founded_on': data.get('founded_on', {}).get('value', None),
        'equity_funding_total': data.get('equity_funding_total', {}).get('value_usd', None),
        'entity': data.get('identifier', {}).get('entity_def_id', None),
        'ipo_status': data.get('ipo_status', None),
        'description': data.get('description', None),
        'num_investors': data.get('num_investors', None),
        'num_lead_investors': data.get('num_lead_investors', None),
        'num_employees_enum': data.get('num_employees_enum', None),
        'status': data.get('status', None),
        'funding_stage': data.get('funding_stage', None),
        'num_articles': data.get('num_articles', None),
        'num_funding_rounds': data.get('num_funding_rounds', None),
        'last_equity_funding_type': data.get('last_equity_funding_type', None)
    }

    # Add variables to list
    for variable, value in variables_to_extract.items():
        add_row(rows, id_value, name_value, variable, value)

    # Add category_groups and categories to list
    for group in flatten_categories(data, 'category_groups'):
        add_row(rows, id_value, name_value, 'category_groups', group['value'], group['group_id'])

    for category in flatten_categories(data, 'categories'):
        add_row(rows, id_value, name_value, 'categories', category['value'], category['group_id'])

    # Convert list to DataFrame
    df = pd.DataFrame(rows)

    # Show the first few rows of the dataframe
    return df


def flatten_crunchbase_investments(crunchbase_sample, company_name, company_uuid):
    investments = crunchbase_sample['cards']['raised_investments']
    # Initialize an empty list to collect data
    data_rows = []

    # Iterate through the investment data to populate the list
    for investment in investments:
        # Common data for all rows related to this investment
        common_data = {
            'uuid': investment['funding_round_identifier']['uuid'],
            'name': investment['funding_round_identifier']['value'].split('-')[0].strip(),
            'parent_uuid': company_uuid,  # investment['organization_identifier']['uuid'],
            'parent_name': company_name,
            'group_id': None,  # investment['investor_identifier']['uuid'],  # This could be the investor's UUID
            'from_date': investment.get('announced_on'),
            'to_date': None,
            'variable': '',
            'value': None
        }

        # Number of investors for the round
        common_data['variable'] = 'num_investors'
        common_data['value'] = 1  # Each entry is an investment, so we count 1 per entry
        data_rows.append(common_data.copy())

        # Other details
        details = [
            ('total_investment_usd', investment['funding_round_money_raised'].get('value_usd')),
            ('investor_type', ', '.join(investment.get('investor_type', []))),
            ('investor_stage', ', '.join(investment.get('investor_stage', []))),
            ('lead_investor', investment.get('is_lead_investor')),
            ('funding_round_investment_type', investment.get('funding_round_investment_type'))
        ]

        for variable, value in details:
            common_data['variable'] = variable
            common_data['value'] = value
            data_rows.append(common_data.copy())

    # Now, we need to aggregate the number of investors and total investment by round
    # To do this, we'll create a DataFrame and then perform groupby operations
    df = pd.DataFrame(data_rows)
    # Aggregate the number of investors by round
    df_num_investors = \
    df[df['variable'] == 'num_investors'].groupby(['uuid', 'name', 'parent_uuid', 'parent_name', 'group_id'])[
        'value'].sum().reset_index()
    df_num_investors['variable'] = 'num_investors'

    # For total investment, we just need to sum up the values by round
    df_total_investment = \
    df[df['variable'] == 'total_investment_usd'].groupby(['uuid', 'name', 'parent_uuid', 'parent_name', 'group_id'])[
        'value'].sum().reset_index()
    df_total_investment['variable'] = 'total_investment_usd'

    # Now we can concatenate these aggregated results back to the main DataFrame
    df_aggregated = pd.concat([df, df_num_investors, df_total_investment], ignore_index=True)

    # Finally, we filter out the rows that were used for aggregation (they are now redundant)
    df_aggregated = df_aggregated.drop_duplicates(subset=['uuid', 'variable'], keep='last')

    # Sort the DataFrame for better readability
    df_aggregated.sort_values(by=['uuid', 'variable'], inplace=True)

    # Display the first few rows of the DataFrame to verify
    df_aggregated
    return df_aggregated

def flatten_linkedin(profile_data):
    # Initialize a list to store each flattened record
    flattened_records = []

    # Flatten experience data
    if 'experience' in profile_data:
        for experience in profile_data['experience']:
            # Add a record for the organization of the experience
            flattened_records.append({
                'uuid': profile_data['uuid'],
                'name': profile_data['name'],
                'parent_uuid': profile_data['parent_uuid'],
                'parent_name': profile_data['companieshouse_company_name'],
                'group_id': None,
                'from_date': experience['from'],
                'to_date': experience['to'],
                'variable': 'experience_organization',
                'value': experience['organization']
            })
            # Add records for each item in the experience
            for item in experience['items']:
                flattened_records.extend([
                    {
                        'uuid': profile_data['uuid'],
                        'name': profile_data['name'],
                        'parent_uuid': profile_data['parent_uuid'],
                        'parent_name': profile_data['companieshouse_company_name'],
                        'group_id': None,
                        'from_date': item['from'],
                        'to_date': item['to'],
                        'variable': 'experience_position',
                        'value': item['position']
                    },
                    {
                        'uuid': profile_data['uuid'],
                        'name': profile_data['name'],
                        'parent_uuid': profile_data['parent_uuid'],
                        'parent_name': profile_data['companieshouse_company_name'],
                        'group_id': None,
                        'from_date': item['from'],
                        'to_date': item['to'],
                        'variable': 'experience_country',
                        'value': item['country']
                    },
                    {
                        'uuid': profile_data['uuid'],
                        'name': profile_data['name'],
                        'parent_uuid': profile_data['parent_uuid'],
                        'parent_name': profile_data['companieshouse_company_name'],
                        'group_id': None,
                        'from_date': item['from'],
                        'to_date': item['to'],
                        'variable': 'experience_city',
                        'value': item['city']
                    },
                    {
                        'uuid': profile_data['uuid'],
                        'name': profile_data['name'],
                        'parent_uuid': profile_data['parent_uuid'],
                        'parent_name': profile_data['companieshouse_company_name'],
                        'group_id': None,
                        'from_date': item['from'],
                        'to_date': item['to'],
                        'variable': 'experience_position_description',
                        'value': item['description']
                    }
                ])

    # Flatten education data
    if 'education' in profile_data:
        for education in profile_data['education']:
            # Add a record for the organization of the education
            flattened_records.append({
                'uuid': profile_data['uuid'],
                'name': profile_data['name'],
                'parent_uuid': profile_data['parent_uuid'],
                'parent_name': profile_data['companieshouse_company_name'],
                'group_id': None,
                'from_date': education['from'],
                'to_date': education['to'],
                'variable': 'education_organization',
                'value': education['organization']
            })
            # Add records for each item in the education
            for item in education['items']:
                flattened_records.extend([
                    {
                        'uuid': profile_data['uuid'],
                        'name': profile_data['name'],
                        'parent_uuid': profile_data['parent_uuid'],
                        'parent_name': profile_data['companieshouse_company_name'],
                        'group_id': None,
                        'from_date': item['from'],
                        'to_date': item['to'],
                        'variable': 'education_degree',
                        'value': item['degree']
                    },
                    {
                        'uuid': profile_data['uuid'],
                        'name': profile_data['name'],
                        'parent_uuid': profile_data['parent_uuid'],
                        'parent_name': profile_data['companieshouse_company_name'],
                        'group_id': None,
                        'from_date': item['from'],
                        'to_date': item['to'],
                        'variable': 'education_country',
                        'value': item['country']
                    }, {
                        'uuid': profile_data['uuid'],
                        'name': profile_data['name'],
                        'parent_uuid': profile_data['parent_uuid'],
                        'parent_name': profile_data['companieshouse_company_name'],
                        'group_id': None,
                        'from_date': item['from'],
                        'to_date': item['to'],
                        'variable': 'education_city',
                        'value': item['city']
                    },
                    {
                        'uuid': profile_data['uuid'],
                        'name': profile_data['name'],
                        'parent_uuid': profile_data['parent_uuid'],
                        'parent_name': profile_data['companieshouse_company_name'],
                        'group_id': None,
                        'from_date': item['from'],
                        'to_date': item['to'],
                        'variable': 'education_degree_description',
                        'value': item['description']
                    }])
    return pd.DataFrame(flattened_records)

