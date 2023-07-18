import time, requests, json
from datetime import datetime

import psycopg2
from .common import PendingStatus, DataSource, get_logger
from .config import DB_NAME, DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, CRUNCHBASE_DIR, CATEGORY_LIST_GROUPS, CRUNCHBASE_KEY
class CrunchBaseBot():
    __version__ = 'CrunchBaseBot 0.9'

    def __init__(self, logger=None):
        self.logger = logger
        if self.logger is None:
            self.logger = get_logger(CrunchBaseBot.__name__)

    def run(self, uuids_filter='*', category_groups_list_filter='*', country_code_filter='*', from_filter=datetime.min, to_filter=datetime.max, force=False):
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            data = self.get_data_from_pending(uuids_filter, force)
            for row in data:
                uuid = row[0]
                name = row[1]
                legal_name = row[2]
                category_groups_list = row[3]
                founded_on = row[4]
                json_rest_api = self.get_rest_api(row)
                self.write(uuid, name, legal_name, category_groups_list, founded_on, json_rest_api)
                time.sleep(5)

        except Exception as ex:
            self.logger.error(ex)
        finally:
            self.conn.close

    # we separate card_ids and field_ids in two separate queries. the reason is that there is a limited character set that can be transferred from the server of 2048 and if we query everything we might get an error
    def get_rest_api(self, row):

        uuid = row[0]

        field_ids = 'acquirer_identifier,aliases,categories,category_groups,closed_on,company_type,contact_email,created_at,delisted_on,demo_days,description,diversity_spotlights,entity_def_id,equity_funding_total,exited_on,facebook,facet_ids,founded_on,founder_identifiers,funding_stage,funding_total,funds_total,hub_tags,identifier*,image_id,image_url,investor_identifiers,investor_stage,investor_type,ipo_status,last_equity_funding_total,last_equity_funding_type,last_funding_at,last_funding_total,last_funding_type,last_key_employee_change_date,last_layoff_date,layout_id,legal_name,linkedin,listed_stock_symbol,location_group_identifiers,location_identifiers,name,num_acquisitions,num_alumni,num_articles,num_current_advisor_positions,num_current_positions,num_diversity_spotlight_investments,num_employees_enum,num_enrollments,num_event_appearances,num_exits,num_exits_ipo,num_founder_alumni,num_founders,num_funding_rounds,num_funds,num_investments,num_investors,num_lead_investments,num_lead_investors,num_past_positions,num_portfolio_organizations,num_sub_organizations,operating_status,override_layout_id,owner_identifier,permalink,permalink_aliases,phone_number,program_application_deadline,program_duration,program_type,rank_delta_d30,rank_delta_d7,rank_delta_d90,rank_org,rank_principal,revenue_range,school_method,school_program,school_type,short_description,status,stock_exchange_symbol,stock_symbol,twitter,updated_at,uuid,valuation,valuation_date,website,website_url,went_public_on'
        url_field_ids = f'https://api.crunchbase.com/api/v4/entities/organizations/{uuid}?field_ids={field_ids}&user_key={CRUNCHBASE_KEY}'
        r_field_ids = requests.get(url_field_ids)

        card_ids = 'acquiree_acquisitions,acquirer_acquisitions,child_organizations,child_ownerships,event_appearances,fields,founders,headquarters_address,investors,ipos,jobs,key_employee_changes,layoffs,parent_organization,parent_ownership,participated_funding_rounds,participated_funds,participated_investments,press_references,raised_funding_rounds,raised_funds,raised_investments'
        card_ids = card_ids.replace(',event_appearances', '') # data not needed
        card_ids = card_ids.replace(',press_references', '') # data not needed
        url_card_ids = f'https://api.crunchbase.com/api/v4/entities/organizations/{uuid}?card_ids={card_ids}&user_key={CRUNCHBASE_KEY}'
        r_card_ids = requests.get(url_card_ids)

        r = {}
        r['properties'] = r_field_ids.json()['properties']
        r['cards'] = r_card_ids.json()['cards']
        r['source'] = DataSource.crunchbase.name
        return r


    def write(self, uuid, name, legal_name, category_groups_list, founded_on, json_rest_api):
        table_name = 'data'
        dt = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        data_str = json.dumps(json_rest_api)

        d = {'uuid': uuid, 'uuid_parent': uuid, 'name': name, 'source': DataSource.crunchbase.name, 'version':self.__version__,'created_at': dt,'updated_at': dt,'data': data_str}
        columns = d.keys()
        values = tuple(d.values())

        # write data to db
        query_data = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) " \
                     f"ON CONFLICT (uuid, source) DO UPDATE SET data = EXCLUDED.data, version = EXCLUDED.version, updated_at = EXCLUDED.updated_at"

        cursor = self.conn.cursor()
        cursor.execute(query_data, values)

        # update pending table
        query_pending = f"UPDATE pending SET status = '{PendingStatus.completed.name}', updated_at = '{dt}'  WHERE uuid = '{uuid}' AND source = '{DataSource.crunchbase.name}'"
        cursor = self.conn.cursor()
        cursor.execute(query_pending)

        # write pending table for companies house. this is done so that we immidiately have pending record for companieshouse
        # when a new crunchbase record is written if a record is already existing in
        # pending table for this company and companies house DO NOTHING ON CONFLICT
        columns_companieshouse = ['uuid', 'uuid_parent', 'name', 'legal_name', 'category_groups_list', 'founded_on', 'source', 'status', 'version', 'created_at', 'updated_at']
        source = DataSource.companieshouse.name
        status = PendingStatus.pending.name
        version = ''
        created_at = dt
        updated_at = dt
        values_pending_companieshouse = [uuid, uuid, name, legal_name, category_groups_list, founded_on, source, status, version, created_at, updated_at]
        query_pending_companieshouse = f"INSERT INTO pending ({', '.join(columns_companieshouse)}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
                        f"ON CONFLICT DO NOTHING"
        cursor = self.conn.cursor()
        cursor.execute(query_pending_companieshouse, values_pending_companieshouse)

        self.conn.commit()
        cursor.close()

        self.logger.info(f'write data successful: source: {DataSource.crunchbase.name} status: {PendingStatus.completed.name} company: {name}')

    def get_data_from_pending(self, uuids_filter='*', force=False):

        uuids_str = ', '.join([f"'{item}'" for item in uuids_filter]) if type(
            uuids_filter) == list else uuids_filter.replace('*', "'%'")


        if force:
            status = f"pending.uuid::text LIKE ANY (ARRAY[{uuids_str}]) and"
        else:
            status = f"pending.uuid::text LIKE ANY (ARRAY[{uuids_str}]) and pending.status = '{PendingStatus.pending.name}' and"

        cursor = self.conn.cursor()
        query = f"SELECT uuid, name, legal_name, category_groups_list, founded_on FROM pending " \
                f"WHERE {status} source = '{DataSource.crunchbase.name}' " \
                f"ORDER BY name ASC"
        cursor.execute(query)
        rows = cursor.fetchall()

        cursor.close()

        return rows

def run_crunchbase_bot(uuids_filter='*', category_groups_list_filter='*', country_code_filter='*',
                        from_filter=datetime.min, to_filter=datetime.max,
                        force=False):
    crunchbasebot = CrunchBaseBot()
    crunchbasebot.run(uuids_filter, category_groups_list_filter, country_code_filter, from_filter, to_filter, force)
