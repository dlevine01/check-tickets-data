# -------- setup
#%%

import pandas as pd
import os
from datetime import date, datetime
import time
from dateutil.relativedelta import relativedelta
import urllib.parse
import requests
from dotenv import load_dotenv

# - read in api key

load_dotenv()
APP_TOKEN = os.environ.get('APP_TOKEN')

# initiate log (could do this a fancier way)

log = []

# - translations

# from https://data.cityofnewyork.us/api/views/nc67-uf89/files/86f59d3d-96cb-46d6-966c-db0bec30538e?download=true&filename=DOF_Open_Parking_and_Camera_Violations_data_dictionary.xlsx
AGENCY_CODES = {
    'A':'PORT AUTHORITY',
    'B':'TRIBOROUGH BRIDGE AND TUNNEL POLICE',
    'C':'CON RAIL',
    'D':'DEPARTMENT OF BUSINESS SERVICES',
    'E':'BOARD OF ESTIMATE',
    'F':'FIRE DEPARTMENT',
    'G':'TAXI AND LIMOUSINE COMMISSION',
    'H':'HOUSING AUTHORITY',
    'I':'STATEN ISLAND RAPID TRANSIT POLICE',
    'J':'AMTRAK RAILROAD POLICE',
    'K':'PARKS DEPARTMENT',
    'L':'LONG ISLAND RAILROAD',
    'M':'TRANSIT AUTHORITY',
    'N':'NYS PARKS POLICE',
    'O':'NYS COURT OFFICERS',
    'P':'POLICE DEPARTMENT',
    'Q':'DEPARTMENT OF CORRECTION',
    'R':'NYC TRANSIT AUTHORITY MANAGERS',
    'S':'DEPARTMENT OF SANITATION',
    'T':'TRAFFIC',
    'U':'PARKING CONTROL UNIT',
    'V':'DEPARTMENT OF TRANSPORTATION',
    'W':'HEALTH DEPARTMENT POLICE',
    'X':'OTHER/UNKNOWN AGENCIES',
    'Y':'HEALTH AND HOSPITAL CORP. POLICE',
    'Z':'METRO NORTH RAILROAD POLICE',
    '1':'NYS OFFICE OF MENTAL HEALTH POLICE',
    '2':'O M R D D',
    '3':'ROOSEVELT ISLAND SECURITY',
    '4':'SEA GATE ASSOCIATION POLICE',
    '5':'SNUG HARBOR CULTURAL CENTER RANGERS',
    '6':'WATERFRONT COMMISSION OF NY HARBOR',
    '7':'SUNY MARITIME COLLEGE',
    '9':'NYC OFFICE OF THE SHERIFF',
}

#%%

# ----- find today, dates of previous months

today = date.today()
year = today.year

first_day_this_month = datetime(year, today.month, 1)
first_day_previous_month = first_day_this_month - relativedelta(months=1)
first_day_previous_previous = first_day_previous_month - relativedelta(months=1)

#%%
# ----- create queries

base_url = 'https://data.cityofnewyork.us/resource/pvqr-7yc4.json?$query='

previous_month_total_query = f'''
SELECT
  count(DISTINCT `summons_number`) AS `count_distinct_summons_number`
WHERE
  (`issue_date` >= "{first_day_previous_month.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
  AND (`issue_date` < "{first_day_this_month.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
'''

previous_month_by_type_query = f'''
SELECT
  count(DISTINCT `summons_number`) AS `count_distinct_summons_number`,
  `violation_code`
WHERE
  (`issue_date` >= "{first_day_previous_month.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
  AND (`issue_date` < "{first_day_this_month.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
GROUP BY `violation_code` 
'''

previous_month_by_agency_query = f'''
SELECT
  count(DISTINCT `summons_number`) AS `count_distinct_summons_number`,
  `issuing_agency`
WHERE
  (`issue_date` >= "{first_day_previous_month.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
  AND (`issue_date` < "{first_day_this_month.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
GROUP BY `issuing_agency` 
'''

previous_previous_total_query = f'''
SELECT
  count(DISTINCT `summons_number`) AS `count_distinct_summons_number`
WHERE
  (`issue_date` >= "{first_day_previous_previous.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
  AND (`issue_date` < "{first_day_previous_month.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
'''

previous_previous_by_type_query = f'''
SELECT
  count(DISTINCT `summons_number`) AS `count_distinct_summons_number`,
  `violation_code`
WHERE
  (`issue_date` >= "{first_day_previous_previous.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
  AND (`issue_date` < "{first_day_previous_month.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
GROUP BY `violation_code` 
'''

previous_previous_by_agency_query = f'''
SELECT
  count(DISTINCT `summons_number`) AS `count_distinct_summons_number`,
  `issuing_agency`
WHERE
  (`issue_date` >= "{first_day_previous_previous.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
  AND (`issue_date` < "{first_day_previous_month.strftime(r'%Y-%m-%dT00:00:00')}" :: floating_timestamp)
GROUP BY `issuing_agency` 
'''

#%%

def fetch_values_from_query(query):
    
    query_url = base_url + urllib.parse.quote(query)
    headers={'X-App-Token': APP_TOKEN}
    
    print(query_url)

    r = requests.get(query_url, headers=headers)

    if r.ok:
        return r.json()
    
    else:
       print(f'error on query.\nresponse: {r.reason}\non:\n{query}\n{query_url}')
       log.append(f'error on query.\nresponse: {r.reason}\non:\n{query}\n{query_url}')

#%%

# ----- run queries

previous_month_total = (
    pd.DataFrame.from_records(
        data=fetch_values_from_query(previous_month_total_query),
        index=[today]
    )
    .rename(columns={'count_distinct_summons_number':'Total'})
)

file_name = 'data/' + today.strftime('%m') + ' - previous month - total.csv'

header = not os.path.exists(file_name)
  
previous_month_total.to_csv(file_name, mode='a', header=header)


time.sleep(1)


previous_month_by_type = (
    pd.DataFrame.from_records(
        fetch_values_from_query(previous_month_by_type_query)
    )
        .set_index('violation_code')
    .reindex([str(index) for index in range(100)])
    .rename(columns={'count_distinct_summons_number':today})
    .T
)

file_name = 'data/' + today.strftime('%m') + ' - previous month - by type.csv'

header = not os.path.exists(file_name)
  
previous_month_by_type.to_csv(file_name, mode='a', header=header)


time.sleep(1)


previous_month_by_agency = (
    pd.DataFrame.from_records(
        fetch_values_from_query(previous_month_by_agency_query)
    )
    .assign(Agency = lambda row: row['issuing_agency'].map(AGENCY_CODES))
    .set_index('Agency')
    .reindex(list(AGENCY_CODES.values()))
    .drop(columns='issuing_agency')
    .rename(columns={'count_distinct_summons_number':today})
    .T
)

file_name = 'data/' + today.strftime('%m') + ' - previous month - by agency.csv'

header = not os.path.exists(file_name)
  
previous_month_by_agency.to_csv(file_name, mode='a', header=header)


time.sleep(1)


previous_previous_total = (
    pd.DataFrame.from_records(
        data=fetch_values_from_query(previous_previous_total_query),
        index=[today]
    )
    .rename(columns={'count_distinct_summons_number':'Total'})
)

file_name = 'data/' + today.strftime('%m') + ' - previous previous - total.csv'

header = not os.path.exists(file_name)
  
previous_previous_total.to_csv(file_name, mode='a', header=header)


time.sleep(1)


previous_previous_by_type = (
    pd.DataFrame.from_records(
        fetch_values_from_query(previous_previous_by_type_query)
    )
        .set_index('violation_code')
    .reindex([str(index) for index in range(100)])
    .rename(columns={'count_distinct_summons_number':today})
    .T
)

file_name = 'data/' + today.strftime('%m') + ' - previous previous - by type.csv'

header = not os.path.exists(file_name)
  
previous_previous_by_type.to_csv(file_name, mode='a', header=header)

time.sleep(1)

previous_previous_by_agency = (
    pd.DataFrame.from_records(
    fetch_values_from_query(previous_previous_by_agency_query)
    )
    .assign(Agency = lambda row: row['issuing_agency'].map(AGENCY_CODES))
    .set_index('Agency')
    .reindex(list(AGENCY_CODES.values()))
    .drop(columns='issuing_agency')
    .rename(columns={'count_distinct_summons_number':today})
    .T
)

file_name = 'data/' + today.strftime('%m') + ' - previous previous - by agency.csv'

header = not os.path.exists(file_name)
  
previous_previous_by_agency.to_csv(file_name, mode='a', header=header)


time.sleep(1)

# %%

# update readme

with open('README.md', 'w') as readme_file:
    readme_file.write(f'Last updated: {today}')


if log:
   
    log = today + log

    with open('log.txt','a') as log_file:
        for item in log:
            log_file.write(f'{item}\n')