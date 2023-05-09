# -------- setup
#%%

import pandas as pd
import os
from datetime import date, datetime
import time
from dateutil.relativedelta import relativedelta
import urllib.parse
import requests
from requests import HTTPError
from dotenv import load_dotenv
from collections import defaultdict
import json

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

VIOLATION_CODES = {
    '1': 'FAILURE TO DISPLAY BUS PERMIT',
    '2': 'NO OPERATOR NAM/ADD/PH DISPLAY',
    '3': 'UNAUTHORIZED PASSENGER PICK-UP',
    '4': 'BUS PARKING IN LOWER MANHATTAN',
    '5': 'BUS LANE VIOLATION',
    '6': 'OVERNIGHT TRACTOR TRAILER PKG',
    '7': 'FAILURE TO STOP AT RED LIGHT',
    '8': 'IDLING',
    '9': 'OBSTRUCTING TRAFFIC/INTERSECT',
    '10': 'NO STOPPING-DAY/TIME LIMITS',
    '11': 'NO STANDING-HOTEL LOADING',
    '12': 'MOBILE BUS LANE VIOLATION',
    '13': 'NO STANDING-TAXI STAND',
    '14': 'NO STANDING-DAY/TIME LIMITS',
    '15': 'NO STANDING-OFF-STREET LOT',
    '16': 'NO STANDING-EXC. TRUCK LOADING',
    '17': 'NO STANDING-EXC. AUTH. VEHICLE',
    '18': 'NO STANDING-BUS LANE',
    '19': 'NO STANDING-BUS STOP',
    '20': 'NO PARKING-DAY/TIME LIMITS',
    '21': 'NO PARKING-STREET CLEANING',
    '22': 'NO STAND TAXI/FHV RELIEF STAND',
    '23': 'NO PARKING-TAXI STAND',
    '24': 'NO PARKING-EXC. AUTH. VEHICLE',
    '25': 'NO STANDING-COMMUTER VAN STOP',
    '26': 'NO STANDING-FOR HIRE VEH STND',
    '27': 'NO PARKING-EXC. DSBLTY PERMIT',
    '28': 'OVERTIME STANDING DP',
    '29': 'ALTERING INTERCITY BUS PERMIT',
    '30': 'NO STOP/STANDNG EXCEPT PAS P/U',
    '31': 'NO STANDING-COMM METER ZONE',
    '32': 'OT PARKING-MISSING/BROKEN METR',
    '33': 'MISUSE PARKING PERMIT',
    '34': 'EXPIRED METER',
    '35': 'SELLING/OFFERING MCHNDSE-METER',
    '36': 'PHTO SCHOOL ZN SPEED VIOLATION',
    '37': 'EXPIRED MUNI METER',
    '38': 'FAIL TO DSPLY MUNI METER RECPT',
    '39': 'OVERTIME PKG-TIME LIMIT POSTED',
    '40': 'FIRE HYDRANT',
    '41': 'MISCELLANEOUS',
    '42': 'EXPIRED MUNI MTR-COMM MTR ZN',
    '43': 'EXPIRED METER-COMM METER ZONE',
    '44': 'PKG IN EXC. OF LIM-COMM MTR ZN',
    '45': 'TRAFFIC LANE',
    '46': 'DOUBLE PARKING',
    '47': 'DOUBLE PARKING-MIDTOWN COMML',
    '48': 'BIKE LANE',
    '49': 'EXCAVATION-VEHICLE OBSTR TRAFF',
    '50': 'CROSSWALK',
    '51': 'SIDEWALK',
    '52': 'INTERSECTION',
    '53': 'SAFETY ZONE',
    '54': 'PCKP DSCHRGE IN PRHBTD ZONE',
    '55': 'ELEVATED/DIVIDED HIGHWAY/TUNNL',
    '56': 'DIVIDED HIGHWAY',
    '57': 'BLUE ZONE',
    '58': 'MARGINAL STREET/WATER FRONT',
    '59': 'ANGLE PARKING-COMM VEHICLE',
    '60': 'ANGLE PARKING',
    '61': 'WRONG WAY',
    '62': 'BEYOND MARKED SPACE',
    '63': 'NIGHTTIME STD/ PKG IN A PARK',
    '64': 'NO STANDING EXCP D/S',
    '65': 'OVERTIME STDG D/S',
    '66': 'DETACHED TRAILER',
    '67': 'PEDESTRIAN RAMP',
    '68': 'NON-COMPLIANCE W/ POSTED SIGN',
    '69': 'FAIL TO DISP. MUNI METER RECPT',
    '70': 'REG. STICKER-EXPIRED/MISSING',
    '71': 'INSP. STICKER-EXPIRED/MISSING',
    '72': "INSP STICKER-MUTILATED/C'FEIT",
    '73': "REG STICKER-MUTILATED/C'FEIT",
    '74': 'FRONT OR BACK PLATE MISSING',
    '75': 'NO MATCH-PLATE/STICKER',
    '76': 'VIN OBSCURED',
    '77': 'PARKED BUS-EXC. DESIG. AREA',
    '78': 'NGHT PKG ON RESID STR-COMM VEH',
    '79': 'UNAUTHORIZED BUS LAYOVER',
    '80': 'MISSING EQUIPMENT',
    '81': 'NO STANDING EXCP DP',
    '82': 'COMML PLATES-UNALTERED VEHICLE',
    '83': 'IMPROPER REGISTRATION',
    '84': 'PLTFRM LFTS LWRD POS COMM VEH',
    '85': 'STORAGE-3HR COMMERCIAL',
    '86': 'MIDTOWN PKG OR STD-3HR LIMIT',
    '87': 'FRAUDULENT USE PARKING PERMIT',
    '88': 'UNALTERED COMM VEH-NME/ADDRESS',
    '89': 'NO STD(EXC TRKS/GMTDST NO-TRK)',
    '90': 'VEH-SALE/WSHNG/RPRNG/DRIVEWAY',
    '91': 'VEHICLE FOR SALE(DEALERS ONLY)',
    '92': 'WASH/REPAIR VEHCL-REPAIR ONLY',
    '93': 'REMOVE/REPLACE FLAT TIRE',
    '96': 'RAILROAD CROSSING',
    '97': 'VACANT LOT',
    '98': 'OBSTRUCTING DRIVEWAY',
    '99': 'OTHER'
}

VIOLATION_CODES_OTHER = defaultdict(lambda: 'Other (miscoded)')

VIOLATION_CODES_OTHER.update(VIOLATION_CODES)

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
  
    for attempt in range(3):
        try:
            r = requests.get(query_url, headers=headers, timeout=(4,60))

            r.raise_for_status()

            return r.json()

        except HTTPError as e:

            if attempt < 3:

                time.sleep(2)
                continue

            else:
                print(f'error on query.\nresponse: {r.reason}\nerror: {e}\non:\n{query}\n{query_url}')
                log.append(f'error on query.\nresponse: {r.reason}\non:\n{query}\n{query_url}')

                raise e

#%%

# ----- run queries


try:
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

    print('✓ previous month total')

except HTTPError:
    pass


time.sleep(1)

try:
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

    print('✓ previous month by type')

except HTTPError:
    pass

time.sleep(1)

try:
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

    print('✓ previous month by agency')

except HTTPError:
    pass


time.sleep(1)

try:
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

    print('✓ previous previous total')

except HTTPError:
    pass


time.sleep(1)

try:

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

    print('✓ previous previous by type')

except HTTPError:
    pass



time.sleep(1)

try:

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

    print('✓ previous previous by agency')

except HTTPError:
    pass



# %%

# update readme

with open('README.md', 'w') as readme_file:
    readme_file.write(f'Last updated: {today}')


if log:
   
    log = today + log

    with open('log.txt','a') as log_file:
        for item in log:
            log_file.write(f'{item}\n')


# %%

# -------- reformat to json and append to json file

# this_month = today.strftime('%m')
previous_month = (today - relativedelta(months=1)).strftime(r'%B')
previous_previous = (today - relativedelta(months=2)).strftime(r'%B')

previous_month_total = previous_month_total.reset_index().rename(columns={'index':'date checked'})
previous_previous_total = previous_previous_total.reset_index().rename(columns={'index':'date checked'})
previous_month_by_type = previous_month_by_type.reset_index().rename(columns={'index':'date checked'})
previous_previous_by_type = previous_previous_by_type.reset_index().rename(columns={'index':'date checked'})
previous_month_by_agency = previous_month_by_agency.reset_index().rename(columns={'index':'date checked'})
previous_previous_by_agency = previous_previous_by_agency.reset_index().rename(columns={'index':'date checked'})

previous_month_total['data month'] = previous_month
previous_previous_total['data month'] = previous_previous
previous_month_by_type['data month'] = previous_month
previous_previous_by_type['data month'] = previous_previous
previous_month_by_agency['data month'] = previous_month
previous_previous_by_agency['data month'] = previous_previous

previous_month_total['look back'] = 'Previous month'
previous_previous_total['look back'] = 'Previous previous month'
previous_month_by_type['look back'] = 'Previous month'
previous_previous_by_type['look back'] = 'Previous previous month'
previous_month_by_agency['look back'] = 'Previous month'
previous_previous_by_agency['look back'] = 'Previous previous month'

previous_month_total['table'] = 'total'
previous_previous_total['table'] = 'total'
previous_month_by_type['table'] = 'by type'
previous_previous_by_type['table'] = 'by type'
previous_month_by_agency['table'] = 'by agency'
previous_previous_by_agency['table'] = 'by agency'

previous_month_total['day of month checked'] = previous_month_total['date checked'].astype(str).str.slice(start=8)
previous_previous_total['day of month checked'] = previous_previous_total['date checked'].astype(str).str.slice(start=8)
previous_month_by_type['day of month checked'] = previous_month_by_type['date checked'].astype(str).str.slice(start=8)
previous_previous_by_type['day of month checked'] = previous_previous_by_type['date checked'].astype(str).str.slice(start=8)
previous_month_by_agency['day of month checked'] = previous_month_by_agency['date checked'].astype(str).str.slice(start=8)
previous_previous_by_agency['day of month checked'] = previous_previous_by_agency['date checked'].astype(str).str.slice(start=8)


day_data_concat = pd.concat([
    (
        previous_month_total
        .rename(columns={'Total':'rows loaded'})
    ),
    (
        previous_previous_total
        .rename(columns={'Total':'rows loaded'})
    ),
    (
        previous_month_by_type
        .melt(
            id_vars=['date checked', 'data month', 'table','look back', 'day of month checked'],
            var_name='Violation Code',
            value_name='rows loaded'
        )
        .assign(
            violation_type = lambda row: row['Violation Code'].map(VIOLATION_CODES_OTHER)
        )
        .rename(columns={'violation_type':'Violation Type'})
        .drop(columns='Violation Code')
    ),
    (
        previous_previous_by_type
        .melt(
            id_vars=['date checked', 'data month', 'table','look back', 'day of month checked'],
            var_name='Violation Code',
            value_name='rows loaded'
        )
        .assign(
            violation_type = lambda row: row['Violation Code'].map(VIOLATION_CODES_OTHER)
        )
        .rename(columns={'violation_type':'Violation Type'})
        .drop(columns='Violation Code')
    ),
    (
        previous_month_by_agency
        .melt(
            id_vars=['date checked', 'data month', 'table','look back', 'day of month checked'],
            var_name='Agency',
            value_name='rows loaded'
        )
    ),
    (
        previous_previous_by_agency
        .melt(
            id_vars=['date checked', 'data month', 'table','look back', 'day of month checked'],
            var_name='Agency',
            value_name='rows loaded'
        )
    ),
])

day_data_concat['date checked'] = day_data_concat['date checked'].astype(str)

# %%

new_data_records = day_data_concat.to_json(orient='records')

with open('docs/assets/data/data.json','r+') as data_file:
    data = json.load(data_file)
    data.extend(new_data_records)
    data_file.seek(0)
    json.dump(data, data_file)