import csv
import json
import pandas as pd
import requests
import sys

_output_filename = 'baltimore_crime_census_blocks.csv'

# Open Baltimore API
_baltimore_crime_url = ('https://data.baltimorecity.gov/resource/'
                        '4ih5-d5d5.json?$select=crimedate,crimecode,location,'
                        'description,district,neighborhood,location_1&'
                        '$where=crimedate>"%s" AND crimedate<"%s" AND '
                        'location_1 is not null&$order=crimedate DESC&'
                        '$limit=50000&$offset=%d')
_baltimore_api_limit = 50000
                                     
# Census Block Lookup API
_census_block_lookup = ('http://www.broadbandmap.gov/broadbandmap/census/'
                        'block?latitude=%f&longitude=%f&format=json')

                        
def get_census_blocks(lat_lon_list):
    print("Looking up Census blocks...")
    census_blocks = []
    i = 0
    for (lat, lon) in lat_lon_list:
        print('\t%d/%d\tFinding block for (%f,%f)...' % (i, len(lat_lon_list),
                                                         lat, lon))
        resp = requests.get(_census_block_lookup % (lat,lon))
        data = json.loads(resp.text)
        result = data['Results']['block'][0]
        census_blocks.append(result['FIPS'])
        i += 1
    df = pd.DataFrame({'lat_lon': lat_lon_list,
                       'block': census_blocks})
    return df

def get_baltimore_crime_data(start_date, end_date):
    print('Pulling crime data for %s to %s...' % (start_date, end_date))    
    
    # Make first request with offset zero
    req_url = _baltimore_crime_url % (start_date, end_date, 0)
    print('Making API request... %s' % req_url)
    sys.stdout.flush()
    resp = requests.get(req_url)
    data = json.loads(resp.text)
    df = pd.DataFrame(data)
    
    # Now iterate until we are sure we got all the data
    i = 1
    while len(data) == _baltimore_api_limit:
        req_url = _baltimore_crime_url % (start_date, end_date, 
                                          (i*_baltimore_api_limit)+1)
        print('Making API request... %s' % req_url)
        sys.stdout.flush()
        resp = requests.get(req_url)
        data = json.loads(resp.text)
        df = df.append(data)
        i += 1
 
    # Clean up the dates
    df['crimedate'] = pd.to_datetime(df['crimedate'])
    date_index = pd.DatetimeIndex(df['crimedate'])
    df['year'] = date_index.year
    df['month'] = date_index.month
    
    # Clean up the lat/lon values
    lat_lon = []
    for i in range(len(df)):
        (lon,lat) = df['location_1'][i]['coordinates']
        lat_lon.append((lat,lon))   
    df['lat_lon'] = lat_lon
    del df['location_1']
 
    return df

if __name__ == '__main__':
    # Grab the Baltimore crime data

# TODO: appending of multiple pulls is broken
#    crime_data = get_baltimore_crime_data('2010-01-01','2015-07-01')

    crime_data = get_baltimore_crime_data('2014-07-01','2015-07-01')
    
    # Extract and dedupe lat/lon pairs. Look up blocks and join.
    lat_lon_list = crime_data['lat_lon'].unique()
    census_blocks = get_census_blocks(lat_lon_list)
    crime_data = pd.merge(crime_data, census_blocks, 
                          how='left', left_on='lat_lon', right_on='lat_lon')
    
    # Reduce the data to 
    grouped_data = crime_data.groupby(['year','month','block',
                                       'crimecode','description'])
    
    with open(_output_filename, 'w') as out_fp:
        writer = csv.writer(out_fp, lineterminator='\n')
        writer.writerow(['YEAR','MONTH','BLOCK','CRIMECODE',
                         'DESCRIPTION','COUNT'])
        for (group, data) in grouped_data:
            writer.writerow(list(group) + [len(data)])
