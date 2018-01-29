import json
import pandas as pd

from azure.datalake.store import core, lib, multithread
token = lib.auth()

adlsFileSystemClient = core.AzureDLFileSystem(token, store_name='rentpathdatalake')

"""
Files to Read in FROM: rentpathdatalake

 /PROD/API-STAT/INPUT/{YYYMMDD}/Bulk/JSON/serps{YYYYMMDD}.json


Write Files TO:  rentpathdatalake

CSV


/PROD/API-STAT/Bulk/csv/{YYYYMMDD}/data{YYYYMMDD}.csv


PARQUET

/PROD/API-STAT/Bulk/parquet/{YYYYMMDD}/data{YYYYMMDD}.parquet

"""

## dates needed to parse

#first one just used for testing
##20170806

#first batch

# 20170808,
# 20170809,
# 20170810,
# 20170811,
# 20170812,
# 20170813,
# 20170814,
# 20170815,
# 20170816,
# 20170817,
# 20170818,


#second batch, fail b/c
##FileNotFoundError: PROD/API-STAT/INPUT/20170903/Bulk/JSON/serps20170903.json
#FileNotFoundError: PROD/API-STAT/INPUT/20170907/Bulk/JSON/serps20170907.json

# 20170819,
# 20170820,
# 20170821,
# 20170822,
# 20170824,
# 20170825,
# 20170826,
# 20170827,
# 20170828,
# 20170831,
# 20171001,
# 20171004,
# 20171008,
# 20171010,
# 20171016,
# 20171017,
### ALL septs seem to be missing ###
# 20170903,
# 20170907,
# 20170909,
# 20170910,
# 20170912,
# 20170914,
# 20170916,
# 20170930,
### ALL septs seem to be missing ###

# also missing
# 20171019,

dates = [20171020, 20171024]

for d in range(0,len(dates)):

    data = adlsFileSystemClient.open('/PROD/API-STAT/INPUT/{}/Bulk/JSON/serps{}.json'.format(dates[d], dates[d])).read()

    data2 = json.loads(data.decode('utf-8'), strict=False)   #fails if you don't use strict=False   - json.decoder.JSONDecodeError: Invalid control character at: line 385626 column 141 (char 29317239)

    # trim down to just actual results of api call
    results = data2['Response']['sites'][0]['KeywordSerps']

    data_frame = []
    for i in range(0, len(results)):
        keyword_id = results[i]['keyword_id']
        device = results[i]['device']
        keyword = results[i]['keyword']
        location = results[i]['location']
        market = results[i]['market']
        searchengine = 'google'  ### variable based on search engine you want

        # now need iterate in list for BaseRank, Protocol, Rank, ResultType, Url, etc

        ## adding KeyError exception for entries that don't have google key
        try:
            for x in range(0, len(results[i][searchengine])):
                BaseRank = results[i][searchengine][x]['BaseRank']
                Protocol = results[i][searchengine][x]['Protocol']
                Rank = results[i][searchengine][x]['Rank']
                ResultType = results[i][searchengine][x]['ResultTypes']['ResultType']
                Url = results[i][searchengine][x]['Url']
                data_frame.append({'keyword_id': keyword_id, 'device': device, 'keyword': keyword,
                                   'location': location, 'market': market, 'BaseRank': BaseRank,
                                   'Protocol': Protocol, 'Rank': Rank, 'ResultType': ResultType,
                                   'Url': Url, 'searchengine': searchengine})
        except KeyError:
            pass


    ## for CSV  ##
    dataframe = pd.DataFrame(data_frame)

    dataframe.to_csv('data.csv', index=False)

    ## Upload a file
    multithread.ADLUploader(adlsFileSystemClient, lpath='/Users/mattlivingston/PycharmProjects/stat_data_processing/data.csv',
                            rpath='/PROD/API-STAT/Bulk/csv/{}/data{}.csv'.format(dates[d],dates[d]), nthreads=64, overwrite=True,
                            buffersize=4194304, blocksize=4194304)





    # ## for parquet  ##
    #
    # import pyarrow as pa
    #
    # table = pa.Table.from_pandas(dataframe)
    #
    # import pyarrow.parquet as pq
    #
    # pq.write_table(table, 'data.parquet')


