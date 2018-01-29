
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

dates = [20170806,
20170808,
20170809,
20170810,
20170811,
20170812,
20170813,
20170814,
20170815,
20170816,
20170817,
20170818,
20170819,
20170820,
20170821,
20170822,
20170824,
20170825,
20170826,
20170827,
20170828,
20170831,
20171001,
20171004,
20171008,
20171010,
20171016,
20171017,
20171020,
20171024]


for d in range(0,len(dates)):

    # Read a file into pandas dataframe
    with adlsFileSystemClient.open('/PROD/API-STAT/Bulk/csv/{}/data{}.csv'.format(dates[d], dates[d]), 'rb') as f:
        df = pd.read_csv(f)

    ## for parquet  ##

    import pyarrow as pa
    table = pa.Table.from_pandas(df)
    import pyarrow.parquet as pq
    pq.write_table(table, 'data.parquet')

    ## Upload a file
    multithread.ADLUploader(adlsFileSystemClient, lpath='/Users/mattlivingston/PycharmProjects/stat_data_processing/data.parquet',
                            rpath='/PROD/API-STAT/Bulk/parquet/{}/data{}.parquet'.format(dates[d],dates[d]), nthreads=64, overwrite=True,
                            buffersize=4194304, blocksize=4194304)
