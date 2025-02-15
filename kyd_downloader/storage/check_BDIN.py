import logging
from datetime import datetime

import bizdays
import google.cloud.storage as storage

# import os
# import sys
# import tempfile
# from kyd.parsers import unzip_to
# from kyd.parsers.b3 import BVBG086
import pandas as pd

logging.basicConfig(level=logging.INFO)

cal = bizdays.Calendar.load('B3')

# ks-rawdata-bovespa/BDI/all_files
storage_client = storage.Client()
bucket = storage.Bucket(
    storage_client, 'ks-rawdata-bovespa', user_project='kyd-storage-001'
)
blobs = list(storage_client.list_blobs(bucket, prefix='BDI/all_files'))

names = [blob.name for blob in blobs if blob.name.endswith('txt')]


def getdate(name):
    try:
        return datetime.strptime(
            name, 'BDI/all_files/BDIN_%Y-%m-%d.txt'
        ).strftime('%Y-%m-%d')
    except:
        return None


dates = [getdate(name) for name in names]
dates = [date for date in dates if date]

bizdates = [date.strftime('%Y-%m-%d') for date in cal.seq(dates[0], dates[-1])]

check_bizdates = [(bizdate in dates) for bizdate in bizdates]

print(len(bizdates))
print(len(check_bizdates))

pd.DataFrame({'bizdate': bizdates, 'check': check_bizdates}).to_csv(
    'check.csv'
)
