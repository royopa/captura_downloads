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

storage_client = storage.Client()
# bucket = storage.Bucket(storage_client, 'ks-rawdata-b3', user_project='kyd-storage-001')
# blobs = list(storage_client.list_blobs(bucket, prefix='PricingReport'))
bucket = storage.Bucket(
    storage_client, 'ks-layer1', user_project='kyd-storage-001'
)
blobs = list(storage_client.list_blobs(bucket, prefix='BVBG028/EqtyInf'))
# ks-layer1/BVBG028/EqtyInf

names = [blob.name for blob in blobs if blob.name.endswith('parquet')]

dates = [
    datetime.strptime(name, 'BVBG028/EqtyInf/%Y-%m-%d.parquet').strftime(
        '%Y-%m-%d'
    )
    for name in names
]

bizdates = [date.strftime('%Y-%m-%d') for date in cal.seq(dates[0], dates[-1])]

check_bizdates = [(bizdate in dates) for bizdate in bizdates]

missing = [bizdate for bizdate in bizdates if bizdate not in dates]

print(len(bizdates))
print(len(missing))
print(missing)

pd.DataFrame({'bizdate': bizdates, 'check': check_bizdates}).to_csv(
    'check_bvbg028.csv', sep=';'
)
