import logging
from datetime import datetime

import bizdays
import google.cloud.storage as storage
import pandas as pd

logging.basicConfig(level=logging.INFO)

extension = "html"
prefix = ""
bucket_name = "ks-rawdata-anbima-vnatitpub"

cal = bizdays.Calendar.load("B3.cal")

storage_client = storage.Client()
# bucket = storage.Bucket(storage_client, 'ks-rawdata-b3', user_project='kyd-storage-001')
# blobs = list(storage_client.list_blobs(bucket, prefix='PricingReport'))
bucket = storage.Bucket(storage_client, bucket_name, user_project="kyd-storage-001")
blobs = list(storage_client.list_blobs(bucket, prefix=prefix))

names = [blob.name for blob in blobs if blob.name.endswith(extension)]
dates = [
    datetime.strptime(name, f"{prefix}%Y-%m-%d.{extension}").strftime("%Y-%m-%d")
    for name in names
]
bizdates = [date.strftime("%Y-%m-%d") for date in cal.seq(dates[0], dates[-1])]
check_bizdates = [(bizdate in dates) for bizdate in bizdates]

assert len(bizdates) == len(check_bizdates)

print("Max bizdate:", max(bizdates))
for d, ck in zip(bizdates, check_bizdates):
    if not ck:
        print(d)
