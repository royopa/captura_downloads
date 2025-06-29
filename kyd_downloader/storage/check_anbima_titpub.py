from datetime import datetime

import bizdays
import google.cloud.storage as storage
import pandas as pd

cal = bizdays.Calendar.load("ANBIMA")

storage_client = storage.Client()
bucket = storage.Bucket(storage_client, "ks-layer1", user_project="kyd-storage-001")
blobs = list(storage_client.list_blobs(bucket, prefix="AnbimaTitpub"))

names = [blob.name for blob in blobs]

dates = [
    datetime.strptime(name, "AnbimaTitpub/%Y-%m-%d.parquet").strftime("%Y-%m-%d")
    for name in names
]

bizdates = [date.strftime("%Y-%m-%d") for date in cal.seq(dates[0], dates[-1])]

check_bizdates = [(bizdate in dates) for bizdate in bizdates]

print(len(bizdates))
print(len(check_bizdates))
missing = [bizdate for bizdate in bizdates if bizdate not in dates]
print(len(missing))
print(missing)

pd.DataFrame({"bizdate": bizdates, "check": check_bizdates}).to_csv(
    "check.csv", sep=";"
)
