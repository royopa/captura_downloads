import logging
import os
import tempfile

import google.cloud.storage as storage
import pandas as pd

from kyd.parsers.anbima import AnbimaTPF

logging.basicConfig(level=logging.INFO)   # filename='AnbimaTitpub.log',

storage_client = storage.Client()
output_bucket = storage.Bucket(
    storage_client, 'ks-layer1', user_project='kyd-storage-001'
)
bucket = storage.Bucket(
    storage_client, 'ks-rawdata-anbima-titpub', user_project='kyd-storage-001'
)
blobs = list(storage_client.list_blobs(bucket, prefix='2022-01-20'))

logging.info('%s blobs to process', len(blobs))

temp = tempfile.gettempdir()
# tempf = os.path.join(temp, 'tpf.txt')
tempf = tempfile.TemporaryFile()
fname = os.path.join(temp, 'tpf')

for blob in blobs:
    logging.info(blob.name)
    if not blob.name.endswith('.txt'):
        logging.error('Not a txt file %s', blob.name)
        continue

    # blob.download_to_filename(tempf, storage_client)
    storage_client.download_blob_to_file(blob, tempf)
    tempf.seek(0)
    print(len(tempf.readlines()))
    print(tempf.mode)
    print(tempf.name)
