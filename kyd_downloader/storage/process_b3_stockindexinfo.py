import logging
import os
import tempfile

import google.cloud.storage as storage
import pandas as pd

from kyd.parsers.b3 import StockIndexInfo

logging.basicConfig(level=logging.INFO)  # filename='BVBG028.log',

storage_client = storage.Client()
output_bucket = storage.Bucket(
    storage_client, 'ks-layer1', user_project='kyd-storage-001'
)

# prefix = 'IPN/TS/BVBG.028.02'
# ks-rawdata-b3/TaxaCDI
# bucket = storage.Bucket(storage_client, 'ks-rawdata-bvmf', user_project='kyd-storage-001')
prefix = 'StockIndexInfo'
bucket = storage.Bucket(
    storage_client, 'ks-rawdata-b3', user_project='kyd-storage-001'
)
blobs = list(storage_client.list_blobs(bucket, prefix=f'{prefix}/2022-09'))

logging.info('%s blobs to process', len(blobs))

temp = tempfile.gettempdir()
fname = os.path.join(temp, 'b3sii')
l1_name = 'B3StockIndexInfo'

for blob in blobs:
    logging.info(blob.name)

    handle, foutput = tempfile.mkstemp()
    tempf = os.fdopen(handle, 'w+b')
    storage_client.download_blob_to_file(blob, tempf)
    tempf.seek(0)
    tempf.flush()
    # logging.info(foutput)
    x = StockIndexInfo(foutput)
    if len(x.data) == 0:
        continue
    df = pd.DataFrame(x.data)
    tempf.close()

    refdate = df['refdate'][0]
    df.to_parquet(fname)
    output_fname = f'{l1_name}/{refdate}.parquet'
    output_blob = storage.Blob(output_fname, output_bucket)
    with open(fname, 'rb') as fp:
        output_blob.upload_from_file(fp)
    logging.info('file saved %s', output_fname)

    os.remove(fname)
    # logging.info("%s removed", fname)
    os.remove(foutput)
    # logging.info("%s removed", foutput)
