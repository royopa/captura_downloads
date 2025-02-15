import logging
import os
import tempfile
from datetime import datetime

import google.cloud.storage as storage
import pandas as pd

from kyd.parsers import unzip_file_to
from kyd.parsers.b3 import BVBG086

logging.basicConfig(level=logging.INFO)

storage_client = storage.Client()
output_bucket = storage.Bucket(
    storage_client, 'ks-layer1', user_project='kyd-storage-001'
)

# ks-rawdata-bvmf/IPN/TRS/BVBG.086.01
# prefix = 'IPN/TRS/BVBG.086.01'
# bucket = storage.Bucket(storage_client, 'ks-rawdata-bvmf', user_project='kyd-storage-001')
# blobs = list(storage_client.list_blobs(bucket, prefix=f'{prefix}/PR211104'))
# ks-rawdata-b3/PricingReport
prefix = 'PricingReport'
bucket = storage.Bucket(
    storage_client, 'ks-rawdata-b3', user_project='kyd-storage-001'
)
blobs = list(
    storage_client.list_blobs(bucket, prefix='PricingReport/PR211222')
)

logging.info('%s blobs to process', len(blobs))

for blob in blobs:
    logging.info(blob.name)
    if not blob.name.endswith('.zip'):
        logging.error('Not a zip file %s', blob.name)
        continue

    with tempfile.NamedTemporaryFile(suffix='.zip') as file_obj:
        storage_client.download_blob_to_file(blob, file_obj)

        outdir = tempfile.gettempdir()
        # fname = 'data/PR.zip' # sys.argv[1]
        try:
            dest = unzip_file_to(file_obj, outdir, -1)
        except:
            logging.error('Problems to unzip %s', blob.name)
            continue

    if not dest.endswith('.xml'):
        logging.error('Problems with %s', blob.name)
        logging.error('not a XML file %s', dest)
        os.remove(dest)
        logging.info('%s removed', dest)
        continue

    x = BVBG086(dest)
    df = pd.DataFrame(x.data)

    dt = datetime.strptime(blob.name, f'{prefix}/PR%y%m%d.zip')
    refdate = dt.strftime('%Y-%m-%d')

    base = os.path.splitext(blob.name)[0].split('/')[-1]
    fname_parquet = f'{base}.parquet'
    df.to_parquet(f'data/{fname_parquet}')

    output_blob = storage.Blob(f'BVBG086/{refdate}.parquet', output_bucket)
    with open(f'data/{fname_parquet}', 'rb') as fp:
        output_blob.upload_from_file(fp)

    os.remove(dest)
    logging.info('%s removed', dest)
    os.remove(f'data/{fname_parquet}')
    logging.info('%s removed', f'data/{fname_parquet}')
