import logging
import os
import tempfile

import google.cloud.storage as storage
import pandas as pd
from black import out
from matplotlib.pyplot import text

from kyd.parsers.anbima import AnbimaTPF

logging.basicConfig(level=logging.INFO)  # filename='AnbimaTitpub.log',

storage_client = storage.Client()
output_bucket = storage.Bucket(
    storage_client, "ks-layer1", user_project="kyd-storage-001"
)
bucket = storage.Bucket(
    storage_client, "ks-rawdata-anbima-titpub", user_project="kyd-storage-001"
)
blobs = list(storage_client.list_blobs(bucket, prefix="2022-01-20"))

logging.info("%s blobs to process", len(blobs))

temp = tempfile.gettempdir()
# tempf = os.path.join(temp, 'tpf.txt')
fname = os.path.join(temp, "tpf")

for blob in blobs:
    logging.info(blob.name)
    if not blob.name.endswith(".txt"):
        logging.error("Not a txt file %s", blob.name)
        continue

    handle, output = tempfile.mkstemp()
    tempf = os.fdopen(handle, "w+b")
    storage_client.download_blob_to_file(blob, tempf)
    tempf.seek(0)
    tempf.flush()
    logging.info(output)
    # tempf = tempfile.TemporaryFile()
    # storage_client.download_blob_to_file(blob, tempf)
    # tempf.seek(0)
    # handle, output = tempfile.mkstemp()
    # with os.fdopen(handle, 'wb') as fp:
    #     fp.writelines(tempf.readlines())
    # logging.info(output)
    x = AnbimaTPF(output)
    df = pd.DataFrame(x.data)
    df["ask_yield"] = pd.to_numeric(df["ask_yield"], errors="coerce")
    df["bid_yield"] = pd.to_numeric(df["bid_yield"], errors="coerce")
    df["ref_yield"] = pd.to_numeric(df["ref_yield"], errors="coerce")
    x = ~df["ref_yield"].isna()
    if any(x):
        df = df[x].copy()
    else:
        logging.error(f"File with invalid data {blob.name}")
        continue
    # df = df.astype(str)
    df["cod_selic"] = df["cod_selic"].astype(int)

    # refdate = df['refdate'][0]
    # df.to_parquet(fname)
    # output_fname = f'AnbimaTitpub/{refdate}.parquet'
    # output_blob = storage.Blob(output_fname, output_bucket)
    # with open(fname, 'rb') as fp:
    #     output_blob.upload_from_file(fp)
    # logging.info('file saved %s', output_fname)

    # os.remove(fname)
    # logging.info('%s removed', fname)
    # os.remove(tempf)
    # logging.info('%s removed', tempf)
