import logging
import os
import tempfile

import google.cloud.storage as storage
import pandas as pd
from black import out
from matplotlib.pyplot import text

from kyd.parsers.anbima import AnbimaVnaTPF

logging.basicConfig(level=logging.INFO)  # filename='AnbimaTitpub.log',

storage_client = storage.Client()
output_bucket = storage.Bucket(
    storage_client, "ks-layer1", user_project="kyd-storage-001"
)
bucket = storage.Bucket(
    storage_client,
    "ks-rawdata-anbima-vnatitpub",
    user_project="kyd-storage-001",
)
blobs = list(storage_client.list_blobs(bucket, prefix="2021-0"))

logging.info("%s blobs to process", len(blobs))

temp = tempfile.gettempdir()
fname = os.path.join(temp, "tpf")

for blob in blobs:
    logging.info(blob.name)
    if not blob.name.endswith(".html"):
        logging.error("Not a txt file %s", blob.name)
        continue

    handle, foutput = tempfile.mkstemp()
    tempf = os.fdopen(handle, "w+b")
    storage_client.download_blob_to_file(blob, tempf)
    tempf.seek(0)
    tempf.flush()
    logging.info(foutput)
    x = AnbimaVnaTPF(foutput)
    if len(x.data) == 0:
        continue
    df = pd.DataFrame(x.data)
    tempf.close()

    refdate = df["refdate"][0]
    df.to_parquet(fname)
    output_fname = f"AnbimaVnaTitpub/{refdate}.parquet"
    output_blob = storage.Blob(output_fname, output_bucket)
    with open(fname, "rb") as fp:
        output_blob.upload_from_file(fp)
    logging.info("file saved %s", output_fname)

    os.remove(fname)
    logging.info("%s removed", fname)
    os.remove(foutput)
    logging.info("%s removed", foutput)
