import logging
import os
import tempfile
from datetime import datetime

import google.cloud.storage as storage
import pandas as pd

from kyd.parsers import unzip_file_to
from kyd.parsers.b3 import BVBG028

logging.basicConfig(level=logging.INFO)  # filename='BVBG028.log',

storage_client = storage.Client()
output_bucket = storage.Bucket(
    storage_client, "ks-layer1", user_project="kyd-storage-001"
)

# prefix = 'IPN/TS/BVBG.028.02'
# bucket = storage.Bucket(storage_client, 'ks-rawdata-bvmf', user_project='kyd-storage-001')
prefix = "CadInstr"
bucket = storage.Bucket(storage_client, "ks-rawdata-b3", user_project="kyd-storage-001")
blobs = list(storage_client.list_blobs(bucket, prefix=f"{prefix}/IN211222"))

logging.info("%s blobs to process", len(blobs))

temp = tempfile.gettempdir()
l1_name = "BVBG028"

for blob in blobs:
    logging.info(blob.name)
    if not blob.name.endswith(".zip"):
        logging.error("Not a zip file %s", blob.name)
        continue

    with tempfile.NamedTemporaryFile(suffix=".zip") as file_obj:
        storage_client.download_blob_to_file(blob, file_obj)

        outdir = tempfile.gettempdir()
        # fname = 'data/PR.zip' # sys.argv[1]
        try:
            dest = unzip_file_to(file_obj, outdir, -1)
        except:
            logging.error("Problems to unzip %s", blob.name)
            continue

    if not dest.endswith(".xml"):
        logging.error("Problems with %s", blob.name)
        logging.error("not a XML file %s", dest)
        os.remove(dest)
        logging.info("%s removed", dest)
        continue

    x = BVBG028(dest)
    instrs = {}
    for instr in x.data:
        typo = instr["instrument_type"]
        try:
            instrs[typo].append(instr)
        except:
            instrs[typo] = [instr]

    dx = {k: pd.DataFrame(instrs[k]) for k in instrs.keys()}

    dt = datetime.strptime(blob.name, f"{prefix}/IN%y%m%d.zip")
    refdate = dt.strftime("%Y-%m-%d")

    os.remove(dest)
    logging.info("%s removed", dest)

    for ix in dx.keys():
        df = dx[ix]
        fname = os.path.join(temp, ix)
        df.to_parquet(fname)
        output_fname = f"{l1_name}/{ix}/{refdate}.parquet"
        output_blob = storage.Blob(output_fname, output_bucket)
        with open(fname, "rb") as fp:
            output_blob.upload_from_file(fp)
        logging.info("file saved %s", output_fname)
        os.remove(fname)
        logging.info("%s removed", fname)
