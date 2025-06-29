import logging
import os
import tempfile
import time
from datetime import datetime, timezone

import google.auth.transport.requests
import google.cloud.storage as storage
import google.oauth2.id_token
import pandas as pd
import pytz
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("log.txt")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

logging.basicConfig(level=logging.INFO)  # filename='AnbimaTitpub.log',

url = "https://kyd-process-file-kb2piwuv2q-uc.a.run.app"


def get_id_token(url):
    _request = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(_request, url)
    logging.info("token %s", id_token)
    return id_token


def call_url(log):
    token = get_id_token(url)
    res = requests.post(url, json=log, headers={"Authorization": f"Bearer {token}"})
    logger.info("finished %s %s %s", log["name"], log["refdate"], res.status_code)
    if res.status_code > 400:
        time.sleep(0.5)
        _token = get_id_token(url)
        res = requests.post(
            url, json=log, headers={"Authorization": f"Bearer {_token}"}
        )
        logger.info("finished %s %s %s", log["name"], log["refdate"], res.status_code)


prefix = "CDI"
ext = "json"
bucket_name = "ks-rawdata-b3"
bucket_prefix = "TaxaCDI/"
processor_name = "cdi"

storage_client = storage.Client()
bucket = storage.Bucket(storage_client, "ks-layer1", user_project="kyd-storage-001")
blobs = list(storage_client.list_blobs(bucket, prefix=f"{prefix}/2022-11"))

for blob in blobs:
    logging.info(blob.name)
    if not blob.name.endswith(".parquet"):
        logging.error("Not a parquet file %s", blob.name)
        continue

    handle, output = tempfile.mkstemp()
    tempf = os.fdopen(handle, "w+b")
    storage_client.download_blob_to_file(blob, tempf)
    tempf.seek(0)
    tempf.flush()

    try:
        pd.read_parquet(tempf)
        logging.info(f"Succeeded reading {blob.name}")
    except:
        logging.error(f"Failed reading {blob.name}")
        logging.info(f"Reprocessing {blob.name}")

        refdate = datetime.strptime(blob.name, f"{prefix}/%Y-%m-%d.parquet")
        refdate = refdate.replace(tzinfo=pytz.timezone("America/Sao_Paulo"))

        log = {
            "bucket": bucket_name,
            "filename": f"{bucket_prefix}{refdate.strftime('%Y-%m-%d')}.{ext}",
            "refdate": refdate.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
            "name": processor_name,
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
            "download_status": 200,
        }

        call_url(log)
        logging.info(f"Reprocessed {blob.name}")
