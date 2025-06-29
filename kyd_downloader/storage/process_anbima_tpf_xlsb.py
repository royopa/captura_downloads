import logging
import os
import tempfile
from datetime import datetime, timedelta

import google.cloud.storage as storage
import pandas as pd

from kyd.parsers.anbima import AnbimaTPF

logging.basicConfig(filename="AnbimaTitpub.log", level=logging.INFO)

storage_client = storage.Client()
output_bucket = storage.Bucket(
    storage_client, "ks-layer1", user_project="kyd-storage-001"
)
bucket = storage.Bucket(
    storage_client, "ks-xlsb-anbima-titpub", user_project="kyd-storage-001"
)
blobs = list(storage_client.list_blobs(bucket))

logging.info("%s blobs to process", len(blobs))

temp = tempfile.gettempdir()
tempf = os.path.join(temp, "tpf.xlsb")
fname = os.path.join(temp, "tpf")
names = [
    "symbol",
    "refdate",
    "cod_selic",
    "issue_date",
    "maturity_date",
    "bid_yield",
    "ask_yield",
    "ref_yield",
    "price",
]
to_date_str = lambda x: (datetime(1900, 1, 1) + timedelta(x - 2)).strftime("%Y-%m-%d")

for blob in blobs:
    logging.info(blob.name)
    if not blob.name.endswith(".xlsb"):
        logging.error("Not a txt file %s", blob.name)
        continue

    blob.download_to_filename(tempf, storage_client)
    _df = pd.read_excel(tempf, engine="pyxlsb", sheet_name="ANBIMA - TÍTULOS PÚBLICOS")
    df = _df.iloc[:, [1, 0, 2, 3, 4, 5, 6, 7, 8]].copy()
    # df = _df[['Categoria', 'DATA BASE', 'Código SELIC', 'Data de Emissão',
    #           'Data de Vencimento', 'Tx. Compra', 'Tx. Venda',
    #           'Tx. Indicativas', 'PU']].copy()
    df.columns = names
    df["ask_yield"] = pd.to_numeric(df["ask_yield"], errors="coerce")
    df["bid_yield"] = pd.to_numeric(df["bid_yield"], errors="coerce")
    df["ref_yield"] = pd.to_numeric(df["ref_yield"], errors="coerce")
    x = ~df["ref_yield"].isna()
    if any(x):
        df = df[x].copy()
    else:
        logging.error(f"File with invalid data {blob.name}")
        continue
    df["refdate"] = df["refdate"].map(to_date_str)
    df["issue_date"] = df["issue_date"].map(to_date_str)
    df["maturity_date"] = df["maturity_date"].map(to_date_str)

    refdate = df["refdate"][0]
    df.to_parquet(fname)
    output_fname = f"AnbimaTitpub/{refdate}.parquet"
    output_blob = storage.Blob(output_fname, output_bucket)
    with open(fname, "rb") as fp:
        output_blob.upload_from_file(fp)
    logging.info("file saved %s", output_fname)

    os.remove(fname)
    logging.info("%s removed", fname)
    os.remove(tempf)
    logging.info("%s removed", tempf)
