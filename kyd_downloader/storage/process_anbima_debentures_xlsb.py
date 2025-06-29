import logging
import os
import tempfile
from datetime import datetime, timedelta

import google.cloud.storage as storage
import pandas as pd

from kyd.parsers.anbima import AnbimaTPF

logging.basicConfig(level=logging.INFO)

storage_client = storage.Client()
output_bucket = storage.Bucket(
    storage_client, "ks-layer1", user_project="kyd-storage-001"
)
bucket = storage.Bucket(
    storage_client, "ks-xlsb-anbima-titpub", user_project="kyd-storage-001"
)
blobs = list(storage_client.list_blobs(bucket, prefix="DADOS DIÁRIOS - ANBIMA - 2021"))

logging.info("%s blobs to process", len(blobs))

temp = tempfile.gettempdir()
tempf = os.path.join(temp, "tpf.xlsb")
fname = os.path.join(temp, "tpf")
names = [
    "symbol",
    "name",
    "maturity_date",
    "underlying",
    "bid_yield",
    "ask_yield",
    "ref_yield",
    "price",
    "perc_price_par",
    "duration",
    "perc_reune",
    "ref_ntnb",
    "refdate",
]


def to_date_str(x):
    f = lambda x: (datetime(1900, 1, 1) + timedelta(x - 2)).strftime("%Y-%m-%d")
    try:
        return f(int(x))
    except:
        try:
            return datetime.strptime(x, "%d/%m/%Y").strftime("%Y-%m-%d")
        except:
            return pd.NA


for blob in blobs:
    logging.info(blob.name)
    if not blob.name.endswith(".xlsb"):
        logging.error("Not a txt file %s", blob.name)
        continue

    blob.download_to_filename(tempf, storage_client)
    _df = pd.read_excel(tempf, engine="pyxlsb", sheet_name="ANBIMA - DEBÊNTURES")
    df = _df.iloc[:, [1, 2, 3, 4, 5, 6, 7, 11, 12, 13, 14, 15, 0]].copy()
    df.columns = names

    df["refdate"] = df["refdate"].map(to_date_str)

    if df["refdate"].iloc[0] >= "2021-04-20" or df["refdate"].iloc[0] < "2019-03-19":
        continue

    df["ask_yield"] = pd.to_numeric(df["ask_yield"], errors="coerce").astype("float64")
    df["bid_yield"] = pd.to_numeric(df["bid_yield"], errors="coerce").astype("float64")
    df["ref_yield"] = pd.to_numeric(df["ref_yield"], errors="coerce").astype("float64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce").astype("float64")
    df["perc_price_par"] = pd.to_numeric(df["perc_price_par"], errors="coerce").astype(
        "float64"
    )
    df["duration"] = pd.to_numeric(df["duration"], errors="coerce").astype("float64")
    df["perc_reune"] = pd.to_numeric(df["perc_reune"], errors="coerce").astype(
        "float64"
    )
    df["maturity_date"] = df["maturity_date"].map(to_date_str)
    df.loc[~df["ref_ntnb"].isna(), "ref_ntnb"] = df.loc[
        ~df["ref_ntnb"].isna(), "ref_ntnb"
    ].map(to_date_str)

    x = ~df["ref_yield"].isna()
    if any(x):
        df = df[x].copy()
    else:
        logging.error(f"File with invalid data {blob.name}")
        continue

    df["underlying"] = df["underlying"].map(str)
    refdate = df["refdate"][0]
    df.to_parquet(fname)
    output_fname = f"AnbimaDebentures_xlsb/{refdate}.parquet"
    output_blob = storage.Blob(output_fname, output_bucket)
    with open(fname, "rb") as fp:
        output_blob.upload_from_file(fp)
    logging.info("file saved %s", output_fname)

    os.remove(fname)
    logging.info("%s removed", fname)
    os.remove(tempf)
    logging.info("%s removed", tempf)
