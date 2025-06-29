import os
from datetime import datetime

import bizdays
import google.cloud.storage as storage
import pandas as pd


def check_missing_dates(
    cal_name, bucket, prefix, filename_pattern=None, project="kyd-storage-001"
):
    cal = bizdays.Calendar.load(cal_name)

    client = storage.Client()
    bucket = storage.Bucket(client, bucket, user_project=project)
    blobs = list(client.list_blobs(bucket, prefix=prefix))

    names = [blob.name for blob in blobs]
    filenames = [os.path.split(n)[1] for n in names]
    if filename_pattern is None:
        dates = [os.path.splitext(f)[0] for f in filenames if f != ""]
    else:
        dates = [
            datetime.strptime(f, filename_pattern).strftime("%Y-%m-%d")
            for f in filenames
            if f != ""
        ]
    dates.sort()
    bizdates = [date.strftime("%Y-%m-%d") for date in cal.seq(dates[0], dates[-1])]
    missing = [bizdate for bizdate in bizdates if bizdate not in dates]
    missing.sort()

    return {
        "bucket": bucket.name,
        "prefix": prefix,
        "start": min(dates),
        "end": max(dates),
        "missing_dates": missing,
        "number_of_files": len(dates),
    }


# print(check_missing_dates("B3", "ks-rawdata-b3", "CetipNegociosRegistrados"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "CompaniesOptions"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "COTAHIST_DAILY", "COTAHIST_D%d%m%Y.ZIP"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "DerivativesOpenPosition"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "EconomicIndicatorPrice"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "InstrumentsConsolidated"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "LendingOpenPosition"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "LoanBalance"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "MarginScenarioLiquidAssets"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "OTCInstrumentsConsolidated"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "OTCTradeInformationConsolidated"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "SettlementPrices"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "StockIndexInfo"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "TradeInformationConsolidatedAfterHours"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "TradeInformationConsolidated"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "CadInstr/IN", "IN%y%m%d.zip"))
# print(check_missing_dates("B3", "ks-rawdata-bvmf", "IPN/TS/BVBG.028.02", "IN%y%m%d.zip"))
# print(check_missing_dates("B3", "ks-rawdata-bvmf", "IPN/TS/BVBG28", "IN%y%m%d.zip"))
# print(check_missing_dates("B3", "ks-layer1", "BVBG082"))
# print(check_missing_dates("B3", "ks-layer1", "BVBG082"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "CadInstrIndic", "II%y%m%d.zip"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "TaxaCDI"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "IndexReport", "IR%y%m%d.zip"))
# print(check_missing_dates("B3", "ks-rawdata-bvmf", "IPN/GPS/BVBG.087.01", "IR%y%m%d.zip"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "IndicadoresAgro"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "IndicadoresFinanceiros"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "PricingReport", "PR%y%m%d.zip"))
# print(check_missing_dates("B3", "ks-rawdata-b3", "TaxaCambioReferencial"))
print(check_missing_dates("ANBIMA", "ks-rawdata-anbima-deb", ""))
# print(check_missing_dates("ANBIMA", "ks-rawdata-anbima-titpub", ""))
# print(check_missing_dates("ANBIMA", "ks-rawdata-anbima-vnatitpub", ""))

# print(check_missing_dates("B3", "ks-layer1", "BVBG087/IndxInf"))
# print(check_missing_dates("B3", "ks-layer1", "BVBG087/BDRInf"))
# print(check_missing_dates("B3", "ks-layer1", "BVBG087/IOPVInf"))
# print(check_missing_dates("ANBIMA", "ks-layer1", "AnbimaDebentures"))
# print(check_missing_dates("ANBIMA", "ks-layer1", "AnbimaTitpub"))
# print(check_missing_dates("ANBIMA", "ks-layer1", "AnbimaVnaTitpub"))
# print(check_missing_dates("B3", "ks-layer1", "B3StockIndexInfo"))
# print(check_missing_dates("B3", "ks-layer1", "BVBG028/EqtyInf"))
# print(check_missing_dates("B3", "ks-layer1", "BVBG028/FutrCtrctsInf"))
# print(check_missing_dates("B3", "ks-layer1", "BVBG028/OptnOnEqtsInf"))
# print(check_missing_dates("B3", "ks-layer1", "CDI"))
# print(check_missing_dates("B3", "ks-layer1", "BVBG086"))


# cal = bizdays.Calendar.load("ANBIMA")

# storage_client = storage.Client()
# bucket = storage.Bucket(storage_client, "ks-rawdata-b3", user_project="kyd-storage-001")
# blobs = list(storage_client.list_blobs(bucket, prefix="CetipNegociosRegistrados"))

# names = [blob.name for blob in blobs]

# dates = [
#     datetime.strptime(name, "CetipNegociosRegistrados/%Y-%m-%d.html").strftime(
#         "%Y-%m-%d"
#     )
#     for name in names
# ]
# dates.sort()

# bizdates = [date.strftime("%Y-%m-%d") for date in cal.seq(dates[0], dates[-1])]

# check_bizdates = [(bizdate in dates) for bizdate in bizdates]

# print(len(bizdates))
# print(len(check_bizdates))
# missing = [bizdate for bizdate in bizdates if bizdate not in dates]
# print(len(missing))
# print(missing)

# pd.DataFrame({"bizdate": bizdates, "check": check_bizdates}).to_csv(
#     "check.csv", sep=";"
# )
