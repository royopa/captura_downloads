import logging
import time
from datetime import datetime, time, timezone

import bizdays
import google.auth.transport.requests
import google.oauth2.id_token
import requests

bizdays.set_option("mode", "datetime")


def get_id_token(url):
    _request = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(_request, url)
    logging.info("token %s", id_token)
    return id_token


url = "https://kyd-process-file-kb2piwuv2q-uc.a.run.app"
token = get_id_token(url)


def call_url(log):
    res = requests.post(url, json=log, headers={"Authorization": f"Bearer {token}"})
    logger.info("finished %s %s %s", log["name"], log["refdate"], res.status_code)
    if res.status_code > 400:
        time.sleep(0.5)
        _token = get_id_token(url)
        res = requests.post(
            url, json=log, headers={"Authorization": f"Bearer {_token}"}
        )
        logger.info("finished %s %s %s", log["name"], log["refdate"], res.status_code)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("log.txt")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

# cal = bizdays.Calendar.load('ANBIMA.cal')
# cal.seq('2021-08-01', '2021-08-10')

# dates = []
# cdate = datetime(2020, 1, 1)
# # dates.append(cdate)
# while cdate <= datetime(2020, 5, 31):
#     dates.append(cdate)
#     cdate += timedelta(1)

# for date in dates:
# log = {
#     "bucket": "ks-rawdata-b3", #"ks-rawdata-bvmf",
#     "filename": date.strftime("IPN/TRS/BVBG.086.01/PR%y%m%d.zip"), # date.strftime("PricingReport/PR%y%m%d.zip")
#     "refdate": date.strftime("%Y-%m-%d"),
#     "name": "pricereport",
#     "time": datetime.now().isoformat(),
#     "download_status": 200
# }
# call_url(log)
# log = {
#     "bucket": "ks-rawdata-b3", #"ks-rawdata-bvmf",
#     "filename": date.strftime("CadInstr/IN%y%m%d.zip"), #date.strftime("IPN/TS/BVBG.028.02/IN%y%m%d.zip")
#     "refdate": date.strftime("%Y-%m-%d"),
#     "name": "cadinstr",
#     "time": datetime.now().isoformat(),
#     "download_status": 200
# }
# call_url(log)
# log = {
#     "bucket": "ks-rawdata-b3", #"ks-rawdata-bvmf", ks-rawdata-b3
#     "filename": date.strftime("IndexReport/IR%y%m%d.zip"), # IPN/GPS/BVBG.087.01/IR%y%m%d.zip IndexReport
#     "refdate": date.strftime("%Y-%m-%d"),
#     "name": "indexreport",
#     "time": datetime.now().isoformat(),
#     "download_status": 200
# }
# call_url(log)

# time.sleep(0.5)

dates = [
    datetime(2023, 4, 25, tzinfo=timezone.utc),
    # datetime(2021, 5, 7, tzinfo=timezone.utc),
    # datetime(2021, 5, 14, tzinfo=timezone.utc),
    # datetime(2021, 5, 21, tzinfo=timezone.utc),
    # datetime(2021, 5, 28, tzinfo=timezone.utc),
    # datetime(2021, 6, 4, tzinfo=timezone.utc),
    # datetime(2021, 6, 11, tzinfo=timezone.utc),
    # datetime(2021, 6, 18, tzinfo=timezone.utc),
    # datetime(2021, 6, 25, tzinfo=timezone.utc),
    # datetime(2021, 7, 2, tzinfo=timezone.utc),
    # datetime(2021, 7, 9, tzinfo=timezone.utc),
    # datetime(2022, 1, 25, tzinfo=timezone.utc),
    # datetime(2023, 1, 13, tzinfo=timezone.utc),
]

# cal = bizdays.Calendar.load('ANBIMA')

# dates = cal.seq("2021-04-21", "2022-01-23")

# dates = [datetime.combine(date, time.min, tzinfo=timezone.utc) for date in dates]

for date in dates:
    # b = "ks-rawdata-anbima-titpub"  # "ks-rawdata-bvmf",
    b = "ks-rawdata-anbima-deb"  # "ks-rawdata-bvmf",
    s = "%Y-%m-%d.txt"  # "IPN/GPS/BVBG.087.01/IR%y%m%d.zip"
    # n = "titpub_anbima"
    n = "deb_anbima"
    log = {
        "bucket": b,
        "filename": date.strftime(s),
        "refdate": date.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "name": n,
        "time": datetime.now(timezone.utc).isoformat(),
        "download_status": 200,
    }
    # logger.info(date)
    call_url(log)
