import logging
import time
from datetime import datetime

import bizdays
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def call_url1(refdate, name):
    url = f"https://kyd-storage-001.rj.r.appspot.com/reprocess?refdate={refdate}&name={name}"
    res = requests.get(url)
    logging.info("finished download %s %s %s", name, refdate, res.status_code)
    logging.info("  url %s", url)
    return res.status_code


# log = {
#     "bucket": "ks-rawdata-b3",
#     "filename": date.strftime("PricingReport/PR%y%m%d.zip"),
#     "refdate": date.strftime("%Y-%m-%d"),
#     "name": "pricereport"
# }
def call_url2(log):
    url = "https://us-central1-kyd-storage-001.cloudfunctions.net/kyd-generic-process-file"
    res = requests.post(url, json=log)
    logging.info("finished convert %s %s", log["refdate"], res.status_code)
    return res.status_code


# dates = [
#     "2023-02-23",
#     "2023-02-22",
#     "2023-02-17",
#     "2023-02-16",
#     "2023-02-15",
#     "2023-02-14",
#     "2023-02-13",
#     "2023-02-10",
#     "2023-02-09",
#     "2023-02-08",
#     "2023-02-07",
#     "2023-02-06",
#     "2023-02-03",
#     "2023-02-02",
#     "2023-02-01",
#     "2023-01-31",
#     "2023-01-30",
#     "2023-01-27",
#     "2023-01-26",
#     "2023-01-25",
#     "2023-01-24",
#     "2023-01-23",
#     "2023-01-20",
#     "2023-01-19",
#     "2023-01-18",
#     "2023-01-17",
#     "2023-01-16",
#     "2023-01-13",
#     "2023-01-12",
#     "2023-01-11",
#     "2023-01-10",
#     "2023-01-09",
#     "2023-01-06",
#     "2023-01-05",
#     "2023-01-04",
#     "2023-01-03",
#     "2023-01-02",
#     "2022-12-30",
#     "2022-12-29",
#     "2022-12-28",
#     "2022-12-27",
#     "2022-12-26",
#     "2022-12-23",
#     "2022-12-22",
# ]


cal = bizdays.Calendar.load("B3")
dates = cal.seq("2022-12-22", "2023-02-23")

for date in dates:
    # date = datetime.strptime(date, '%Y-%m-%d')
    # log = {
    #     "bucket": "ks-rawdata-b3",
    #     "filename": date.strftime("PricingReport/PR%y%m%d.zip"),
    #     "refdate": date.strftime("%Y-%m-%d"),
    #     "name": "pricereport"
    # }
    # call_url2(log)
    date = date.strftime("%Y-%m-%d")
    call_url1(date, "b3-otc-instruments-consolidated")
    # call_url1(date, 'b3-otc-trade-information-consolidated')
    time.sleep(5)

# call_url1('2021-06-10', 'pricereport')
# call_url1('2021-03-23', 'cadinstr')
