import logging
import threading
import time

import bizdays
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
fh = logging.FileHandler('log.txt')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


def call_url(log):
    url = 'https://us-central1-kyd-storage-001.cloudfunctions.net/kyd-generic-process-file'
    res = requests.post(url, json=log)
    logger.info('finished %s %s', log['refdate'], res.status_code)


cal = bizdays.Calendar.load('ANBIMA.cal')

# threads = []
# for date in cal.seq('2020-01-01', '2020-12-31'):
#     print(date)
#     log = {
#         "bucket": "ks-rawdata-b3",
#         "filename": date.strftime("CadInstr/IN%y%m%d.zip"),
#         "refdate": date.strftime("%Y-%m-%d"),
#         "name": "cadinstr"
#     }

#     call_url(log)

# t = threading.Thread(target=call_url, args=(log,))
# threads.append(t)

# for t in threads:
#     t.start()
#     time.sleep(1)


# 2021-07-13

log = {
    'bucket': 'ks-rawdata-b3',
    'filename': 'CadInstr/IN210908.zip',
    'refdate': '2021-09-08',
    'name': 'cadinstr',
}

call_url(log)
