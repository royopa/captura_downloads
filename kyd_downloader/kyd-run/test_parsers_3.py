import logging

import requests

logging.basicConfig(level=logging.DEBUG)

url = 'https://us-central1-kyd-storage-001.cloudfunctions.net/kyd-generic-process-file'

# log = {
#     "bucket": "ks-rawdata-b3",
#     "filename": "CadInstr/IN210708.zip",
#     "refdate": "2021-07-08",
#     "name": "cadinstr1"
# }

# requests.get(url, json=log)

log = {
    'bucket': 'ks-rawdata-bovespa',
    'filename': 'COTAHIST_ZIP/COTAHIST_A2019.ZIP',
    'refdate': '2019',
    'name': 'cotahist',
}


logging.info('before call')
requests.post(url, json=log)
logging.info('after call')
