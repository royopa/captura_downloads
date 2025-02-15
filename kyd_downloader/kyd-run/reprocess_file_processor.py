from datetime import datetime, timedelta
from itertools import groupby

import pytz
from google.cloud import datastore

ds_client = datastore.Client()
q = ds_client.query(kind='ProcessorLog')

SP_TZ = pytz.timezone('America/Sao_Paulo')
UTC = pytz.utc
# date = datetime.now(SP_TZ) + timedelta(-1)
date = datetime(2022, 1, 28)
t1 = date.replace(hour=0, minute=0, second=0, microsecond=0)
t2 = date.replace(hour=23, minute=59, second=59, microsecond=999999)
t1 = t1.astimezone(UTC)
t2 = t2.astimezone(UTC)
q.add_filter('time', '>=', t1)
q.add_filter('time', '<=', t2)

# logs = list(q.fetch())

# keyfunc = lambda x: (x['refdate'], x['name'])

# logs = filter(lambda x: x['download_status'] == 200, logs)

# for k, g in groupby(logs, keyfunc):
#     print(k, len(list(g)))

import json


def myconverter(o):
    if isinstance(o, datetime):
        return o.strftime('%Y-%m-%dT%H:%M:%S.%f%z')


import google.auth.transport.requests
import google.oauth2.id_token


def get_id_token(url):
    _request = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(_request, url)
    return id_token


import requests

url = 'https://kyd-process-file-kb2piwuv2q-uc.a.run.app'
id_token = get_id_token(url)

parents = {}
for log in q.fetch():
    key = log.key
    if key.parent:
        parent = ds_client.get(key.parent)
        if parent:
            # print(log['processor_name'], 'parent', parent['name'])
            parents[parent.key] = parent


for parent in parents.values():
    parent['time'] = parent['time'].strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    if parent['refdate']:
        parent['refdate'] = parent['refdate'].astimezone(
            pytz.timezone('America/Sao_Paulo')
        )
        parent['refdate'] = parent['refdate'].strftime(
            '%Y-%m-%dT%H:%M:%S.%f%z'
        )

    payload = dict(parent)
    payload.pop('message')
    res = requests.post(
        url, json=payload, headers={'Authorization': f'bearer {id_token}'}
    )
    print(parent['name'], res)
