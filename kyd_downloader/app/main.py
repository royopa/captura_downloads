import logging
from datetime import datetime, timedelta, timezone
from itertools import groupby

import bizdays
import google.auth.transport.requests
import google.oauth2.id_token
import pytz
import requests
from flask import Flask, redirect, render_template, request, url_for
from flask_table import Col, Table
from flask_table.html import element

# from google.cloud import storage
from google.cloud import datastore

logging.basicConfig(level=logging.INFO)


# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)
client = datastore.Client()
SP_TZ = pytz.timezone('America/Sao_Paulo')
UTC = pytz.utc


class URLCol(Col):
    def __init__(self, name, **kwargs):
        super(URLCol, self).__init__(name, **kwargs)

    def td_contents(self, item, attr_list):
        name = self.from_attr_list(item, 'name')
        refdate = self.from_attr_list(item, 'refdate')
        # url = '/reprocess?filter={}&refdate={}'.format(name, refdate)
        url = 'https://us-central1-kyd-storage-001.cloudfunctions.net/kyd-generic-download-publish?filter={}&refdate={}'.format(
            name, refdate
        )
        return element('a', {'href': url}, content='Link')


class DownloadLogTable(Table):
    name = Col('Name')
    time = Col('Time')
    refdate = Col('Refdate')
    download_status = Col('DownloadStatus')
    status = Col('Status')
    filename = Col('Filename')
    message = Col('Message')
    url = URLCol('URL')
    allow_sort = False
    table_id = 'data'
    classes = ['table', 'table-striped', 'table-sm']

    def sort_url(self, col_key, reverse=False):
        if reverse:
            direction = 'desc'
        else:
            direction = 'asc'
        return url_for('index', sort=col_key, direction=direction)


def get_id_token(url):
    _request = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(_request, url)
    logging.info('token %s', id_token)
    return id_token


@app.route('/reprocess')
def reprocess():
    url = 'https://us-central1-kyd-storage-001.cloudfunctions.net/kyd-generic-download-publish'
    id_token = get_id_token(url)
    refdate = request.args.get('refdate')
    name = request.args.get('name')
    url = f'{url}?filter={name}&refdate={refdate}'
    res = requests.get(url, headers={'Authorization': f'bearer {id_token}'})
    return res.text, res.status_code


DEFAULT_TASKNAME = 'pricereport'


@app.route('/')
def index():
    return redirect('/download-log')


@app.route('/download-log')
def download_log():
    key = client.key('Session', 'date')
    date = client.get(key)
    if request.args.get('date'):
        date = request.args.get('date')
        date = datetime.strptime(date, '%Y-%m-%d')
        date = SP_TZ.localize(date)
        logging.info('Request date %s', date)
    elif date:
        date = date['value']
        date = date.astimezone(SP_TZ)
        logging.info('Datastore date %s', date)
    else:
        date = datetime.now(SP_TZ) + timedelta(-1)
        logging.info('Now date %s', date)
    session_entry = datastore.Entity(key)
    session_entry.update({'value': date.astimezone(UTC)})
    client.put(session_entry)

    q = client.query(kind='DownloadLog')
    t1 = date.replace(hour=0, minute=0, second=0, microsecond=0)
    t2 = date.replace(hour=23, minute=59, second=59, microsecond=999999)
    t1 = t1.astimezone(UTC)
    t2 = t2.astimezone(UTC)
    logging.info('time %s %s %s', date, t1, t2)
    q.add_filter('time', '>=', t1)
    q.add_filter('time', '<=', t2)

    logs = execute_query(q)

    key = client.key('Session', 'status')
    status = client.get(key)
    if request.args.getlist('status'):
        status = request.args.getlist('status')
        status = [int(s) for s in status]
    elif status:
        status = status['value']
    else:
        status = [-1, 0, 1, 2]
    session_entry = datastore.Entity(key)
    session_entry.update({'value': status})
    client.put(session_entry)

    logs = [l for l in logs if l['status'] in status]
    logs.sort(key=lambda x: (x['refdate'], x['name']), reverse=True)

    return render_template(
        'download-log.html',
        page_name='download-log',
        date=date,
        length=len(logs),
        status=status,
        logs=logs,
    )


def execute_query(q):
    logs = list(q.fetch())
    for log in logs:
        if log['refdate']:
            log['refdate'] = (
                log['refdate'].astimezone(SP_TZ).strftime('%Y-%m-%d')
            )
        else:
            log['refdate'] = ''
        log['time'] = (
            log['time'].astimezone(SP_TZ).strftime('%Y-%m-%d %H:%M:%S')
        )

        if log['filename']:
            log['filename'] = 'gs://{}/{}'.format(
                log['bucket'], log['filename']
            )
        else:
            log['filename'] = ''

        url = f'/reprocess?refdate={log["refdate"]}&name={log["name"]}'
        log['url'] = url

    return logs


@app.route('/historical-download-log')
def historical_download_log():
    key = client.key('Session', 'taskname')
    taskname = client.get(key)
    if request.args.get('taskname'):
        taskname = request.args.get('taskname')
    elif taskname:
        taskname = taskname['value']
    else:
        taskname = DEFAULT_TASKNAME
    session_entry = datastore.Entity(key)
    session_entry.update({'value': taskname})
    client.put(session_entry)

    key = client.key('Session', 'compress')
    compress = client.get(key)
    if request.args.get('compress'):
        compress = request.args.get('compress') == '1'
        logging.info('compress from request %s', compress)
    else:
        compress = False
        logging.info('compress default %s', compress)

    q = client.query(kind='DownloadLog')
    q.projection = ['name']
    q.distinct_on = ['name']
    q.order = ['name']
    names = [n['name'] for n in q.fetch()]

    q = client.query(kind='DownloadLog')
    q.add_filter('name', '=', taskname)

    logs = execute_query(q)
    logs.sort(key=lambda x: (x['time'], x['refdate']), reverse=True)

    if compress:
        check_ok = {}
        for log in logs:
            key = f'{log["name"]}-{log["refdate"]}'
            if log['status'] == 0:
                check_ok[key] = log
        for log in logs:
            key = f'{log["name"]}-{log["refdate"]}'
            if log['status'] != 0 and key not in check_ok:
                check_ok[key] = log
        logs = list(check_ok.values())

    return render_template(
        'historical-download-log.html',
        page_name='historical-download-log',
        names=names,
        taskname=taskname,
        length=len(logs),
        compress=compress,
        logs=logs,
    )


@app.route('/processor-log')
def processor_log():
    key = client.key('Session', 'date')
    date = client.get(key)
    if request.args.get('date'):
        date = request.args.get('date')
        date = datetime.strptime(date, '%Y-%m-%d')
        date = SP_TZ.localize(date)
        logging.info('Request date %s', date)
    elif date:
        date = date['value']
        date = date.astimezone(SP_TZ)
        logging.info('Datastore date %s', date)
    else:
        date = datetime.now(SP_TZ) + timedelta(-1)
        logging.info('Now date %s', date)
    session_entry = datastore.Entity(key)
    session_entry.update({'value': date.astimezone(UTC)})
    client.put(session_entry)

    key = client.key('Session', 'filter_unprocessed')
    filter_unprocessed = client.get(key)
    if request.args.get('filter_unprocessed'):
        filter_unprocessed = True
    elif not filter_unprocessed:
        filter_unprocessed = False
    session_entry = datastore.Entity(key)
    session_entry.update({'value': filter_unprocessed})
    client.put(session_entry)

    q = client.query(kind='ProcessorLog')
    t1 = date.replace(hour=0, minute=0, second=0, microsecond=0)
    t2 = date.replace(hour=23, minute=59, second=59, microsecond=999999)
    t1 = t1.astimezone(UTC)
    t2 = t2.astimezone(UTC)
    logging.info('time %s %s %s', date, t1, t2)
    q.add_filter('time', '>=', t1)
    q.add_filter('time', '<=', t2)

    logs = list(q.fetch())
    for log in logs:
        log['time'] = (
            log['time'].astimezone(SP_TZ).strftime('%Y-%m-%d %H:%M:%S.%f')
        )

    if filter_unprocessed:
        logs = [l for l in logs if l.get('error', '') != 'Processor not found']

    logs.sort(
        key=lambda x: (x.get('error', ''), x.get('file_refdate', '')),
        reverse=False,
    )

    return render_template(
        'processor-log.html',
        page_name='processor-log',
        date=date,
        length=len(logs),
        logs=logs,
        filter_unprocessed=filter_unprocessed,
    )


@app.route('/historical-processor-log')
def historical_processor_log():
    q = client.query(kind='ProcessorLog')
    q.projection = ['processor_name']
    q.distinct_on = ['processor_name']
    q.order = ['processor_name']
    processor_names = [n['processor_name'] for n in q.fetch()]

    key = client.key('Session', 'processor_name')
    processor_name = client.get(key)
    if request.args.get('processor_name'):
        processor_name = request.args.get('processor_name')
    elif processor_name:
        processor_name = processor_name['value']
    else:
        processor_name = processor_names[0]
    session_entry = datastore.Entity(key)
    session_entry.update({'value': processor_name})
    client.put(session_entry)

    q = client.query(kind='ProcessorLog')
    q.add_filter('processor_name', '=', processor_name)
    logs = list(q.fetch())
    for log in logs:
        if log.get('time'):
            log['time'] = (
                log['time'].astimezone(SP_TZ).strftime('%Y-%m-%d %H:%M:%S.%f')
            )
        else:
            log['time'] = None

    logs.sort(key=lambda x: x.get('time', ''), reverse=True)

    return render_template(
        'historical-processor-log.html',
        page_name='historical-processor-log',
        processor_name=processor_name,
        processor_names=processor_names,
        length=len(logs),
        logs=logs,
    )


@app.route('/processor-log-calendar-check')
def processor_log_calendar_check():
    q = client.query(kind='ProcessorLog')
    logs = list(q.fetch())
    cal = bizdays.Calendar.load('ANBIMA')

    report = []
    for k, g in groupby(logs, lambda x: x['processor_name']):
        print(k)
        refdates = [log.get('file_refdate', '') for log in g]
        refdates = [d for d in refdates if d != '']
        if len(refdates) == 0:
            continue
        try:
            dates = [
                d.strftime('%Y-%m-%d')
                for d in cal.seq(min(refdates), max(refdates))
            ]
        except:
            continue
        missing_dates = [d for d in dates if d not in refdates]
        report.append(
            dict(missing_dates=missing_dates, count=len(missing_dates), name=k)
        )

    return render_template(
        'processor-log-calendar-check.html',
        page_name='processor-log-calendar-check',
        report=report,
    )


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
