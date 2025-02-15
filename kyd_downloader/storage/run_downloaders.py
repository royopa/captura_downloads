import requests

dates = [
    # "2016-05-02",
    # "2016-05-03",
    # "2016-05-04",
    # "2016-05-06",
    # "2016-05-12",
    # "2016-06-30",
    # "2017-05-02",
    '2022-02-23',
]

name = 'indexreport'
for date in dates:
    url = f'https://kyd-storage-001.rj.r.appspot.com/reprocess?refdate={date}&name={name}'
    res = requests.get(url)
    print(res.status_code)
