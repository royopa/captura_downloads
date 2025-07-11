import json
import logging
from datetime import date, datetime, timedelta

from google.cloud import storage

from kyd.data.downloaders import download_by_config
from main import save_file_to_output_bucket

# import bizdays


logging.basicConfig(level=logging.DEBUG)

# cal = bizdays.Calendar(weekdays=('sat', 'sun'))

dt = date(2021, 7, 16)
with open("../config/vna_anbima.json") as fp:
    content = fp.read()
    download_by_config(content, save_file_to_output_bucket, dt)
# with open('../config/pricereport.json') as fp:
#     content = fp.read()
#     download_by_config(content, save_file_to_output_bucket, dt)
# with open('../config/cadinstrindic.json') as fp:
#     content = fp.read()
#     download_by_config(content, save_file_to_output_bucket, dt)
# with open('../config/cadinstr.json') as fp:
#     content = fp.read()
#     download_by_config(content, save_file_to_output_bucket, dt)

# dt = date(2020, 7, 7)
# with open('../config/fpr.json') as fp:
#     content = fp.read()
#     download_by_config(content, save_file_to_output_bucket, dt)
# with open('../config/riskformulas.json') as fp:
#     content = fp.read()
#     download_by_config(content, save_file_to_output_bucket, dt)
# with open('../config/indexreport.json') as fp:
#     content = fp.read()
#     dt = date(2020, 7, 6)
#     download_by_config(content, save_file_to_output_bucket, dt)
# with open('../config/cvm_fundos_informe_diario_1.json') as fp:
#     content = fp.read()
#     config = json.loads(content)
#     config.pop('download_weekdays', None)
#     download_by_config(json.dumps(config), save_file_to_output_bucket, dt)
