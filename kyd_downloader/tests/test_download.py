import datetime
import logging
import sys

import kyd.data.downloaders as dw
from kyd.data.logs import save_download_logs

sys.path.append("../functions/")


logging.basicConfig(level=logging.DEBUG)

# logging.basicConfig(level=logging.INFO)


dt = datetime.date(2023, 2, 24)

# with open('../config/td_ntnb.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/vna_anbima.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/titpub_anbima.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/pricereport.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cdi.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cadinstr.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/btc.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cvm_fundos_info_cadastral.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cvm_fundos_informe_diario_0.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cotahist.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

with open("../config/b3-otc-trade-information-consolidated.json") as fp:
    config = fp.read()
    res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
    save_download_logs(res)
