import logging
import os
import sys
from datetime import date, datetime

import kyd.data.downloaders as dw
from kyd.data.logs import save_download_logs


def main():
    sys.path.append('./kyd_downloader/functions/')

    # logging.basicConfig(level=logging.DEBUG)

    logging.basicConfig(level=logging.INFO)

    dt = datetime.today()
    dt = date(2025, 2, 14)

    config_path = './kyd_downloader/config/'

    for file_name in os.listdir(config_path):
        with open(os.path.join(config_path, file_name)) as fp:
            config = fp.read()
            res = dw.download_by_config(
                config, dw.save_file_to_local_download_folder, refdate=dt
            )
            # save_download_logs(res)
            print(res)


if __name__ == '__main__':
    main()
