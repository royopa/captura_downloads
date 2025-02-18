import os
from datetime import datetime
from pathlib import Path

import fire
from dotenv import load_dotenv

from captura_downloads.etls.anbima import ima_composicao_carteira, ima_totais

load_dotenv()


def main():
    data_referencia = datetime.now().date()
    download_path = os.path.join('downloads_bulk')

    for file_name in os.listdir(download_path):
        file_date = file_name.split('_')[0]
        processor_name = file_name.split('_')[1]
        file_path = os.path.join(download_path, file_name)
        print(file_date, processor_name, file_name)

        if processor_name == 'anbima':
            print(f'Processor {processor_name} is running')
            if (
                f'{data_referencia.strftime("%Y%m%d")}_anbima_ima_completo'
                in file_name
            ):
                ima_composicao_carteira.main()
                ima_totais.main()
        if processor_name == 'b3':
            print(f'Processor {processor_name} is running')
        if processor_name == 'bacen':
            print(f'Processor {processor_name} is running')
        if processor_name == 'debentures':
            print(f'Processor {processor_name} is running')


if __name__ == '__main__':
    # fire.Fire()
    main()
