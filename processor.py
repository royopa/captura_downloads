import os
from datetime import datetime

from dotenv import load_dotenv

from captura_downloads.etls.anbima import (
    ima_composicao_carteira,
    ima_totais,
    mercado_secundario_debentures,
    mercado_secundario_titulos_publicos,
)
from captura_downloads.etls.b3 import (
    capital_social_empresas,
    carteiras_teoricas,
    instrumentos_listados,
)

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
            if '_anbima_ima_completo.txt' in file_name:
                ima_composicao_carteira.main()
                ima_totais.main()
            if '_anbima_mercado_secundario_debentures.txt' in file_name:
                mercado_secundario_debentures.main()
            if '_anbima_mercado_secundario_titulos_publicos.txt' in file_name:
                mercado_secundario_titulos_publicos.main()
        if processor_name == 'b3':
            print(f'Processor {processor_name} is running')
            if '_b3_capital_social_empresas.json' in file_name:
                capital_social_empresas.main()
            if '_b3_instrumentos_listados.csv' in file_name:
                instrumentos_listados.main()
            if '_b3_carteira_teorica_' in file_name:
                try:
                    carteiras_teoricas.main(file_path, data_referencia)
                except Exception as e:
                    continue
        """
        if processor_name == 'bacen':
            print(f'Processor {processor_name} is running')
        if processor_name == 'debentures':
            print(f'Processor {processor_name} is running')
        """


if __name__ == '__main__':
    main()
