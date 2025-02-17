import os
from datetime import datetime
from pathlib import Path

import fire
from dotenv import load_dotenv

load_dotenv()


def main():
    data_referencia = datetime.now().date()
    download_path = os.path.join(
        'downloads', data_referencia.strftime('%Y-%m-%d')
    )
    for file_name in os.listdir(download_path):
        file_data = file_name.split('_')[0]
        processor_name = file_name.split('_')[1]
        print(file_data, processor_name)
        if processor_name == 'anbima':
            if not file_name.endswith('_anbima_ima_completo.txt'):
                continue
            print(f'Processor {processor_name} is running...')
            file_path_totais, file_path_composicao_carteira = parser_anbima(os.path.join(download_path, file_name))
            load_ima_totais(file_path_totais)
            load_ima_composicao_carteira(file_path_composicao_carteira)

        if processor_name == 'b3':
            print(f'Processor {processor_name} is running')
        if processor_name == 'bacen':
            print(f'Processor {processor_name} is running')
        if processor_name == 'debentures':
            print(f'Processor {processor_name} is running')


def parser_anbima(file_path):
    print(f'Processing file {file_path}...')
    file_name = Path(file_path).name
    if not file_name.endswith('_anbima_ima_completo.txt'):
        return False

    # Define os nomes dos arquivos de saída
    bulk_path = os.path.join('downloads_bulk')
    layout1_path = os.path.join(
        bulk_path, file_name.replace('_ima_completo.txt', '_ima_totais.txt')
    )
    layout2_path = os.path.join(
        bulk_path,
        file_name.replace('_ima_completo.txt', '_ima_composicao_carteira.txt'),
    )

    # Abre os arquivos de saída
    with open(layout1_path, 'w', encoding='utf-8') as layout1_file, open(
        layout2_path, 'w', encoding='utf-8'
    ) as layout2_file:
        # Abre o arquivo de entrada e processa linha por linha
        with open(file_path, 'r', encoding='latin1') as file:
            for line in file:
                if line.startswith('1@TOTAIS'):
                    continue
                if line.startswith('2@COMPOSIÇÃO DE CARTEIRA'):
                    continue
                # Verifica o primeiro caractere da linha para determinar o layout
                if line.startswith('1'):
                    layout1_file.write(line)
                elif line.startswith('2'):
                    layout2_file.write(line)

    return layout1_path, layout2_path


def load_ima_totais(file_path):
    file_path = file_path.replace('.txt', '.csv')
    file_name = Path(file_path).name    
    schema = file_name.split('_')[1]
    table_name = file_name.split('_anbima_')[1].replace('.csv', '')
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')

    full_table_name = f'{database}.{schema}.{table_name}'
    command = f'bcp {full_table_name} in "{file_path}" -C 65001 -c -t";" -r"\\n" -F 2 -S {server} -U {user} -P {password}'
    print(command)

    # Executa o comando bcp
    print(f'Importando dados para a tabela {table_name}...')
    os.system(command)


def load_ima_composicao_carteira(file_path):
    file_path = file_path.replace('.txt', '.csv')
    file_name = Path(file_path).name
    schema = file_name.split('_')[1]
    table_name = file_name.split('_anbima_')[1].replace('.csv', '')
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    
    full_table_name = f'{database}.{schema}.{table_name}'
    command = f'bcp {full_table_name} in "{file_path}" -C 65001 -c -t";" -r"\\n" -F 2 -S {server} -U {user} -P {password}'
    print(command)

    # Executa o comando bcp
    print(f'Importando dados para a tabela {table_name}...')
    os.system(command)


if __name__ == '__main__':
    fire.Fire()
