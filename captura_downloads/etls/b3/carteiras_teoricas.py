import os
from datetime import datetime
from pathlib import Path

import bizdays
import pandas as pd
from dotenv import load_dotenv
from termcolor import colored, cprint

from .utils import (
    convert_columns_dtypes,
    get_engine,
    get_sqlalchemy_dtypes,
    load_with_bcp,
)

load_dotenv()


def main(file_path, data_referencia):
    data_referencia = datetime.now()
    data_arquivo = data_referencia.strftime('%Y%m%d')
    project_root_folder = Path(__file__).resolve().parents[3]
    folder_path = Path(file_path).parent
    file_name = Path(file_path).name

    if not os.path.exists(file_path):
        raise FileNotFoundError(f'File not found: {file_path}')

    file_path_extracted = extract(file_path)
    if file_path_extracted is None or not os.path.exists(file_path_extracted):
        raise FileNotFoundError(
            f'File extracted not found: {file_path_extracted}'
        )

    file_path_transformed = transform(file_path_extracted)
    if file_path_transformed is None or not os.path.exists(
        file_path_transformed
    ):
        raise FileNotFoundError(
            f'File transformed not found: {file_path_transformed}'
        )

    load(file_path_transformed)


def extract(file_path):
    folder_path = Path(file_path).parent
    file_name = Path(file_path).name

    if not file_name.endswith('.base64'):
        return

    if file_name.endswith('_utf8.base64'):
        return

    carteira_name = file_name.split('_')[-1].split('.')[0]
    file_name_out = file_name.replace('.base64', '_utf8.base64')

    print(f'Processing file {file_name} in folder {folder_path}')
    print(f'Carteira {carteira_name}')

    layout_path = os.path.join(folder_path, file_name_out)

    with open(layout_path, 'w', encoding='utf-8') as layout_file:
        with open(file_path, 'r', encoding='latin1') as file:
            for line in file:
                # if last element for line = ';', remove it
                if line.startswith('Quantidade Teórica Total'):
                    continue
                if line.startswith('Redutor'):
                    continue
                if line.startswith('Código;'):
                    line = 'Código;Ação;Tipo;Qtde. Teórica;Part. (%);' + '\n'
                layout_file.write(line)

    cprint(
        f'Success. Generated file {file_name_out} in folder {folder_path}',
        'green',
    )

    return layout_path


def transform(file_path):
    file_name = Path(file_path).name

    if '.csv_utf8.base64' not in file_name:
        print('File not expected')
        return

    carteira_name = file_name.split('.csv')[0].split('_')[4]

    if file_path.endswith('.base64') is False:
        print('File not expected')
        return

    data_atual = datetime.now()
    cal = bizdays.Calendar.load('B3')
    data_referencia = cal.offset(data_atual, 0)

    df = pd.read_csv(
        file_path,
        sep=';',
        encoding='utf-8',
        skiprows=1,
        decimal=',',
        thousands='.',
    )

    del df['Unnamed: 5']

    df.insert(0, 'DT_REF', data_referencia)
    df.insert(0, 'CO_CARTEIRA_TEORICA', carteira_name.upper())

    a_renomear = {
        'Código': 'CO_ACAO',
        'Ação': 'NO_EMPRESA',
        'Tipo': 'DE_TIPO',
        'Qtde. Teórica': 'NU_QTDE_TEORICA',
        'Part. (%)': 'PC_PARTICIPACAO',
    }

    df = df.rename(columns=a_renomear)

    df['DT_REF'] = pd.to_datetime(df['DT_REF'])

    df = convert_columns_dtypes(df)

    file_name = Path(file_path).name
    schema = file_name.split('_')[1]
    table_name = file_name.split('_b3_')[1].split('.')[0]
    table_name = table_name.replace('.csv', '')
    table_name = table_name.replace('.json', '')
    table_name = table_name.replace('.txt', '')
    table_name = table_name.replace('.csv', '')
    table_name = table_name.replace('_utf8', '')

    print(f'Creating table {schema}.{table_name} in database...', end=' ')
    df.head(0).to_sql(
        table_name,
        con=get_engine(),
        if_exists='replace',
        index=False,
        schema=schema,
        dtype=get_sqlalchemy_dtypes(df),
    )
    cprint('Ok', 'green')

    file_path_out = file_path.replace('.csv_utf8.base64', '.csv')
    print(f'Reformatting csv file to bcp import... {file_path_out}', end=' ')

    df.to_csv(
        file_path_out,
        sep=';',
        index=False,
        encoding='utf-8',
        lineterminator='\n',
    )
    cprint('Ok', 'green')

    return file_path_out


def load(file_path):
    return load_with_bcp(file_path)


if __name__ == '__main__':
    main()
