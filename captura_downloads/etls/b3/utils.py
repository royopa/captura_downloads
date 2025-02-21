import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.types import Date, DateTime, Float, Integer, String

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()


def get_engine():
    # Obter as informações do banco de dados das variáveis de ambiente
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_SERVER')
    db_name = os.getenv('DB_DATABASE')

    # Criar a URL de conexão com o banco de dados
    database_url = f'mssql+pyodbc://{user}:{password}@{host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server'

    # Criar e retornar a engine
    return create_engine(database_url)


def convert_columns_dtypes(df):
    for column_name in df.columns:
        if column_name.startswith('PC_') or column_name.startswith('VR_'):
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
        if column_name.startswith('NU_'):
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
        if column_name.startswith('DT_'):
            df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
    return df


def get_sqlalchemy_dtypes(df):
    dtype_mapping = {
        'int64': Integer,
        'float64': Float,
        'object': String,
        'datetime64[ns]': DateTime,
    }

    column_dtypes = {}
    for column_name in df.columns:
        dtype = str(df[column_name].dtype)
        sqlalchemy_dtype = dtype_mapping.get(dtype, String)

        if column_name.startswith('PC_'):
            sqlalchemy_dtype = Float
        if column_name.startswith('VR_'):
            sqlalchemy_dtype = Float
        if column_name.startswith('NU_'):
            sqlalchemy_dtype = Float
        if column_name.startswith('DT_'):
            sqlalchemy_dtype = Date

        column_dtypes[column_name] = sqlalchemy_dtype

    return column_dtypes


def load_with_bcp(file_path):
    file_name = Path(file_path).name
    if not file_name.endswith('.csv'):
        return

    schema = file_name.split('_')[1]
    table_name = file_name.split('_b3_')[1].split('.')[0]
    table_name = table_name.replace('.csv', '')
    table_name = table_name.replace('.json', '')
    table_name = table_name.replace('.txt', '')
    table_name = table_name.replace('.csv', '')
    table_name = table_name.replace('_utf8', '')

    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')

    full_table_name = f'{database}.{schema}.{table_name}'
    command = f'bcp {full_table_name} in "{file_path}" -C 65001 -c -t";" -r"\\n" -F 2 -S {server} -U {user} -P {password}'
    print(command)

    print(f'Importing data to table {full_table_name}...')
    os.system(command)
