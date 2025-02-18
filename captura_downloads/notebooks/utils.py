import os

from dotenv import load_dotenv
from sqlalchemy import create_engine


def get_engine():
    # Carregar as variáveis de ambiente do arquivo .env
    load_dotenv()

    # Obter as informações do banco de dados das variáveis de ambiente
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_SERVER')
    db_name = os.getenv('DB_DATABASE')

    # Criar a URL de conexão com o banco de dados
    database_url = f'mssql+pyodbc://{user}:{password}@{host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server'

    # Criar e retornar a engine
    return create_engine(database_url)
