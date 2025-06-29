"""
Utilitários para processamento de dados da ANBIMA.

Este módulo fornece funções utilitárias para processamento e carregamento
de dados da ANBIMA (Associação Brasileira das Entidades dos Mercados
Financeiro e de Capitais) no banco de dados.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.types import Date, DateTime, Float, Integer, String

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()


class AnbimaDataProcessor:
    """Classe para processamento de dados da ANBIMA."""
    
    def __init__(self):
        """Inicializa o processador de dados ANBIMA."""
        self.logger = self._setup_logging()
        self.engine = self._get_engine()
    
    def _setup_logging(self) -> logging.Logger:
        """
        Configura o sistema de logging.
        
        Returns:
            Logger configurado
        """
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _get_engine(self):
        """
        Cria e retorna a engine de conexão com o banco de dados.
        
        Returns:
            SQLAlchemy engine configurado
            
        Raises:
            ValueError: Se variáveis de ambiente obrigatórias não estiverem definidas
        """
        # Obter as informações do banco de dados das variáveis de ambiente
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_SERVER")
        db_name = os.getenv("DB_DATABASE")
        
        # Validar variáveis obrigatórias
        missing_vars = []
        if not user:
            missing_vars.append("DB_USER")
        if not password:
            missing_vars.append("DB_PASSWORD")
        if not host:
            missing_vars.append("DB_SERVER")
        if not db_name:
            missing_vars.append("DB_DATABASE")
        
        if missing_vars:
            raise ValueError(
                f"Variáveis de ambiente obrigatórias não definidas: {', '.join(missing_vars)}"
            )
        
        # Criar a URL de conexão com o banco de dados
        database_url = (
            f"mssql+pyodbc://{user}:{password}@{host}/{db_name}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
        )
        
        self.logger.info(f"Conectando ao banco de dados: {host}/{db_name}")
        
        # Criar e retornar a engine
        return create_engine(database_url)
    
    def convert_columns_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte tipos de dados das colunas baseado em prefixos.
        
        Args:
            df: DataFrame pandas a ser processado
            
        Returns:
            DataFrame com tipos de dados convertidos
        """
        try:
            self.logger.info("Convertendo tipos de dados das colunas")
            
            for column_name in df.columns:
                # Converter colunas de percentual para numérico
                if column_name.startswith("PC_"):
                    df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
                    self.logger.debug(f"Coluna {column_name} convertida para numérico")
                
                # Converter colunas de valor para numérico
                if column_name.startswith("VR_"):
                    df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
                    self.logger.debug(f"Coluna {column_name} convertida para numérico")
                
                # Converter colunas numéricas para numérico
                if column_name.startswith("NU_"):
                    df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
                    self.logger.debug(f"Coluna {column_name} convertida para numérico")
                
                # Converter colunas de data para datetime
                if column_name.startswith("DT_"):
                    df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
                    self.logger.debug(f"Coluna {column_name} convertida para datetime")
            
            self.logger.info("Conversão de tipos de dados concluída")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao converter tipos de dados: {e}")
            raise
    
    def get_sqlalchemy_dtypes(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Obtém mapeamento de tipos SQLAlchemy para as colunas do DataFrame.
        
        Args:
            df: DataFrame pandas
            
        Returns:
            Dicionário com mapeamento de tipos SQLAlchemy
        """
        try:
            self.logger.info("Gerando mapeamento de tipos SQLAlchemy")
            
            dtype_mapping = {
                "int64": Integer,
                "float64": Float,
                "object": String,
                "datetime64[ns]": DateTime,
            }
            
            column_dtypes = {}
            for column_name in df.columns:
                dtype = str(df[column_name].dtype)
                sqlalchemy_dtype = dtype_mapping.get(dtype, String)
                
                # Aplicar regras específicas baseadas em prefixos
                if column_name.startswith("PC_"):
                    sqlalchemy_dtype = Float
                if column_name.startswith("VR_"):
                    sqlalchemy_dtype = Float
                if column_name.startswith("NU_"):
                    sqlalchemy_dtype = Float
                if column_name.startswith("DT_"):
                    sqlalchemy_dtype = Date
                
                column_dtypes[column_name] = sqlalchemy_dtype
            
            self.logger.info(f"Mapeamento gerado para {len(column_dtypes)} colunas")
            return column_dtypes
            
        except Exception as e:
            self.logger.error(f"Erro ao gerar mapeamento de tipos: {e}")
            raise
    
    def load_with_bcp(self, file_path: str) -> bool:
        """
        Carrega dados usando o utilitário BCP (Bulk Copy Program).
        
        Args:
            file_path: Caminho para o arquivo a ser carregado
            
        Returns:
            True se o carregamento foi bem-sucedido, False caso contrário
            
        Raises:
            FileNotFoundError: Se o arquivo não for encontrado
            ValueError: Se variáveis de ambiente não estiverem definidas
        """
        try:
            file_path_obj = Path(file_path)
            
            if not file_path_obj.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
            # Extrair informações do nome do arquivo
            file_name = file_path_obj.name
            
            # Validar formato do nome do arquivo
            if "_anbima_" not in file_name:
                raise ValueError(f"Formato de nome inválido: {file_name}")
            
            # Extrair schema e nome da tabela
            parts = file_name.split("_anbima_")
            if len(parts) != 2:
                raise ValueError(f"Formato de nome inválido: {file_name}")
            
            schema = "anbima"  # Schema padrão para dados ANBIMA
            table_name = parts[1].replace(".csv", "").replace("_utf8", "")
            
            # Obter configurações do banco
            server = os.getenv("DB_SERVER")
            database = os.getenv("DB_DATABASE")
            user = os.getenv("DB_USER")
            password = os.getenv("DB_PASSWORD")
            
            if not all([server, database, user, password]):
                raise ValueError("Variáveis de ambiente do banco não definidas")
            
            full_table_name = f"{database}.{schema}.{table_name}"
            
            # Construir comando BCP
            command = (
                f'bcp {full_table_name} in "{file_path}" '
                f'-C 65001 -c -t";" -r"\\n" -F 2 '
                f'-S {server} -U {user} -P {password}'
            )
            
            self.logger.info(f"Executando BCP para tabela: {full_table_name}")
            self.logger.debug(f"Comando: {command}")
            
            # Executar comando
            result = os.system(command)
            
            if result == 0:
                self.logger.info(f"Dados carregados com sucesso na tabela {full_table_name}")
                return True
            else:
                self.logger.error(f"Erro no BCP (código: {result}) para tabela {full_table_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados com BCP: {e}")
            return False
    
    def load_to_database(self, df: pd.DataFrame, table_name: str, 
                        schema: str = "anbima", if_exists: str = "replace") -> bool:
        """
        Carrega DataFrame para o banco de dados usando SQLAlchemy.
        
        Args:
            df: DataFrame pandas a ser carregado
            table_name: Nome da tabela de destino
            schema: Schema do banco de dados (padrão: anbima)
            if_exists: Comportamento se a tabela existir (replace, append, fail)
            
        Returns:
            True se o carregamento foi bem-sucedido, False caso contrário
        """
        try:
            self.logger.info(f"Carregando dados para tabela: {schema}.{table_name}")
            
            # Converter tipos de dados
            df = self.convert_columns_dtypes(df)
            
            # Obter tipos SQLAlchemy
            dtypes = self.get_sqlalchemy_dtypes(df)
            
            # Carregar para o banco
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                dtype=dtypes,
                method='multi'
            )
            
            self.logger.info(f"Dados carregados com sucesso: {len(df)} linhas")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados para banco: {e}")
            return False


# Funções de conveniência para compatibilidade com código existente
def get_engine():
    """
    Obtém engine de conexão com o banco de dados.
    
    Returns:
        SQLAlchemy engine configurado
    """
    processor = AnbimaDataProcessor()
    return processor.engine


def convert_columns_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte tipos de dados das colunas baseado em prefixos.
    
    Args:
        df: DataFrame pandas a ser processado
        
    Returns:
        DataFrame com tipos de dados convertidos
    """
    processor = AnbimaDataProcessor()
    return processor.convert_columns_dtypes(df)


def get_sqlalchemy_dtypes(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Obtém mapeamento de tipos SQLAlchemy para as colunas do DataFrame.
    
    Args:
        df: DataFrame pandas
        
    Returns:
        Dicionário com mapeamento de tipos SQLAlchemy
    """
    processor = AnbimaDataProcessor()
    return processor.get_sqlalchemy_dtypes(df)


def load_with_bcp(file_path: str) -> bool:
    """
    Carrega dados usando o utilitário BCP (Bulk Copy Program).
    
    Args:
        file_path: Caminho para o arquivo a ser carregado
        
    Returns:
        True se o carregamento foi bem-sucedido, False caso contrário
    """
    processor = AnbimaDataProcessor()
    return processor.load_with_bcp(file_path)
