"""
Processador de dados financeiros baixados.

Este módulo implementa o processamento e transformação dos dados
financeiros baixados, aplicando ETLs específicos para cada fonte.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

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

# Carregar variáveis de ambiente
load_dotenv()


class DataProcessor:
    """Classe responsável pelo processamento de dados financeiros."""
    
    def __init__(self, bulk_download_path: str = "downloads_bulk"):
        """
        Inicializa o processador de dados.
        
        Args:
            bulk_download_path: Caminho para a pasta de downloads em lote
        """
        self.bulk_download_path = Path(bulk_download_path)
        self.reference_date = datetime.now().date()
        self.logger = self._setup_logging()
        
        # Mapeamento de processadores por tipo de arquivo
        self.processor_mapping = self._setup_processor_mapping()
    
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
    
    def _setup_processor_mapping(self) -> Dict[str, Dict[str, callable]]:
        """
        Configura o mapeamento de processadores por fonte e tipo de arquivo.
        
        Returns:
            Dicionário com mapeamento de processadores
        """
        return {
            "anbima": {
                "_anbima_ima_completo.txt": self._process_anbima_ima_completo,
                "_anbima_mercado_secundario_debentures.txt": 
                    self._process_anbima_mercado_secundario_debentures,
                "_anbima_mercado_secundario_titulos_publicos.txt": 
                    self._process_anbima_mercado_secundario_titulos_publicos,
            },
            "b3": {
                "_b3_capital_social_empresas.json": 
                    self._process_b3_capital_social_empresas,
                "_b3_instrumentos_listados.csv": 
                    self._process_b3_instrumentos_listados,
                "_b3_carteira_teorica_": self._process_b3_carteira_teorica,
            }
        }
    
    def get_files_to_process(self) -> List[Path]:
        """
        Obtém lista de arquivos para processamento.
        
        Returns:
            Lista de caminhos para arquivos a serem processados
            
        Raises:
            FileNotFoundError: Se o diretório de downloads não existir
        """
        if not self.bulk_download_path.exists():
            raise FileNotFoundError(
                f"Diretório de downloads não encontrado: {self.bulk_download_path}"
            )
        
        files = list(self.bulk_download_path.glob("*"))
        self.logger.info(f"Encontrados {len(files)} arquivos para processamento")
        return files
    
    def _process_anbima_ima_completo(self) -> None:
        """Processa dados completos do IMA da ANBIMA."""
        try:
            self.logger.info("Processando IMA composição da carteira")
            ima_composicao_carteira.main()
            
            self.logger.info("Processando IMA totais")
            ima_totais.main()
            
        except Exception as e:
            self.logger.error(f"Erro ao processar IMA completo: {e}")
            raise
    
    def _process_anbima_mercado_secundario_debentures(self) -> None:
        """Processa dados do mercado secundário de debêntures da ANBIMA."""
        try:
            self.logger.info("Processando mercado secundário de debêntures")
            mercado_secundario_debentures.main()
            
        except Exception as e:
            self.logger.error(
                f"Erro ao processar mercado secundário de debêntures: {e}"
            )
            raise
    
    def _process_anbima_mercado_secundario_titulos_publicos(self) -> None:
        """Processa dados do mercado secundário de títulos públicos da ANBIMA."""
        try:
            self.logger.info("Processando mercado secundário de títulos públicos")
            mercado_secundario_titulos_publicos.main()
            
        except Exception as e:
            self.logger.error(
                f"Erro ao processar mercado secundário de títulos públicos: {e}"
            )
            raise
    
    def _process_b3_capital_social_empresas(self) -> None:
        """Processa dados de capital social das empresas da B3."""
        try:
            self.logger.info("Processando capital social das empresas")
            capital_social_empresas.main()
            
        except Exception as e:
            self.logger.error(f"Erro ao processar capital social: {e}")
            raise
    
    def _process_b3_instrumentos_listados(self) -> None:
        """Processa dados de instrumentos listados da B3."""
        try:
            self.logger.info("Processando instrumentos listados")
            instrumentos_listados.main()
            
        except Exception as e:
            self.logger.error(f"Erro ao processar instrumentos listados: {e}")
            raise
    
    def _process_b3_carteira_teorica(self, file_path: Path) -> None:
        """
        Processa dados de carteira teórica da B3.
        
        Args:
            file_path: Caminho para o arquivo de carteira teórica
        """
        try:
            self.logger.info(f"Processando carteira teórica: {file_path.name}")
            carteiras_teoricas.main(str(file_path), self.reference_date)
            
        except Exception as e:
            self.logger.error(f"Erro ao processar carteira teórica: {e}")
            # Não re-raise para permitir continuar com outros arquivos
    
    def process_file(self, file_path: Path) -> bool:
        """
        Processa um arquivo específico.
        
        Args:
            file_path: Caminho para o arquivo a ser processado
            
        Returns:
            True se o processamento foi bem-sucedido, False caso contrário
        """
        try:
            file_name = file_path.name
            self.logger.info(f"Processando arquivo: {file_name}")
            
            # Extrair informações do nome do arquivo
            parts = file_name.split("_")
            if len(parts) < 2:
                self.logger.warning(f"Formato de nome inválido: {file_name}")
                return False
            
            file_date = parts[0]
            processor_name = parts[1]
            
            self.logger.info(f"Data: {file_date}, Processador: {processor_name}")
            
            # Verificar se existe processador para esta fonte
            if processor_name not in self.processor_mapping:
                self.logger.warning(
                    f"Nenhum processador encontrado para: {processor_name}"
                )
                return False
            
            # Encontrar e executar o processador apropriado
            processors = self.processor_mapping[processor_name]
            processed = False
            
            for pattern, processor_func in processors.items():
                if pattern in file_name:
                    self.logger.info(f"Executando processador: {pattern}")
                    
                    if pattern == "_b3_carteira_teorica_":
                        processor_func(file_path)
                    else:
                        processor_func()
                    
                    processed = True
                    break
            
            if not processed:
                self.logger.warning(
                    f"Nenhum processador específico encontrado para: {file_name}"
                )
                return False
            
            self.logger.info(f"Arquivo {file_name} processado com sucesso")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao processar {file_path.name}: {e}")
            return False
    
    def run(self) -> None:
        """
        Executa o processo completo de processamento.
        """
        self.logger.info("Iniciando processamento de dados")
        
        try:
            # Obter arquivos para processamento
            files = self.get_files_to_process()
            
            # Processar cada arquivo
            successful_processes = 0
            total_files = len(files)
            
            for file_path in files:
                if self.process_file(file_path):
                    successful_processes += 1
            
            # Resumo final
            self.logger.info(
                f"Processamento concluído: {successful_processes}/{total_files} "
                f"arquivos processados com sucesso"
            )
            
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise


def main() -> None:
    """Função principal para execução do processador."""
    processor = DataProcessor()
    processor.run()


if __name__ == "__main__":
    main()
