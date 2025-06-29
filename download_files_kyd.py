"""
Download de arquivos usando o framework KYD (Know Your Data).

Este módulo implementa o download de dados financeiros utilizando o framework
KYD para processamento e armazenamento de dados estruturados.
"""

import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import kyd.data.downloaders as dw
from kyd.data.logs import save_download_logs


class KYDDownloader:
    """Classe responsável pelo download de dados usando o framework KYD."""
    
    def __init__(self, config_path: str = "./kyd_downloader/config/"):
        """
        Inicializa o downloader KYD.
        
        Args:
            config_path: Caminho para o diretório de configurações
        """
        self.config_path = Path(config_path)
        self.logger = self._setup_logging()
        
        # Adicionar caminho do KYD ao sys.path
        kyd_functions_path = "./kyd_downloader/functions/"
        if kyd_functions_path not in sys.path:
            sys.path.append(kyd_functions_path)
    
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
    
    def get_config_files(self) -> List[Path]:
        """
        Obtém lista de arquivos de configuração.
        
        Returns:
            Lista de caminhos para arquivos de configuração
            
        Raises:
            FileNotFoundError: Se o diretório de configuração não existir
        """
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Diretório de configuração não encontrado: {self.config_path}"
            )
        
        config_files = list(self.config_path.glob("*"))
        self.logger.info(f"Encontrados {len(config_files)} arquivos de configuração")
        return config_files
    
    def process_config_file(self, config_file: Path, reference_date: date) -> Dict[str, Any]:
        """
        Processa um arquivo de configuração específico.
        
        Args:
            config_file: Caminho para o arquivo de configuração
            reference_date: Data de referência para o download
            
        Returns:
            Resultado do processamento
            
        Raises:
            FileNotFoundError: Se o arquivo de configuração não existir
            Exception: Se houver erro no processamento
        """
        try:
            self.logger.info(f"Processando arquivo: {config_file.name}")
            
            with open(config_file, 'r', encoding='utf-8') as fp:
                config_content = fp.read()
            
            result = dw.download_by_config(
                config_content, 
                dw.save_file_to_local_download_folder, 
                refdate=reference_date
            )
            
            self.logger.info(f"Arquivo {config_file.name} processado com sucesso")
            return result
            
        except FileNotFoundError:
            self.logger.error(f"Arquivo de configuração não encontrado: {config_file}")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao processar {config_file.name}: {e}")
            raise
    
    def run(self, reference_date: Optional[date] = None) -> None:
        """
        Executa o processo completo de download KYD.
        
        Args:
            reference_date: Data de referência para o download. 
                           Se None, usa a data atual
        """
        if reference_date is None:
            reference_date = datetime.today().date()
        
        self.logger.info(f"Iniciando download KYD para data: {reference_date}")
        
        try:
            # Obter arquivos de configuração
            config_files = self.get_config_files()
            
            # Processar cada arquivo de configuração
            successful_downloads = 0
            total_files = len(config_files)
            
            for config_file in config_files:
                try:
                    result = self.process_config_file(config_file, reference_date)
                    print(f"Resultado para {config_file.name}: {result}")
                    successful_downloads += 1
                    
                    # Salvar logs de download (opcional)
                    # save_download_logs(result)
                    
                except Exception as e:
                    self.logger.error(f"Falha ao processar {config_file.name}: {e}")
                    continue
            
            # Resumo final
            self.logger.info(
                f"Download KYD concluído: {successful_downloads}/{total_files} "
                f"arquivos processados com sucesso"
            )
            
        except Exception as e:
            self.logger.error(f"Erro durante o processo de download KYD: {e}")
            raise


def main() -> None:
    """Função principal para execução do downloader KYD."""
    # Configurar data de referência (pode ser modificada conforme necessário)
    reference_date = date(2025, 2, 14)
    
    downloader = KYDDownloader()
    downloader.run(reference_date)


if __name__ == "__main__":
    main()
