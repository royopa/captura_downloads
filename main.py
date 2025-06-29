"""
Interface principal para execução de diferentes tipos de download.

Este módulo fornece uma interface unificada para executar diferentes
métodos de download de dados financeiros.
"""

import logging
import subprocess
from typing import Optional

import fire

import download_files
import download_files_kyd
import download_files_selenium
import processor


class DownloadManager:
    """Gerenciador principal para diferentes tipos de download."""
    
    def __init__(self):
        """Inicializa o gerenciador de downloads."""
        self.logger = self._setup_logging()
    
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
    
    def kyd(self) -> None:
        """
        Executa downloads usando o framework KYD.
        
        Este método utiliza o framework Know Your Data (KYD) para
        download e processamento de dados estruturados.
        """
        try:
            self.logger.info("Iniciando download KYD")
            print("Executando download KYD...")
            download_files_kyd.main()
            self.logger.info("Download KYD concluído com sucesso")
        except Exception as e:
            self.logger.error(f"Erro no download KYD: {e}")
            raise
    
    def legacy(self) -> None:
        """
        Executa downloads usando o sistema legado.
        
        Este método utiliza o sistema de download original com
        configurações YAML e múltiplas fontes de dados.
        """
        try:
            self.logger.info("Iniciando download legacy")
            print("Executando download legacy...")
            download_files.main()
            self.logger.info("Download legacy concluído com sucesso")
        except Exception as e:
            self.logger.error(f"Erro no download legacy: {e}")
            raise
    
    def selenium(self) -> None:
        """
        Executa downloads usando automação Selenium.
        
        Este método utiliza automação web com Selenium para
        sites que requerem interação com navegador.
        """
        try:
            self.logger.info("Iniciando download Selenium")
            print("Executando download Selenium...")
            download_files_selenium.main()
            self.logger.info("Download Selenium concluído com sucesso")
        except Exception as e:
            self.logger.error(f"Erro no download Selenium: {e}")
            raise
    
    def powershell(self) -> None:
        """
        Executa downloads usando scripts PowerShell.
        
        Este método executa scripts PowerShell para download
        de dados financeiros.
        """
        try:
            self.logger.info("Iniciando download PowerShell")
            print("Executando download PowerShell...")
            
            result = subprocess.run(
                ["pwsh", "-File", "./download_files.ps1"], 
                check=True,
                capture_output=True,
                text=True
            )
            
            if result.stdout:
                print("Saída do PowerShell:")
                print(result.stdout)
            
            self.logger.info("Download PowerShell concluído com sucesso")
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Erro no script PowerShell: {e}"
            if e.stderr:
                error_msg += f"\nStderr: {e.stderr}"
            self.logger.error(error_msg)
            raise
        except FileNotFoundError:
            self.logger.error("PowerShell não encontrado no sistema")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado no download PowerShell: {e}")
            raise
    
    def all(self) -> None:
        """
        Executa todos os tipos de download disponíveis.
        
        Este método executa sequencialmente todos os métodos
        de download implementados.
        """
        try:
            self.logger.info("Iniciando todos os downloads")
            print("Executando todos os downloads...")
            
            # Executar downloads em sequência
            self.kyd()
            self.legacy()
            # self.selenium()  # Comentado para evitar execução automática
            self.powershell()
            
            self.logger.info("Todos os downloads concluídos com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro durante execução de todos os downloads: {e}")
            raise
    
    def run_processor(self) -> None:
        """
        Executa o processador de dados.
        
        Este método executa o processamento dos dados baixados,
        aplicando transformações e carregando no banco de dados.
        """
        try:
            self.logger.info("Iniciando processamento de dados")
            print("Executando processador...")
            processor.main()
            self.logger.info("Processamento concluído com sucesso")
        except Exception as e:
            self.logger.error(f"Erro no processamento: {e}")
            raise


def main() -> None:
    """
    Função principal que inicializa o gerenciador de downloads.
    
    Esta função utiliza a biblioteca Fire para criar uma interface
    de linha de comando automática baseada nos métodos da classe.
    """
    manager = DownloadManager()
    fire.Fire(manager)


if __name__ == "__main__":
    main()
