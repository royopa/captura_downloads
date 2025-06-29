"""
Download de arquivos financeiros de múltiplas fontes.

Este módulo implementa o download de dados financeiros de diversas fontes
brasileiras como ANBIMA, B3, BACEN e outras instituições financeiras.
"""

import base64
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml
from bizdays import Calendar
from termcolor import cprint


class FinancialDataDownloader:
    """Classe responsável pelo download de dados financeiros."""
    
    def __init__(self, config_path: str = "resources_python.yaml"):
        """
        Inicializa o downloader de dados financeiros.
        
        Args:
            config_path: Caminho para o arquivo de configuração YAML
        """
        self.config_path = config_path
        self.calendar_b3 = Calendar.load("B3")
        self.session = requests.Session()
        self.session.verify = False  # Para compatibilidade com certificados
        
        # Configurar logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Configurar diretórios
        self._setup_directories()
    
    def _setup_directories(self) -> None:
        """Configura os diretórios necessários para o download."""
        current_dir = Path(__file__).parent
        date_str = datetime.now().strftime("%Y-%m-%d")
        
        self.downloads_dir = current_dir / "downloads" / date_str
        self.downloads_bulk_dir = current_dir / "downloads_bulk"
        
        # Criar diretórios se não existirem
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_bulk_dir.mkdir(parents=True, exist_ok=True)
        
        # Limpar pasta downloads_bulk
        for file_path in self.downloads_bulk_dir.glob("*"):
            if file_path.is_file():
                file_path.unlink()
    
    def read_yaml_config(self) -> List[Dict[str, Any]]:
        """
        Lê o arquivo de configuração YAML.
        
        Returns:
            Lista de recursos configurados para download
            
        Raises:
            FileNotFoundError: Se o arquivo de configuração não for encontrado
            yaml.YAMLError: Se houver erro na leitura do YAML
        """
        try:
            with open(self.config_path, "r", encoding="utf-8") as file:
                config = yaml.safe_load(file)
                return config.get("resources", [])
        except FileNotFoundError:
            self.logger.error(
                f"Arquivo de configuração não encontrado: {self.config_path}"
            )
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Erro ao ler arquivo YAML: {e}")
            raise
    
    def replace_date_variables(self, url: str, date_type: str = "data_atual") -> str:
        """
        Substitui variáveis de data na URL.
        
        Args:
            url: URL com variáveis de data
            date_type: Tipo de data (data_atual, dia_anterior, mes_anterior)
            
        Returns:
            URL com as variáveis de data substituídas
        """
        if date_type == "dia_anterior":
            date_obj = self.calendar_b3.offset(datetime.now(), -1)
        elif date_type == "mes_anterior":
            first_day_of_current_month = datetime.now().replace(day=1)
            date_obj = first_day_of_current_month - timedelta(days=1)
        else:
            date_obj = datetime.now()
        
        # Formatos de data
        date_formats = {
            "DD/MM/YYYY": date_obj.strftime("%d/%m/%Y"),
            "YYYY-MM-DD": date_obj.strftime("%Y-%m-%d"),
            "YYYYMMDD": date_obj.strftime("%Y%m%d"),
            "YYMMDD": date_obj.strftime("%y%m%d"),
            "YYYYMM": date_obj.strftime("%Y%m")
        }
        
        # Substituir variáveis na URL
        for pattern, formatted_date in date_formats.items():
            url = url.replace(pattern, formatted_date)
        
        return url
    
    def get_b3_token(self, url_token: str) -> Optional[str]:
        """
        Obtém token de autenticação da B3.
        
        Args:
            url_token: URL para obter o token
            
        Returns:
            Token de autenticação ou None se falhar
        """
        try:
            self.logger.info(f"Obtendo token B3: {url_token}")
            response = self.session.get(url_token)
            response.raise_for_status()
            
            token = response.json().get("token")
            if token:
                self.logger.info("Token B3 obtido com sucesso")
                return token
            else:
                self.logger.warning("Token não encontrado na resposta")
                return None
                
        except requests.RequestException as e:
            self.logger.error(f"Erro ao obter token B3: {e}")
            return None
    
    def save_response(self, url: str, destination_path: Path, 
                     response_type: str) -> None:
        """
        Salva a resposta HTTP como arquivo.
        
        Args:
            url: URL do recurso
            destination_path: Caminho de destino do arquivo
            response_type: Tipo de resposta (json, base64, binário)
            
        Raises:
            requests.RequestException: Se houver erro no download
            ValueError: Se o tipo de resposta for inválido
        """
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            if response_type == "json":
                with open(destination_path, "w", encoding="utf-8") as file:
                    file.write(response.text)
            elif response_type == "base64":
                content = response.text.replace('"', "")
                decoded_bytes = base64.b64decode(content)
                with open(destination_path, "wb") as file:
                    file.write(decoded_bytes)
            else:
                with open(destination_path, "wb") as file:
                    file.write(response.content)
            
            message = f"Arquivo {response_type} salvo em '{destination_path}'"
            cprint(message, "green")
            self.logger.info(message)
            
        except requests.RequestException as e:
            self.logger.error(f"Erro no download de {url}: {e}")
            raise
        except ValueError as e:
            self.logger.error(
                f"Tipo de resposta inválido '{response_type}': {e}"
            )
            raise
    
    def print_resource_info(self, resource: Dict[str, Any]) -> None:
        """
        Imprime informações do recurso.
        
        Args:
            resource: Dicionário com informações do recurso
        """
        print(f"Nome: {resource['name']}")
        print(f"URL: {resource.get('url', 'N/A')}")
        print(f"Nome do arquivo: {resource.get('file_name', 'N/A')}")
        print(f"Tipo de resposta: {resource['type_response']}")
        print(f"Tipo de data: {resource.get('type_date', 'data_atual')}")
        print("=" * 50)
    
    def download_resource(self, resource: Dict[str, Any]) -> bool:
        """
        Faz o download de um recurso específico.
        
        Args:
            resource: Dicionário com informações do recurso
            
        Returns:
            True se o download foi bem-sucedido, False caso contrário
        """
        try:
            # Obter token B3 se necessário
            token_b3 = None
            if resource.get("url_token"):
                self.logger.info(f"Processando recurso: {resource['name']}")
                url_token = self.replace_date_variables(
                    resource["url_token"],
                    resource.get("type_date", "data_atual")
                )
                token_b3 = self.get_b3_token(url_token)
            
            # Processar URL principal
            if not resource.get("url"):
                self.logger.warning(
                    f"Recurso '{resource['name']}' não possui URL"
                )
                return False
            
            self.logger.info(f"Processando recurso: {resource['name']}")
            
            # Substituir variáveis de data na URL
            url = self.replace_date_variables(
                resource["url"],
                resource.get("type_date", "data_atual")
            )
            
            # Substituir token B3 se disponível
            if token_b3 and "B3_TOKEN" in url:
                url = url.replace("B3_TOKEN", token_b3)
            
            # Gerar nome do arquivo
            date_str = datetime.now().strftime("%Y%m%d")
            file_name = resource.get('file_name', Path(url).name)
            file_name = f"{date_str}_{file_name}"
            destination_path = self.downloads_dir / file_name
            
            self.logger.info(f"Download: {url} -> {destination_path}")
            
            # Fazer o download
            self.save_response(url, destination_path, resource["type_response"])
            
            success_message = f"Recurso '{resource['name']}' baixado com sucesso."
            cprint(success_message, "green")
            self.logger.info(success_message)
            
            return True
            
        except Exception as e:
            error_message = f"Falha ao baixar o recurso '{resource['name']}': {e}"
            cprint(error_message, "red")
            self.logger.error(error_message)
            return False
    
    def run(self) -> None:
        """
        Executa o processo completo de download.
        """
        self.logger.info("Iniciando download de dados financeiros")
        print("Hello from captura-downloads!")
        
        try:
            # Ler configuração
            resources = self.read_yaml_config()
            
            # Imprimir informações dos recursos
            for resource in resources:
                if resource.get("url"):
                    self.print_resource_info(resource)
            
            # Fazer download de cada recurso
            successful_downloads = 0
            total_resources = len([r for r in resources if r.get("url")])
            
            for resource in resources:
                if self.download_resource(resource):
                    successful_downloads += 1
            
            # Resumo final
            self.logger.info(
                f"Download concluído: {successful_downloads}/{total_resources} "
                f"recursos baixados com sucesso"
            )
            
        except Exception as e:
            self.logger.error(f"Erro durante o processo de download: {e}")
            raise


def main() -> None:
    """Função principal para execução do downloader."""
    downloader = FinancialDataDownloader()
    downloader.run()


if __name__ == "__main__":
    main()
