import base64
import os
from datetime import date, datetime, timedelta

import requests
import yaml
from bizdays import Calendar
from clint.textui import colored


# Função para ler o conteúdo do YAML
def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)['resources']


# Função para substituir variáveis de data na URL
def replace_date_variables(url, type_date, calendar_b3):
    if type_date == 'dia_anterior':
        date = calendar_b3.offset(datetime.now(), -1)
    elif type_date == 'mes_anterior':
        first_day_of_current_month = datetime.now().replace(day=1)
        date = first_day_of_current_month - timedelta(days=1)
    else:
        date = datetime.now()

    date_formatada = date.strftime('%d/%m/%Y')
    date_curta = date.strftime('%Y-%m-%d')
    date_arquivo = date.strftime('%Y%m%d')
    date_curta_ano2digitos = date.strftime('%y%m%d')
    date_arquivo_mes = date.strftime('%Y%m')

    url = url.replace('DD/MM/YYYY', date_formatada)
    url = url.replace('YYYY-MM-DD', date_curta)
    url = url.replace('YYYYMMDD', date_arquivo)
    url = url.replace('YYMMDD', date_curta_ano2digitos)
    url = url.replace('YYYYMM', date_arquivo_mes)
    return url


# Função para salvar a resposta como arquivo
def save_response(url, destination_path, type_response):
    response = requests.get(url)
    response.raise_for_status()  # Lança um erro se o download falhar

    if type_response == 'json':
        with open(destination_path, 'w', encoding='utf-8') as file:
            file.write(response.text)
    elif type_response == 'base64':
        content = response.text.replace('"', '')
        decoded_bytes = base64.b64decode(content)
        with open(destination_path, 'wb') as file:
            file.write(decoded_bytes)
    else:
        with open(destination_path, 'wb') as file:
            file.write(response.content)

    print(
        colored.green(f"Arquivo {type_response} salvo em '{destination_path}'")
    )


def get_b3_token(url_token: str):
    print(url_token)
    session = requests.Session()
    response = session.get(url_token, verify=False)
    print(response)
    if response.ok:
        return response.json()['token']


# Função para imprimir informações dos recursos
def print_resource_info(resource):
    print(f"Nome: {resource['name']}")
    print(f"URL: {resource['url']}")
    print(f"Nome do arquivo: {resource.get('file_name', 'N/A')}")
    print(f"Tipo de resposta: {resource['type_response']}")
    print(f"Tipo de data: {resource.get('type_date', 'data_atual')}")
    print('###########################################')


def main():
    calendar_b3 = Calendar.load('B3')
    print(calendar_b3)
    print('Hello from captura-downloads!')

    # Obtenha o diretório onde o script está localizado
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))

    # Defina o caminho da pasta downloads e da nova pasta com a data atual
    date_curta = datetime.now().strftime('%Y-%m-%d')
    pasta_downloads = os.path.join(diretorio_atual, 'downloads', date_curta)
    pasta_downloads_bulk = os.path.join(diretorio_atual, 'downloads_bulk')

    # Crie as pastas de download, se necessário
    os.makedirs(pasta_downloads, exist_ok=True)
    os.makedirs(pasta_downloads_bulk, exist_ok=True)

    # Limpe a pasta downloads_bulk
    for file in os.listdir(pasta_downloads_bulk):
        os.remove(os.path.join(pasta_downloads_bulk, file))

    # Defina o caminho do arquivo resources.yaml
    caminho_yaml = os.path.join(diretorio_atual, 'resources_python.yaml')

    # Leia o conteúdo do arquivo YAML
    resources = read_yaml(caminho_yaml)

    # Imprima as informações de cada recurso
    for resource in resources:
        if resource.get('url'):
            print_resource_info(resource)

    # Faça o download de cada recurso
    for resource in resources:
        token_b3 = None
        if resource.get('url_token'):
            print(f"Processando recurso: {resource['name']}")
            print(f"URL Token original: {resource['url_token']}")

            # Substitua variáveis de data na URL
            url_token = replace_date_variables(
                resource['url_token'],
                resource.get('type_date', 'data_atual'),
                calendar_b3,
            )
            print(f'URL processada: {url_token}')
            token_b3 = get_b3_token(url_token)

        if resource.get('url'):
            print(f"Processando recurso: {resource['name']}")
            print(f"URL original: {resource['url']}")

            # Substitua variáveis de data na URL
            url = replace_date_variables(
                resource['url'],
                resource.get('type_date', 'data_atual'),
                calendar_b3,
            )

            # Substitua variáveis de token na URL
            if 'B3_TOKEN' in url:
                url = url.replace('B3_TOKEN', token_b3)

            print(f'URL processada: {url}')

            nome_arquivo = f"{datetime.now().strftime('%Y%m%d')}_{resource.get('file_name', os.path.basename(url))}"
            caminho_destino = os.path.join(pasta_downloads, nome_arquivo)
            print(f'Nome do arquivo: {nome_arquivo}')
            print(f'Caminho do arquivo: {caminho_destino}')
            print(f"Tipo de resposta: {resource['type_response']}")

            try:
                # Salve a resposta conforme o tipo
                save_response(url, caminho_destino, resource['type_response'])
                print(
                    colored.green(
                        f"Recurso '{resource['name']}' baixado com sucesso."
                    )
                )
            except Exception as e:
                print(
                    colored.red(
                        f"Falha ao baixar o recurso '{resource['name']}' de '{url}'. Erro: {e}"
                    )
                )


if __name__ == '__main__':
    main()
