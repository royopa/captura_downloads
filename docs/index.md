# Captura Downloads - Documentação

## Visão Geral

O **Captura Downloads** é um sistema abrangente para download, processamento e armazenamento de dados financeiros brasileiros. O projeto automatiza a coleta de dados de múltiplas fontes institucionais, incluindo ANBIMA, B3, CVM e outras entidades financeiras.

## Arquitetura do Sistema

### Componentes Principais

#### 1. **Downloaders**
- **Python Downloader** (`download_files.py`): Sistema principal de download usando requests
- **KYD Downloader** (`download_files_kyd.py`): Framework Know Your Data para dados estruturados
- **Selenium Downloader** (`download_files_selenium.py`): Automação web para sites complexos
- **PowerShell Downloader** (`download_files.ps1`): Scripts PowerShell para compatibilidade Windows

#### 2. **Processadores ETL**
- **ANBIMA Processors**: Processamento de dados de índices e mercado secundário
- **B3 Processors**: Processamento de dados de bolsa e instrumentos financeiros
- **CVM Processors**: Processamento de dados regulatórios

#### 3. **Utilitários**
- **Database Utils**: Conexão e operações com SQL Server
- **File Utils**: Manipulação de arquivos e conversões
- **Logging**: Sistema de logs estruturado

## Configuração

### Pré-requisitos

```bash
# Python 3.11+
python --version

# Poetry
poetry --version

# PowerShell (Windows)
pwsh --version

# Docker (para SQL Server)
docker --version
```

### Instalação

```bash
# 1. Clonar repositório
git clone <repository-url>
cd captura_downloads

# 2. Instalar dependências Python
poetry install

# 3. Instalar dependências PowerShell
pwsh ./dependencias.ps1

# 4. Configurar variáveis de ambiente
cp .env.example .env
# Editar .env com suas configurações
```

### Variáveis de Ambiente

```env
# Database Configuration
DB_SERVER=localhost
DB_PORT=1433
DB_DATABASE=captura_downloads
DB_USER=sa
DB_PASSWORD=your_password

# API Keys (se necessário)
ANBIMA_API_KEY=your_key
B3_API_KEY=your_key
```

## Uso

### Execução Rápida

```bash
# Executar todos os downloads
task start

# Executar apenas processamento
task processor

# Executar testes
task test
```

### Execução Individual

```bash
# Python downloads
python main.py legacy
python main.py kyd
python main.py selenium

# PowerShell downloads
python main.py powershell

# Processamento
python processor.py
```

### Scripts PowerShell

```powershell
# Instalar dependências
.\dependencias.ps1

# Download de arquivos
.\download_files.ps1

# Criar pasta diária
.\cria_pasta_diaria.ps1

# Executar SQL
.\run_sqlcmd.ps1 -SqlFilePath ".\sql\create_tables.sql"
```

## Estrutura de Dados

### Fontes de Dados

#### ANBIMA
- **IMA (Índice de Mercado Anbima)**: Composição e totais dos índices
- **Mercado Secundário**: Dados de debêntures e títulos públicos
- **Indicadores Financeiros**: Taxas e indicadores de mercado

#### B3
- **Carteiras Teóricas**: Composição de índices (IBOV, SMLL, etc.)
- **Instrumentos Listados**: Dados de ativos negociados
- **Capital Social**: Informações societárias das empresas
- **ETFs**: Fundos negociados em bolsa

#### CVM
- **Fundos de Investimento**: Dados cadastrais e informes diários
- **Empresas Abertas**: Informações societárias
- **Extratos**: Dados de movimentação

### Estrutura de Arquivos

```
captura_downloads/
├── downloads/                    # Downloads organizados por data
│   └── YYYY-MM-DD/
│       ├── YYYYMMDD_anbima_*.txt
│       ├── YYYYMMDD_b3_*.csv
│       └── YYYYMMDD_cvm_*.csv
├── downloads_bulk/              # Arquivos para processamento
├── captura_downloads/           # Código principal
│   ├── etls/                   # Processadores ETL
│   │   ├── anbima/            # Processadores ANBIMA
│   │   └── b3/                # Processadores B3
│   └── notebooks/             # Jupyter notebooks
├── kyd/                       # Framework KYD
├── kyd_downloader/            # Aplicação KYD
└── config/                    # Arquivos de configuração
    ├── resources.yaml         # Configuração PowerShell
    └── resources_python.yaml  # Configuração Python
```

## Desenvolvimento

### Padrões de Código

#### Python
- **Type Hints**: Uso obrigatório de type hints
- **Docstrings**: Documentação completa de funções e classes
- **Logging**: Sistema de logs estruturado
- **Error Handling**: Tratamento robusto de erros

#### PowerShell
- **Comment-Based Help**: Documentação inline
- **Error Handling**: Try-catch blocks
- **Parameter Validation**: Validação de parâmetros
- **Colorized Output**: Saída colorida para melhor UX

### Testes

```bash
# Executar todos os testes
task test

# Executar testes específicos
pytest tests/test_download.py
pytest tests/test_processors.py

# Cobertura de código
pytest --cov=captura_downloads tests/
```

### Qualidade de Código

```bash
# Linting
task lint

# Formatação
blue .
ruff format .

# Verificação de tipos
mypy captura_downloads/
```

## Monitoramento e Logs

### Estrutura de Logs

Os logs são estruturados com:
- **Timestamp**: Data e hora da operação
- **Level**: INFO, WARNING, ERROR, DEBUG
- **Module**: Módulo que gerou o log
- **Message**: Mensagem descritiva

### Exemplo de Log

```
2025-01-15 10:30:45 - captura_downloads.download_files - INFO - Iniciando download de dados financeiros
2025-01-15 10:30:46 - captura_downloads.download_files - INFO - Processando recurso: ANBIMA - IMA Completo
2025-01-15 10:30:47 - captura_downloads.download_files - INFO - Download: https://... -> downloads/2025-01-15/20250115_anbima_ima_completo.txt
2025-01-15 10:30:48 - captura_downloads.download_files - INFO - Recurso 'ANBIMA - IMA Completo' baixado com sucesso
```

## Troubleshooting

### Problemas Comuns

#### 1. Erro de Conexão com Banco
```
Erro: Variáveis de ambiente obrigatórias não definidas: DB_SERVER, DB_DATABASE
Solução: Verificar arquivo .env e variáveis de ambiente
```

#### 2. Erro de Download
```
Erro: Falha ao baixar o recurso 'ANBIMA - IMA Completo'
Solução: Verificar conectividade de internet e disponibilidade da fonte
```

#### 3. Erro de Processamento
```
Erro: Arquivo não encontrado: downloads_bulk/anbima_ima_completo.txt
Solução: Verificar se o download foi executado com sucesso
```

### Logs de Debug

```bash
# Habilitar logs detalhados
export LOG_LEVEL=DEBUG
python download_files.py

# PowerShell
$env:LOG_LEVEL="DEBUG"
.\download_files.ps1
```

## Contribuição

### Fluxo de Desenvolvimento

1. **Fork** do repositório
2. **Branch** para feature: `git checkout -b feature/nova-funcionalidade`
3. **Desenvolvimento** seguindo padrões estabelecidos
4. **Testes** locais antes do commit
5. **Commit** com mensagem descritiva
6. **Push** para o branch
7. **Pull Request** com descrição detalhada

### Padrões de Commit

```
feat: adiciona novo processador para dados BACEN
fix: corrige erro de parsing em arquivos CSV
docs: atualiza documentação de configuração
test: adiciona testes para downloader KYD
refactor: reorganiza estrutura de classes
```

## Licença

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE](LICENSE) para detalhes.

## Suporte

Para suporte e dúvidas:
- **Issues**: [GitHub Issues](https://github.com/username/captura_downloads/issues)
- **Email**: royopa@gmail.com
- **Documentação**: [docs/](docs/)

## Changelog

### v1.0.0 (2025-01-15)
- ✅ Sistema completo de download multi-fonte
- ✅ Processadores ETL para ANBIMA, B3 e CVM
- ✅ Suporte a Python e PowerShell
- ✅ Integração com SQL Server
- ✅ Sistema de logs estruturado
- ✅ Documentação completa
