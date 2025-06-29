# Captura Downloads

Um sistema abrangente e profissional para download, processamento e armazenamento de dados financeiros brasileiros. Este projeto automatiza a coleta de dados de múltiplas fontes institucionais incluindo ANBIMA, B3, CVM e outras entidades financeiras, com suporte a múltiplas tecnologias e plataformas.

## 🚀 Características Principais

### ✨ **Multi-tecnologia**
- **Python**: Sistema principal com type hints e logging estruturado
- **PowerShell**: Scripts otimizados para ambiente Windows
- **Selenium**: Automação web para sites complexos
- **KYD Framework**: Processamento de dados estruturados

### 📊 **Fontes de Dados**
- **ANBIMA**: Índices IMA, mercado secundário, indicadores financeiros
- **B3**: Carteiras teóricas, instrumentos listados, capital social
- **CVM**: Fundos de investimento, empresas abertas, extratos
- **BACEN**: Dados de mercado e indicadores econômicos

### 🔧 **Funcionalidades Avançadas**
- **ETL Automatizado**: Processamento e transformação de dados
- **Integração SQL Server**: Carregamento direto no banco de dados
- **Sistema de Logs**: Monitoramento completo das operações
- **Tratamento de Erros**: Recuperação robusta de falhas
- **Configuração Flexível**: Arquivos YAML para diferentes ambientes

## 📋 Pré-requisitos

- **Python 3.11+** com Poetry
- **PowerShell 7+** (para scripts Windows)
- **Docker** (para SQL Server)
- **Conexão com internet** para downloads

## 🛠️ Instalação Rápida

### 1. **Clone e Configure**
```bash
git clone <repository-url>
cd captura_downloads
```

### 2. **Instale Dependências Python**
```bash
poetry install
poetry shell
```

### 3. **Instale Dependências PowerShell**
```powershell
pwsh ./dependencias.ps1
```

### 4. **Configure Variáveis de Ambiente**
```bash
cp .env.example .env
# Edite .env com suas configurações de banco
```

## 🚀 Uso Rápido

### **Execução Completa**
```bash
# Download e processamento completo
task start
task processor
```

### **Execução Individual**
```bash
# Downloads específicos
python main.py legacy      # Sistema Python principal
python main.py kyd         # Framework KYD
python main.py selenium    # Automação web
python main.py powershell  # Scripts PowerShell

# Processamento
python processor.py
```

### **Scripts PowerShell**
```powershell
# Download de arquivos
.\download_files.ps1

# Criação de pastas
.\cria_pasta_diaria.ps1

# Execução SQL
.\run_sqlcmd.ps1 -SqlFilePath ".\sql\create_tables.sql"
```

## 🏗️ Arquitetura do Sistema

```
captura_downloads/
├── 📁 downloads/                    # Downloads organizados por data
│   └── 📁 YYYY-MM-DD/
├── 📁 downloads_bulk/              # Arquivos para processamento
├── 📁 captura_downloads/           # Código principal
│   ├── 📁 etls/                   # Processadores ETL
│   │   ├── 📁 anbima/            # Processadores ANBIMA
│   │   └── 📁 b3/                # Processadores B3
│   └── 📁 notebooks/             # Jupyter notebooks
├── 📁 kyd/                       # Framework KYD
├── 📁 kyd_downloader/            # Aplicação KYD
├── 📄 download_files.py          # Downloader Python principal
├── 📄 download_files_kyd.py      # Downloader KYD
├── 📄 download_files_selenium.py # Downloader Selenium
├── 📄 processor.py               # Processador ETL
├── 📄 main.py                    # Interface principal
└── 📄 *.ps1                      # Scripts PowerShell
```

## 🔧 Configuração Detalhada

### **Variáveis de Ambiente**
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

### **Arquivos de Configuração**
- `resources.yaml`: Configuração para PowerShell
- `resources_python.yaml`: Configuração para Python
- `pyproject.toml`: Dependências Python
- `mkdocs.yml`: Configuração da documentação

## 🧪 Desenvolvimento

### **Executar Testes**
```bash
# Todos os testes
task test

# Testes específicos
pytest tests/test_download.py
pytest tests/test_processors.py

# Cobertura
pytest --cov=captura_downloads tests/
```

### **Qualidade de Código**
```bash
# Linting e formatação
task lint
blue .
ruff format .

# Verificação de tipos
mypy captura_downloads/
```

### **Documentação**
```bash
# Servir documentação local
task docs
# Acesse: http://localhost:8000
```

## 📊 Monitoramento e Logs

### **Estrutura de Logs**
Os logs são estruturados com timestamp, nível, módulo e mensagem:

```
2025-01-15 10:30:45 - captura_downloads.download_files - INFO - Iniciando download
2025-01-15 10:30:46 - captura_downloads.download_files - INFO - Processando: ANBIMA - IMA
2025-01-15 10:30:47 - captura_downloads.download_files - INFO - Download concluído
```

### **Níveis de Log**
- **INFO**: Operações normais
- **WARNING**: Situações que merecem atenção
- **ERROR**: Erros que impedem a operação
- **DEBUG**: Informações detalhadas para desenvolvimento

## 🔍 Troubleshooting

### **Problemas Comuns**

#### 1. **Erro de Conexão com Banco**
```
Erro: Variáveis de ambiente obrigatórias não definidas
Solução: Verificar arquivo .env e variáveis DB_*
```

#### 2. **Erro de Download**
```
Erro: Falha ao baixar recurso
Solução: Verificar conectividade e disponibilidade da fonte
```

#### 3. **Erro de Processamento**
```
Erro: Arquivo não encontrado
Solução: Verificar se download foi executado com sucesso
```

### **Logs de Debug**
```bash
# Python
export LOG_LEVEL=DEBUG
python download_files.py

# PowerShell
$env:LOG_LEVEL="DEBUG"
.\download_files.ps1
```

## 🤝 Contribuição

### **Fluxo de Desenvolvimento**
1. **Fork** do repositório
2. **Branch** para feature: `git checkout -b feature/nova-funcionalidade`
3. **Desenvolvimento** seguindo padrões estabelecidos
4. **Testes** locais antes do commit
5. **Commit** com mensagem descritiva
6. **Push** para o branch
7. **Pull Request** com descrição detalhada

### **Padrões de Commit**
```
feat: adiciona novo processador para dados BACEN
fix: corrige erro de parsing em arquivos CSV
docs: atualiza documentação de configuração
test: adiciona testes para downloader KYD
refactor: reorganiza estrutura de classes
```

## 📚 Documentação Completa

Para documentação detalhada, acesse:
- **📖 [Documentação Completa](docs/)** - Guia completo do sistema
- **🔧 [Configuração](docs/configuration.md)** - Configurações avançadas
- **📊 [Estrutura de Dados](docs/data-structure.md)** - Formato dos dados
- **🚀 [API Reference](docs/api.md)** - Referência da API

## 📄 Licença

Este projeto está licenciado sob a **MIT License** - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 👨‍💻 Autor

**Rodrigo Prado de Jesus** - [royopa@gmail.com](mailto:royopa@gmail.com)

## 🙏 Agradecimentos

- **ANBIMA** por fornecer dados de mercado financeiro
- **B3** por dados de bolsa e instrumentos financeiros
- **CVM** por dados regulatórios e de fundos
- **Comunidade financeira brasileira** por padrões e protocolos

## 📈 Roadmap

### **v1.1.0 (Próxima versão)**
- [ ] Suporte a BigQuery
- [ ] Interface web para monitoramento
- [ ] Processamento em paralelo
- [ ] Cache inteligente de downloads
- [ ] Alertas por email/Slack

### **v1.2.0 (Futuro)**
- [ ] Machine Learning para detecção de anomalias
- [ ] API REST para integração
- [ ] Dashboard interativo
- [ ] Suporte a mais fontes de dados

---

**⭐ Se este projeto foi útil, considere dar uma estrela no repositório!**