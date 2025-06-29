<#
.SYNOPSIS
    Script para criar pasta diária para downloads.

.DESCRIPTION
    Este script cria uma pasta com a data atual dentro do diretório downloads
    para organizar os arquivos baixados por data. Se a pasta já existir,
    nenhuma ação será realizada.

.PARAMETER BasePath
    Caminho base onde a pasta será criada. Padrão: diretório atual.

.PARAMETER DateFormat
    Formato da data para o nome da pasta. Padrão: "yyyy-MM-dd".

.EXAMPLE
    .\cria_pasta_diaria.ps1
    Cria a pasta com a data atual no diretório downloads.

.EXAMPLE
    .\cria_pasta_diaria.ps1 -BasePath "C:\Downloads" -DateFormat "yyyyMMdd"
    Cria a pasta com formato de data diferente em caminho específico.

.NOTES
    Autor: Rodrigo Prado de Jesus
    Data: 2025
    Versão: 1.0
#>

param(
    [string]$BasePath = "",
    [string]$DateFormat = "yyyy-MM-dd"
)

# Função para obter o diretório atual do script
function Get-ScriptDirectory {
    return Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
}

# Função para criar diretório se não existir
function New-DirectoryIfNotExists {
    param([string]$Path, [string]$Description)
    
    if (-not (Test-Path -Path $Path)) {
        try {
            New-Item -ItemType Directory -Path $Path -Force | Out-Null
            Write-Host "Pasta '$Description' criada com sucesso: $Path" -ForegroundColor Green
            return $true
        } catch {
            Write-Host "Erro ao criar pasta '$Description': $($_.Exception.Message)" -ForegroundColor Red
            return $false
        }
    } else {
        Write-Host "A pasta '$Description' já existe: $Path" -ForegroundColor Yellow
        return $true
    }
}

# Função principal
function New-DailyFolder {
    Write-Host "Iniciando criação de pasta diária..." -ForegroundColor Yellow
    Write-Host "=" * 50 -ForegroundColor Gray
    
    try {
        # Obter data atual formatada
        $currentDate = Get-Date -Format $DateFormat
        Write-Host "Data atual: $currentDate" -ForegroundColor Cyan
        
        # Definir caminhos
        if ([string]::IsNullOrEmpty($BasePath)) {
            $scriptDir = Get-ScriptDirectory
            $downloadsPath = [System.IO.Path]::Combine($scriptDir, "downloads")
        } else {
            $downloadsPath = $BasePath
        }
        
        $dailyFolderPath = [System.IO.Path]::Combine($downloadsPath, $currentDate)
        
        Write-Host "Caminho base: $downloadsPath" -ForegroundColor Cyan
        Write-Host "Pasta diária: $dailyFolderPath" -ForegroundColor Cyan
        Write-Host "-" * 30 -ForegroundColor Gray
        
        # Criar diretório downloads se não existir
        $downloadsCreated = New-DirectoryIfNotExists -Path $downloadsPath -Description "downloads"
        if (-not $downloadsCreated) {
            Write-Host "Falha ao criar diretório downloads. Abortando..." -ForegroundColor Red
            return 1
        }
        
        # Criar pasta diária
        $dailyCreated = New-DirectoryIfNotExists -Path $dailyFolderPath -Description "diária"
        if (-not $dailyCreated) {
            Write-Host "Falha ao criar pasta diária. Abortando..." -ForegroundColor Red
            return 1
        }
        
        # Resumo final
        Write-Host "=" * 50 -ForegroundColor Gray
        Write-Host "Operação concluída com sucesso!" -ForegroundColor Green
        Write-Host "Pasta criada/verificada: $dailyFolderPath" -ForegroundColor Green
        
        return 0
        
    } catch {
        Write-Host "Erro crítico durante a criação da pasta: $($_.Exception.Message)" -ForegroundColor Red
        return 1
    }
}

# Executar função principal
$exitCode = New-DailyFolder
exit $exitCode
