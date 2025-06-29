<#
.SYNOPSIS
    Script para instalar dependências PowerShell necessárias para o projeto.

.DESCRIPTION
    Este script instala os módulos PowerShell necessários para o funcionamento
    do sistema de download de dados financeiros, incluindo:
    - FXPSYaml: Para processamento de arquivos YAML
    - SqlServer: Para conexão com SQL Server
    - PsSqlClient: Para operações avançadas com SQL Server

.PARAMETER Force
    Força a reinstalação dos módulos mesmo se já estiverem instalados.

.EXAMPLE
    .\dependencias.ps1
    Instala as dependências necessárias.

.EXAMPLE
    .\dependencias.ps1 -Force
    Força a reinstalação de todas as dependências.

.NOTES
    Autor: Rodrigo Prado de Jesus
    Data: 2025
    Versão: 1.0
#>

param(
    [switch]$Force
)

# Configurar política de execução para permitir instalação de módulos
Write-Host "Configurando política de execução..." -ForegroundColor Yellow
Set-PSRepository PSGallery -InstallationPolicy Trusted

# Lista de módulos necessários
$modules = @(
    @{
        Name = "FXPSYaml"
        Description = "Processamento de arquivos YAML"
    },
    @{
        Name = "SqlServer"
        Description = "Conexão com SQL Server"
    },
    @{
        Name = "PsSqlClient"
        Description = "Operações avançadas com SQL Server"
        AllowPrerelease = $true
        AllowClobber = $true
    }
)

# Função para verificar se um módulo está instalado
function Test-ModuleInstalled {
    param([string]$ModuleName)
    
    $module = Get-Module -ListAvailable -Name $ModuleName
    return $null -ne $module
}

# Função para instalar um módulo
function Install-ModuleWithRetry {
    param(
        [string]$ModuleName,
        [string]$Description,
        [hashtable]$Options = @{}
    )
    
    Write-Host "Verificando módulo: $ModuleName ($Description)" -ForegroundColor Cyan
    
    $isInstalled = Test-ModuleInstalled -ModuleName $ModuleName
    
    if ($isInstalled -and -not $Force) {
        Write-Host "Módulo $ModuleName já está instalado. Use -Force para reinstalar." -ForegroundColor Green
        return $true
    }
    
    try {
        Write-Host "Instalando módulo: $ModuleName..." -ForegroundColor Yellow
        
        $installParams = @{
            Name = $ModuleName
            Scope = "CurrentUser"
            Force = $true
        }
        
        # Adicionar opções específicas se fornecidas
        foreach ($key in $Options.Keys) {
            $installParams[$key] = $Options[$key]
        }
        
        Install-Module @installParams
        
        Write-Host "Módulo $ModuleName instalado com sucesso!" -ForegroundColor Green
        return $true
        
    } catch {
        Write-Host "Erro ao instalar módulo $ModuleName : $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Função principal de instalação
function Install-Dependencies {
    Write-Host "Iniciando instalação de dependências PowerShell..." -ForegroundColor Yellow
    Write-Host "=" * 60 -ForegroundColor Gray
    
    $successCount = 0
    $totalModules = $modules.Count
    
    foreach ($module in $modules) {
        $options = @{}
        
        # Adicionar opções específicas para PsSqlClient
        if ($module.Name -eq "PsSqlClient") {
            $options["AllowPrerelease"] = $true
            $options["AllowClobber"] = $true
        }
        
        if (Install-ModuleWithRetry -ModuleName $module.Name -Description $module.Description -Options $options) {
            $successCount++
        }
        
        Write-Host "-" * 40 -ForegroundColor Gray
    }
    
    # Resumo final
    Write-Host "=" * 60 -ForegroundColor Gray
    Write-Host "Resumo da instalação:" -ForegroundColor Yellow
    Write-Host "Módulos instalados com sucesso: $successCount/$totalModules" -ForegroundColor Green
    
    if ($successCount -eq $totalModules) {
        Write-Host "Todas as dependências foram instaladas com sucesso!" -ForegroundColor Green
        return 0
    } else {
        Write-Host "Alguns módulos falharam na instalação. Verifique os erros acima." -ForegroundColor Red
        return 1
    }
}

# Executar instalação
try {
    $exitCode = Install-Dependencies
    exit $exitCode
} catch {
    Write-Host "Erro crítico durante a instalação: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}