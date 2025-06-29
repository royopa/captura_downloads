<#
.SYNOPSIS
    Script para executar comandos SQL usando sqlcmd.

.DESCRIPTION
    Este script executa arquivos SQL usando o utilitário sqlcmd, carregando
    as configurações de conexão do arquivo .env na raiz do projeto.

.PARAMETER SqlFilePath
    Caminho para o arquivo SQL a ser executado.

.PARAMETER EnvFilePath
    Caminho para o arquivo .env (opcional). Padrão: .env na raiz do projeto.

.EXAMPLE
    .\run_sqlcmd.ps1 -SqlFilePath ".\sql\create_tables.sql"
    Executa o arquivo SQL especificado usando configurações do .env.

.EXAMPLE
    .\run_sqlcmd.ps1 -SqlFilePath "C:\temp\script.sql" -EnvFilePath "C:\config\.env"
    Executa arquivo SQL específico usando arquivo .env personalizado.

.NOTES
    Autor: Rodrigo Prado de Jesus
    Data: 2025
    Versão: 1.0
    
    Requisitos:
    - sqlcmd instalado e disponível no PATH
    - Arquivo .env com variáveis: DB_SERVER, DB_DATABASE, DB_USER, DB_PASSWORD
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$SqlFilePath,
    
    [string]$EnvFilePath = ""
)

# Função para carregar variáveis de ambiente do arquivo .env
function Load-EnvironmentVariables {
    param([string]$EnvFilePath)
    
    try {
        # Definir caminho padrão se não fornecido
        if ([string]::IsNullOrEmpty($EnvFilePath)) {
            $EnvFilePath = Join-Path (Get-Location) ".env"
        }
        
        Write-Host "Carregando variáveis de ambiente de: $EnvFilePath" -ForegroundColor Cyan
        
        if (-not (Test-Path -Path $EnvFilePath)) {
            throw "Arquivo .env não encontrado: $EnvFilePath"
        }
        
        # Carregar variáveis do arquivo .env
        $envVars = Get-Content -Path $EnvFilePath | ForEach-Object {
            if ($_ -match '^([^=]+)=(.*)$') {
                @{
                    Name = $matches[1].Trim()
                    Value = $matches[2].Trim()
                }
            }
        } | Where-Object { $null -ne $_ }
        
        # Aplicar variáveis ao ambiente
        $requiredVars = @("DB_SERVER", "DB_DATABASE", "DB_USER", "DB_PASSWORD")
        $missingVars = @()
        
        foreach ($var in $requiredVars) {
            $envVar = $envVars | Where-Object { $_.Name -eq $var }
            if ($envVar) {
                Set-Variable -Name "env:$var" -Value $envVar.Value -Scope Global
                Write-Host "Variável carregada: $var" -ForegroundColor Green
            } else {
                $missingVars += $var
            }
        }
        
        if ($missingVars.Count -gt 0) {
            throw "Variáveis obrigatórias não encontradas: $($missingVars -join ', ')"
        }
        
        Write-Host "Todas as variáveis de ambiente foram carregadas com sucesso!" -ForegroundColor Green
        
    } catch {
        Write-Host "Erro ao carregar variáveis de ambiente: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

# Função para verificar se sqlcmd está disponível
function Test-SqlCmdAvailable {
    try {
        $null = Get-Command sqlcmd -ErrorAction Stop
        Write-Host "sqlcmd encontrado no sistema" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "sqlcmd não encontrado no PATH do sistema" -ForegroundColor Red
        Write-Host "Certifique-se de que o SQL Server Command Line Utilities está instalado" -ForegroundColor Yellow
        return $false
    }
}

# Função para executar comando SQL
function Invoke-SqlCommand {
    param([string]$SqlFilePath)
    
    try {
        # Verificar se o arquivo SQL existe
        if (-not (Test-Path -Path $SqlFilePath)) {
            throw "Arquivo SQL não encontrado: $SqlFilePath"
        }
        
        Write-Host "Executando arquivo SQL: $SqlFilePath" -ForegroundColor Yellow
        Write-Host "=" * 60 -ForegroundColor Gray
        
        # Construir comando sqlcmd
        $sqlCmdArgs = @(
            "-S", $env:DB_SERVER,
            "-d", $env:DB_DATABASE,
            "-U", $env:DB_USER,
            "-P", $env:DB_PASSWORD,
            "-i", $SqlFilePath
        )
        
        Write-Host "Comando sqlcmd:" -ForegroundColor Cyan
        Write-Host "sqlcmd $($sqlCmdArgs -join ' ')" -ForegroundColor Gray
        Write-Host "-" * 40 -ForegroundColor Gray
        
        # Executar comando
        $result = & sqlcmd @sqlCmdArgs 2>&1
        
        # Verificar resultado
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Script SQL executado com sucesso!" -ForegroundColor Green
            Write-Host "Saída do banco de dados:" -ForegroundColor Cyan
            Write-Host $result -ForegroundColor White
            return $true
        } else {
            Write-Host "Erro ao executar o script SQL (código: $LASTEXITCODE)" -ForegroundColor Red
            Write-Host "Saída de erro:" -ForegroundColor Red
            Write-Host $result -ForegroundColor Red
            return $false
        }
        
    } catch {
        Write-Host "Erro ao executar comando SQL: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Função principal
function Start-SqlExecution {
    Write-Host "Iniciando execução de comando SQL..." -ForegroundColor Yellow
    Write-Host "=" * 60 -ForegroundColor Gray
    
    try {
        # Verificar disponibilidade do sqlcmd
        if (-not (Test-SqlCmdAvailable)) {
            return 1
        }
        
        # Carregar variáveis de ambiente
        Load-EnvironmentVariables -EnvFilePath $EnvFilePath
        
        # Executar comando SQL
        $success = Invoke-SqlCommand -SqlFilePath $SqlFilePath
        
        # Resumo final
        Write-Host "=" * 60 -ForegroundColor Gray
        if ($success) {
            Write-Host "Execução concluída com sucesso!" -ForegroundColor Green
            return 0
        } else {
            Write-Host "Execução falhou!" -ForegroundColor Red
            return 1
        }
        
    } catch {
        Write-Host "Erro crítico durante a execução: $($_.Exception.Message)" -ForegroundColor Red
        return 1
    }
}

# Executar função principal
$exitCode = Start-SqlExecution
exit $exitCode
