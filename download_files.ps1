<#
.SYNOPSIS
    Script para download de arquivos financeiros usando PowerShell.

.DESCRIPTION
    Este script faz o download de dados financeiros de múltiplas fontes
    brasileiras (ANBIMA, B3, CVM, etc.) usando configurações YAML.
    Os arquivos são organizados por data e copiados para pasta de processamento.

.PARAMETER ConfigPath
    Caminho para o arquivo de configuração YAML. Padrão: resources.yaml.

.PARAMETER OutputPath
    Caminho base para salvar os downloads. Padrão: downloads com data atual.

.EXAMPLE
    .\download_files.ps1
    Executa download usando configuração padrão.

.EXAMPLE
    .\download_files.ps1 -ConfigPath "custom_resources.yaml" -OutputPath "C:\Downloads"
    Executa download com configuração e caminho personalizados.

.NOTES
    Autor: Rodrigo Prado de Jesus
    Data: 2025
    Versão: 1.0
    
    Requisitos:
    - Módulo FXPSYaml instalado
    - Conexão com internet
#>

param(
    [string]$ConfigPath = "resources.yaml",
    [string]$OutputPath = ""
)

# Carregar a biblioteca FXPSYaml
Import-Module FXPSYaml

# Função para obter o diretório atual do script
function Get-ScriptDirectory {
    return Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
}

# Função para ler o conteúdo do YAML usando FXPSYaml
function Read-YamlConfig {
    param([string]$Path)
    
    try {
        Write-Host "Lendo arquivo de configuração: $Path" -ForegroundColor Cyan
        
        if (-not (Test-Path -Path $Path)) {
            throw "Arquivo de configuração não encontrado: $Path"
        }
        
        $yaml = Get-Content -Raw -Path $Path
        $config = ConvertFrom-Yaml -YamlString $yaml
        
        if (-not $config.resources) {
            throw "Seção 'resources' não encontrada no arquivo YAML"
        }
        
        Write-Host "Configuração carregada com sucesso. Recursos encontrados: $($config.resources.Count)" -ForegroundColor Green
        return $config.resources
        
    } catch {
        Write-Host "Erro ao ler arquivo YAML: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

# Função para substituir variáveis de data na URL
function Replace-DateVariables {
    param(
        [string]$Url,
        [string]$DataFormatada,
        [string]$DataCurta,
        [string]$DataCurtaAno2Digitos,
        [string]$DataArquivo
    )
    
    $processedUrl = $Url -replace "DD/MM/YYYY", $DataFormatada
    $processedUrl = $processedUrl -replace "YYYY-MM-DD", $DataCurta
    $processedUrl = $processedUrl -replace "YYYYMMDD", $DataArquivo
    $processedUrl = $processedUrl -replace "YYMMDD", $DataCurtaAno2Digitos
    
    return $processedUrl
}

# Função para salvar a resposta como arquivo
function Save-Response {
    param(
        [string]$Url,
        [string]$DestinationPath,
        [string]$TypeResponse
    )
    
    try {
        Write-Host "Fazendo download de: $Url" -ForegroundColor Yellow
        
        switch ($TypeResponse) {
            'json' {
                # Baixar e salvar como JSON
                Invoke-WebRequest -Uri $Url -OutFile $DestinationPath -UseBasicParsing
                Write-Host "Arquivo JSON salvo em '$DestinationPath'" -ForegroundColor Green
            }
            'base64' {
                # Baixar e salvar como Base64
                $responseString = Invoke-WebRequest -Uri $Url -UseBasicParsing
                $content = $responseString.Content.Trim('"')
                
                try {
                    $decodedBytes = [System.Convert]::FromBase64String($content)
                    [System.IO.File]::WriteAllBytes($DestinationPath, $decodedBytes)
                    Write-Host "Arquivo Base64 salvo em '$DestinationPath'" -ForegroundColor Green
                } catch {
                    Write-Host "Falha ao decodificar string Base64: $($_.Exception.Message)" -ForegroundColor Red
                    throw
                }
            }
            default {
                # Baixar e salvar como binário
                Invoke-WebRequest -Uri $Url -OutFile $DestinationPath -UseBasicParsing
                Write-Host "Arquivo binário salvo em '$DestinationPath'" -ForegroundColor Green
            }
        }
        
        return $true
        
    } catch {
        Write-Host "Erro ao salvar arquivo: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Função para processar um recurso
function Process-Resource {
    param(
        [object]$Resource,
        [string]$DataFormatada,
        [string]$DataCurta,
        [string]$DataCurtaAno2Digitos,
        [string]$DataArquivo,
        [string]$DownloadsPath
    )
    
    try {
        if ($null -eq $Resource.url) {
            Write-Host "Recurso '$($Resource.name)' não possui URL válida" -ForegroundColor Yellow
            return $false
        }
        
        Write-Host "Processando recurso: $($Resource.name)" -ForegroundColor Cyan
        Write-Host "URL original: $($Resource.url)" -ForegroundColor Gray
        
        # Substituir variáveis de data na URL
        $processedUrl = Replace-DateVariables -Url $Resource.url `
                                             -DataFormatada $DataFormatada `
                                             -DataCurta $DataCurta `
                                             -DataCurtaAno2Digitos $DataCurtaAno2Digitos `
                                             -DataArquivo $DataArquivo
        
        Write-Host "URL processada: $processedUrl" -ForegroundColor Gray
        
        # Gerar nome do arquivo
        $fileName = if ($null -ne $Resource.file_name) { 
            "$DataArquivo`_$($Resource.file_name)" 
        } else { 
            "$DataArquivo`_$([System.IO.Path]::GetFileName($processedUrl))" 
        }
        
        $destinationPath = [System.IO.Path]::Combine($DownloadsPath, $fileName)
        
        Write-Host "Nome do arquivo: $fileName" -ForegroundColor Cyan
        Write-Host "Caminho do arquivo: $destinationPath" -ForegroundColor Cyan
        Write-Host "Tipo de resposta: $($Resource.type_response)" -ForegroundColor Cyan
        
        # Fazer o download
        $success = Save-Response -Url $processedUrl `
                                -DestinationPath $destinationPath `
                                -TypeResponse $Resource.type_response
        
        if ($success) {
            Write-Host "Recurso '$($Resource.name)' baixado com sucesso." -ForegroundColor Green
            return $true
        } else {
            Write-Host "Falha ao baixar o recurso '$($Resource.name)'" -ForegroundColor Red
            return $false
        }
        
    } catch {
        Write-Host "Erro ao processar recurso '$($Resource.name)': $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Função para copiar arquivos para pasta de processamento
function Copy-FilesToBulkFolder {
    param(
        [string]$DownloadsPath,
        [string]$BulkPath
    )
    
    try {
        Write-Host "Copiando arquivos para pasta de processamento..." -ForegroundColor Yellow
        
        $files = Get-ChildItem -Path $DownloadsPath -File
        $copyCount = 0
        
        foreach ($file in $files) {
            $destination = [System.IO.Path]::Combine($BulkPath, $file.Name)
            Copy-Item -Path $file.FullName -Destination $destination -Force
            $copyCount++
        }
        
        Write-Host "Todos os arquivos foram copiados para '$BulkPath' ($copyCount arquivos)" -ForegroundColor Green
        return $true
        
    } catch {
        Write-Host "Erro ao copiar arquivos: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Função principal
function Start-DownloadProcess {
    Write-Host "Iniciando processo de download de arquivos financeiros..." -ForegroundColor Yellow
    Write-Host "-" * 70 -ForegroundColor Gray
    
    try {
        # Obter diretório do script
        $scriptDir = Get-ScriptDirectory
        
        # Definir caminhos
        if ([string]::IsNullOrEmpty($OutputPath)) {
            $dataCurta = (Get-Date).ToString("yyyy-MM-dd")
            $downloadsPath = [System.IO.Path]::Combine($scriptDir, "downloads", $dataCurta)
        } else {
            $downloadsPath = $OutputPath
        }
        
        $downloadsBulkPath = [System.IO.Path]::Combine($scriptDir, "downloads_bulk")
        $configFilePath = [System.IO.Path]::Combine($scriptDir, $ConfigPath)
        
        Write-Host "Caminho de downloads: $downloadsPath" -ForegroundColor Cyan
        Write-Host "Caminho de processamento: $downloadsBulkPath" -ForegroundColor Cyan
        Write-Host "Arquivo de configuração: $configFilePath" -ForegroundColor Cyan
        Write-Host "-" * 50 -ForegroundColor Gray
        
        # Criar diretórios se necessário
        if (-not (Test-Path -Path $downloadsPath)) {
            New-Item -ItemType Directory -Path $downloadsPath -Force | Out-Null
            Write-Host "Diretório de downloads criado: $downloadsPath" -ForegroundColor Green
        }
        
        if (-not (Test-Path -Path $downloadsBulkPath)) {
            New-Item -ItemType Directory -Path $downloadsBulkPath -Force | Out-Null
            Write-Host "Diretório de processamento criado: $downloadsBulkPath" -ForegroundColor Green
        }
        
        # Limpar pasta de processamento
        Get-ChildItem -Path $downloadsBulkPath -File | ForEach-Object {
            Remove-Item -Path $_.FullName -Force
        }
        Write-Host "Pasta de processamento limpa" -ForegroundColor Green
        
        # Obter formatos de data
        $dataFormatada = (Get-Date).ToString("dd/MM/yyyy")
        $dataCurta = (Get-Date).ToString("yyyy-MM-dd")
        $dataArquivo = (Get-Date).ToString("yyyyMMdd")
        $dataCurtaAno2Digitos = (Get-Date).ToString("yyMMdd")
        
        # Ler configuração
        $resources = Read-YamlConfig -Path $configFilePath
        
        # Processar cada recurso
        $successfulDownloads = 0
        $totalResources = ($resources | Where-Object { $null -ne $_.url }).Count
        
        Write-Host "Iniciando download de $totalResources recursos..." -ForegroundColor Yellow
        Write-Host "-" * 50 -ForegroundColor Gray
        
        foreach ($resource in $resources) {
            if (Process-Resource -Resource $resource `
                               -DataFormatada $dataFormatada `
                               -DataCurta $dataCurta `
                               -DataCurtaAno2Digitos $dataCurtaAno2Digitos `
                               -DataArquivo $dataArquivo `
                               -DownloadsPath $downloadsPath) {
                $successfulDownloads++
            }
            Write-Host "-" * 30 -ForegroundColor Gray
        }
        
        # Copiar arquivos para pasta de processamento
        Copy-FilesToBulkFolder -DownloadsPath $downloadsPath -BulkPath $downloadsBulkPath
        
        # Resumo final
        Write-Host "=" * 70 -ForegroundColor Gray
        Write-Host "Processo de download concluído!" -ForegroundColor Green
        Write-Host "Downloads bem-sucedidos: $successfulDownloads/$totalResources" -ForegroundColor Cyan
        
        if ($successfulDownloads -eq $totalResources) {
            Write-Host "Todos os downloads foram concluídos com sucesso!" -ForegroundColor Green
            return 0
        } else {
            Write-Host "Alguns downloads falharam. Verifique os erros acima." -ForegroundColor Yellow
            return 1
        }
        
    } catch {
        Write-Host "Erro crítico durante o processo: $($_.Exception.Message)" -ForegroundColor Red
        return 1
    }
}

# Executar função principal
$exitCode = Start-DownloadProcess
exit $exitCode
