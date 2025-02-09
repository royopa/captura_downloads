# Carregar a biblioteca FXPSYaml
Import-Module FXPSYaml

# Função para ler o conteúdo do YAML usando FXPSYaml
function Read-Yaml {
    param (
        [string]$Path
    )
    $yaml = Get-Content -Raw -Path $Path
    $resources = ConvertFrom-Yaml -YamlString $yaml
    return $resources.resources
}

# Função para substituir variáveis de data na URL
function Replace-DateVariables {
    param (
        [string]$url,
        [string]$dataFormatada,
        [string]$dataCurta,
        [string]$dataCurtaAno2Digitos,
        [string]$dataArquivo
    )
    $url = $url -replace "DD/MM/YYYY", $dataFormatada
    $url = $url -replace "YYYY-MM-DD", $dataCurta
    $url = $url -replace "YYYYMMDD", $dataArquivo
    $url = $url -replace "YYMMDD", $dataCurtaAno2Digitos
    return $url
}

# Função para salvar a resposta como arquivo
function Save-Response {
    param (
        [string]$url,
        [string]$destinationPath,
        [string]$typeResponse
    )
    
    switch ($typeResponse) {
        'json' {
            # Baixar e salvar como JSON
            Invoke-WebRequest -Uri $url -OutFile $destinationPath
            Write-Host "Arquivo JSON salvo em '$destinationPath'" -ForegroundColor Green
        }
        'base64' {
            # Baixar e salvar como Base64
            $responseString = Invoke-WebRequest -Uri $url -UseBasicParsing
            $content = $responseString.Content.Trim('"')  # Remove as aspas do início e do final, se existirem
            try {
                $decodedBytes = [System.Convert]::FromBase64String($content)
                [System.IO.File]::WriteAllBytes($destinationPath, $decodedBytes)
                Write-Host "Arquivo Base64 salvo em '$destinationPath'" -ForegroundColor Green
            } catch {
                Write-Host "Falha ao decodificar a string Base64." -ForegroundColor Red
            }
        }
        default {
            # Baixar e salvar como binário
            Invoke-WebRequest -Uri $url -OutFile $destinationPath
            Write-Host "Arquivo binário salvo em '$destinationPath'" -ForegroundColor Green
        }
    }
}

# Obtenha a data atual no formato "DD/MM/YYYY", "YYYY-MM-DD", "YYYYMMDD" e "YYMMDD"
$dataFormatada = (Get-Date).ToString("dd/MM/yyyy")
$dataCurta = (Get-Date).ToString("yyyy-MM-dd")
$dataArquivo = (Get-Date).ToString("yyyyMMdd")
$dataCurtaAno2Digitos = (Get-Date).ToString("yyMMdd")

# Obtenha o diretório onde o script está localizado
$diretorioAtual = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent

# Defina o caminho da pasta downloads e da nova pasta com a data atual
$pastaDownloads = [System.IO.Path]::Combine($diretorioAtual, "downloads", $dataCurta)
$pastaDownloadsBulk = [System.IO.Path]::Combine($diretorioAtual, "downloads_bulk")

# Verifique se a pasta downloads com a data atual existe, e crie se necessário
if (-not (Test-Path -Path $pastaDownloads)) {
    New-Item -ItemType Directory -Path $pastaDownloads -Force
}

# Verifique se a pasta downloads_bulk existe, e crie se necessário
if (-not (Test-Path -Path $pastaDownloadsBulk)) {
    New-Item -ItemType Directory -Path $pastaDownloadsBulk -Force
}

# Limpe a pasta downloads_bulk
Get-ChildItem -Path $pastaDownloadsBulk -File | ForEach-Object {
    Remove-Item -Path $_.FullName -Force
}

# Defina o caminho do arquivo resources.yaml
$caminhoYaml = [System.IO.Path]::Combine($diretorioAtual, "resources.yaml")

# Leia o conteúdo do arquivo YAML
$resources = Read-Yaml -Path $caminhoYaml

# Faça o download de cada recurso
foreach ($resource in $resources) {
    if ($null -ne $resource.url) {
        Write-Host "Processando recurso: $($resource.name)" -ForegroundColor Cyan
        Write-Host "URL original: $($resource.url)" -ForegroundColor Cyan

        # Substitua variáveis de data na URL
        $url = Replace-DateVariables -url $resource.url -dataFormatada $dataFormatada -dataCurta $dataCurta -dataCurtaAno2Digitos $dataCurtaAno2Digitos -dataArquivo $dataArquivo
        
        Write-Host "URL processada: $url" -ForegroundColor Cyan

        $nomeArquivo = if ($null -ne $resource.file_name) { "$dataArquivo" + "_" + $resource.file_name } else { "$dataArquivo" + "_" + [System.IO.Path]::GetFileName($url) }
        $caminhoDestino = [System.IO.Path]::Combine($pastaDownloads, $nomeArquivo)
        
        Write-Host "Nome do arquivo: $nomeArquivo" -ForegroundColor Cyan
        Write-Host "Caminho do arquivo: $caminhoDestino" -ForegroundColor Cyan
        Write-Host "Tipo de resposta: $($resource.type_response)" -ForegroundColor Cyan

        try {
            # Salve a resposta conforme o tipo
            Save-Response -url $url -destinationPath $caminhoDestino -typeResponse $resource.type_response
            Write-Host "Recurso '$($resource.name)' baixado com sucesso." -ForegroundColor Green
        } catch {
            Write-Host "Falha ao baixar o recurso '$($resource.name)' de '$url'." -ForegroundColor Red
        }
    }
}

# Copie todos os arquivos baixados para a pasta downloads_bulk
Get-ChildItem -Path $pastaDownloads -File | ForEach-Object {
    Copy-Item -Path $_.FullName -Destination $pastaDownloadsBulk
}

Write-Host "Todos os arquivos foram copiados para '$pastaDownloadsBulk'" -ForegroundColor Green
