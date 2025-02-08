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
        [string]$dataCurta
    )
    $url = $url -replace "DD/MM/YYYY", $dataFormatada
    $url = $url -replace "YYYY-MM-DD", $dataCurta
    $url = $url -replace "YYYYMMDD", $dataCurta -replace "-",""
    return $url
}

# Obtenha a data atual no formato "DD/MM/YYYY", "YYYY-MM-DD" e "YYYYMMDD"
$dataFormatada = (Get-Date).ToString("dd/MM/yyyy")
$dataCurta = (Get-Date).ToString("yyyy-MM-dd")
$dataArquivo = (Get-Date).ToString("yyyyMMdd")

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
        # Substitua variáveis de data na URL
        $url = Replace-DateVariables -url $resource.url -dataFormatada $dataFormatada -dataCurta $dataCurta
        
        $nomeArquivo = if ($null -ne $resource.file_name) { "$dataArquivo" + "_" + $resource.file_name } else { "$dataArquivo" + "_" + [System.IO.Path]::GetFileName($url) }
        $caminhoDestino = [System.IO.Path]::Combine($pastaDownloads, $nomeArquivo)
        
        try {
            # Baixe o arquivo
            Invoke-WebRequest -Uri $url -OutFile $caminhoDestino
            
            Write-Output "Arquivo '$nomeArquivo' baixado para '$caminhoDestino'"
        } catch {
            Write-Output "Falha ao baixar '$nomeArquivo' de '$url'."
        }
    }
}

# Copie todos os arquivos baixados para a pasta downloads_bulk
Get-ChildItem -Path $pastaDownloads -File | ForEach-Object {
    Copy-Item -Path $_.FullName -Destination $pastaDownloadsBulk
}

Write-Output "Todos os arquivos foram copiados para '$pastaDownloadsBulk'"
