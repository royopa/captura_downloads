# Obtenha a data atual no formato "AAAA-MM-DD"
$dataAtual = (Get-Date).ToString("yyyy-MM-dd")

# Obtenha o diretório onde o script está localizado
$diretorioAtual = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent

# Defina o caminho da pasta downloads e da nova pasta
$pastaDownloads = [System.IO.Path]::Combine($diretorioAtual, "downloads")
$pastaNova = [System.IO.Path]::Combine($pastaDownloads, $dataAtual)

# Verifique se a pasta downloads existe
if (-not (Test-Path -Path $pastaDownloads)) {
    # Crie a pasta downloads se não existir
    New-Item -ItemType Directory -Path $pastaDownloads
}

# Verifique se a subpasta com a data atual já existe
if (-not (Test-Path -Path $pastaNova)) {
    # Crie a subpasta com a data atual se não existir
    New-Item -ItemType Directory -Path $pastaNova
    Write-Output "Pasta '$pastaNova' criada com sucesso!"
} else {
    Write-Output "A pasta '$pastaNova' já existe. Nenhuma ação foi realizada."
}
