param (
    [string]$sqlFilePath
)

function Invoke-SqlCmd {
    param (
        [string]$sqlFilePath
    )

    # Caminho do arquivo .env na raiz do projeto
    $envFilePath = "$(Get-Location)\.env"

    # Carregar variáveis de ambiente do arquivo .env
    $envVars = Get-Content -Path $envFilePath | ForEach-Object {
        $parts = $_ -split "="
        New-Object PSObject -Property @{
            Name = $parts[0].Trim()
            Value = $parts[1].Trim()
        }
    }

    $envVars | ForEach-Object {
        if ($_.Name -eq "DB_SERVER") {
            $env:DB_SERVER = $_.Value
        } elseif ($_.Name -eq "DB_DATABASE") {
            $env:DB_DATABASE = $_.Value
        } elseif ($_.Name -eq "DB_USER") {
            $env:DB_USER = $_.Value
        } elseif ($_.Name -eq "DB_PASSWORD") {
            $env:DB_PASSWORD = $_.Value
        }
    }

    # Executa o script SQL usando sqlcmd
    $command = "sqlcmd -S $env:DB_SERVER -d $env:DB_DATABASE -U $env:DB_USER -P $env:DB_PASSWORD -i $sqlFilePath"
    Write-Host "Executando script SQL..."
    
    try {
        $output = Invoke-Expression $command 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Script SQL executado com sucesso!" -ForegroundColor Green
            Write-Host "Saída do banco de dados:"
            Write-Host $output -ForegroundColor Green
        } else {
            Write-Host "Erro ao executar o script SQL:" -ForegroundColor Red
            Write-Host $output -ForegroundColor Red
        }
    } catch {
        Write-Host "Erro ao executar o script SQL:" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
    }
}

# Executa a função Invoke-SqlCmd para criar as tabelas
Invoke-SqlCmd -sqlFilePath $sqlFilePath
