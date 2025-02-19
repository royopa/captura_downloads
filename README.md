# Getting Started

O Python >=3.12 e o poetry >=2.0.0 e <3.0.0 são necessários para esse projeto.

# Configurar o ambiente

```sh
$ poetry install
```

# Inicializar o ambiente

```sh
$ poetry env activate
```

# Running tests

```sh
$ task test
```

# Running linter

```sh
$ task lint
```

# Running docs

```sh
$ task docs
```

# Applying Blue linter

```sh
$ blue .
```

# Applying Ruff linter

```sh
$ ruff format .
```

# Running PowerShell Scripts to Download Files

```sh
$ pwsh ./dependencias.ps1
```

```sh
$ pwsh ./download_files.ps1
```

# Running Python Scripts to Download Files

```sh
$ python ./download_files.py
```

# Running Python Scripts KYD to Download Files

```sh
$ python ./download_files_kyd.py
```

# Running Python Scripts Selenium to Download Files

```sh
$ python ./download_files_selenium.py
```

# Running all downloads

```sh
$ task start
```

# Running the importers to staging area

```sh
$ task processor
```


# Installing SQL Server via Docker

https://appsmith.hashnode.dev/running-mssql-server-in-your-mac-m1m2m3-using-docker

```sh
sudo docker pull mcr.microsoft.com/azure-sql-edge

```

```sh
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=DB_Password" -e "MSSQL_PID=Developer" -e "MSSQL_USER=SA" -p 1433:1433 -d --name=sql mcr.microsoft.com/azure-sql-edge
```

```sh
sudo npm install -g sql-cli
```

```sh
mssql -u sa -p d
```