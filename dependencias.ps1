#Install-PackageProvider -Name NuGet -Force -Scope CurrentUser
#Install-Module PowerShellGet -AllowClobber -Force -Scope CurrentUser -SkipPublisherCheck
Install-Module -Name FXPSYaml -Scope CurrentUser
Install-Module -Name SqlServer -Scope CurrentUser -Force
Install-Module -Name PsSqlClient -AllowPrerelease -Scope CurrentUser -Force -AllowClobber
Install-Module -Name PSScriptAnalyzer -Force -Scope CurrentUser -AllowClobber