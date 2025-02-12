#Install-PackageProvider -Name NuGet -Force -Scope CurrentUser
#Install-Module PowerShellGet -AllowClobber -Force -Scope CurrentUser -SkipPublisherCheck
#Install-Module -Name PSScriptAnalyzer -Force -Scope CurrentUser -AllowClobber
Install-Module -Name FXPSYaml -Scope CurrentUser -Yes
Install-Module -Name SqlServer -Scope CurrentUser -Force -Yes
Install-Module -Name PsSqlClient -AllowPrerelease -Scope CurrentUser -Force -AllowClobber -Yes