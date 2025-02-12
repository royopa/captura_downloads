#Install-PackageProvider -Name NuGet -Force -Scope CurrentUser
Set-PSRepository PSGallery -InstallationPolicy Trusted
#Install-Module PowerShellGet -AllowClobber -Force -Scope CurrentUser -SkipPublisherCheck
#Install-Module -Name PSScriptAnalyzer -Force -Scope CurrentUser -AllowClobber
Install-Module -Name FXPSYaml -Scope CurrentUser -Force
Install-Module -Name SqlServer -Scope CurrentUser -Force
Install-Module -Name PsSqlClient -AllowPrerelease -Scope CurrentUser -Force -AllowClobber