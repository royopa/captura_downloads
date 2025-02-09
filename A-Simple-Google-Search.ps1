# The line below will Import-Module Selenium if it fails it will display the 
# installation command and stop the script. 
Import-Module -Name Selenium

# Run Chrome with alternative download folder
$Driver = Start-SeEdgeDriver -DefaultDownloadPath /home/rodrigo/Downloads

# Run Chrome and go to a URL in one command
#$Driver = Start-SeChrome -StartURL 'https://www.google.com/ncr'

# Run Chrome with multiple Arguments
#$Driver = Start-SeChrome -Arguments @('Incognito','start-maximized')

# Run Chrome with an existing profile.
# The default profile paths are as follows:
# Windows: C:\Users\<username>\AppData\Local\Google\Chrome\User Data
# Linux: /home/<username>/.config/google-chrome
# MacOS: /Users/<username>/Library/Application Support/Google/Chrome

# Once we are done with the web driver and we finished with all our 
# testing/automation we can release the driver by running the Stop-SeDriver 
# command.
Stop-SeDriver -Driver $Driver