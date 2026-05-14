# setup-credentials.ps1
# Run this ONCE manually to store your Gmail App Password encrypted on disk.
# Encryption is tied to your Windows user account — only you can decrypt it.
#
# Before running this script, generate a Gmail App Password:
#   1. Go to myaccount.google.com -> Security -> 2-Step Verification -> App Passwords
#   2. Create one named "Commodity Pipeline"
#   3. Copy the 16-character password (no spaces)

$CredDir = Join-Path $env:USERPROFILE ".credentials"
$OutFile = Join-Path $CredDir "gmail-app-pw.xml"

New-Item -ItemType Directory -Force -Path $CredDir | Out-Null

$pw = Read-Host -Prompt "Enter Gmail App Password (16 chars, no spaces)" -AsSecureString
$pw | Export-Clixml -Path $OutFile

Write-Host ""
Write-Host "Saved to: $OutFile" -ForegroundColor Green
Write-Host "Only your Windows account ($env:USERNAME) can decrypt this file." -ForegroundColor Cyan
