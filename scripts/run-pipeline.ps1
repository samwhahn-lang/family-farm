# run-pipeline.ps1
# Runs bronze ingest only (NoaaIngestJob).
# On failure: sends email and exits with non-zero code.
# Scheduled via Task Scheduler — see register-task.ps1.

param()

$ProjectDir = "C:\Users\samwh\OneDrive\Documents\GitHub\Commodity"
$LogDir     = Join-Path $ProjectDir "logs"
$AppPwFile  = Join-Path $env:USERPROFILE ".credentials\gmail-app-pw.xml"
$FromEmail  = "samwhahn@gmail.com"
$ToEmail    = "samwhahn@gmail.com"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$LogFile = Join-Path $LogDir "pipeline-$(Get-Date -Format 'yyyy-MM-dd').log"

function Write-Log {
    param([string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $Message"
    Write-Output $line
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

function Send-FailureEmail {
    param([int]$ExitCode, [string]$LogTail)
    try {
        $pw   = Import-Clixml -Path $AppPwFile
        $cred = New-Object System.Management.Automation.PSCredential($FromEmail, $pw)
        $body = @"
Gage County bronze ingest failed on $(Get-Date -Format 'yyyy-MM-dd') at $(Get-Date -Format 'HH:mm:ss').

Exit code: $ExitCode

Last 30 log lines:
$LogTail

Full log: $LogFile
"@
        Send-MailMessage `
            -From       $FromEmail `
            -To         $ToEmail `
            -Subject    "Bronze Ingest FAILED -- $(Get-Date -Format 'yyyy-MM-dd')" `
            -Body       $body `
            -SmtpServer "smtp.gmail.com" `
            -Port        587 `
            -UseSsl `
            -Credential  $cred
        Write-Log "Failure email sent to $ToEmail."
    } catch {
        Write-Log "ERROR sending failure email: $_"
    }
}

Write-Log "=========================================="
Write-Log "Bronze ingest starting"
Write-Log "=========================================="

$env:NOAA_TOKEN = [System.Environment]::GetEnvironmentVariable("NOAA_TOKEN", "User")
if (-not $env:NOAA_TOKEN) {
    Write-Log "ERROR: NOAA_TOKEN is not set in user environment variables."
    Send-FailureEmail -ExitCode 1 -LogTail "NOAA_TOKEN missing — job aborted before sbt launch."
    exit 1
}

Set-Location $ProjectDir
Write-Log "Working directory: $ProjectDir"
Write-Log "Running: sbt runMain com.cornbelt.bronze.NoaaIngestJob"

$output   = & sbt "runMain com.cornbelt.bronze.NoaaIngestJob" 2>&1
$exitCode = $LASTEXITCODE

$output | ForEach-Object { Add-Content -Path $LogFile -Value $_.ToString() -Encoding UTF8 }
Write-Log "sbt exit code: $exitCode"

if ($exitCode -ne 0) {
    $tail = (Get-Content -Path $LogFile -Tail 30) -join "`n"
    Write-Log "Bronze ingest FAILED."
    Send-FailureEmail -ExitCode $exitCode -LogTail $tail
    exit $exitCode
}

Write-Log "Bronze ingest completed successfully."
Write-Log "Output: $ProjectDir\data\bronze\noaa_observations\"
