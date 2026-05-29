# Personal Finance Assistant - Windows PowerShell Start Script
param(
    [switch]$Help
)

if ($Help) {
    Write-Host "Usage: .\start.ps1 [-Help]"
    exit 0
}

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $ScriptDir

$Port = if ($env:PORT) { $env:PORT } else { "8080" }
$BindHost = if ($env:HOST) { $env:HOST } else { "0.0.0.0" }
$PythonBin = Join-Path $ScriptDir ".venv\Scripts\python.exe"

if (-not (Test-Path $PythonBin)) {
    $PythonBin = "python"
    Write-Warning "No .venv found, falling back to system python"
}

New-Item -ItemType Directory -Force -Path "logs" | Out-Null

Write-Host ""
Write-Host "========================================"
Write-Host "  Starting Personal Finance Assistant"
Write-Host "========================================"

$PidFile = "logs\app.pid"
if (Test-Path $PidFile) {
    $OldPid = Get-Content $PidFile -ErrorAction SilentlyContinue
    if ($OldPid) {
        try {
            Stop-Process -Id ([int]$OldPid) -Force -ErrorAction SilentlyContinue
            Write-Host "Stopped old process (PID: $OldPid)"
        }
        catch {
            # ignore
        }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

$Occupied = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($Occupied) {
    $OccPid = $Occupied[0].OwningProcess
    Write-Warning "Port $Port is in use (PID: $OccPid), releasing..."
    Stop-Process -Id $OccPid -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "  Dir:    $ScriptDir"
Write-Host "  Listen: ${BindHost}:${Port}"
Write-Host "  Python: $PythonBin"
Write-Host ""

$env:PYTHONPATH = "$ScriptDir;$env:PYTHONPATH"
$env:FLASK_APP = "app.py"
$env:USE_RELOADER = "false"
$env:PYTHONIOENCODING = "utf-8"

$LogFile = Join-Path $ScriptDir "logs\app.log"
$ErrFile = Join-Path $ScriptDir "logs\app_err.log"
$Process = Start-Process `
    -FilePath $PythonBin `
    -ArgumentList "app.py" `
    -WorkingDirectory $ScriptDir `
    -RedirectStandardOutput $LogFile `
    -RedirectStandardError $ErrFile `
    -PassThru `
    -WindowStyle Hidden

$Process.Id | Set-Content $PidFile
Write-Host "[OK] Process started (PID: $($Process.Id))"

Write-Host -NoNewline "Waiting for service"
$Started = $false
$i = 0
while ($i -lt 15) {
    Start-Sleep -Seconds 1
    Write-Host -NoNewline "."
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:$Port/health" -UseBasicParsing -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($resp -and $resp.StatusCode -eq 200) {
            $Started = $true
            break
        }
    }
    catch {
        # not ready yet
    }
    $i++
}

Write-Host ""
if ($Started) {
    Write-Host "[OK] Service started successfully!"
}
else {
    Write-Warning "Service may still be starting. Check logs\app.log"
}

Write-Host ""
Write-Host "========================================"
Write-Host "  Status"
Write-Host "========================================"
Write-Host "  PID:     $($Process.Id)"
Write-Host "  URL:     http://localhost:$Port"
Write-Host "  Health:  http://localhost:$Port/health"
Write-Host ""
Write-Host "Commands:"
Write-Host "  View log:  Get-Content logs\app.log -Wait"
Write-Host "  Stop:      .\stop.ps1"
Write-Host "  Status:    .\status.ps1"