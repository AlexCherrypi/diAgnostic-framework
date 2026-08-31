# Start the diAgnostic core (dev mode) on 127.0.0.1:8765.
# Windows PowerShell 5.1 compatible.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root "backend\.venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    Write-Host "Backend venv not found at $python" -ForegroundColor Red
    Write-Host "Create it first:  py -3.14 -m venv $root\backend\.venv"
    Write-Host "Then install:     $python -m pip install -e `"$root\backend[dev]`""
    exit 1
}

Set-Location (Join-Path $root "backend")
Write-Host "Starting core: python -m diagcore (http://127.0.0.1:8765)" -ForegroundColor Cyan
& $python -m diagcore
exit $LASTEXITCODE
