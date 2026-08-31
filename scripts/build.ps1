# Production build: typecheck + build the frontend into frontend\dist.
# The core serves dist\ at http://127.0.0.1:8765/ when it exists.
# Windows PowerShell 5.1 compatible.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$node = Join-Path $root "tools\node"

if (-not (Test-Path (Join-Path $node "npm.cmd"))) {
    Write-Host "Portable Node not found at $node" -ForegroundColor Red
    exit 1
}

$env:PATH = "$node;" + $env:PATH
Set-Location (Join-Path $root "frontend")

if (-not (Test-Path "node_modules")) {
    Write-Host "node_modules missing - running npm install first" -ForegroundColor Yellow
    & npm.cmd install
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Host "Building frontend (tsc -b + vite build)..." -ForegroundColor Cyan
& npm.cmd run build
if ($LASTEXITCODE -ne 0) {
    Write-Host "BUILD FAILED" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "BUILD OK -> $root\frontend\dist" -ForegroundColor Green
exit 0
