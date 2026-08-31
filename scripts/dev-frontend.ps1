# Start the Vite dev server on http://localhost:5173 (proxies /ws + /api to the core).
# Uses the portable Node install; Windows PowerShell 5.1 compatible.

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

Write-Host "Starting Vite dev server (http://localhost:5173)" -ForegroundColor Cyan
& npm.cmd run dev
exit $LASTEXITCODE
