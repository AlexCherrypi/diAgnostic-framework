# Full test gate: backend pytest + frontend typecheck/build.
# Prints a clear PASS/FAIL summary; exit code 0 only if everything is green.
# Windows PowerShell 5.1 compatible (no && chains).

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root "backend\.venv\Scripts\python.exe"
$node = Join-Path $root "tools\node"

$backendOk = $false
$frontendOk = $false

# -- Backend: pytest ---------------------------------------------------------
Write-Host ""
Write-Host "=== Backend: pytest ===" -ForegroundColor Cyan
if (-not (Test-Path $python)) {
    Write-Host "Backend venv not found at $python" -ForegroundColor Red
} else {
    Set-Location (Join-Path $root "backend")
    & $python -m pytest -q
    if ($LASTEXITCODE -eq 0) { $backendOk = $true }
}

# -- Frontend: typecheck + build --------------------------------------------
Write-Host ""
Write-Host "=== Frontend: typecheck + build ===" -ForegroundColor Cyan
if (-not (Test-Path (Join-Path $node "npm.cmd"))) {
    Write-Host "Portable Node not found at $node" -ForegroundColor Red
} else {
    $env:PATH = "$node;" + $env:PATH
    Set-Location (Join-Path $root "frontend")
    if (-not (Test-Path "node_modules")) {
        Write-Host "node_modules missing - running npm install first" -ForegroundColor Yellow
        & npm.cmd install
    }
    # "npm run build" runs tsc -b (typecheck) and vite build.
    & npm.cmd run build
    if ($LASTEXITCODE -eq 0) { $frontendOk = $true }
}

# -- Summary -----------------------------------------------------------------
Write-Host ""
Write-Host "=== Summary ===" -ForegroundColor Cyan
if ($backendOk) {
    Write-Host "backend  : PASS" -ForegroundColor Green
} else {
    Write-Host "backend  : FAIL" -ForegroundColor Red
}
if ($frontendOk) {
    Write-Host "frontend : PASS" -ForegroundColor Green
} else {
    Write-Host "frontend : FAIL" -ForegroundColor Red
}

if ($backendOk -and $frontendOk) {
    Write-Host "ALL GREEN" -ForegroundColor Green
    exit 0
} else {
    Write-Host "TESTS FAILED" -ForegroundColor Red
    exit 1
}
