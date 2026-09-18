[CmdletBinding()]
param([switch]$WithBrowser)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Missing .venv. Run scripts\setup.ps1 first."
}

function Assert-LastExitCode([string]$Step) {
    if ($LASTEXITCODE -ne 0) {
        throw "$Step failed with exit code $LASTEXITCODE"
    }
}

Push-Location $root
try {
    & $python -m pytest -q
    Assert-LastExitCode "Workbench tests"
    Push-Location (Join-Path $root "research")
    try {
        & $python -m pytest -q
        Assert-LastExitCode "Research tests"
    } finally { Pop-Location }
    & $python -m compileall -q src research
    Assert-LastExitCode "Python byte compilation"
    & node --check src/vibe_trading/web/app.js
    Assert-LastExitCode "Browser JavaScript syntax check"
    & $python scripts/audit_write_entrypoints.py
    Assert-LastExitCode "Write-entrypoint audit"
    & $python -m pip wheel --no-deps . -w dist
    Assert-LastExitCode "Wheel build"
    if ($WithBrowser) {
        & $python scripts/browser_smoke.py
        Assert-LastExitCode "Browser smoke test"
    }
} finally {
    Pop-Location
}
