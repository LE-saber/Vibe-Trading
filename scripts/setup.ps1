[CmdletBinding()]
param([switch]$WithBrowser)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$venv = Join-Path $root ".venv"
$python = Join-Path $venv "Scripts\python.exe"

if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    & py -3.13 -m venv $venv
}
& $python -m pip install --upgrade pip
& $python -m pip install -e "${root}[research,dev]"
if ($WithBrowser) {
    & $python -m playwright install chromium
}
Write-Host "Setup complete: $python"
Write-Host "Copy .env.example to .env and fill local secrets before real research."
