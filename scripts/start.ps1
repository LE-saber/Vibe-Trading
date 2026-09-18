[CmdletBinding()]
param(
    [string]$ListenHost = "127.0.0.1",
    [int]$Port = 8765,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
$envFile = Join-Path $root ".env"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Missing .venv. Run scripts\setup.ps1 first."
}
if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Missing .env. Copy .env.example to .env and fill local values."
}
if ($ListenHost -ne "127.0.0.1") {
    throw "Only loopback host 127.0.0.1 is supported."
}

$allowed = @("VIBE_", "NINEF_", "TUSHARE_", "RESEARCH_LLM_")
foreach ($line in Get-Content -LiteralPath $envFile -Encoding UTF8) {
    $text = $line.Trim()
    if (-not $text -or $text.StartsWith("#")) { continue }
    $pair = $text.Split("=", 2)
    if ($pair.Count -ne 2 -or -not ($allowed | Where-Object { $pair[0].StartsWith($_) })) {
        throw "Unsupported .env entry: $($pair[0])"
    }
    $value = $pair[1].Trim()
    if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
        ($value.StartsWith("'") -and $value.EndsWith("'"))) {
        $value = $value.Substring(1, $value.Length - 2)
    }
    [Environment]::SetEnvironmentVariable($pair[0], $value, "Process")
}

$env:VIBE_RESEARCH_PROVIDER = "vibe_trading.research.competition:create_backend"
if (-not $env:VIBE_RESEARCH_ROOT) { $env:VIBE_RESEARCH_ROOT = Join-Path $root "research" }
if (-not $env:VIBE_RESEARCH_RUNS_ROOT) { $env:VIBE_RESEARCH_RUNS_ROOT = Join-Path $root ".local\research-runs" }
# The normal launcher is research/read-only. Write enablement requires a separate,
# deliberately constructed operator launch after local verification.
$env:VIBE_ENABLE_NINEF_WRITES = ""

if (-not $NoBrowser) {
    Start-Process "http://$ListenHost`:$Port" | Out-Null
}
Push-Location $root
try {
    & $python -m vibe_trading.cli serve --mode local --provider examples.local_provider:create_provider --port $Port
} finally {
    Pop-Location
}
