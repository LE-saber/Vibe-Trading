# Research runbook

Run all commands from the repository root after `scripts/setup.ps1`.

## Canonical path

`scripts/start.ps1` starts the workbench and configures
`vibe_trading.research.competition:create_backend`. A Web research request then
prepares a point-in-time dataset and runs S0-S5. The workbench adds fresh account
and market context and creates S6; this package does not submit orders.

## Standalone reproducibility tools

The supported scripts are source-relative and write only to ignored local
artifact directories. Use `--help` before supplying a date, phase, manifest or
snapshot:

```powershell
.\.venv\Scripts\python.exe research\scripts\prepare-canonical.py --help
.\.venv\Scripts\python.exe research\scripts\run-research.py --help
.\.venv\Scripts\python.exe research\scripts\build-universe-snapshot.py --help
```

`vibe-review-worker.py` and `vibe-technical-worker.py` are compatibility command
names used by the bounded subprocess adapters. Their implementation and
dependencies are bundled in this repository; they do not import another Vibe
checkout.

## Verification

```powershell
Push-Location research
..\.venv\Scripts\python.exe -m pytest -q
Pop-Location
```

Optional packages under `services/` keep separate dependency manifests and are
not required by deterministic S0-S4 or by the default Web launcher.
