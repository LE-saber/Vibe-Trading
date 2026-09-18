# Competition research package

This directory contains the point-in-time A-share research implementation used
by the canonical workbench. It owns Universe construction, S0-S5 screening,
EvidenceBundle generation, deterministic diversification and bounded AI review.

It is not an execution authority. Its migrated execution state-machine modules
remain only as fail-closed compatibility/test code; the runtime always uses a
disabled provider and cannot be enabled by environment variables. Portfolio
sizing, approval, order submission, cancellation and reconciliation belong to
`src/vibe_trading`.

## Development

Install from the repository root with `scripts/setup.ps1`, then run:

```powershell
Push-Location research
..\.venv\Scripts\python.exe -m pytest -q
Pop-Location
```

The canonical launcher imports the backend from
`src/vibe_trading/research/competition.py`. Direct research commands under
`scripts/` are for reproducible datasets and audits, not for placing orders.

## Data and secrets

Generated `data/`, `runs/`, `storage/`, caches and local environments are not
source files and must stay untracked. Tushare, ninef and model credentials are
read from the process environment or explicit local secret files and must never
be committed.

See [RUNBOOK.md](docs/RUNBOOK.md) for the maintained command surface.
