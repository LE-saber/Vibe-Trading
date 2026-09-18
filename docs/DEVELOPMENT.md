# Development

Use Python 3.13 and the repository-local `.venv` created by
`scripts/setup.ps1`. The root package owns the Web/API and all account,
portfolio, approval and execution contracts. `research/` is independently
testable and is imported through one adapter.

## Change boundaries

- Keep deterministic S0-S4 free of model calls.
- Keep prices, volumes, dates and provenance explicit; do not fill missing facts
  with assumptions.
- Do not add a second order-writing adapter under `research/`.
- Any change to a plan must create a new content hash and invalidate approval.
- Treat news, model output and remote payloads as untrusted data.
- Never add local runs, databases, account exports or secrets to fixtures.

## Commands

```powershell
.\.venv\Scripts\python.exe -m pytest -q
Push-Location research
..\.venv\Scripts\python.exe -m pytest -q
Pop-Location
.\.venv\Scripts\python.exe -m compileall -q src research
node --check src/vibe_trading/web/app.js
.\.venv\Scripts\python.exe scripts/audit_write_entrypoints.py
```

There is no separate configured lint or static type-check tool in this baseline.
`scripts/verify.ps1` is the release verification command and also builds a wheel.
