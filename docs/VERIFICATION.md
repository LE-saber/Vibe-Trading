# Verification

Run the release checks from the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

The script runs both Python test suites, byte-compiles source, syntax-checks the
browser JavaScript, audits execution write entrypoints and builds a wheel. Add
`-WithBrowser` only after installing Chromium with `scripts/setup.ps1
-WithBrowser`.

## Acceptance criteria

- clean install succeeds from an arbitrary path;
- root and research test suites pass;
- no hard-coded source-workspace path or credential is tracked;
- local startup binds only `127.0.0.1`;
- normal startup leaves all external writes disabled;
- a real research request can report S0-S5 progress and produce a schema-valid
  result; missing data/model/account facts are explicit blockers;
- S6 remains the only route from research output to a target portfolio.
- terminal evaluations and portfolio attempts remain shadow-only and cannot
  create an execution plan, approval, intent or order;
- terminal CLI commands operate without FastAPI and redact provider failures.

## Consolidation baseline

Verified on Windows with Python 3.13:

- canonical workbench: 159 passed;
- research: 1067 collected, 1066 passed and 1 intentionally skipped;
- Python byte compilation: passed;
- browser JavaScript syntax: passed;
- write-entrypoint audit: passed, no violations;
- wheel build: passed.

The terminal regression includes dynamic remaining-session derivation, immutable
evaluation persistence, imported Ninef account/order evidence, uncommitted S5.5
observations, 10/5/2–3 funnel behavior, mode-aware allocation, a real synthetic
shadow-plan attempt and explicit execution-plan rejection.

The remaining warning is Starlette's upstream deprecation of an AnyIO alias.
