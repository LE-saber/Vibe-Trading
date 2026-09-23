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
- S6 follows the original portfolio and execution-plan path;
- the main research API exposes no terminal routes;
- terminal allocation contracts are not accepted by S6 or the portfolio runner;
- the IPO sprint route has its own input/output contracts, carries the
  `IPO_SPRINT` origin marker and cannot create a plan or order;
- IPO candidates must pass listing-window, risk, liquidity and current
  two-sided-book gates independently of S0-S6.

## Verified baseline

Verified on Windows with Python 3.13:

- canonical workbench: 140 passed;
- research package: 1032 collected; full suite passed with one intentional skip;
- IPO sprint collector/evaluator: 24 passed;
- original S0-S6 files match the pre-terminal-integration baseline;
- Python byte compilation and Git whitespace check passed.

The remaining warning is Starlette's upstream deprecation of an AnyIO alias.

## Focused regression

The detachment regression covers the existing S6 portfolio pipeline,
execution-plan validation and daily workflow schedule, together with the
absence of terminal routes on the main research API.
