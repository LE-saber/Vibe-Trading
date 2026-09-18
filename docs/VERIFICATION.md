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

## Consolidation baseline

Verified on Windows with Python 3.13:

- canonical workbench: 140 passed;
- research: 886 collected, 885 passed and 1 intentionally skipped;
- Python byte compilation: passed;
- browser JavaScript syntax: passed;
- write-entrypoint audit: passed, no violations;
- wheel build: passed.

The remaining warning is Starlette's upstream deprecation of an AnyIO alias.
