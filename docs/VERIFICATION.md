# Verification evidence and limits

Execution date: 2026-09-05. This is implementer self-verification, not an independent review.

## Executed successfully

- `python -m pytest -q`: **94 passed** (also rerun under coverage).
- `python -m pytest --cov=vibe_trading --cov-report=term-missing`: **94 passed; 90% displayed statement coverage**, 1,499 statements, 146 missed. Coverage is not a safety proof and is not legacy-project coverage.
- `python -m compileall -q src tests scripts`: passed.
- `node --check src/vibe_trading/web/app.js`: passed.
- `PYTHONPATH=src python scripts/export_contracts.py`: exported 13 runtime schemas.
- `python -m pip wheel --no-deps --no-build-isolation --no-index . -w ...`: built installable wheel including Web assets.
- `PYTHONPATH=src python scripts/browser_smoke.py --offline-bridge`: passed in Chromium with zero page errors. Actual HTML/JS and FastAPI executed through an in-process HTTP bridge. Checked login, top-30, own rank, news, polling progress, four-order plan, plan refresh/rehydration, consent, full batch fills, evidence viewer and 390px layout without horizontal overflow. Desktop/completed/mobile screenshots were generated and inspected.

Tested runtime: Python 3.13.5, FastAPI 0.128.2, Pydantic 2.13.4, Uvicorn 0.48.0, pytest 9.0.2, httpx 0.28.1, Playwright 1.57.0. Network package installation was unavailable; the existing environment and offline wheel builder were used.

## High-value adversarial coverage

Money/quantity/boolean validation, NAV consistency, source cutoff/dedup, unknown/partial order handling, T+1/sellable and fee/rule/quote gates, no silent liquidation, WATCH not authorizing buys, no projected-cash spending, explicit authenticated approval, CSRF/origin/host/login limits, duplicate concurrent starts, persistent account locks, outbox payload tampering, lost submission responses, restart before/after dispatch, cancellation uncertainty, full/partial fills, fee/position/order-snapshot drift, latest-plan invalidation and generic exception handling without raw secret echoes.

SSE **server framing and Last-Event-ID replay** were tested directly. That is distinct from browser network transport.

## Blocked or not executed

- Networked `python scripts/browser_smoke.py`: the server started, but Chromium navigation to loopback failed with `net::ERR_BLOCKED_BY_ADMINISTRATOR`. Browser network SSE, reconnection/cookie transport must be tested locally/CI. The offline bridge did not evade or disable browser policy; it tested a separate in-process UI boundary, explicitly stubbing EventSource and exercising polling.
- Current ninef reads/writes and current AI/research: unavailable in this environment. No credentials were requested or used; **zero external broker writes** were performed.
- Historical zip/Codex attachments: no readable text/mount, materialization failed with 403 user_mismatch. Existing local algorithms, exact old schemas and old test counts were not verified.
- Fresh-context independent review, Windows lock branch, full live-adapter lifecycle, current legacy write bypasses, long-run profiling, static type checker and linter: not completed here.
- GitHub Actions status is reported separately in the final delivery/PR; writing a workflow file is not a claim that CI passed.

## Release gate

Current level: **implemented + self-verified integration kernel/reference workflow**. Not release-ready for ninef. Local Codex must finish actual adapter/research/UI integration, real transport testing, write-entrypoint isolation and exact-evidence recovery; then an independent reviewer must approve before the local write switch is enabled. External simulation-account acceptance writes still require separate owner consent.
