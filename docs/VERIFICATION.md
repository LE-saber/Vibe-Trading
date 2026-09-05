# Verification evidence and limits

Execution date: 2026-09-05. Test results below are implementer verification; a separate fresh-context read-only review was also run before delivery.

## Executed successfully

- Original branch baseline: `python -m pytest -q`: **94 passed in 22.86 s**.
- Local integration regression with coverage: **138 passed, 88% in 38.48 s** on a clean Python 3.12 virtual environment. The final Python 3.13 coverage run also passed **138 tests, 89% in 38.72 s**. Coverage is not a safety proof or legacy-project coverage.
- `python -m compileall -q src tests scripts`: passed.
- `node --check src/vibe_trading/web/app.js`: passed.
- `PYTHONPATH=src python scripts/export_contracts.py`: exported 14 runtime schemas, including immutable `OperatorResolution` evidence.
- `python scripts/audit_write_entrypoints.py`: passed; the three ninef write paths exist only in `adapters/ninef.py`, and provider submit/cancel calls exist only in `ExecutionService`.
- `python -m pip wheel --no-deps --no-build-isolation --no-index . -w ...`: built installable wheel including Web assets.
- `.venv/Scripts/python scripts/browser_smoke.py`: **NETWORK_BROWSER passed in 12.48 s**, zero page errors, two real SSE connections and zero external broker writes. Checked login, top-30, own rank, news, progress, plan refresh/rehydration, consent, fixture fills, evidence and 390 px layout. The fresh Windows venv exposed a missing IANA timezone dependency; adding conditional `tzdata` fixed the clean-install failure.

Tested runtime: Windows, Python 3.12 and 3.13, FastAPI 0.128.8, Pydantic 2.13.5, Uvicorn 0.48.0, pytest 9.1.1, httpx 0.28.1 and Playwright 1.62.0 with Chromium 151. Python 3.13 intermittently printed a recoverable native access-violation diagnostic despite successful exit; it did not occur in the clean Python 3.12 run.

## Local ninef read verification

The 2026-09-04 local ninef release source and sanitized response structure were inspected. The final live read-only doctor run completed in **3.09 s**:

- account/context: `READ_AND_CONTRACT_OK`;
- rankings: `READ_AND_CONTRACT_OK`;
- news: `READ_AND_CONTRACT_OK`;
- buy/sell/cancel capability flags: false;
- write attempts: zero.

Anonymous adapter tests cover cursor pagination (including cursor-only continuation), redirect rejection, exact IDs, status normalization,
cumulative fill/fee mapping, position/cash/NAV semantics, independent own rank,
Top 30 truncation, news symbols/timestamps, strict market-input degradation and
single-attempt transport behavior. Provider and collection timestamps remain separate,
with only the existing two-second clock-skew allowance. No live values, raw IDs or credentials were
recorded in this repository.

## High-value adversarial coverage

Money/quantity/boolean validation, NAV consistency, source cutoff/dedup, unknown/partial order handling, T+1/sellable and fee/rule/quote gates, no silent liquidation, WATCH not authorizing buys, no projected-cash spending, explicit authenticated approval, CSRF/origin/host/login limits, duplicate concurrent starts, persistent account locks, outbox payload tampering, lost submission responses, restart before/after dispatch, cancellation uncertainty, full/partial fills, fee/position/order-snapshot drift, latest-plan invalidation and generic exception handling without raw secret echoes.

SSE **server framing and Last-Event-ID replay** were tested directly. That is distinct from browser network transport.

## Blocked or not executed

- Current ninef external writes were not attempted and remain unverified/disabled.
- The current local Vibe repository's S0-S6 route is only a loopback proxy. Its actual research implementation is not present there. The explicitly excluded project was not inspected or migrated, so a real `VIBE_RESEARCH_PROVIDER` remains required for a full non-DEMO run.
- Live quote structure did not provide an authoritative tradability/suspension permission field. Effective fee and instrument rules also require a separately verified local market profile; no production defaults were guessed.
- Long-run real-research profiling, static type checker and linter remain incomplete. A fresh-context Luna max read-only review found four execution/evidence boundary issues; all four were remediated, covered by regression tests and included in the final 138-test runs.
- GitHub Actions status is reported separately in the final delivery/PR; writing a workflow file is not a claim that CI passed.

## Release gate

Current level: **implemented + fixture-verified kernel, live-read-verified ninef facade, network-browser-verified UI**. It is not release-ready because real research, authoritative tradability/fee/rule inputs and external writes are not verified. Exact-evidence UNKNOWN recovery is implemented and tested. External simulation-account acceptance writes still require separate owner consent.
