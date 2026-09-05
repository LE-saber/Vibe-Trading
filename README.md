# Vibe-Trading

A local-first workbench connecting **research -> account-aware portfolio -> user approval -> execution -> reconciliation**, with visible research progress, competition rankings and news evidence.

**Status: tested integration kernel and usable Chinese Web UI with a deterministic DEMO provider. NOT a completed ninef migration, real AI analysis, or release-ready competition-account system.** The current local research/ninef adapter must be implemented and verified on the owner's machine. No production endpoint, fee schedule, instrument rule or legacy class signature has been guessed.

## Run the offline demonstration

Python 3.11+; this delivery was tested with Python 3.13.5.

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# Linux/macOS: source .venv/bin/activate
python -m pip install -e ".[dev]"
vibe-trading serve --mode demo
```

Open `http://127.0.0.1:8765`. The demonstration password is `demo`, overridable with `VIBE_UI_PASSWORD`. Start analysis, inspect buys/sells and evidence, explicitly approve, then watch accepted -> partially filled -> filled -> account reconciliation. The fixture makes no outbound network calls. Names, symbols, quotes, rules, fees, rankings, news and analysis are synthetic; they are not investment information.

The UI shows assets, cash, holdings, own rank, top-30, source-timestamped news, stage/item progress, plan/blocker review, approval, order/fill tracking, stop-future-dispatch and separately approved cancellation. Reload restores persisted state, including the latest refreshed plan. Closing a page does NOT stop an already authorized server-side batch.

## Integrate the CURRENT local system

Read `docs/LOCAL_CODEX_TASK.md` as an implementation brief, not another requirements questionnaire. Preserve existing valid stock-selection behavior; the fixture is not a replacement strategy. Implement the seam in `examples/local_provider.py` using `ports.py` and the runtime models. The example intentionally raises `NotImplementedError`; it does not claim to be a ninef adapter.

```bash
# Only after implementing the local adapter:
vibe-trading doctor --mode local --provider examples.local_provider:create_provider
# Set a separate VIBE_UI_PASSWORD of at least 12 characters locally.
vibe-trading serve --mode local --provider examples.local_provider:create_provider
```

Local-account execution is **off by default**. In addition to verified capabilities and a verification reference, it requires the local switch:

```text
VIBE_ENABLE_NINEF_WRITES=I_HAVE_VERIFIED_THE_LOCAL_ADAPTER
```

The switch is not user approval. Every batch still requires a fresh unblocked plan, exact-hash approval, an account lock and per-dispatch revalidation. Do not set it before adapter tests, legacy write-entrypoint isolation and independent review pass. An external simulation-account write is still a side effect and is not authorized by the migration task.

## Verify

```bash
python -m pytest -q
python -m compileall -q src
node --check src/vibe_trading/web/app.js
PYTHONPATH=src python scripts/export_contracts.py
python -m pip wheel --no-deps --no-build-isolation . -w dist
# Normal local browser/network proof:
python -m playwright install chromium
python scripts/browser_smoke.py
# Explicitly limited fallback, NOT real browser SSE/cookie transport:
PYTHONPATH=src python scripts/browser_smoke.py --offline-bridge
```

On Windows, editable installation makes `PYTHONPATH=src` unnecessary. See `docs/VERIFICATION.md` for actual results and blocked checks. A configured CI workflow is not proof that it has passed.

## Safety boundaries

No arbitrary buy/sell API is exposed. Missing current-holding reviews never imply liquidation. WATCH/REJECT cannot authorize increased exposure. Unrelated open/partial/unknown orders block plans rather than being guessed away. Available cash and sellable shares are net resources; frozen amounts are not subtracted twice. Sale proceeds cannot fund buys until exact fills and account cash reconcile.

Plans are immutable. Approval binds the exact plan hash, account/market/fee inputs and expiry. Refreshing a plan supersedes unstarted approvals; it does not silently stop an already running batch. Changed conditions require a new plan and consent, not invisible repricing/resizing. Durable SENDING precedes network I/O. Unknown submission outcomes retain the account lock and are never blindly retried or matched heuristically. Restart does not replay writes.

This release intentionally has no button to clear UNKNOWN locks. Exact broker evidence and an audited operator-resolution flow are needed. Deleting a database is not recovery. Only loopback, one owner and one backend process are supported. The trusted local administrator can alter code/data; hashes are not digital signatures. Unrelated legacy programs are not magically prevented from trading; their drift must be detected.

Only source and fabricated fixtures belong in this PUBLIC repository. Never commit tokens, cookies, environment files, `ninef.db`, captures, actual account data, personal ranking details or raw provider logs. Runtime SQLite holds private activity data and is excluded from Git.

## Structure

`models.py` contains versioned contracts; `planner.py` computes current/target/delta with fees and constraints; `reconciliation.py` normalizes order states; `execution.py` enforces approval and durable dispatch; `progress.py` records real work; `news.py` selects untrusted evidence; `service.py` joins research and cheap fresh-account replanning; `ports.py` isolates local integrations; `store.py` persists tasks/events/intents; `api.py` and `cli.py` serve the loopback application; `web/` is a build-free Chinese reference interface.

The independent decisions and product acceptance matrix are in `docs/ARCHITECTURE.md`. `stock-competition-platform` is excluded from implementation scope. The orchestrator methodology repository remains read-only.
