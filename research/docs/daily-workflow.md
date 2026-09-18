# Daily competition workflow

The daily workflow policy is separate from the frozen S0-S6 research configuration. Updating the operating schedule therefore does not mutate or invalidate an existing research run's configuration hash.

## Schedule (Asia/Shanghai)

- Credential, service, clock and quote-source preflight: 08:45-09:00.
- Pre-open: 09:00-09:25.
- Intraday checks: 09:35, 10:30, 11:25, 13:05, 13:35, 14:15 and 14:35.
- Critical portfolio review: 13:35. Final decision: 14:15. Execution watchdog: 14:35.
- Internal execution completion target: 14:50; ordinary order cutoff: 14:57. The final seven minutes are emergency reconciliation buffer, not normal decision time.
- Post-close: from 15:05.
- Leaderboard checkpoints: pre-open, midday, tail and post-close.

The authoritative policy is `config/daily-workflow.yaml`. `GET /pipeline/daily-workflow` returns the validated policy and the current phase. It never submits an order. The 08:45 preflight must fail fast on missing or rejected Ninef credentials, unavailable dependencies, unsafe clock skew, or unusable quote freshness so that these issues are resolved before research begins.

## Decision states

The permitted order is:

`DATA_READY -> RESEARCH_READY -> PLAN_DRAFT -> ACCOUNT_BOUND -> QUOTE_BOUND -> APPROVED -> SENDING -> RECONCILED -> CLOSED`

An ambiguous submission must instead pass through:

`SENDING -> RECONCILIATION_REQUIRED -> RECONCILED`

There is no transition from `SENDING` directly to `CLOSED`. The configured policy is authoritative readback with no retry of an unknown submission.

## Read-only account and ranking checkpoints

With `NINEF_API_TOKEN` configured in the process:

```powershell
.\.venv\Scripts\python.exe scripts\capture-daily-status.py `
  --checkpoint preopen `
  --artifact-root runs\daily-status
```

The command reads the Ninef account, complete order history and competition leaderboard, writes an immutable checkpoint directory, and prints a bounded summary. It does not submit or cancel orders. `--token-file` can be used with a one-line credential file; credentials are never written to artifacts.

Each checkpoint contains:

- `account.json`
- `leaderboard.json`
- `competition-status.json`
- `run_manifest.json`

When the leaderboard provides total assets, the checkpoint compares them with the authoritative account total. Missing leaderboard assets are recorded as `UNAVAILABLE`, not inferred.

## Phase artifacts

Pre-open adds `preopen-analysis.json` and `risk-gates.json`. Intraday adds `intraday-decisions.json` and requires manual confirmation; an explicit empty trigger list generates no order intents. Post-close adds `pnl-attribution.json`, `postclose-summary.json`, `thesis-tracker.json`, `next-day-watchlist.json` and `daily-manifest.json`.

The current execution policy remains `MANUAL_CONFIRM`; automatic order submission is rejected by configuration validation.
