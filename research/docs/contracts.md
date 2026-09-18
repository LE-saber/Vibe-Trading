# Contracts

## Health and version

Both `GET /health` and `GET /version` return the same stable JSON shape:

```json
{
  "service": "competition-core",
  "status": "ok",
  "version": "0.1.0"
}
```

These endpoints are intentionally read-only and do not require credentials.

## Configuration

`config/competition.yaml` contains the non-secret competition settings:

- `competition.start_date` and `competition.end_date`: ISO dates, with end on or after start; P1 uses `2026-08-31` through `2026-09-30`;
- `competition.advertised_trading_days`: a positive integer; P1 advertises `23`;
- `competition.initial_capital`: a finite number greater than zero;
- `competition.trading_market`: `CN_A` in P1;
- `competition.exchanges`: exactly `SSE` and `SZSE` in P1;
- `competition.allowed_boards`: a non-empty list of unique board identifiers;
- `competition.default_cash_ratio`: a finite value from `0` through `1`;
- `competition.candidate_limits`: a non-empty mapping of names to positive integers;
- `runtime.endpoint`: an `http` or `https` URL with a port.

Only `COMPETITION_RUNTIME_ENDPOINT` can override a YAML value. Capital, dates, boards, candidate limits, and all secrets are not read from environment variables.

## CanonicalSymbol

`CanonicalSymbol` is the single symbol identity owned by Competition Core. Its
only core spelling is the strict six-digit form `<code>.<suffix>`, where the
suffix is `SH` or `SZ`; for example, `000977.SZ`. Inputs are not trimmed,
case-folded, or guessed.

The immutable JSON contract contains these primitive fields:

```json
{
  "symbol": "000977.SZ",
  "code": "000977",
  "exchange": "SZSE",
  "market": "CN_A",
  "board": "MAIN",
  "currency": "CNY",
  "lot_size": 100
}
```

Only Shanghai and Shenzhen A-share code families are accepted. `SH` maps to
`SSE` and `SZ` maps to `SZSE`; code prefixes also determine `MAIN`, `STAR`, or
`CHINEXT`. Beijing symbols, ETFs/funds/indices/bonds, and unrecognized formats are
rejected. Any explicit exchange that disagrees with an embedded suffix or
prefix is rejected fail-closed.

Provider conversion is explicit and bidirectional:

| Provider | Example | Core result |
| --- | --- | --- |
| Tushare | `000977.SZ` / `600000.SH` | unchanged |
| Qlib | `SZ000977` / `SH600000` | `000977.SZ` / `600000.SH` |
| vn.py | `000977.SZSE` / `600000.SSE` | `000977.SZ` / `600000.SH` |

Use `CanonicalSymbol.from_provider(value, provider)` to parse and
`canonical.to_provider(provider)` to format. `to_dict()` or `to_json()` gives
the JSON representation; no provider-specific spelling may become the core
identity.

## Universe and candidates

`GET /universe/snapshot?date=YYYY-MM-DD&phase=preopen|postclose` returns the
complete deterministic snapshot, including `snapshot_id`, phase,
`market_data_reference_date`, member exclusion evidence, field provenance,
liquidity units, source endpoints, and warnings.

For `preopen`, identity/status/suspension are evaluated for the requested date
while `daily` liquidity uses the previous completed SSE trading day. For
`postclose`, liquidity uses the requested date. A preopen snapshot fails closed
if current-day suspension evidence is unavailable.

`GET /candidates` accepts `sort_by=amount|turnover|volume_ratio|momentum_5d|momentum_10d|momentum_20d`
and returns at most the configured shortlist limit. Ranking is deterministic;
all response symbols are Core canonical IDs.

`GET /symbols/{symbol}` validates and returns the Core-owned identity. It does
not query an LLM or guess an exchange.

## EvidenceBundle

Evidence is a content-addressed, schema-versioned record with one canonical
symbol, timezone-aware bundle `as_of`, Universe `snapshot_id`, eight explicit
sections (`market`, `technical`, `flow`, `margin`, `dragon_tiger`,
`fundamental`, `events`, `risk`), `missing`, and field-level provenance.

Every non-null leaf must have a unique `source` and timezone-aware `as_of` not
later than the bundle. NaN/infinity, future provenance, a valued field also
listed as missing, an unproven value, or a tampered `evidence_id` is rejected.
Numeric zero remains a value and is never converted to missing.

SQLite stores the canonical symbol/evidence index; immutable JSON files store
the complete body. This permits Agent/tool messages to be compacted while
RSI, MACD, flow, missingness, and source timestamps remain retrievable by ID.

The Vibe ingestion adapter accepts only an already-compressed direct-runner
envelope. Each non-empty section must explicitly provide `values`, `source`,
and `as_of`; arbitrary raw payloads and unknown sections are rejected rather
than guessed or silently mixed.

## Qlib research artifacts

The Canonical Parquet input uses one row per canonical symbol and trading date.
It must contain raw `open/high/low/close`, existing
`open_qfq/high_qfq/low_qfq/close_qfq`, raw `volume` in shares, and
`adj_factor`. Conversion does not compute a new qfq series and does not modify
volume. The derived Qlib store keeps qfq fields under Qlib's standard OHLC
names and also stores `raw_*`, `adj_factor`, `factor`, and `tradable`.

The PIT Universe artifact has `snapshot_date`, canonical `symbol`, and
`is_eligible`. Every historical factor/label row is joined to the exact same
date; a current-day Universe is never retroactively applied to history.

Research requests define non-overlapping train/valid/test windows,
`training_cutoff`, `inference_as_of`, and `competition_start`. The contract
rejects any training cutoff on or after competition start. Labels are future
1D/3D/5D close returns, while rolling training uses a horizon-sized embargo so
the label is complete before the prediction date.

Competition score records contain canonical `symbol`, `prediction`, `rank`,
`horizon`, `model_version`, `as_of`, the effective training window, and
validation metrics. Each run writes JSON/Parquet/CSV plus a manifest containing
the source hashes, request, conversion metadata, and framework version.

## Portfolio and order intent

`TargetWeights` is long-only. Cash plus all symbol weights must equal one,
every symbol is canonical, and no name may exceed `max_single_weight`. It is
content-addressed as `target_id` and always carries its Universe `snapshot_id`,
source, as-of, and `MANUAL_CONFIRM` execution mode.

`OrderIntent` is derived from a target and a fresh realtime reference price.
It contains BUY/SELL, whole-lot quantity, target weight, maximum slippage,
target ID, and reason. The contract has no submitted/filled state and rejects
any execution mode other than `MANUAL_CONFIRM`.

`PortfolioState` is a read-only mark-to-market snapshot containing cash,
canonical long-only positions, total assets, initial capital, return, source,
and timezone-aware as-of. Core exposes latest state/target/intents through GET
only; workflows and explicit account synchronization own writes.
