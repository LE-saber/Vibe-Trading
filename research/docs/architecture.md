# Architecture

## P1 shape

Competition Core is a separate, thin Python service. The initial dependency direction is deliberately small:

```text
HTTP client
    |
    v
competition_core.api  -->  competition_core.config  -->  config/competition.yaml
           |
           v
      contracts
```

`adapters/`, `services/`, `workflows/`, and `storage/` are reserved boundaries for later migration. They contain no Vibe-Trading imports and no P1 business logic.

## Runtime

The P1 competition window is `2026-08-31` through `2026-09-30`, advertised as 23 trading days, for `CN_A` across `SSE` and `SZSE`. The default runtime endpoint is `http://127.0.0.1:8910`. `python -m competition_core.api` loads and validates the YAML configuration before starting Uvicorn. The endpoint may be overridden only with `COMPETITION_RUNTIME_ENDPOINT`; other environment variables are ignored by the loader.

## Isolated sidecars after P7

The service boundary now has three independently installed sidecars. Market
Runtime uses the vn.py Python 3.13 environment, AI Decision uses the
TradingAgents Python 3.12 environment, and Qlib Research uses its own Python
3.12 environment. They communicate through localhost JSON or immutable
artifacts and never import one another's internal objects.

Canonical Parquet remains the research source of truth. The Qlib binary store
is a content-recorded derivative that can be rebuilt from a source hash and
cutoff. Qlib consumes existing qfq OHLC fields without changing their
adjustment logic and copies raw share volume without applying price factors.
Historical samples are inner-joined to the exact daily point-in-time Universe.

The online competition path does not depend on Qlib or TradingAgents being
available. Qlib produces versioned scores; AI Decision produces analysis-only
advice. Portfolio and order workflow remains a later phase.

## P8 workflow boundary

Daily workflows now produce one artifact directory per run:

```text
preopen:   Universe summary -> Top20 -> Evidence -> optional AI -> Top8
intraday:  fresh quotes -> risk checks -> TargetWeights -> Manual OrderIntent
postclose: close marks -> PortfolioState -> performance input
nightly:   data quality -> optional Qlib refresh
```

SQLite stores workflow status and the latest portfolio/target/intent index;
JSON/Parquet/CSV artifacts remain the audit body. Optional service failures are
recorded as `fallback_used`; malformed Evidence, stale realtime data, and
missing close marks fail closed because using a fallback would change facts.
