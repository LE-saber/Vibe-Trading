# Architecture

## System boundary

```text
Tushare + optional research sources
        ↓
Competition research (Universe, S0-S5, evidence, AI review)
        ↓  vibe.research.v1
Canonical workbench (account + quotes + fees + rules)
        ↓
S6 plan → exact-hash human approval → guarded execution → reconciliation
```

`src/vibe_trading` is the sole portfolio and execution authority. `research/`
does not receive an approval, does not size broker quantities for the canonical
application, and is not launched as a second Web trading service.

## Research stages

| Stage | Input | Output | AI |
| --- | --- | --- | --- |
| S0 | PIT Universe snapshot | eligible SSE/SZSE equities | no |
| S1 | qfq price, raw volume, liquidity and quality features | deterministic Top 300 | no |
| S2 | benchmark and limit-history supplements | deterministic Top 60 | no |
| S3 | immutable EvidenceBundle per candidate | deterministic Top 20 | no |
| S4 | correlations, board/cluster/overheat constraints | diversified 10–15 | no |
| S5 | frozen candidate evidence and bounded news context | independent schema-validated reviews | yes |
| S5.5 | fresh quote/tradability facts | workbench market blockers | no |
| S6 | research + fresh account + quote/rule/fee context | target quantities and ordered deltas | no |

The research adapter maps S4/S5 into `vibe.research.v1`. Candidate evidence and
news context are hash-bound. Existing holdings outside S4 remain explicit WATCH
targets unless the supplemental review contract supports a stronger action;
they are never silently liquidated.

## Data conventions

Daily A-share OHLC qfq uses `daily + adj_factor`. Original source volume is not
multiplied by the adjustment factor. Loaders retain declared units so share/lot
normalization remains explicit. Missing factors fail the qfq safety gate.

Universe snapshots record phase and market-data reference date. Preopen identity
uses the snapshot date while daily/liquidity uses the previous completed trading
day resolved from `trade_cal`; postclose uses the snapshot date.

## Account and S6

The canonical `AccountSnapshot` contains total assets, available/frozen cash,
positions with total/sellable/frozen quantities, and a complete order snapshot.
`TradingContext` adds quotes, effective fees, instrument rules, market state and
trading date. Decimal financial values and strict integer quantities are enforced.

The planner blocks stale or inconsistent accounts, open/unknown external orders,
missing holding reviews, stale quotes, suspension/untradability, missing price
bands, unverified fees/rules, T+1 sellability violations, position caps and cash
reserve breaches. Sell proceeds cannot fund later buys until fills reconcile.

## Approval and execution

Plans are immutable and content-hashed. Approval binds plan ID, hash, actor and
expiry. Execution persists a SENDING intent before network I/O, performs at most
one submission attempt, and never assumes acknowledgement is a fill. UNKNOWN
outcomes retain the account lock and require exact broker-ID evidence.

The default launcher disables external writes. DEMO uses synthetic data under
the same contracts but is visibly labelled and cannot replace real research.

## Optional sidecars

`research/services/market_runtime`, `ai_decision` and `qlib_research` are isolated
optional packages. Deterministic S0–S4 does not depend on TradingAgents or Qlib.
The canonical S5 path uses a bounded OpenAI-compatible direct call with no agent
tool loop.
