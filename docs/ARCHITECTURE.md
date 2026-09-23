# Architecture

## System boundary

```text
Tushare + optional research sources
        ↓
Competition research (Universe, S0-S5, evidence, AI review)
        ↓ vibe.research.v1
Canonical workbench (account + quotes + rules)
        ↓
S6 → approval → execution → reconciliation

Independent IPO sources → IPO_SPRINT snapshot → candidates/rejected JSON
                              (never enters S0-S6 or execution)
```

`src/vibe_trading` is the sole portfolio and execution authority. `research/`
does not receive an approval, does not size broker quantities for the canonical
application, and is not launched as a second Web trading service.

The previously added terminal-strategy integration is disabled. The main
research API does not mount terminal routes, there is no CLI entrypoint, and
terminal calculations do not feed S6, the portfolio builder, approval, or
execution. The normal research and S6 path therefore retains its original
contracts and behavior.

`competition_core.ipo_sprint` is a separate read-only route for newly listed
stocks in their exchange-rule-sourced unbounded sessions. It collects its own
identity, calendar, daily/liquidity, announcement risk review and live level-1
facts. It writes an independent snapshot/result pair tagged `IPO_SPRINT` and
does not update a research run, candidate stage, portfolio attempt or plan.

## IPO sprint operational stages

```text
new_share + stock_basic + trade_cal
        ↓ identity and unbounded-session screen
daily + daily_basic + anns_d + fresh level-1 quote
        ↓ permanent risk, liquidity and tradability gates
ranked IPO_SPRINT candidates
        ↓ repeated snapshots at separate observation times
operator-maintained pre-observation pool
        ↓ full recollection immediately before the decision
final heavy-position candidates (human decision only)
```

The collector and evaluator automate the first ranked candidate result and the
fresh pre-trade gate. A pre-observation pool is formed by comparing consecutive
standalone result files; it is not written into S4/S5 or represented as an S6
portfolio. The final heavy-position decision additionally depends on the live
award-line gap, remaining sessions, account position limits and T+1 exposure.
Those account-aware decisions remain outside `IPO_SPRINT`, which deliberately
cannot create plans or orders. See
[`research/docs/IPO_SPRINT_GUIDE_ZH.md`](../research/docs/IPO_SPRINT_GUIDE_ZH.md)
for the operator workflow.

## Disabled terminal-strategy experiment

The terminal state machine selects `PROTECT`, `NORMAL`, `ATTACK` or
`LAST_SHOT` from the buffered predicted award-line gap and remaining confirmed
sessions. A null award cutoff or rule source produces `UNAVAILABLE`; the system
never treats a displayed Top 30 as proof that 30 places receive awards.

Candidates are narrowed deterministically from S4/S5 and fresh market facts to
at most 10 watch, 5 focus and 2–3 aggressive-sleeve candidates. Protect may have
no aggressive candidates. Quote freshness, identity, suspension, price-lock,
liquidity, evidence, account completeness and unknown-order gates are permanent
in every mode. The configured competition calendar is immutable for the event;
the runtime derives sessions on or after its explicit Shanghai observation date
and rejects holidays or dates outside the confirmed window.

The module retains deterministic mode and candidate calculations for internal
research use. It has no public API or command-line entrypoint and does not build
portfolio or execution artifacts.

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
