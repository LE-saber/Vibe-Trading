# V2 scoring specification

Status: S0–S4 accepted at the scoped P10C/P10D/P10E gates.
The real P10B gate has passed; AI/quote/account/Web acceptance remains separate.
This baseline is an engineering choice, not a claim of predictive profitability.

## Common rules

Use only the frozen canonical panel and the exact-date eligible Universe.
Prices are qfq; traded volume and amount are raw, with explicit normalized units.
Do not fill suspension gaps, missing factors, flow or margin observations with zero.
Reject unusable history with a specific reason; keep valid optional missing fields
and renormalize weights over the available components. Never fabricate finalists.

Align each symbol to the frozen exchange session calendar before computing windows.
A missing/suspended day is not removed to turn a 20-session return into a return over
20 observed bars. Endpoint returns use the exact session dates; indicators requiring
a complete rolling price window are missing when that window has gaps. Never forward
fill suspension prices or silently let a skip-null mean stand in for the requested
complete window. The canonical quality report carries independently confirmed
suspension dates separately from unclassified source gaps.

Every ranking is stable: descending unrounded score, then ascending canonical
symbol. Store raw features, percentiles, component scores, effective weights,
missing fields and rejection reasons. Presentation rounding never selects stocks.

## S1 baseline

| Component | Configured weight | Implemented inputs |
|---|---:|---|
| Trend | 0.25 | MA5/10/20 alignment, close/MA20, MA20 slope |
| Momentum | 0.25 | 3/5/10/20-session qfq returns |
| Relative strength | 0.15 | cross-sectional relative returns |
| Liquidity | 0.15 | latest and 20-session median amount, turnover, volume ratio |
| Volatility quality | 0.10 | ATR14/close, 20-session volatility, 5/20-session drawdown |
| Data quality | 0.10 | historical coverage, valid factors, missingness, provenance |

Continuous inputs use cross-sectional percentiles from 0 to 100. Tied observations
receive average rank; a constant feature is neutral, not evidence of superiority.
Risk direction is explicit: lower volatility/drawdown is better for the volatility
quality component. Technical outputs include existing Vibe RSI14, MACD and
Bollinger calculations, reused as pure functions in Vibe's isolated environment.

`overheat_score` is a separate diagnostic using RSI, turnover, volume ratio,
acceleration and distance from MA20. RSI above 80 alone never deletes a stock.
S1 does not request money flow, margin, dragon-tiger, news or any LLM output.

The six component weights remain the plan baseline. Features within a component
are equal-weighted over available values; unavailable components are removed from
the denominator. Reject only a missing reference qfq close, fewer than60 valid
historical sessions (configured), or available component weight below0.60
(configured). These are research-quality gates, not a change to Universe membership.
Constant/single-observation features receive50, including a uniformly high RSI.
S1 percentiles use the history-qualified full eligible pool; S2 percentiles use S1's
qualified candidates. No rounding is applied before ranking or canonical-symbol ties.

`data_confidence = history_coverage * available_component_weight * feature_completeness`.
Feature completeness counts available numerical inputs in the active stage's score.
`source_confidence=1` denotes verified frozen-source integrity, not a claim that the
provider's economic data is infallible. Dataset corruption never receives that value.

Exact calculations (versioned by config and source-code hashes): momentum uses
session endpoints; acceleration5 is recent5-session return minus the preceding5.
MA alignment averages MA5>MA10 and MA10>MA20. MA20 slope is MA20/MA20_lag5-1.
ATR14 is the simple mean of14 true ranges divided by current qfq close; volatility20
is sample standard deviation of20 daily returns, not annualized. Drawdown5/20 uses
6/21 closes and a running peak. Amount median requires20 complete session values.
Vibe's unchanged helpers supply SMA, Wilder-EWM RSI, MACD and rounded Bollinger bands.
RSI/MACD use only the contiguous tail after the last gap (minimum15/35 sessions);
the tail length is recorded. SMA/Bollinger windows must themselves be complete.

## S2 refinement

Add 20/60-session relative strength, trend consistency, breakout quality,
drawdown recovery, volume-price confirmation, gap behavior, recent limit states
and a dated benchmark-relative return. Sector-relative signals require reliable
dated sector membership; otherwise record missing and do not infer an industry
from a stock name. Price-limit states require actual rule/price-limit metadata,
not an assumed universal 10% rule. S2 also makes zero LLM calls.

S2 retains the six configurable component weights and expands their inputs:
trend adds20-session close-above-MA20 consistency and close/prior20-session high-1;
relative strength adds20/60-session gross-return ratios to the dated benchmark;
liquidity adds5-session signed-return-volume balance (an activity proxy, not fund flow);
volatility quality adds mean absolute20-session opening gap and close position in the
20-session closing-price range (a flat range is neutral0.5). Cross-sectional relative
strength is stock return minus the full-Universe median return. These are baselines,
not independent sources of proven predictive information.

Default benchmark is000300.SH, separately typed as an index, never admitted as an
equity candidate. Historical index_daily and5 dated whole-market stk_limit batches
are frozen with requests/hashes/observations; short pages are continued to explicit
empty termination. Source docs: [index daily](https://tushare.pro/document/1?doc_id=95),
[daily price limits](https://tushare.pro/document/2?doc_id=183). Index return is a
price-index comparator, not a dividend-reinvested total-return benchmark.
Recent limit counts require all5 actual dated limit prices and observed raw closes;
latest flags need the reference-day pair. Unavailable/unbounded bands stay unknown.
There is no assumed10% reconstruction. Limit-up count contributes to the separate
S2 overheat diagnostic, not an automatic score-based rejection. No reliable dated
sector data is available yet, so sector-relative values remain null explicitly.

## S3 and S4

S3 attaches dated source evidence to S2 candidates. Complete successful queries
with no event and failed queries are different states. Evidence failures are
isolated and reduce confidence transparently; they must not become a zero flow
or zero margin observation. Any evidence-based adjustment must be bounded,
versioned and saved separately from S1/S2 scores.

S3 v1.2 uses exactly the final five completed sessions. It requests five whole-market
`moneyflow` batches, five `margin_detail` batches and one reference-day `top_list`
batch, with pagination to empty termination and local canonical-symbol joins.
The large+extra-large net proxy is `(buy_lg-sell_lg+buy_elg-sell_elg)*10000 CNY`.
It is not substituted for the separately retained vendor `net_mf_amount`, nor
represented as identically defined Eastmoney main flow. See the official
[moneyflow definitions](https://tushare.pro/document/2?doc_id=170).
All four legs are required; the five-session sum and flow/actual-amount ratio require
all five sessions. Signed and zero net flow are valid, missing legs are not zero.

The separate selection baseline is85% original S2 QuantScore,10% cross-sectional
five-session net-proxy/amount percentile,5% Evidence coverage quality. Missing flow
removes its weight and the remaining weights are renormalized; QuantScore itself
does not change. `evidence_confidence=min(S2.data_confidence,mean(flow,margin,dragon
coverage))`; quality score is100 times that value. Source coverage is a measurement
availability diagnostic, not an economic zero or a claim of predictive success.
Require original S2 data confidence>=0.50. This is a configured research-quality gate,
not a change to Universe eligibility. Do not pad the selected20 when fewer qualify.

No dragon-tiger row after a successful whole-market query means `listed=false`;
source failure means `listed=null`. Multiple reasons/windows are retained but their
net amounts are not added, avoiding double-counted1-day/3-day activity. See
[top_list fields](https://tushare.pro/document/2?doc_id=106). Margin CNY balances and
share quantities retain the vendor units; missing coverage does not assert an
ineligible stock or zero financing. The five-day financing-net-buy sum requires all
buy/repay pairs. The [margin_detail publication description](https://tushare.pro/document/2?doc_id=59)
places prior-day publication around08:30 the next morning. Requests whose research
as_of predates that boundary are frozen as SOURCE_NOT_PUBLISHED without fetching.
Passing this earliest-time gate is not a guarantee that a delayed source is available.

Every S2 candidate gets a content-addressed Core EvidenceBundle plus a run-local
body and per-stock checkpoint, even when not selected into20. Missing fields and
their source/date/reason survive agent-context loss. Latest daily_basic fields are
copied from the verified reference-day canonical row; news and unverified sector
data stay explicitly unavailable. There is no LLM at S3.

## S4 diversified shortlist (v1)

S4 keeps QuantScore and S3 selection_score unchanged. The default target remains12.
It consumes only selected S3 rows, the frozen Universe and verified canonical panel.
No new provider request, Agent Tool call or LLM is involved.

- Exact60 daily qfq close returns need61 completed exchange-calendar price sessions.
  Missing bars are not filled or bridged. Pairwise Spearman uses average tied ranks,
  at least40 shared valid returns, and nonconstant series; unknown correlation is null.
- Edges at absolute correlation≥0.85 form deterministic connected components.
  Both positive and negative high correlations are flagged, as required by the plan.
- Greedily scan S3 selection_score descending, symbol ascending as tie-breaker.
  Keep at most2 from each connected component,3 from each verified sector,6 with
  overheat_score≥80, and at most the configured target. Unknown self/pair correlation
  or missing overheat cannot be treated as independence/cool style.
- Sector is used only from optional frozen `sector_metadata` containing nonempty
  `value`, `classification`, `source`, and nonfuture timezone-aware `as_of`. Sector
  grouping includes source and classification; different taxonomies are not merged.
  Current Universe has no such data: report it missing, never infer it from names,
  board, model output or a current stock_basic request. Board stays separate metadata.
- Original turnover, RSI, volume ratio,5D momentum and MA distance remain visible
  alongside the already-computed overheat_score. No predictive model is introduced.
- Record every candidate's selection/rejection reasons, comparison to already selected
  stocks, cluster, style/sector information and naive TopN counterfactual. Constraints
  are never relaxed to fill a shortage.10–target is scoped SUCCESS; fewer than10 is
  PARTIAL. Missing sectors mean sector diversification is unverified, not achieved.

Full correlation/count matrices, high-correlation pairs, missing pairs and clusters
are in S4 JSON. Selected rows also persist as Parquet. This is candidate diversity,
not portfolio weights, orders or a profitability claim.

S4 computes pairwise Spearman correlation of aligned 60-session qfq returns,
requiring the configured minimum overlap (baseline 40). Absolute correlation
at least 0.85 marks redundancy; missing pairs remain unknown. Select 10–15
finalists subject to configured cluster, reliable-sector and overheat limits.
Save both correlation evidence and the exact reason for each exclusion. Do not
silently relax constraints merely to reach twelve names.

## AI and account boundary

AI may add evidence-grounded KEEP/WATCH/EXCLUDE/REVIEW judgments after S4.
It does not overwrite quantitative scores or invent additional symbols.
Account-bound allocation is a separate transparent heuristic, not an optimizer
with invented expected returns. Neither this document nor a ranked list supplies
an actual account state or authorizes trading.
