# V2 pipeline data contract

## Context

Every output carries research_date, phase, market_data_reference_date, as_of,
timezone=Asia/Shanghai, source, fallback_used, stale and snapshot_id. Research as_of
is a timezone-aware cutoff on research_date; observed_at separately records fetch time.
Historical daily rows may not exceed market_data_reference_date. No fabricated timestamps.

## Canonical daily (P10B real intraday-history gate passed)

Keys: symbol (strict Core identity), trade_date (ISO date), unique per day/symbol.
Prices: open_raw, high_raw, low_raw, close_raw in CNY; strictly positive and finite.
volume_raw: Tushare vol, actual traded lots (100 shares per lot), never factor-scaled.
amount_raw: Tushare amount, actual thousands of CNY, never factor-scaled.
Normalized companions volume_shares=volume_raw*100; amount_cny=amount_raw*1000.
adj_factor: positive finite cumulative factor with exact-date provenance.
open_qfq/high_qfq/low_qfq/close_qfq: raw*factor/final_known_factor, anchoring at or
before the explicit reference date. Missing factors mean unavailable qfq, not raw fallback.
The anchor must exist exactly on the reference date; no future anchor, older-anchor fallback,
or factor backfill in research cache. Original loader behavior is untouched.
daily_basic: turnover_rate (%), volume_ratio, total_mv/circ_mv (10k CNY), missing stays null.

Endpoint batches must retain request parameters, source, cutoff, observation time, row count,
completion assessment and SHA256. Canonical manifests freeze all batch hashes. Replays read
the frozen manifest; incremental refresh only fetches absent/new days and never overwrites a
referenced batch. A cache hit verifies its hash and schema before using it.

Compatible-source no-trade placeholders are not price bars. Only exact
open=high=low=0, close>0, volume=amount=0 rows independently confirmed by same-date
full-day suspend_d evidence may be excluded from the calculation panel. They remain
unchanged in raw batches; their source proof path/hash is frozen in suspension_refs.
Quality retains confirmed_suspension_dates and counts them as missing history.
Unconfirmed zeros, intraday-only suspension evidence, negative prices, invalid OHLC
or positive traded volume on such a zero-price row still fail closed. This does not
change the native Loader or Universe and does not fill any prices or returns.

## Quality

Report per-stock history_coverage, missing_ratio, qfq availability and source_confidence.
At least120 completed-session bars for the majority of the current eligible Universe are
required for dataset acceptance. Listing/suspension gaps remain gaps, not fabricated rows.
Daily publication completeness cannot be inferred merely from a nonempty response.
For postclose, publication_proof binds the snapshot_id, independent expected membership,
three endpoint coverage counts, reference date, source and aware observation/cutoff times.
The manifest freezes an exact-hash publication snapshot; load recomputes membership and
coverage from the snapshot and hashed raw batches. Identity/listing must be sourced from
stock_basic and suspension from a successful same-date suspend_d, not inferred from daily.
Snapshot volume/amount must agree with the verified daily batch. Unverified current-day
pages and incomplete snapshots are not committed as completed artifacts.

## Evidence

Reuse Core EvidenceBundle v1 and its validated per-field provenance. Its `market` section
carries identity/snapshot/cutoff/quantitative context; `technical`, `flow`, `dragon_tiger`,
`margin`, `risk` preserve typed values and explicit missingness. The external research
packet may present identity/snapshot/quant at top-level without mutating the store contract.
A successful complete dragon query with no row means listed=false; request failure means
unknown. Margin missing never means zero. Returned observations after cutoff are rejected.

## Account and realtime

Account: actual source/observation time, CNY cash/assets, unique canonical holdings and
per-stock total/sellable shares. Tushare market data is not an account-state integration.
Quote: actual provider time and trade date, receipt time separately, source, raw timestamp,
quality, price/OHLC/previous close, bid/ask, volume/amount units. Missing or time-only dates
are not silently upgraded to verified current trade_date.

Board lot policy must be explicit instrument metadata (minimum order versus increment are
distinct). Core canonical legacy lot_size100 alone is insufficient for STAR order sizing.

S6 accepts an explicit dated competition FeeSchedule; no unconfirmed commission/tax
defaults are applied. Current per-symbol trading metadata identifies its source,
observation and trade dates, tradability/suspension/special-listing status and exact
upper/lower prices. It is not synthesized from board names or model memory. These
inputs are hashed separately with the account and quote receipt for each draft.
Detailed fields and command interfaces are in `pipeline-execution-inputs.md`.

A NOT_READY portfolio contains no executable target/delta quantities. A synthetic
account may produce labeled DRY_RUN quantities but never a production S6 checkpoint.
S6 includes held symbols outside S4 for explicit valuation/exit checks, not as new
research candidates. Cash balances, available cash and projected net sale proceeds
are distinct; unfilled proceeds never fund a buy. All amounts are decimal CNY and
order quantities are shares; prices are raw current quotes, not qfq history.

Automatic execution references additionally record per-field source labels,
`status_scope=daily_reference_not_live_halt_feed`, missing/issue lists and an
optional `valid_until` at an announced intraday suspension transition. Reads expire
at that transition even within the normal quote/status TTL. No source receipt time
is substituted for an actual exchange event time or the Market Runtime quote date.

Current portfolio reads require an explicitly resupplied account snapshot matching
the sealed snapshot hash, normal account eligibility/freshness and, when originally
supplied, the exact export-byte hash. Missing/changed evidence leaves a historical
draft only; it cannot preserve current readiness by checking the old timestamp alone.
Target quantities requiring multiple orders yield NOT_READY, not an implicitly
truncated target or an automatic split order.
