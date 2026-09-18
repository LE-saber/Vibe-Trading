# V2 research pipeline architecture

Status: implementation in progress; NOT_ACCEPTED until the strict acceptance ledger passes.

Competition Core owns explicit research clocks, canonical identities, immutable input
manifests, deterministic screening, durable checkpoints, Evidence and portfolio arithmetic.
Vibe remains the authenticated research UI and the configured LLM runtime. Market Runtime
keeps mootdx isolated from Vibe's incompatible HTTP dependencies. There are no order APIs.

## Execution boundary

S0 complete Universe -> S1 300 -> S2 60 -> S3 Evidence/20 -> S4 diversified12 ->
S5 independent evidence-only AI reviews -> S5.5 current quotes -> S6 account-bound5–8.

No Agent Tool.execute calls in the pipeline. Reuse pure indicator calculations, provider
adapters and service methods. The legacy candidate UI limit20 is not an internal limit.
AI does not generate identities, scores, weights, quantities, or orchestrate the market scan.
Qlib and TradingAgents remain separately identified optional shadow paths (production weight0).

Vibe integration will use a bounded worker in Vibe's existing Python environment:
one batch for pure technical calculations over canonical prices, then independent
evidence-only ChatLLM calls for finalists. This preserves existing provider/indicator
implementations without importing Vibe dependencies into Core or using Agent Tool wrappers.
Worker inputs/outputs are files with hashes, bounded timeouts and explicit context; no
shell command strings, model-generated paths, or secret-bearing logs are permitted.

## Program-owned Web jobs

The `/competition` workbench calls the authenticated Vibe `/api/competition/pipeline`
proxy, then Core `/pipeline` routes. `research_tasks.py` freezes an explicit request,
uses process handles and OS leases, and starts `research_worker.py` asynchronously.
The worker reuses canonical preparation and the S0–S6 functions directly. Snapshot
binding occurs before dataset preparation so retries cannot silently rebuild membership.
Saved requests and completed stage artifacts remain immutable. Later AI authorization
and account/fee imports are separate, request-bound inputs; changed implementation
requires a new task instead of modifying an old run.

`pipeline_views.py` exposes bounded, hash-checked stage/Evidence projections without
creating directories on reads. `research_portfolio.py` distinguishes unsealed diagnostic
attempts from historical sealed drafts and requires explicit current inputs for a manual
review receipt. Public GET responses never imply current execution readiness. Account
imports remain local and are not included in AI messages. See `research-workbench.md`.

## Clock and snapshot ownership

ResearchContext is explicit and immutable. It resolves the previous open date using
trade_cal for preopen/intraday, and requires verified complete daily publication for postclose.
Daily data and realtime data have separate cutoffs. Wall clocks are permitted for operational
timestamps, source-observation audit and realtime-age validation only.

Fresh Universe construction uses current stock_basic metadata; it must not be represented as
retrospective point-in-time membership. Historical reruns require a saved exact-date snapshot.
The Universe's screening rules remain unchanged. Production rejects stale fallback snapshots.

## Checkpoint contract (S0–S4 implemented and scoped gates accepted)

Each run freezes config, context, snapshot, dataset manifest and code version/hash. Stage
outputs record inputs, outputs, scores, rejection reasons, missing fields, provenance and
duration. Atomic persisted checkpoints, not in-memory agent context, control resume.
S3 additionally checkpoints every completed Evidence and each batch source response, so a
process interruption cannot cause completed requests to be fetched again.

`pipeline/runner.py` consumes a hash-verified full Universe and CanonicalDataset,
then owns `features.py`/`scoring.py` and `persistence.py`. `adapters/vibe/technical.py`
invokes only the Vibe pure-helper batch worker. The Vibe environment does not need
PyArrow: the boundary is a deterministic hashed CSV plus JSON request/result.
`dated_cache.py` preserves S2/S3 explicit-date supplemental sources, distinguishing
successful empty queries from failed/missing queries. No fallback field mixing.

Each S0/S1/S2 stage stores a full-audit JSON and selected-set Parquet. A semantic
hash excludes operational latency/provenance observation clocks but includes all
deterministic rows, scores, lists and reasons. Artifact hashes include the complete
envelope. Checkpoints bind feature/source hashes as well. OS file locks serialize a
run and release on process exit, allowing crash recovery without guessing stale PIDs.
The final JSON artifact is the stage commit marker; a missing run-state update is
recovered from it. Resume never silently accepts changed code/config/inputs.

S3 follows the same full-audit JSON/selected Parquet contract. `evidence_builder.py`
orchestrates dated whole-market batches, `evidence_fields.py` normalizes source units,
and `evidence_bundle.py` creates field-level provenance in the existing Evidence
contract. Each completed stock is committed to the Core SQLite/JSON Evidence Store,
then a run-local immutable body and checkpoint; the progress event is emitted last.
Resume validates input hashes, source proofs, body hash, canonical identity and
Core-store equality before reusing a stock. A frozen-only CLI mode rejects any
uncached request rather than silently fetching additional data during an audit.

S4 adds pure `correlation.py` and `diversification.py`, consuming verified S3 candidates,
the same saved Universe, and canonical qfq prices. Full pairwise correlation and
overlap counts, clusters and every decision are part of the semantic checkpoint.
It preserves scores and enforces configured cluster/verified-sector/overheat caps;
unknown data and capacity shortages stay explicit. The S4 source proof binds S3's
artifact hash. A resumed S4 requires no source or technical recomputation. Its
completion remains a PARTIAL full pipeline, pending S5 review and later gates.

## Safety boundary

Strict S6 now consists of `rebalance_quantity.py` (pure one-order quantity math),
`portfolio.py` (ranked sleeves, explicit account/status/fee constraints and cash),
and `portfolio_runner.py` (source binding, append-only attempts, sealing/resume and
current-read freshness). It does not call legacy intraday allocation or an Agent Tool.
Fee schedules and status observations are separate immutable execution inputs;
they never silently change the historical screening config or S0–S5 results.
`execution_status.py` collects current-day listing/calendar/suspension/exact-limit
references through the shared Tushare factory. `execution_status_runner.py` binds
their immutable receipts to frozen S0–S5 and the account hash; it never relaxes
historical ResearchDataClient cutoffs. Current dated quotes, actual account proof
and Web acceptance remain open; synthetic engineering tests are not production evidence.

Missing account or current quote data prevents execution-ready quantities. A manual account
import must identify its source, actual observation time, cash, assets, holdings and sellable
quantities. Initial competition capital is never an implicit current account. Model reviews
are advisory risk gates; manual confirmation records review, never submits orders.
