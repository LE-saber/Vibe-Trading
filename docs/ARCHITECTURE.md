# Independent architecture and product baseline

## Evidence and limits

Verified: the owner's latest goals and exclusions; the token/local-Codex boundary; the initially empty public writable target repository; the current Deep Project Orchestrator Skill and software/verification guidance; and the new source/test results delivered here.

Not independently verified: historical archive and two Codex attachments, current local source, latest raw ninef fields/endpoints, sale/cancel lifecycle, fee discovery, original UI framework, algorithm runtime, or old test counts. Files retrieval returned no readable report text, no source uploads were mounted, and materialization returned `403 user_mismatch`. Earlier assistant conclusions are leads, not evidence. No exact old schema or alleged old defect is assumed in this implementation.

The owner explicitly excluded stock-competition-platform. That exclusion is NOT reinterpreted as permission to copy it as a legacy backend.

## Mandatory acceptance matrix

| Product goal | Implemented and fixture-verified here | Local proof still required |
|---|---|---|
| Unified research/trading Web workflow | UI/API, typed result/plan, approval and execution state machine | Connect CURRENT research/ninef and isolate old write entrypoints |
| Meaningful analysis progress | Stage/item counts, coalescing, durable events, SSE/replay endpoint, polling and reload | Actual analysis callbacks/timings; real browser SSE transport |
| Own ranking plus top 30 | Independent own-rank field, timestamped top-30 interface/UI, degraded reads | Current contest/pagination/tie/own-rank API mapping |
| News in final AI review | Source/time/symbol evidence, cutoff/dedup/budget, context hash and viewer | Current news provider and actual AI prompt/citation tests |
| Buy/sell plan from real holdings/cash | Decimal quantities, explicit holding review, lots, sellability, fees, limits and reserve | Authoritative live account/fee/instrument semantics |
| Consent then automatic Web execution | Authenticated hash-bound approval, serial sell/buy dispatch, partial/full fill tracking, account reconciliation | Verified ninef adapter, lifecycle fault tests and separately authorized acceptance writes |

The demonstration proves application integration using artificial data, not completion of the real migration or investment performance.

## Decisions

### Integrate first; preserve valid alpha

Wrap the current research/data implementations behind a narrow port. Do not replace proven stock selection with a new speculative algorithm. Neither a wholesale rewrite nor migration from a different excluded repository is justified without source inspection. The S0...S6 labels are a stable presentation vocabulary to map to real work, not a verified claim about the unavailable legacy implementation.

Use a local modular monolith: FastAPI, SQLite WAL, one research executor and a serialized execution pump. This fits one owner and credentials that cannot leave the machine. No cloud secret bridge, Redis queue or distributed microservices are needed. The provided static Chinese UI is a usable reference; a mature existing frontend may consume the same API instead of being discarded.

### Separate expensive research from fast execution inputs

Research and evidence remain immutable. Account/quote/fee/rule revalidation happens per plan and per dispatch. Refreshing a portfolio reuses the research rather than rescanning stocks, but does not refresh the research timestamp. The latest plan pointer survives reload. Old unstarted approvals are superseded; active batches continue until explicitly stopped or gated.

Reporter validates genuine stage order and item counts. It coalesces interior updates to about five database writes per second while preserving boundaries. Weighted percentage is labeled as phase completion, never a fabricated ETA. Unknown totals may remain indeterminate until measured. Durable event IDs support replay; UI polling and authoritative snapshot reload prevent replayed old events from regressing the display.

The first speed improvement is avoiding unnecessary repeated full analyses, not claiming a measured speedup of unavailable algorithms. Stage-level caching, bounded IO/model concurrency, provider throttling and historical ETA estimates must be added against actual profiling data locally.

### Typed new contracts, no imaginary legacy upgrade

`vibe.account.v1`, `vibe.research.v1`, `vibe.plan.v1` are new application schemas. There is no claimed upgrade of an unseen `competition.account.v1`. Map existing contracts once at the local boundary and retain their regression tests.

Finite decimal strings/integers represent finance values; share counts and readiness booleans are strict. Timestamps are aware UTC. Duplicate positions/orders/quotes/rules are rejected. Canonical NAV reconciles available cash + frozen cash + position market value. The adapter must establish those semantics rather than rename similarly named fields. `orders_complete` and `consistent` default false.

Hashes identify exact approved payloads and lineage. They are NOT signatures or independent authentication. Authorization is the persisted record made by the authenticated Web endpoint. The trusted OS administrator can change code or database; this is not a tamper-proof regulatory archive.

### News and competition context are not trading authority

Own rank is independent of the top-30 list, so an owner outside the list is still shown. Ties are allowed. Rankings never automatically increase risk because the account is behind.

News is source-linked, time/cutoff bounded, symbol/holding aware, deduplicated and size limited. It is untrusted evidence, not prompt instructions. Missing news access differs from a valid empty result. AI receives allowed symbols/evidence, not tokens or broker tools. The bound news hash proves input identity, not that the model used the news correctly; actual prompt/citation tests remain mandatory.

The current evidence snapshot is taken at research start. A long-running local pipeline may need a new, explicitly versioned snapshot immediately before final review; update both the request/result validation together rather than silently inject future information into the old cutoff.

### Reconcile actual resources, not hoped-for fills

Existing unrelated pending/partial/cancel-pending orders block a new plan. Unknown statuses remain UNKNOWN. Do not automatically net or cancel them. Available cash/sellable shares are already net resources: no double subtraction of frozen components.

Every nonzero holding needs an explicit review/target. Disappearance from the candidate list is not liquidation authority. WATCH/REJECT cannot increase exposure. T+1/locked shares block an impossible reduction rather than silently change the reviewed target. Lot rounding and resulting concentration are explicit. No unsupported fixed number of holdings is inherited from another project.

### Durable execution state machine

```
completed research + fresh account/market inputs
 -> immutable exact-quantity/exact-limit plan
 -> authenticated human approval + hash + expiry
 -> one account lock + durable ordered intents
 -> fresh account/quote/fee/rule/resource checks
 -> PLANNED -> SENDING (committed before network)
 -> SUBMITTED -> pending/partial/full/terminal observations
 -> final account reconciliation -> COMPLETED
```

Every persisted intent must still equal its corresponding approved order. Sell orders precede buys. Prior orders must be fully filled, with authoritative average prices and fees; portfolio quantities, available cash and order snapshots must reconcile before another submission. Prices/quantities are never silently changed after approval.

A submission timeout is UNKNOWN. The lock remains; there is no automatic retry or heuristic matching by symbol/price/time. This is at-most-one automatic dispatch attempt, NOT exactly-once delivery to the broker. No manual-unlock button exists yet; an audited exact-evidence recovery process must be implemented locally.

Stop prevents future dispatch, but cannot retract a request already SENDING and does not imply cancellation. Cancellation requires separate explicit consent, is attempted once and read back. Acknowledgment is not cancellation. Restart marks research interrupted and stops further dispatch from old batches while reading known orders. No writes are replayed.

### Security and privacy

Loopback only, one process lock, separate Web password, HttpOnly/SameSite sessions, Origin and CSRF checks, no permissive CORS, no arbitrary order API, text-only rendering of provider content, generic errors caught before provider exceptions can escape into server logs. ninef credentials live only in the trusted local provider. DEMO and ninef databases are identity-separated; local mode cannot silently fall back to fixtures.

The public repository contains source and fabricated data only. Runtime DBs are private and excluded. This is not a remote multi-user deployment: that needs TLS, identity/roles, tenant isolation, secret storage, worker coordination and a new threat review. Unrelated local trading programs remain outside the process's authority.

## Material assumptions

One local owner, one simulation account/provider, one backend process; long-only cash-account accounting. Defaults are a 300-second plan lifetime, 20% cash reserve, 30% position cap and 1% drift threshold. These are conservative configurable application choices, NOT exchange rules, optimal strategy parameters or established ninef facts. Production instrument/fee/session rules come from verified provider inputs; only the DEMO fixture defines hypothetical ones.

Release readiness requires local real-data integration and fresh-context review. The completed work here is a self-verified kernel/reference workflow, not the entire live product.
