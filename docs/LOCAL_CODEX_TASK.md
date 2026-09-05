# Local Codex: implement and verify the real integration

## Mission

Use the branch `pro/integrated-workbench-kernel` in `LE-saber/Vibe-Trading`. Read AGENTS.md, ARCHITECTURE.md and VERIFICATION.md. Finish the owner's real local research + ninef + Web workflow. Do not return another broad investigation questionnaire. Inspect only the necessary current code, implement, test and commit focused changes.

Work in a separate clone/worktree; preserve the working local system as rollback. Do not investigate or migrate stock-competition-platform. The owner excluded it. The orchestrator repository is read-only. The DEMO strategy is not a replacement for current valid stock analysis.

Only local Codex may use credentials. Never print/upload tokens, cookies, Authorization headers, raw API captures, databases, real balances/account IDs or personal ranking names. This repository is PUBLIC. Use anonymous fixtures with invented numbers and source structure only. Do not enable the write switch, deploy, or make an external buy/sell/cancel during this implementation task without separate owner authorization.

## A. Current ninef read facade

Implement examples/local_provider.py or a proper adapter package behind ports.py. Use runtime models and scripts/export_contracts.py, not old assistant descriptions or guessed URLs.

Locate current account/holding/order/activity/quote/own-rank/top-30/news methods. Read ALL open-order pages. Reuse credential configuration locally without exposing a token API. Establish the precise total/available/frozen cash and total/sellable/frozen quantity semantics, source timestamps, collection window and NAV reconciliation. Strict false defaults must not be flipped without evidence.

Normalize actual raw order statuses centrally, preserving UNKNOWN. Pending/unfilled/partial/cancel-pending are active. Obtain authoritative cumulative filled quantity, average execution price, fees and exact broker ID. Do not substitute a limit price when fill price is missing or treat missing order data as cancellation.

Provide effective fee and instrument/session rules with provenance. Do not invent production commission/lot/T+1/holiday rules or assume an old trade's feeRule is globally current. Missing authoritative data must block, not silently default. Map own rank independently from actual top-30, preserving contest identity, pagination, ties and timestamps. Do not fabricate rows. News requires real IDs, sources, symbol association, publication/fetch time and text; market-level news can have no symbols.

Acceptance: adapter contract tests with fabricated/sanitized fixtures and `vibe-trading doctor --mode local --provider ...` using read-only calls. Report only capability flags, mappings, source paths and non-sensitive evidence references, not payloads. Do not mark write capabilities verified merely because reads work.

## B. Preserve current research; add truthful progress and final news review

Locate the actual current Vibe-Trading research entrypoint. Wrap it with Provider.research(request,report,cancelled). Preserve scoring/selection unless a specific defect is demonstrated by a regression test. Selectively migrate clean source/tests while keeping datasets/caches/models/artifacts local.

Map real phases to the UI stage vocabulary. Report measured item counts, use total=None when unknown, and a concrete final count before advancing. Check cancellation between bounded units; add timeouts to IO/model calls. Never advance progress based only on elapsed time.

Final review must cover both candidates AND current holdings. Missing candidate membership is not a sell decision. Build review_context for the actual candidate/holding union, include selected news with source IDs and untrusted-data instructions, and bind the result's news hash. Test actual prompt construction/citation validation, not merely a hash field. AI cannot invent symbols/evidence, turn WATCH into a buy, or obtain broker tools.

For long analyses, implement an explicitly versioned fresh news snapshot immediately before final review if needed. Preserve cutoff lineage and update request/result validation together; never silently mix new news into an old sealed snapshot.

Use measured profiling to add stage/per-symbol content-addressed caching, bounded concurrency, provider-aware throttling and read-only retry backoff. Keep expensive deterministic/evidence caches separate from news/AI/account/quote freshness. Replanning already avoids rescanning the market. Do not claim a speedup without before/after timings on representative real runs.

Acceptance: a full actual analysis displays correct stage/count progress, can be cancelled, reloads correctly and produces current/target/delta holdings. News about an existing holding must appear in its AI input and valid citations. Provide sanitized timing evidence.

## C. Verified execution adapter and legacy bypass isolation

Implement submit/get_order/cancel against CURRENT ninef behavior. Submission receives the exact approved order; do not parse advice.md, pick new symbols, reprice or resize. At most one transport attempt per invocation. If ninef has no broker-side idempotency, maintain local intent IDs without claiming otherwise.

Inventory all CURRENT Web/agent/SDK/MCP/headless paths reachable from the integrated application that can write. Disable them or route them through ExecutionService. Adding a safe endpoint beside an unsafe existing one does not complete the task. Unrelated programs cannot be prevented from trading by this process; detect their account/order drift instead.

Fault-inject a fake broker first: partial fills, rejection, cancel race, response lost before/after acceptance, duplicates, concurrent tabs/workers, stale fees/rules/quotes, T+1, frozen quantities, external pending orders and account drift. No buying against projected proceeds. Track late fills after approval expiry. Stop does not equal cancel. Test the persistent intent payload against the approved plan too.

Implement audited operator resolution of UNKNOWN only with exact authoritative order evidence. Preserve original intent, operator identity, evidence reference and resolution event. Never delete the DB, clear a lock ad hoc, resend, or fuzzy-match similar orders. When proof is absent, remain blocked.

Acceptance: no external writes in automated tests; write-entrypoint audit and failure tests pass. A minimal simulation-account acceptance write, if needed, requires separate explicit owner authorization after independent review. Ordinary migration does not authorize it.

## D. UI integration and fresh-context acceptance

Keep the usable Chinese reference UI or connect the mature existing frontend to this API, whichever preserves more capability with less disruption. Do not maintain two independent trading workflows. Preserve DEMO/LOCAL labels, account/news/ranking source times, blockers, expiry, exact-order consent, accepted-versus-filled state, explicit cancel and automatic account refresh.

Run unit/integration tests, compile/build, JS syntax check, and `python scripts/browser_smoke.py` in the normal local browser environment. The remote offline bridge does NOT prove real browser SSE/cookies. Test actual reconnect/Last-Event-ID replay, polling fallback, reload, plan refresh, concurrent tabs and session expiry. Test Windows locking; only POSIX was exercised remotely.

Use a fresh-context reviewer, with requirements and adversarial tests rather than a request to agree with the author. Reject for any secret in repo/log/browser; direct write bypass; fixtures presented as real; unknown-order retries; projected sale proceeds funding buys; silent holding liquidation; fake progress; absent own rank; or news missing from actual final AI inputs.

## Required completion evidence

Commit implementation and tests. Return one sanitized report containing commits, actual modules reused/changed, field mapping evidence locations, commands/results, current live-read coverage, anonymous screenshots, measured timing, write-entrypoint audit, remaining blockers and independent review findings. Distinguish IMPLEMENTED, FIXTURE-VERIFIED, LIVE-READ-VERIFIED and EXTERNAL-WRITE-VERIFIED. Do not claim release-ready while a mandatory boundary remains unverified.
