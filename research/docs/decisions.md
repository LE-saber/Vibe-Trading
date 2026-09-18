# Architecture decisions

## ADR-001 — Point-in-time Universe owns canonical identity

Competition Universe is the source of eligible SSE/SZSE A-share identities.
Downstream code consumes suffixed `ts_code` values and does not guess exchanges.
Snapshots preserve phase, reference date, eligibility and field provenance.

## ADR-002 — Research and execution are separate authorities

`research/` owns Universe and S0-S5. `src/vibe_trading` owns account validation,
S5.5 market checks, S6 quantities, immutable approval, submission and
reconciliation. The research runtime is permanently fail-closed for writes.

## ADR-003 — Evidence is persisted outside model context

Every reviewed candidate receives a schema-valid EvidenceBundle with content ID,
source timestamps, provenance and explicit missingness. Model output cannot add
uncited numeric facts or authorize a symbol outside the frozen candidate set.

## ADR-004 — Price adjustment does not rewrite volume

A-share qfq OHLC is derived from `daily + adj_factor`. Raw source volume remains
unchanged and carries a declared unit for later normalization. Missing adjustment
factors fail closed.

## ADR-005 — Model review is bounded and analysis-only

S5 performs one schema-constrained OpenAI-compatible request per frozen context,
with no tools, redirects or agent loop. Missing configuration is reported rather
than replaced with fabricated analysis.

## ADR-006 — Optional sidecars remain isolated

Qlib, TradingAgents and realtime market runtimes have separate manifests and may
fail without changing deterministic S0-S4. Canonical Parquet and PIT snapshots
remain authoritative; sidecar stores are derived artifacts.

## ADR-007 — Human approval binds exact portfolio content

An approval binds plan ID, content hash, actor and expiry. Any account, quote,
fee, rule, quantity or order change creates a new plan. Ambiguous external writes
are UNKNOWN and cannot be blindly retried.
